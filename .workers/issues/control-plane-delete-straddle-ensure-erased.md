# [control-plane] A 200-acked PUT Ensure is erased ~64s later by the recovered purge when a crash splits the two-phase stream delete

Stream delete is two-phase and non-atomic: a terminal trim through the streamer,
then `mark_stream_deleted` in a separate transaction. A SIGKILL between the two
phases leaves a *divided* durable state — `trim_point == ..MAX` persisted but
`deleted_at == None`. In that state a `PUT /v1/streams/{name}` with
ProvisionMode::Ensure is admitted (the provision gate checks only
`meta.deleted_at`, not the persisted trim point), commits fresh meta, and
**200-acks**. About 64 seconds later the recovered stream's first purge tick runs
`finalize_trim` — guarded only by trim-point equality — and erases the
just-acked meta, id-mapping, and tail.

In other words: a client gets `200` on `PUT /v1/streams/{name}`, reads its config
back, and the stream `404`s a minute later with no client-visible cause. An
acknowledged control-plane operation silently un-happens.

## Environment observed

- s2 source: s2-workload fork, s2-lite `0.38.0`, prepared image commit
  `743d0e0` (product source pinned at fork HEAD `33f6396`).
- Runtime: `s2 lite` single-node, slatedb backend, across three
  `SL8_FLUSH_INTERVAL` settings (default 5ms / 500ms / 2s).
- Reproduced deterministically: 5/5 across three flush settings and three seeds
  (`1000000`, `999999`, `1000001`, plus a fresh `424242`).
- Also reproduced **locally** with a stock `cargo build --release`
  of the `s2` CLI on macOS (`--local-root` on-disk, no S3): the straddle landed
  on trial 2 (kill +925ms into DELETE); PUT Ensure 200-acked; the stream 404'd
  at **+64s** after its ack. Standalone script + output below.

## Minimal repro

A standalone script (included below) drives this exact ordering:

1. Create a stream and append a known ledger.
2. Fire `DELETE /v1/streams/{name}` on a thread and `SIGKILL` the server at a
   bisection-chosen offset that lands **between** the two delete phases (the
   script self-tunes the offset; it lands the straddle by trial 2-3 on every
   flush setting). This leaves the divided state `trim_point == ..MAX`,
   `deleted_at == None`.
3. Restart the server (same root).
4. Classify the seam by probing: a **STRADDLE** shows `GET /v1/streams/{name}`
   → `200` with a live config body while `POST …/records` (append) →
   `409 stream_deletion_pending`.
5. Send the kill shot: `PUT /v1/streams/{name}` with ProvisionMode::Ensure and a
   marker config (e.g. `retention age: 7777`). It returns **`200`** with
   `deleted_at: None` in the body.
6. Watch the stream for 180s.

A trial is RED (`acked_ensure_erased`) if the 200-acked stream subsequently
404s.

Observed terminal state (representative, seed 1000000, 500ms arm):

```text
t1 SIGKILL +300ms into DELETE → seam EARLY (delete un-happened: GET 200, append 200)
t2 SIGKILL +925ms into DELETE → seam STRADDLE:
     GET  /v1/streams/{name}         → 200  (live config body)
     POST /v1/streams/{name}/records → 409  {"code":"stream_deletion_pending",...}
     PUT  /v1/streams/{name} (Ensure)→ 200  (marker retention age=7777, deleted_at: None)
   watch: GONE at +64s → GET 404 {"code":"stream_not_found",...}  (double-probed 2s apart)
VERDICT: RED — acked_ensure_erased
```

**Reliability:** 5/5 reproduced across three flush settings and three seeds. Erasure
instants +64 / +72 / +64 / +64 / +64s sit exactly in the first jittered purge
tick (54-66s) — there is no startup tick (bgtasks/mod.rs:20-27, :100-115), and
the delete's own event trigger died with the killed process, so the purge only
resumes on that first post-restart tick.

Secondary defect witnessed in the same divided state (spec clause a): the state
is itself incoherent — `GET` serves the stream as live (`200`, config body) while
appends refuse `409 stream_deletion_pending` (core.rs:118-120).

<details>
<summary>Standalone local reproduction (only a built <code>s2</code> binary — no harness, no S3)</summary>

```sh
#!/bin/sh
# Standalone local reproduction: a 200-acked PUT Ensure is erased ~64s
# later by the recovered purge when a SIGKILL splits the two-phase delete.
#
#   S2_BIN=/path/to/s2 sh repro_delete_straddle.sh
# Runs on localhost with an on-disk root. No S3, no network.
# Runtime ~2-5 min: a short bisection to land a crash between the two
# delete phases, then a 180s watch across the recovery purge tick.
# Exit 1 = REPRODUCED (200-acked Ensure 404s within the recovery tick).
exec python3 - <<'PYEOF'
import http.client, json, os, shutil, subprocess, sys, threading, time

S2 = os.environ.get("S2_BIN", "s2")
PORT = int(os.environ.get("PORT", "8080"))
FLUSH = os.environ.get("SL8_FLUSH_INTERVAL", "500ms")   # widens the window
BASIN = "straddle-repro"
WORK = "/tmp/s2-straddle-repro"
DATA = os.path.join(WORK, "root")
WATCH_S = 180        # erased-watch across recovery tick (60s +/-10%)
MAX_TRIALS = 12

def log(m): print(m, flush=True)

def http_call(method, path, body=None, timeout=15):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    try:
        h = {"S2-Basin": BASIN, "Authorization": "Bearer ignored"}
        p = None
        if body is not None:
            p = json.dumps(body); h["Content-Type"] = "application/json"
        c.request(method, path, body=p, headers=h)
        r = c.getresponse(); return r.status, r.read()
    finally:
        c.close()

def start():
    os.makedirs(WORK, exist_ok=True)
    env = dict(os.environ); env["SL8_FLUSH_INTERVAL"] = FLUSH
    out = open(os.path.join(WORK, "server.log"), "ab")
    return subprocess.Popen([S2, "lite", "--port", str(PORT),
                             "--local-root", DATA],
                            stdout=out, stderr=subprocess.STDOUT, env=env)

def wait_health(sec=60):
    end = time.monotonic() + sec
    while time.monotonic() < end:
        try:
            s, _ = http_call("GET", "/health", timeout=2)
            if s == 200: return
        except OSError: pass
        time.sleep(0.2)
    sys.exit("server never became healthy")

def create_basin():
    # Default basin config => auto-create OFF (POST is CreateOnly).
    s, d = http_call("POST", "/v1/basins", {"basin": BASIN})
    if s not in (200, 201, 409):
        sys.exit(f"create-basin failed: {s} {d.decode('utf-8','replace')[:300]}")

def create(name):     return http_call("POST", "/v1/streams",
                                  {"stream": name, "config": {"retention_policy": {"age": 3600}}})
def ensure(name, cfg): return http_call("PUT", f"/v1/streams/{name}", cfg)
def get(name):        return http_call("GET", f"/v1/streams/{name}", timeout=10)
def append(name, s):  return http_call("POST", f"/v1/streams/{name}/records",
                                  {"records": [{"body": s}]})

def classify(name):
    s, d = get(name); body = d.decode("utf-8", "replace")[:400]
    if s == 200:
        try:
            if json.loads(body).get("deleted_at") is not None:
                return "marked", s, body
        except Exception: pass
        return "live", s, body
    if s == 404: return "gone", s, body
    if s == 409 and "deletion_pending" in body: return "marked", s, body
    return "other", s, body

flush_ms = {"5ms": 5, "500ms": 500, "2s": 2000}.get(FLUSH, 500)
lo, hi = 0.0, flush_ms * 2.5 + 300
if os.path.isdir(DATA): shutil.rmtree(DATA)
os.makedirs(DATA)
server = start(); wait_health(); create_basin()
log(f"flush={FLUSH}; bisecting kill offset in [{lo:.0f},{hi:.0f}]ms to land "
    f"a crash BETWEEN the two delete phases")

straddle = None
for trial in range(1, MAX_TRIALS + 1):
    name = f"ds-t{trial:02d}"
    s, d = create(name)
    if s not in (200, 201): sys.exit(f"create {name}: {s} {d[:200]}")
    for i in range(4):
        s, d = append(name, f"{name}-r{i}")
        if s != 200: sys.exit(f"append {name}: {s} {d[:200]}")

    off = (lo + hi) / 2 if trial > 1 else min(flush_ms * 0.6, hi * 0.5)
    res = {}
    def fire():
        try:
            s, d = http_call("DELETE", f"/v1/streams/{name}")
            res["o"] = ("acked", s)
        except OSError as e:
            res["o"] = ("conn-error", str(e)[:80])
    t = threading.Thread(target=fire); t.start()
    time.sleep(off / 1000.0)
    server.kill(); server.wait(); t.join(timeout=20)
    server = start(); wait_health()

    state, s, body = classify(name)
    ap_s, ap_d = append(name, f"{name}-postrestart")
    if state == "live" and ap_s == 200:
        seam = "EARLY"; lo = off
    elif state == "live":
        seam = "STRADDLE"
    elif state in ("marked", "gone"):
        seam = "LATE"; hi = off
    else:
        log(f"t{trial} unclassifiable GET {s}; retry"); continue
    log(f"t{trial}: kill +{off:.0f}ms -> seam={seam} (GET {s}, append {ap_s})")
    if seam == "STRADDLE":
        log(f"t{trial} DIVIDED STATE: GET 200 live but append {ap_s} "
            f"{ap_d.decode('utf-8','replace')[:120]} "
            f"(trim_point==MAX durable, deleted_at==None)")
        straddle = name; break
    if hi - lo < 2.0 and trial >= 4: break

if straddle is None:
    server.terminate(); sys.exit("no straddle landed; widen SL8_FLUSH_INTERVAL")

# The kill shot: PUT Ensure with a distinctive marker config through the
# deleted_at-only provision gate (streams.rs:106-112).
name = straddle
marker = {"retention_policy": {"age": 7777}}
s, d = ensure(name, marker)
if s not in (200, 201):
    time.sleep(1); s, d = ensure(name, marker)
if s not in (200, 201):
    server.terminate()
    sys.exit(f"Ensure refused ({s}) — divided state gated the PUT (a "
             f"different, milder outcome than the erasure bug)")
t_ack = time.monotonic()
log(f"[!] PUT Ensure 200-ACKED on the straddled name with marker age=7777; "
    f"watching {WATCH_S}s across the recovery purge tick")

while time.monotonic() < t_ack + WATCH_S:
    time.sleep(8); now = time.monotonic()
    state, s, body = classify(name)
    if state == "live":
        continue
    time.sleep(2)
    state2, s2, body2 = classify(name)   # double-probe (transient-404 guard)
    if state2 != "live":
        server.terminate()
        print()
        print("=== REPRODUCED: acked PUT Ensure erased by recovered purge ===")
        print(f"PUT /v1/streams/{name} (Ensure) returned 200 at t0")
        print(f"GET /v1/streams/{name} -> {s2} {body2[:200]}")
        print(f"erased at +{now-t_ack:.0f}s after its own 200-ack")
        print("VERDICT: FAIL (bug reproduced)")
        sys.exit(1)
    log(f"    watch +{now-t_ack:.0f}s: transient non-live recovered on re-probe")

server.terminate()
print("\nacked Ensure survived the full watch — bug NOT reproduced on this build.")
print("VERDICT: PASS")
sys.exit(0)
PYEOF
```

Observed output (local, `cargo build --release` of the CLI at fork HEAD, macOS,
`SL8_FLUSH_INTERVAL=500ms`):

```text
flush=500ms; bisecting kill offset in [0,1550]ms to land a crash BETWEEN the two delete phases
t1: kill +300ms -> seam=EARLY (GET 200, append 200)
t2: kill +925ms -> seam=STRADDLE (GET 200, append 409)
t2 DIVIDED STATE: GET 200 live but append 409 {"code":"stream_deletion_pending","message":"stream deletion pending"} (trim_point==MAX durable, deleted_at==None)
[!] PUT Ensure 200-ACKED on the straddled name with marker age=7777; watching 180s across the recovery purge tick

=== REPRODUCED: acked PUT Ensure erased by recovered purge ===
PUT /v1/streams/ds-t02 (Ensure) returned 200 at t0
GET /v1/streams/ds-t02 -> 404 {"code":"stream_not_found","message":"stream `ds-t02` in basin `straddle-repro` not found"}
erased at +64s after its own 200-ack
VERDICT: FAIL (bug reproduced)
```

</details>

## Expected behavior

A `200`-acked `PUT /v1/streams/{name}` must be durable: the stream and the config
it returned must survive. Equivalently, in the divided post-crash state the
Ensure should be **refused** (so the client learns the op did not take), or the
recovered purge must **not** erase freshly-committed meta.

## Actual behavior

The provision gate admits the Ensure on `deleted_at == None` alone, ignoring the
durable `trim_point == ..MAX`. It commits new meta and 200-acks. The recovered
`finalize_trim`, gated only by trim-point equality, then deletes that meta,
id-mapping, and tail with no re-check of `deleted_at` or meta freshness. The
name is left both gone and (via the orphaned trim point) append-wedged.

## Relevant implementation path

Two-phase, non-atomic delete:

```
// lite/src/control_plane/streams.rs:338-358  terminal trim (streamer)
// lite/src/control_plane/streams.rs:360-379  mark_stream_deleted (separate txn)
//   SIGKILL between → trim_point == ..MAX durable, deleted_at == None
```

Provision gate ignores the trim point:

```
// lite/src/control_plane/streams.rs:106-112 — gate checks meta.deleted_at ONLY
// lite/src/control_plane/streams.rs:133-151, :221-226 — (Some(existing), Ensure)
//   arm commits fresh meta and 200-acks
```

Recovered purge erases without re-checking freshness:

```
// lite/src/backend/bgtasks/stream_trim.rs:123-134 — finalize guarded only by trim-point equality
// lite/src/backend/bgtasks/stream_trim.rs:136-146 — erases meta, id-mapping, tail
```

Excluded alternatives (source + timeline): no coherent linearization of {GET 200
live, append 409 pending, Ensure 200, GET 404} exists — the server's own model
refuses provisioning in deletion-pending and routes a real delete through the
observable marked state, which the purge path skips. "Created on finalized meta"
is excluded because Created ⇒ 201+`created` header while runs recorded 200 ⇒ meta
existed at ack (handlers/v1/streams.rs:242-246). Auto-create is off by basin
default (config.rs:324), corroborated by EARLY/LATE trials' appends refusing.

**Invariant:** an acknowledged control-plane write is durable — a `200` PUT
Ensure and the config it returns must not be silently erased by asynchronous
recovery.

## Suggested fix direction

Either (A) the provision gate should treat a durable `trim_point == ..MAX` as
deletion-pending and refuse/deny the Ensure (so the client is never falsely
acked), and/or (B) `finalize_trim` should re-check `deleted_at` / meta freshness
before erasing, and must also resolve the orphaned trim point (otherwise the name
stays append-wedged even if the meta is spared). (A) prevents the false ack at
the source; (B) prevents recovery from destroying committed metadata.
