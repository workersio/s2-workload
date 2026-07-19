# [retention] Delete-on-empty deadline of a deleted stream survives the purge and wrongfully deletes the stream recreated under the same name

When a stream configured with delete-on-empty (DOE) is deleted and then a new
stream is created with the same basin+name, the new stream can be silently
deleted minutes later by the *dead* incarnation's leftover DOE deadline — even
though the new stream's own empty-floor (`min_age`) is nowhere near elapsed.

In other words: `finalize_trim` erases five key families of a purged stream but
not its `stream_doe_deadline` key; because `StreamId` is a deterministic hash of
`(basin, stream)`, the recreated stream inherits the orphaned armed deadline,
and when that stale deadline comes due the DOE scan deletes the new, healthy
stream. An acknowledged, config-defying stream deletion with no error surfaced
anywhere.

## Environment observed

- s2 source: s2-workload fork, s2-lite `0.38.0`, prepared image commit
  `743d0e0` (product source pinned at fork HEAD `33f6396`).
- Runtime: `s2 lite` single-node, slatedb backend.
- Reproduced deterministically: 3/3 across seeds (`2492750010`, plus a fresh
  `555000333`).
- Also reproduced **locally** with a stock `cargo build --release`
  of the `s2` CLI on macOS (`--local-root` on-disk, no S3): stream A's inc2 (own
  `min_age` 3600s, empty ~672s) was deleted at **+672s** — inside inc1's stale
  window — while the no-DOE control B survived. Standalone script + output below.

## Minimal repro

A standalone script (included below) drives this ordering:

1. Stream **A** incarnation 1: config `{retention age: 1s, doe.min_age: 1s}`,
   one append. This arms a DOE deadline at `t_arm + 602s`
   (`doe_arm_delay = age + min_age + 600s`, streamer.rs:59-63).
2. Delete A inc1 (DELETE `/v1/streams/A` → 202 async ack). The event-triggered
   purge runs `finalize_trim`, which erases meta / tail / id-mapping / fencing /
   trim-point — **but leaves the `stream_doe_deadline` key** (stream_trim.rs:135-146).
3. Recreate the same name **A** incarnation 2: config `{retention age: 1s,
   doe.min_age: 3600s}`, and leave it **empty** (its own deadline would be
   ~`t_arm+4210s`). Same `(basin, name)` → same `StreamId` → the stale deadline
   from step 1 now points at inc2.
4. Control stream **B**: identical inc1 shape, but inc2 recreated **without**
   DOE (`min_age: None`). Under source this makes B ineligible (streamer.rs:460),
   so B must survive.
5. GET-only probes on both streams through the window `t_arm + [602, 668]s`
   (GET cannot bump `last_tail_write_timestamp`, so probing does not perturb the
   outcome).

A trial is RED (`wrongful_delete`) if A's inc2 starts 404ing inside the DOE
window while B stays alive; GREEN only if both survive the window **and** a
post-window append to A succeeds.

Observed terminal state (representative, seed 2492750010):

```text
A inc2 last alive at +645s ; GONE by +673s   (DOE window [597,668])
B (control, no DOE) alive through +703s
A 404 body: {"code":"stream_not_found","message":"stream `doe-a-…` in basin `lifecycle-wl-01` not found"}
VERDICT: RED — wrongful_delete
```

**Negative control:** stream B — same inc1, but inc2 recreated with `min_age:
None` — survived every trial (ineligible escape, streamer.rs:460). The only
difference between A and B is whether inc2 re-declares DOE; A (whose stale
deadline value is never re-validated) dies, B lives.

**Reliability:** 3/3 in-window wrongful deletions — seed 2492750010 (initial +
same-seed replay) and fresh seed 555000333. Death instant varies within the
window across runs (probe granularity ~28-33s over a ±10% 60s tick); the window
itself held every time.

<details>
<summary>Standalone local reproduction (only a built <code>s2</code> binary — no harness, no S3)</summary>

Runtime ~11 min: the wait is dominated by the hardcoded
`DOE_DEADLINE_REFRESH_PERIOD = 600s` (streamer.rs:57) that pads the arm delay;
inc1's deadline is `age(1s) + min_age(1s) + 600s ≈ 602s`, and the wrongful
delete fires on the first purge tick after that (~602-668s).

```sh
#!/bin/sh
# Standalone local reproduction: DOE stale-deadline survives purge and
# wrongfully deletes a same-name recreate.
#
#   S2_BIN=/path/to/s2 sh repro_doe_stale_deadline.sh
# Runs entirely on localhost with an on-disk root. No S3, no network.
# Exit 1 = REPRODUCED (stream A wrongfully deleted while control B survived).
exec python3 - <<'PYEOF'
import http.client, json, os, subprocess, sys, time

S2 = os.environ.get("S2_BIN", "s2")
PORT = int(os.environ.get("PORT", "8080"))
BASIN = "doe-repro"
WORK = "/tmp/s2-doe-repro"
DATA = os.path.join(WORK, "root")

ARM_PAD = 602        # age(1) + min_age(1) + DOE_DEADLINE_REFRESH_PERIOD(600)
TICK_MAX = 66        # purge bgtask tick 60s +10%
MARGIN = 40

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
    out = open(os.path.join(WORK, "server.log"), "ab")
    return subprocess.Popen([S2, "lite", "--port", str(PORT),
                             "--local-root", DATA],
                            stdout=out, stderr=subprocess.STDOUT)

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
    # Default basin config => auto-create OFF. Auto-create would resurrect
    # the deleted name under a probe and mask the bug.
    s, d = http_call("POST", "/v1/basins", {"basin": BASIN})
    if s not in (200, 201, 409):
        sys.exit(f"create-basin failed: {s} {d.decode('utf-8','replace')[:300]}")

def create(name, cfg): return http_call("POST", "/v1/streams",
                                   {"stream": name, "config": cfg}, timeout=120)
def get(name):         return http_call("GET", f"/v1/streams/{name}")
def append(name, s):   return http_call("POST", f"/v1/streams/{name}/records",
                                   {"records": [{"body": s}]})
def delete(name):      return http_call("DELETE", f"/v1/streams/{name}")

A, B = "doe-a", "doe-b"
if os.path.isdir(DATA):
    import shutil; shutil.rmtree(DATA)
os.makedirs(DATA)

server = start(); wait_health(); create_basin()

# Step 1: inc1 for A and B, identical shape {age:1s, doe.min_age:1s}.
# Appending arms a DOE deadline at ~t_arm + 602s for each.
inc1 = {"retention_policy": {"age": 1}, "delete_on_empty": {"min_age_secs": 1}}
for n in (A, B):
    s, d = create(n, inc1)
    if s not in (200, 201): sys.exit(f"inc1 create {n}: {s} {d[:200]}")
t_arm = time.monotonic()
for n in (A, B):
    s, d = append(n, f"{n}-inc1")
    if s != 200: sys.exit(f"inc1 append {n}: {s} {d[:200]}")
log(f"[0] inc1 A,B created+appended (DOE armed ~t+{ARM_PAD}s); deleting")
time.sleep(3)

# Step 2: delete both. finalize_trim purges meta/tail/id-map/fencing/
# trim-point but NOT stream_doe_deadline -> the armed deadline is orphaned.
for n in (A, B):
    s, d = delete(n)
    if s not in (200, 202, 204): sys.exit(f"delete {n}: {s} {d[:200]}")
t_del = time.monotonic()

# Step 3: recreate under the SAME names. StreamId=hash(basin,name) is
# deterministic, so the orphaned deadline now points at inc2.
#   A inc2: DOE min_age 3600s (own deadline ~70min out) -> must survive.
#   B inc2: NO DOE (control) -> min_age None is Ineligible even vs a stale
#           deadline (streamer.rs:460).
inc2_a = {"retention_policy": {"age": 1}, "delete_on_empty": {"min_age_secs": 3600}}
inc2_b = {"retention_policy": {"age": 1}}
rec = {}
for n, cfg in ((A, inc2_a), (B, inc2_b)):
    end = time.monotonic() + 300
    while time.monotonic() < end:
        s, d = create(n, cfg)
        if s in (200, 201): rec[n] = time.monotonic(); break
        time.sleep(2)
    else:
        sys.exit(f"recreate {n} gated 300s")
    log(f"[1] recreated {n} at +{rec[n]-t_del:.0f}s after delete")
t2 = max(rec.values())

# Confirm inc2 configs read back as acked (attribution baseline).
for n, want in ((A, 3600), (B, None)):
    s, d = get(n); cfg = json.loads(d)
    got = (cfg.get("delete_on_empty") or {}).get("min_age_secs")
    if got != want and not (want is None and got in (None, 0)):
        sys.exit(f"inc2 {n} config readback {got!r} != {want!r}")
log("[2] inc2 configs confirmed: A doe.min_age=3600s, B no DOE; both empty")

# Step 4: probe both through inc1's stale-deadline window. NEVER append to
# inc2 (an append would bump last_tail_write_timestamp past the stale
# cutoff and neutralize the trial). GET-only is safe.
window_end = t_arm + ARM_PAD + TICK_MAX + MARGIN
gone = {}
while time.monotonic() < window_end and len(gone) < 2:
    time.sleep(min(28, max(1, window_end - time.monotonic())))
    now = time.monotonic()
    for n in (A, B):
        if n in gone: continue
        s, d = get(n)
        if s == 200: continue
        gone[n] = (now, s, d.decode("utf-8", "replace")[:200])
        log(f"[!] probe +{now-t_del:.0f}s {n}: GONE {s} {gone[n][2]}")
for n in (A, B):
    if n not in gone:
        log(f"    probe: {n} alive through window end (+{time.monotonic()-t_del:.0f}s)")

server.terminate()
print()
if A in gone and B not in gone:
    tg, s, body = gone[A]
    print("=== REPRODUCED: DOE stale-deadline wrongful delete ===")
    print(f"stream A (inc2, own min_age=3600s, empty ~{tg-t2:.0f}s) DELETED "
          f"at +{tg-t_del:.0f}s — inside inc1's stale-deadline window")
    print(f"stream B (control, no DOE) survived — the only difference is "
          f"whether inc2 re-declared DOE")
    print(f"A GET: {s} {body}")
    print("VERDICT: FAIL (bug reproduced)")
    sys.exit(1)
if A in gone and B in gone:
    print("Both A and B deleted — not the DOE-specific bug (some other "
          "mechanism). VERDICT: INCONCLUSIVE"); sys.exit(2)
print("stream A survived the window — bug NOT reproduced on this build.")
print("VERDICT: PASS (no wrongful delete)")
sys.exit(0)
PYEOF
```

Observed output (local, `cargo build --release` of the CLI at fork HEAD, macOS):

```text
[0] inc1 A,B created+appended (DOE armed ~t+602s); deleting
[1] recreated doe-a at +0s after delete
[1] recreated doe-b at +0s after delete
[2] inc2 configs confirmed: A doe.min_age=3600s, B no DOE; both empty
[!] probe +672s doe-a: GONE 404 {"code":"stream_not_found","message":"stream `doe-a` in basin `doe-repro` not found"}
    probe: doe-b alive through window end (+705s)

=== REPRODUCED: DOE stale-deadline wrongful delete ===
stream A (inc2, own min_age=3600s, empty ~672s) DELETED at +672s — inside inc1's stale-deadline window
stream B (control, no DOE) survived — the only difference is whether inc2 re-declared DOE
A GET: 404 {"code":"stream_not_found","message":"stream `doe-a` in basin `doe-repro` not found"}
VERDICT: FAIL (bug reproduced)
```

</details>

## Expected behavior

A stream's delete-on-empty deletion must be governed by *that stream's current*
configuration and emptiness duration. Deleting a stream and recreating the same
name must not carry a deletion schedule across incarnations; a fresh stream with
`min_age: 3600s` and no elapsed empty time must not be deleted ~10 minutes after
creation.

## Actual behavior

The recreated stream is terminally trimmed and deleted at the *dead*
incarnation's inherited deadline. The eligibility recheck at fire time consults
the current config only for `min_age` **presence**, never comparing the current
value (3600s) against the stale cutoff; a never-appended inc2 has no tail key, so
`last_tail_write_timestamp = TimestampSecs::ZERO` (core.rs:110-111) — maximally
below the cutoff — and the stream is judged eligible and deleted.

## Relevant implementation path

The purge that runs on delete cleans five key families but omits the DOE
deadline:

```
// lite/src/backend/bgtasks/stream_trim.rs:135-146  (finalize_trim)
//   deletes: trim-point, meta, id-mapping, tail, fencing
//   does NOT delete: stream_doe_deadline  ← orphaned, armed
```

`StreamId` reuse guarantees the orphan lands on the successor:

```
// lite/src/backend/kv/stream_id.rs:24-29 — StreamId = hash(basin, stream), deterministic
```

At fire time the eligibility recheck is presence-only:

```
// lite/src/backend/streamer.rs:457-461, 494-505
//   consults CURRENT config for min_age PRESENCE, but the stale cutoff
//   (deadline − min_age, kv/stream_doe_deadline.rs:17-21) is never compared
//   against the current min_age value.
```

No other stream-deleting mechanism operates in the observed band: age retention
is a slatedb TTL on record keys only (streamer.rs:1021-1037; inc2-A had zero
appends → zero record keys, and meta/tail/mapping are never TTL'd); the
acked-Ensure-erased corridor (issue #2 below) kills within ~1-66s of recreate,
not at +617s; auto-create is excluded by the persistent 404. The A-vs-B control
asymmetry is exactly the source-predicted discrimination.

**Invariant:** delete-on-empty deletion of a stream must depend only on that
stream's own current `min_age` and its own elapsed empty duration — never on a
schedule armed by a prior incarnation of the same name.

## Suggested fix direction

Either (a) `finalize_trim` should delete the `stream_doe_deadline` key(s)
alongside the other five families when a stream is purged, and/or (b) the DOE
fire-time recheck should compare the **current** `min_age` value against the
actual empty duration rather than checking `min_age` presence only. (a) closes
the cross-incarnation leak at the source; (b) hardens the fire path against any
stale-deadline survival.
