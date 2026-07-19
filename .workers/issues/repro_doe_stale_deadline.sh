#!/bin/sh
# Standalone local reproduction: DOE stale-deadline survives purge and
# wrongfully deletes a same-name recreate.
#
# Requires only a locally-built `s2` binary (the `s2 lite` server).
#   S2_BIN=/path/to/s2 sh repro_doe_stale_deadline.sh
# Defaults to `s2` on PATH. Runs entirely on localhost with an on-disk root
# (so state survives across the server lifecycle). No S3, no network.
#
# Runtime ~11 min: dominated by the hardcoded DOE_DEADLINE_REFRESH_PERIOD
# = 600s (streamer.rs:57). inc1's DOE deadline is armed at
# age(1s)+min_age(1s)+600s = ~602s; the wrongful delete fires ~602-668s in.
#
# Exit 0 = stream A survived (bug NOT reproduced). Exit 1 = REPRODUCED
# (stream A wrongfully deleted while control stream B survived).
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
