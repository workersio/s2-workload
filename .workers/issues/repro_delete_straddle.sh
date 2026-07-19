#!/bin/sh
# Standalone local reproduction: a 200-acked PUT Ensure is erased ~64s
# later by the recovered purge when a SIGKILL splits the two-phase delete.
#
# Requires only a locally-built `s2` binary.
#   S2_BIN=/path/to/s2 sh repro_delete_straddle.sh
# Runs on localhost with an on-disk root. No S3, no network.
#
# Runtime ~2-5 min: a short bisection to land a crash between the two
# delete phases, then a 180s watch across the recovery purge tick.
#
# Exit 0 = Ensure survived (bug NOT reproduced). Exit 1 = REPRODUCED
# (200-acked Ensure 404s within the recovery tick).
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
