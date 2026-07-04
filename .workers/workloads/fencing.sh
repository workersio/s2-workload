#!/bin/sh
# Workload: fencing excludes stale writers (s2 lite, --local-root).
#
# Mode stale-across-restart:
#   write under token T1 -> fence to T2 -> attempt stale (T1), tokenless,
#   and control (T2) appends -> SIGKILL -> restart on the same root ->
#   repeat the same attempts. The restart boundary is the attack: the
#   recovered token (deserialized from storage) must still reject the
#   stale writer, and behavior must be identical across the boundary.
#
# API notes (verified in source): fence = append one record with a single
# header ["", "fence"] and body = the new token; guarded appends carry
# "fencing_token" in AppendInput; a mismatch returns an explicit
# AppendConditionFailed error. Command records are sequenced into the
# stream, so read-back includes the fence records themselves.
#
# Oracle (see .workers/promises/fencing-excludes-stale-writers.md):
#   - stale_rejected: T1 appends after the fence get an explicit HTTP
#     rejection, both before the kill and after the restart (an accepted
#     stale append, or a dropped connection instead of a rejection, fails)
#   - behavior_consistent: rejection identity (status + error code) and
#     tokenless acceptance behavior are identical across the restart
#   - content_exact: read-back [0, tail) is dense and holds exactly the
#     accepted records (acked ranges tile; no stale payload anywhere)
#   - anti-vacuous: control (T2) appends must succeed on both sides, and
#     every attempt must get an HTTP response
#
# Self-contained: sh wrapper + embedded python3 (injection layers one file).
# Exit codes: 0 green, 1 red (finding), 3 void/blocked.
MODE="${1:-stale-across-restart}"
exec python3 - "$MODE" <<'PYEOF'
import http.client
import json
import os
import signal
import subprocess
import sys
import time

S2 = os.path.join(".workers", "vendor", "bin", "s2-linux-amd64")
PORT = 8080
BASIN = "durability-wl-04"
STREAM = "fencing"
WORK_DIR = "/tmp/wl-fencing"  # /workspace (the repo checkout) is read-only
DATA_DIR = os.path.join(WORK_DIR, "s2root")


def log(msg):
    print(msg, flush=True)


def invariant(inv_id, name, ok, summary):
    log(f"INVARIANT {inv_id} {name} {'PASS' if ok else 'FAIL'} {summary}")


def fail(code, msg, inv=None):
    if code == 1 and inv:
        invariant(inv[0], inv[1], False, msg)
    log(f"VERDICT: {'RED' if code == 1 else 'VOID'} — {msg}")
    sys.exit(code)


def derive_seed():
    env = os.environ.get("SEED")
    if env:
        return int(env, 0) & 0xFFFFFFFF
    with open("/dev/urandom", "rb") as f:
        return int.from_bytes(f.read(4), "little")


def http_call(method, path, body=None, timeout=10):
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    try:
        headers = {"S2-Basin": BASIN, "Authorization": "Bearer ignored"}
        payload = None
        if body is not None:
            payload = json.dumps(body)
            headers["Content-Type"] = "application/json"
        conn.request(method, path, body=payload, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, data
    finally:
        conn.close()


def start_server():
    out = open(os.path.join(WORK_DIR, "server.log"), "ab")
    return subprocess.Popen(
        [S2, "lite", "--port", str(PORT), "--local-root", DATA_DIR],
        stdout=out, stderr=subprocess.STDOUT,
    )


def wait_health(deadline_s=60):
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        try:
            status, _ = http_call("GET", "/health", timeout=2)
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(0.2)
    fail(3, "server did not become healthy in time")


def wait_stream_ready(deadline_s=120):
    deadline = time.monotonic() + deadline_s
    last = None
    while time.monotonic() < deadline:
        try:
            status, data = http_call(
                "GET", f"/v1/streams/{STREAM}/records/tail", timeout=2)
            if status == 200:
                return json.loads(data)["tail"]["seq_num"]
            last = (status, data[:200])
        except OSError as e:
            last = repr(e)
        time.sleep(0.2)
    fail(3, f"stream tail not readable after restart: {last}")


def create_basin():
    cli_env = dict(
        os.environ,
        S2_ACCOUNT_ENDPOINT=f"http://127.0.0.1:{PORT}",
        S2_BASIN_ENDPOINT=f"http://127.0.0.1:{PORT}",
        S2_ACCESS_TOKEN="ignored",
    )
    r = subprocess.run(
        [S2, "create-basin", BASIN, "--create-stream-on-append",
         "--create-stream-on-read"],
        env=cli_env, capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        fail(3, f"create-basin failed: {r.stderr.strip()[:300]}")


def attempt(body_str, token=None, headers=None):
    """One append attempt. Returns a manifest entry with the outcome:
    acked (start, end) on 200, otherwise (status, error-code) or exception."""
    record = {"body": body_str}
    if headers is not None:
        record["headers"] = headers
    payload = {"records": [record]}
    if token is not None:
        payload["fencing_token"] = token
    entry = {"body": body_str, "token": token, "ack": None, "reject": None,
             "err": None}
    try:
        status, data = http_call(
            "POST", f"/v1/streams/{STREAM}/records", body=payload, timeout=15)
    except OSError as e:
        entry["err"] = type(e).__name__
        return entry
    if status == 200:
        ack = json.loads(data)
        entry["ack"] = (ack["start"]["seq_num"], ack["end"]["seq_num"])
    else:
        try:
            code = json.loads(data).get("code")
        except (ValueError, AttributeError):
            code = None
        entry["reject"] = (status, code)
    return entry


def fence(new_token, current_token):
    return attempt(new_token, token=current_token,
                   headers=[["", "fence"]])


def phase(tag, seed, t1, t2, n_stale):
    """Stale, tokenless, and control attempts. Returns outcome summary."""
    out = {"stale": [], "tokenless": None, "control": None}
    for i in range(n_stale):
        out["stale"].append(attempt(f"s{seed}-{tag}-stale-{i}", token=t1))
    out["tokenless"] = attempt(f"s{seed}-{tag}-tokenless")
    out["control"] = attempt(f"s{seed}-{tag}-control", token=t2)
    return out


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "stale-across-restart"
    if mode != "stale-across-restart":
        fail(3, f"mode {mode!r} not implemented")
    seed = derive_seed()
    n1 = 5 + seed % 11          # writes under T1
    n_stale = 2 + (seed >> 5) % 3
    t1 = f"t1-{seed:08x}"
    t2 = f"t2-{seed:08x}"
    log(f"mode={mode} seed={seed} n1={n1} n_stale={n_stale}")
    os.makedirs(DATA_DIR, exist_ok=True)

    server = start_server()
    wait_health()
    create_basin()

    accepted = []  # manifest entries with acks, in issue order

    primer = attempt(f"s{seed}-primer")
    if primer["ack"] is None:
        fail(3, f"primer append refused: {primer} — setup")
    accepted.append(primer)

    f1 = fence(t1, None)  # stream token starts empty; untokened fence ok
    if f1["ack"] is None:
        fail(3, f"fence to T1 refused: {f1} — setup")
    accepted.append(f1)

    for i in range(n1):
        e = attempt(f"s{seed}-under-t1-{i:03d}", token=t1)
        if e["ack"] is None:
            fail(3, f"append under current token T1 refused: {e} — setup")
        accepted.append(e)

    f2 = fence(t2, t1)
    if f2["ack"] is None:
        fail(3, f"fence to T2 refused: {f2} — setup")
    accepted.append(f2)

    pre = phase("pre", seed, t1, t2, n_stale)
    server.send_signal(signal.SIGKILL)
    server.wait(timeout=30)
    server = start_server()
    wait_health()
    tail_r = wait_stream_ready()
    log(f"restarted; recovered tail = {tail_r}")
    post = phase("post", seed, t1, t2, n_stale)

    for tag, out in (("pre", pre), ("post", post)):
        log(f"{tag}: stale={[e['reject'] or e['ack'] or e['err'] for e in out['stale']]} "
            f"tokenless={'acked' if out['tokenless']['ack'] else out['tokenless']['reject'] or out['tokenless']['err']} "
            f"control={'acked' if out['control']['ack'] else out['control']['reject'] or out['control']['err']}")

    # anti-vacuous: controls must work on both sides; every attempt answered
    for tag, out in (("pre", pre), ("post", post)):
        if out["control"]["ack"] is None:
            fail(3, f"{tag}: control append under current token T2 did not "
                    f"ack: {out['control']} — cannot attribute rejections to "
                    f"fencing, void")
        for e in out["stale"] + [out["tokenless"]]:
            if e["err"]:
                fail(3, f"{tag}: attempt got no HTTP response ({e}) — void")
    for out in (pre, post):
        if out["tokenless"]["ack"]:
            accepted.append(out["tokenless"])
        accepted.append(out["control"])

    if os.environ.get("ORACLE_SELFTEST"):
        victim = post["stale"][0]
        log(f"ORACLE_SELFTEST: pretending stale append {victim['body']!r} "
            f"was accepted post-restart")
        victim["reject"] = None
        victim["ack"] = (10**9, 10**9 + 1)

    # 1. stale_rejected — explicit rejection on both sides of the restart
    for tag, out in (("pre-kill", pre), ("post-restart", post)):
        for e in out["stale"]:
            if e["reject"] is None:
                fail(1, f"stale-token append ACCEPTED {tag}: {e['body']!r} "
                        f"acked as {e['ack']} under superseded token",
                     inv=("stale_rejected", "stale-token-rejected"))
    invariant("stale_rejected", "stale-token-rejected", True,
              f"{len(pre['stale'])}+{len(post['stale'])} stale attempts "
              f"explicitly rejected pre-kill and post-restart")

    # 2. behavior_consistent — identical rejection + tokenless handling
    pre_r = {e["reject"] for e in pre["stale"]}
    post_r = {e["reject"] for e in post["stale"]}
    if pre_r != post_r:
        fail(1, f"stale rejection changed across restart: pre {pre_r} vs "
                f"post {post_r}",
             inv=("behavior_consistent", "identical-across-restart"))
    pre_t = pre["tokenless"]["ack"] is not None
    post_t = post["tokenless"]["ack"] is not None
    if pre_t != post_t:
        fail(1, f"tokenless acceptance changed across restart: pre="
                f"{'acked' if pre_t else pre['tokenless']['reject']} post="
                f"{'acked' if post_t else post['tokenless']['reject']}",
             inv=("behavior_consistent", "identical-across-restart"))
    invariant("behavior_consistent", "identical-across-restart", True,
              f"stale rejection {sorted(pre_r)} and tokenless="
              f"{'accepted' if pre_t else 'rejected'} identical across "
              f"restart")

    # 3. content_exact — acked ranges tile [0, tail); bodies match exactly.
    # Ack `end` is exclusive on this API (established by the tail-gapless
    # runs: single-record acks come back as [n, n+1]).
    tail_seq = wait_stream_ready()
    ranges = sorted((e["ack"][0], e["ack"][1], e["body"]) for e in accepted
                    if e["ack"])
    cursor = 0
    for start, end, body in ranges:
        if start != cursor:
            kind = "overlap" if start < cursor else "gap"
            fail(1, f"{kind} at seq {min(cursor, start)}: accepted acks do "
                    f"not tile (next range [{start}, {end}))",
                 inv=("content_exact", "accepted-records-only"))
        cursor = end
    if cursor != tail_seq:
        fail(1, f"accepted ranges tile [0, {cursor}) but tail is {tail_seq}",
             inv=("content_exact", "accepted-records-only"))

    records = []
    cur = 0
    while cur < tail_seq:
        status, data = http_call(
            "GET", f"/v1/streams/{STREAM}/records?seq_num={cur}&count=1000",
            timeout=30)
        if status != 200:
            fail(3, f"read failed at seq {cur}: HTTP {status} {data[:200]}")
        batch = json.loads(data).get("records", [])
        if not batch:
            fail(1, f"no records at seq {cur} < tail {tail_seq} — gap",
                 inv=("content_exact", "accepted-records-only"))
        for rec in batch:
            records.append((rec["seq_num"], rec.get("body", "")))
        cur = records[-1][0] + 1
    bodies = {b for _, b in records}
    stale_bodies = {e["body"] for out in (pre, post) for e in out["stale"]}
    leaked = bodies & stale_bodies
    if leaked:
        fail(1, f"stale-token payload(s) present in read-back: "
                f"{sorted(leaked)[:5]}",
             inv=("content_exact", "accepted-records-only"))
    expected_bodies = {e["body"] for e in accepted if e["ack"]}
    phantom = bodies - expected_bodies
    if phantom:
        fail(1, f"read-back contains record(s) never accepted: "
                f"{sorted(phantom)[:5]}",
             inv=("content_exact", "accepted-records-only"))
    invariant("content_exact", "accepted-records-only", True,
              f"{len(records)} records dense in [0, {tail_seq}); exactly the "
              f"accepted set, no stale payload leaked")

    server.terminate()
    log("VERDICT: GREEN")


main()
PYEOF
