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
# Mode fence-ack-straddles-kill:
#   the fence WRITE itself races the crash. Establish T1 and prove it
#   settled (fence acked + bogus-token append 412 + T1 append accepted —
#   required because a MISSING token recovers to FencingToken::default(),
#   so without a settled T1 the unacked-fence XOR branch conflates "T1
#   governs" with "default governs"). Write under T1, fence to T2, SIGKILL
#   at a seed-chosen offset around the fence ack (in-flight | just-acked |
#   acked+settled), across SL8_FLUSH_INTERVAL arms. Restart; probe T1, T2,
#   tokenless — twice. Oracle:
#   - t1_settled: T1 established + enforced pre-straddle
#   - fence_durable (fence ACKED pre-kill): recovered token is T2 — T1
#     412-rejected, T2 accepted; regression to T1 is RED
#   - governs_xor (fence UNACKED): exactly one of T1/T2 governs (both
#     rejected = default-token conflation / token lost; both accepted =
#     fencing broken)
#   - governs_consistent: identical probe outcomes across both rounds
#   - fence_record: the T2 fence record appears in read-back iff T2
#     governs (always, when the fence was acked)
#   - content_exact: read-back dense; exactly the accepted bodies (the
#     indeterminate unacked fence record allowed at most once)
#
# Mode baseline:
#   no faults; the pure cooperative-contract rung (ladder floor, producer
#   #8). One server, one seed, three governance regimes probed in series:
#   default (empty token) -> fence to T1 -> fence to T2. Four required
#   pins (strategy-critic):
#   - tokenless_accepted: tokenless appends ALWAYS accepted — before any
#     fence, under T1, and under T2 (streamer.rs:341 only checks when a
#     token is provided)
#   - wrong_token_rejected: the full wrong-token class — stale
#     (previously-valid T1 after the T2 fence) AND never-valid (tX) —
#     gets an explicit HTTP 412 in every regime, and leaves no trace in
#     read-back. In a quiescent no-fault run any non-412 response to a
#     wrong-token append is RED, not void.
#   - disclosure_412: every 412 body is exactly
#     {"fencing_token_mismatch": "<current governing token>"}
#     (externally-tagged snake_case variant of
#     AppendConditionFailed, handler clones `actual` —
#     lite/src/handlers/v1/error.rs:256-257). Body pinned, not just
#     status; default regime discloses "".
#   - atomic_flip: governance flips exactly at the fence record's
#     position (applied_point, streamer.rs:344-347/368-376): every
#     T1-accepted record sits strictly between fence-T1 and fence-T2
#     positions, every T2-accepted after fence-T2, the fence records'
#     bodies sit at their acked positions, and the FIRST T1 attempt
#     issued after the fence-T2 ack is already rejected (no post-ack
#     window where the old token still governs).
#   Plus content_exact (acks tile [0, tail); read-back dense; exactly
#   the accepted set; no rejected payload anywhere) and anti-vacuity
#   (every regime witnessed >=1 wrong-token 412 + >=1 accepted control).
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
import threading
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


def start_server(env_extra=None):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    out = open(os.path.join(WORK_DIR, "server.log"), "ab")
    return subprocess.Popen(
        [S2, "lite", "--port", str(PORT), "--local-root", DATA_DIR],
        stdout=out, stderr=subprocess.STDOUT, env=env,
    )


def wait_health(deadline_s=60, red_on_fail=False):
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        try:
            status, _ = http_call("GET", "/health", timeout=2)
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(0.2)
    if red_on_fail:
        # post-kill: failing to recover from a crash-consistent root IS the
        # finding (recovery choking on the straddled write), not a void
        fail(1, "server did not become healthy after the kill — recovery "
                "failed on crash-consistent state",
             inv=("restart_serves", "restart-serves"))
    fail(3, "server did not become healthy in time")


def wait_stream_ready(deadline_s=120, red_on_fail=False):
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
    if red_on_fail:
        fail(1, f"stream tail not readable after post-kill restart: {last} — "
                "recovery failed on crash-consistent state",
             inv=("restart_serves", "restart-serves"))
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
        entry["raw"] = data.decode("utf-8", "replace")[:500]
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


def read_dense(tail_seq, inv, red_on_http=False):
    """Read [0, tail) and require density. Returns [(seq, body)]."""
    records = []
    cur = 0
    while cur < tail_seq:
        status, data = http_call(
            "GET", f"/v1/streams/{STREAM}/records?seq_num={cur}&count=1000",
            timeout=30)
        if status != 200:
            if red_on_http:
                fail(1, f"post-restart read failed at seq {cur} < tail "
                        f"{tail_seq}: HTTP {status} {data[:200]} — recovered "
                        f"state unreadable",
                     inv=("restart_serves", "restart-serves"))
            fail(3, f"read failed at seq {cur}: HTTP {status} {data[:200]}")
        batch = json.loads(data).get("records", [])
        if not batch:
            fail(1, f"no records at seq {cur} < tail {tail_seq} — gap",
                 inv=inv)
        for rec in batch:
            records.append((rec["seq_num"], rec.get("body", "")))
        cur = records[-1][0] + 1
    return records


def main_straddle(seed):
    arm = ["", "500ms", "2s"][seed % 3]
    window = {"": 0.005, "500ms": 0.5, "2s": 2.0}[arm]
    # 0 in-flight sweep | 1 just-acked | 2 settled; style 0 weighted 2× —
    # it is the only style that can yield the unacked-fence XOR branch
    kill_style = (0, 0, 1, 2)[(seed >> 4) % 4]
    frac = ((seed >> 7) % 1000) / 1000.0
    # style-0 delay must land BELOW the fence's ack latency (~one flush
    # window) or the straddle degenerates to just-acked; sweep [0, window)
    kill_delay = frac * window
    n1 = 4 + (seed >> 17) % 8
    t1 = f"t1-{seed:08x}"
    t2 = f"t2-{seed:08x}"
    log(f"mode=fence-ack-straddles-kill seed={seed} arm={arm or 'default'} "
        f"kill_style={kill_style} kill_delay={kill_delay * 1000:.1f}ms n1={n1}")
    os.makedirs(DATA_DIR, exist_ok=True)
    env_extra = {"SL8_FLUSH_INTERVAL": arm} if arm else None

    server = start_server(env_extra)
    wait_health()
    create_basin()
    accepted = []

    # prime: slow arms 404 lazily-created streams until creation is durable
    primer = None
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        primer = attempt(f"s{seed}-primer")
        if primer["ack"]:
            break
        time.sleep(0.25)
    if primer is None or primer["ack"] is None:
        fail(3, f"primer never acked: {primer} — setup")
    accepted.append(primer)

    # establish + settle T1 (missing token recovers to default — a settled
    # T1 is what makes the unacked-fence XOR branch meaningful)
    f1 = fence(t1, None)
    if f1["ack"] is None:
        fail(3, f"fence to T1 refused: {f1} — setup")
    accepted.append(f1)
    bogus = attempt(f"s{seed}-bogus-probe", token=f"tX-{seed:08x}")
    if bogus["reject"] is None:
        fail(1, f"bogus-token append not rejected while T1 set: {bogus}",
             inv=("t1_settled", "t1-established-and-enforced"))
    if bogus["reject"][0] != 412:
        fail(3, f"bogus-token probe got non-412 rejection {bogus['reject']} — "
                "cannot attribute to fencing, void")
    settle1 = attempt(f"s{seed}-settle-t1", token=t1)
    if settle1["ack"] is None:
        fail(3, f"T1 settle append refused: {settle1} — setup")
    accepted.append(settle1)
    invariant("t1_settled", "t1-established-and-enforced", True,
              f"T1 fence acked at {f1['ack']}; bogus token rejected "
              f"{bogus['reject']}; T1 append accepted")

    for i in range(n1):
        e = attempt(f"s{seed}-under-t1-{i:03d}", token=t1)
        if e["ack"] is None:
            fail(3, f"append under T1 refused: {e} — setup")
        accepted.append(e)

    # the straddle: fence to T2 races SIGKILL
    fence_res = {}

    def do_fence():
        fence_res.update(fence(t2, t1))

    tf = threading.Thread(target=do_fence)
    tf.start()
    if kill_style == 0:
        time.sleep(kill_delay)
    else:
        tf.join(timeout=window * 2 + 30)
        if fence_res.get("reject"):
            fail(3, f"fence to T2 rejected with current token: "
                    f"{fence_res['reject']} — setup")
        if kill_style == 2 and fence_res.get("ack"):
            settle2 = attempt(f"s{seed}-settle-t2", token=t2)
            if settle2["ack"] is None:
                fail(3, f"T2 settle append refused after acked fence: "
                        f"{settle2} — setup")
            accepted.append(settle2)
    server.send_signal(signal.SIGKILL)
    server.wait(timeout=30)
    tf.join(timeout=30)
    fence_acked = bool(fence_res.get("ack"))
    log(f"fence outcome at kill: "
        + (f"ACKED {fence_res['ack']}" if fence_acked else
           f"UNACKED ({fence_res.get('reject') or fence_res.get('err')})"))

    server = start_server(env_extra)
    wait_health(red_on_fail=True)
    tail_r = wait_stream_ready(red_on_fail=True)
    log(f"restarted; recovered tail = {tail_r}")

    # probes resolve to ack or an explicit 412 — anything else (transient
    # 5xx, dropped connection) retries bounded, then voids; unresolved
    # attempt bodies may have persisted, so they are tolerated at-most-once
    maybe_bodies = set()

    def probe(tag, token):
        last = None
        for ri in range(3):
            body = f"s{seed}-{tag}" + (f"-r{ri}" if ri else "")
            e = attempt(body, token=token)
            if e["ack"] or (e["reject"] and e["reject"][0] == 412):
                return e
            maybe_bodies.add(body)
            last = e
            log(f"probe {tag} unresolved ({last['reject'] or last['err']}) "
                f"— retrying")
            time.sleep(0.3)
        fail(3, f"probe {tag} never resolved to ack or 412: {last} — void")

    def probe_round(r):
        p1 = probe(f"post{r}-t1", t1)
        p2 = probe(f"post{r}-t2", t2)
        pt = probe(f"post{r}-tokenless", None)
        log(f"probe round {r}: T1={p1['ack'] or p1['reject']} "
            f"T2={p2['ack'] or p2['reject']} "
            f"tokenless={pt['ack'] or pt['reject']}")
        return p1, p2, pt

    r1 = probe_round(1)
    r2 = probe_round(2)

    if os.environ.get("ORACLE_SELFTEST"):
        log("ORACLE_SELFTEST: forging both post-restart T1 probes as accepted")
        for r in (r1, r2):
            r[0]["reject"], r[0]["ack"] = None, (10 ** 9, 10 ** 9 + 1)

    t1_acc = [r[0]["ack"] is not None for r in (r1, r2)]
    t2_acc = [r[1]["ack"] is not None for r in (r1, r2)]
    tl_acc = [r[2]["ack"] is not None for r in (r1, r2)]
    if t1_acc[0] != t1_acc[1] or t2_acc[0] != t2_acc[1] or tl_acc[0] != tl_acc[1]:
        fail(1, f"probe outcomes flapped across rounds: T1 {t1_acc}, "
                f"T2 {t2_acc}, tokenless {tl_acc}",
             inv=("governs_consistent", "one-token-governs-consistently"))

    if fence_acked:
        if t1_acc[0] or not t2_acc[0]:
            fail(1, f"acked fence to T2 regressed after restart: "
                    f"T1={'accepted' if t1_acc[0] else r1[0]['reject']}, "
                    f"T2={'accepted' if t2_acc[0] else r1[1]['reject']}",
                 inv=("fence_durable", "acked-fence-survives"))
        governs = "T2"
        invariant("fence_durable", "acked-fence-survives", True,
                  "acked fence recovered: T1 rejected, T2 accepted post-restart")
    else:
        if t1_acc[0] == t2_acc[0]:
            fail(1, "no single token governs after unacked-fence kill: "
                    + ("both accepted — fencing broken" if t1_acc[0] else
                       f"both rejected (T1 {r1[0]['reject']}, T2 "
                       f"{r1[1]['reject']}) — default-token conflation or "
                       f"token lost"),
                 inv=("governs_xor", "exactly-one-token-governs"))
        governs = "T2" if t2_acc[0] else "T1"
        if governs == "T1":
            log("residual: unacked fence resolved to T1 — the "
                "durable-but-unacked (T2-governs) corner not observed "
                "this trial")
        invariant("governs_xor", "exactly-one-token-governs", True,
                  f"unacked fence resolved to {governs}")
    invariant("governs_consistent", "one-token-governs-consistently", True,
              f"{governs} governs; probe outcomes identical across rounds; "
              f"tokenless {'accepted' if tl_acc[0] else 'rejected'} both rounds")

    for r in (r1, r2):
        for e in r:
            if e["ack"]:
                accepted.append(e)

    tail_seq = wait_stream_ready(red_on_fail=True)
    records = read_dense(tail_seq, ("content_exact", "accepted-records-only"),
                         red_on_http=True)
    counts = {}
    for _, b in records:
        counts[b] = counts.get(b, 0) + 1
    acked_bodies = {e["body"] for e in accepted if e["ack"]}

    fence_present = counts.get(t2, 0) > 0
    if counts.get(t2, 0) > 1:
        fail(1, f"fence record duplicated: {counts[t2]} copies of T2 fence",
             inv=("fence_record", "fence-record-iff-t2-governs"))
    if fence_acked and not fence_present:
        fail(1, "acked fence record absent from read-back",
             inv=("fence_record", "fence-record-iff-t2-governs"))
    if not fence_acked and fence_present != (governs == "T2"):
        fail(1, f"fence record {'present' if fence_present else 'absent'} "
                f"but {governs} governs — token state and stream content "
                f"disagree",
             inv=("fence_record", "fence-record-iff-t2-governs"))
    invariant("fence_record", "fence-record-iff-t2-governs", True,
              f"fence record {'present' if fence_present else 'absent'}, "
              f"{governs} governs "
              f"({'acked' if fence_acked else 'unacked'} fence)")

    for b, n in counts.items():
        if b == t2:
            continue
        if n > 1:
            fail(1, f"record duplicated: {b!r} ×{n}",
                 inv=("content_exact", "accepted-records-only"))
        if b in maybe_bodies:
            continue  # unresolved probe attempt — indeterminate, once is ok
        if b not in acked_bodies:
            fail(1, f"read-back contains a record never accepted: {b!r}",
                 inv=("content_exact", "accepted-records-only"))
    missing = [b for b in acked_bodies if b != t2 and not counts.get(b)]
    if missing:
        fail(1, f"{len(missing)} acked record(s) absent after restart: "
                f"{sorted(missing)[:5]}",
             inv=("content_exact", "accepted-records-only"))
    invariant("content_exact", "accepted-records-only", True,
              f"{len(records)} records dense in [0, {tail_seq}); exactly the "
              f"accepted set")

    server.terminate()
    log("VERDICT: GREEN")


def main_baseline(seed):
    n1 = 4 + seed % 6
    n2 = 4 + (seed >> 8) % 6
    n_stale = 2 + (seed >> 5) % 3
    t1 = f"t1-{seed:08x}"
    t2 = f"t2-{seed:08x}"
    tx = f"tX-{seed:08x}"  # never-valid: never governed this stream
    log(f"mode=baseline seed={seed} n1={n1} n2={n2} n_stale={n_stale}")
    os.makedirs(DATA_DIR, exist_ok=True)

    server = start_server()
    wait_health()
    create_basin()

    accepted = []   # entries with acks, in issue order
    rejected = []   # (entry, governing_token_at_issue) for wrong-token attempts
    # ORACLE_SELFTEST arms: "1" relabels a stale attempt as accepted
    # (-> wrong_token_rejected), "flip" perturbs the fence-T2 position
    # (-> atomic_flip), "disclose" forges a 412 body (-> disclosure_412)
    selftest = os.environ.get("ORACLE_SELFTEST") or ""

    def must_ack(e, what):
        # correct-token / fence 412 in a quiescent no-fault run is a
        # finding (governance state wrong), not a void — attribution is
        # exactly what this fault-free rung isolates. Transport errors
        # and other statuses stay void.
        if e["ack"] is None:
            if e["reject"] and e["reject"][0] == 412:
                fail(1, f"{what} 412-rejected in a quiescent no-fault run "
                        f"— governing-token state wrong: {e.get('raw')}",
                     inv=("atomic_flip",
                          "governance-flips-at-fence-position"))
            fail(3, f"{what} did not ack: {e} — setup")
        accepted.append(e)
        return e

    def alive_or_red(e, tag, inv):
        # no faults + localhost: a dropped connection almost certainly
        # means the server died on this attempt — a crash-on-append is a
        # product finding, not a void
        if server.poll() is not None:
            fail(1, f"server process exited (code {server.poll()}) "
                    f"processing attempt {tag}: {e} — crash on append "
                    f"in a no-fault run", inv=inv)
        fail(3, f"attempt {tag} got no HTTP response with server still "
                f"running: {e} — void")

    def wrong(tag, token, governing):
        """One wrong-token attempt while `governing` governs."""
        e = attempt(f"s{seed}-{tag}", token=token)
        if e["err"]:
            alive_or_red(e, tag, ("wrong_token_rejected",
                                  "wrong-token-class-rejected"))
        rejected.append((e, governing))
        return e

    tokenless_acks = {}

    def tokenless(tag, regime):
        """Tokenless append: rejection is the pin-1 RED, never a void
        (only the pre-fence primer, a setup probe, may void)."""
        e = attempt(f"s{seed}-{tag}")
        if e["err"]:
            alive_or_red(e, tag, ("tokenless_accepted",
                                  "tokenless-always-accepted"))
        if e["ack"] is None:
            fail(1, f"tokenless append rejected in {regime} regime: "
                    f"{e['reject']} {e.get('raw')} — cooperative contract "
                    f"broken (streamer.rs:341 only checks provided tokens)",
                 inv=("tokenless_accepted", "tokenless-always-accepted"))
        accepted.append(e)
        tokenless_acks.setdefault(regime, []).append(e)
        return e

    # primer is setup (stream lazy-creation), not the pin witness
    must_ack(attempt(f"s{seed}-primer"), "primer (tokenless)")

    # --- regime: default (empty) token governs ---
    wrong("default-nevervalid", tx, "")
    tokenless("tokenless-default", "default")

    # tokenless fence: cooperative contract allows it
    f1 = must_ack(fence(t1, None), "fence to T1")
    p1 = f1["ack"][0]
    if f1["ack"][1] != p1 + 1:
        fail(3, f"fence to T1 acked a multi-record range {f1['ack']} — "
                f"cannot derive fence position")

    # --- regime: T1 governs ---
    for i in range(n1):
        must_ack(attempt(f"s{seed}-under-t1-{i:03d}", token=t1),
                 f"append under T1 #{i}")
    tokenless("tokenless-t1", "t1")
    wrong("t1-nevervalid", tx, t1)

    f2 = must_ack(fence(t2, t1), "fence to T2 (with current token T1)")
    p2 = f2["ack"][0]
    if f2["ack"][1] != p2 + 1:
        fail(3, f"fence to T2 acked a multi-record range {f2['ack']} — "
                f"cannot derive fence position")

    # --- regime: T2 governs ---
    # first stale attempt fires immediately after the fence ack: pins that
    # there is no post-ack window where T1 still governs (atomic_flip)
    first_stale = wrong("t2-stale-0", t1, t2)
    for i in range(1, n_stale):
        wrong(f"t2-stale-{i}", t1, t2)
    wrong("t2-nevervalid", tx, t2)
    tokenless("tokenless-t2", "t2")
    for i in range(n2):
        must_ack(attempt(f"s{seed}-under-t2-{i:03d}", token=t2),
                 f"append under T2 #{i}")

    if selftest == "1":
        log(f"ORACLE_SELFTEST: pretending stale append "
            f"{first_stale['body']!r} was accepted under superseded T1")
        first_stale["reject"], first_stale["raw"] = None, None
        first_stale["ack"] = (10 ** 9, 10 ** 9 + 1)
    elif selftest == "disclose":
        log("ORACLE_SELFTEST: forging first 412 body to disclose a "
            "non-governing token")
        rejected[0][0]["raw"] = json.dumps(
            {"fencing_token_mismatch": "forged-not-governing"})
    elif selftest == "flip":
        log(f"ORACLE_SELFTEST: perturbing fence-T2 position {p2} -> {p2 + 1}")
        p2 += 1

    # 1. tokenless_accepted — pin 1; rejection RED-exits inside tokenless()
    if set(tokenless_acks) != {"default", "t1", "t2"}:
        fail(3, f"tokenless witness missing a regime: {set(tokenless_acks)}")
    invariant("tokenless_accepted", "tokenless-always-accepted", True,
              "tokenless appends acked in all three regimes "
              "(default, T1, T2 — incl. after fences)")

    # 2 + 3. wrong_token_rejected + disclosure_412 (pins 4 and 2)
    for e, governing in rejected:
        if e["reject"] is None:
            fail(1, f"wrong-token append ACCEPTED: {e['body']!r} acked as "
                    f"{e['ack']} while {governing or 'default'!r} governs",
                 inv=("wrong_token_rejected", "wrong-token-class-rejected"))
        if e["reject"][0] != 412:
            fail(1, f"wrong-token append {e['body']!r} rejected with "
                    f"{e['reject']} {e.get('raw')}, not 412 — quiescent "
                    f"no-fault run, contract violation",
                 inv=("wrong_token_rejected", "wrong-token-class-rejected"))
    invariant("wrong_token_rejected", "wrong-token-class-rejected", True,
              f"{len(rejected)} wrong-token attempts (stale + never-valid) "
              f"all 412 across default/T1/T2 regimes")

    for e, governing in rejected:
        if e["reject"] is None:
            continue  # selftest-forged entry already flagged above
        try:
            body = json.loads(e["raw"])
            disclosed = body["fencing_token_mismatch"]
        except (ValueError, TypeError, KeyError):
            fail(1, f"412 body for {e['body']!r} is not the externally-"
                    f"tagged fencing_token shape: {e.get('raw')!r}",
                 inv=("disclosure_412", "412-body-is-governing-token"))
        if disclosed != governing:
            fail(1, f"412 for {e['body']!r} disclosed {disclosed!r} but "
                    f"{governing!r} governed at issue time",
                 inv=("disclosure_412", "412-body-is-governing-token"))
    invariant("disclosure_412", "412-body-is-governing-token", True,
              f"every 412 body == current governing token "
              f"(incl. '' in the default regime)")

    tail_seq = wait_stream_ready()
    records = read_dense(tail_seq, ("content_exact", "accepted-records-only"))

    # 4. atomic_flip — governance boundary at the fence records (pin 3)
    flip_why = []
    for e in accepted:
        s = e["ack"][0]
        if e["token"] == t1 and e["body"] != t2 and not (p1 < s < p2):
            flip_why.append(
                f"T1-accepted {e['body']!r} at seq {s} outside ({p1},{p2})")
        if e["token"] == t2 and e["body"] != t2 and s <= p2:
            flip_why.append(
                f"T2-accepted {e['body']!r} at seq {s} <= fence pos {p2}")
    if first_stale["reject"] is None and not selftest:
        flip_why.append("first stale attempt after fence-T2 ack accepted")
    fence_at = {s: b for s, b in records if s in (p1, p2)}
    if fence_at.get(p1) != t1 or fence_at.get(p2) != t2:
        flip_why.append(
            f"fence record bodies not at their acked positions: "
            f"seq {p1} -> {fence_at.get(p1)!r} (want {t1!r}), "
            f"seq {p2} -> {fence_at.get(p2)!r} (want {t2!r})")
    if flip_why:
        fail(1, "; ".join(flip_why),
             inv=("atomic_flip", "governance-flips-at-fence-position"))
    invariant("atomic_flip", "governance-flips-at-fence-position", True,
              f"fence records at {p1} and {p2}; T1-accepted all in "
              f"({p1},{p2}), T2-accepted all > {p2}; first post-fence "
              f"stale attempt already 412")

    # 5. content_exact — acks tile [0, tail); read-back exact
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
    counts = {}
    for _, b in records:
        counts[b] = counts.get(b, 0) + 1
    acked_bodies = {e["body"] for e in accepted if e["ack"]}
    wrong_bodies = {e["body"] for e, _ in rejected}
    leaked = set(counts) & wrong_bodies - acked_bodies
    if leaked:
        fail(1, f"rejected wrong-token payload(s) present in read-back: "
                f"{sorted(leaked)[:5]}",
             inv=("content_exact", "accepted-records-only"))
    for b, n in counts.items():
        if n > 1:
            fail(1, f"record duplicated: {b!r} ×{n}",
                 inv=("content_exact", "accepted-records-only"))
        if b not in acked_bodies:
            fail(1, f"read-back contains a record never accepted: {b!r}",
                 inv=("content_exact", "accepted-records-only"))
    missing = [b for b in acked_bodies if not counts.get(b)]
    if missing:
        fail(1, f"{len(missing)} acked record(s) absent: "
                f"{sorted(missing)[:5]}",
             inv=("content_exact", "accepted-records-only"))
    invariant("content_exact", "accepted-records-only", True,
              f"{len(records)} records dense in [0, {tail_seq}); exactly "
              f"the accepted set; no wrong-token payload leaked")

    server.terminate()
    log("VERDICT: GREEN")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "stale-across-restart"
    if mode == "fence-ack-straddles-kill":
        main_straddle(derive_seed())
        return
    if mode == "baseline":
        main_baseline(derive_seed())
        return
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
    wait_health(red_on_fail=True)
    tail_r = wait_stream_ready(red_on_fail=True)
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
    tail_seq = wait_stream_ready(red_on_fail=True)
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
