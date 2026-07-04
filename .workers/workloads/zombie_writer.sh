#!/bin/sh
# Workload: zombie writer cannot corrupt (s2 lite, --local-root).
#
# Mode sigstop-takeover:
#   start A and write through it; leave appends in-flight on A's open
#   connections and SIGSTOP A (not kill); start B on the SAME root
#   (different port — A still holds its listener); SIGCONT A at a seeded
#   offset that sweeps the whole takeover window (during B's fencing
#   sleep, during B's first writes, or after B is established); push
#   appends through A while B keeps writing; read back through B.
#
# Classification is by TIME against T (B's first ack) and by SEQ against
# the takeover boundary (B's recovered tail, read before B's first
# append). Persist-time decides corruption, not ack-time: a post-T A ack
# whose range lies below the boundary is a late ack of a pre-takeover
# durable write — allowed.
#
# Oracle (see .workers/promises/zombie-writer-cannot-corrupt.md):
#   - b_acked_present: every B-acked record appears exactly once
#   - no_zombie_persisted: no A-accepted record at/beyond the takeover
#     boundary appears in read-back (acked or not); rejected/dropped/lost
#     zombie appends are fine (a post-T ack for an absent record is a lie
#     told to a fenced client — logged, not a finding)
#   - stream_content: read-back [0, tail) is dense; contents are exactly
#     (A-pre-takeover writes, truncated suffix aside) ∪ (B-acked), each
#     at most once
#   - anti-vacuous gate: >= 1 post-CONT attempt must get an HTTP response
#     from A (ack or storage-layer rejection) — pure connection failures
#     mean the zombie never reached the write path, trial VOID
#
# Self-contained: sh wrapper + embedded python3 (injection layers one file).
# Exit codes: 0 green, 1 red (finding), 3 void/blocked.
MODE="${1:-sigstop-takeover}"
exec python3 - "$MODE" <<'PYEOF'
import http.client
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time

S2 = os.path.join(".workers", "vendor", "bin", "s2-linux-amd64")
PORT_A = 8080
PORT_B = 8081
BASIN = "durability-wl-03"
STREAM = "zombie-writer"
WORK_DIR = "/tmp/wl-zombie"  # /workspace (the repo checkout) is read-only
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


def http_call(port, method, path, body=None, timeout=10):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
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


def start_server(port, tag):
    out = open(os.path.join(WORK_DIR, f"server-{tag}.log"), "ab")
    return subprocess.Popen(
        [S2, "lite", "--port", str(port), "--local-root", DATA_DIR],
        stdout=out, stderr=subprocess.STDOUT,
    )


def wait_health(port, deadline_s=60):
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        try:
            status, _ = http_call(port, "GET", "/health", timeout=2)
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(0.2)
    fail(3, f"server on port {port} did not become healthy in time")


def wait_stream_ready(port, deadline_s=120):
    """Takeover readiness: startup sleeps one manifest_poll_interval
    (time-based fencing) — poll check-tail, never fixed sleeps."""
    deadline = time.monotonic() + deadline_s
    last = None
    while time.monotonic() < deadline:
        try:
            status, data = http_call(
                port, "GET", f"/v1/streams/{STREAM}/records/tail", timeout=2)
            if status == 200:
                return json.loads(data)["tail"]["seq_num"]
            last = (status, data[:200])
        except OSError as e:
            last = repr(e)
        time.sleep(0.2)
    fail(3, f"stream tail not readable on port {port}: {last}")


def create_basin():
    cli_env = dict(
        os.environ,
        S2_ACCOUNT_ENDPOINT=f"http://127.0.0.1:{PORT_A}",
        S2_BASIN_ENDPOINT=f"http://127.0.0.1:{PORT_A}",
        S2_ACCESS_TOKEN="ignored",
    )
    r = subprocess.run(
        [S2, "create-basin", BASIN, "--create-stream-on-append",
         "--create-stream-on-read"],
        env=cli_env, capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        fail(3, f"create-basin failed: {r.stderr.strip()[:300]}")


def append_timed(port, payload, timeout=10):
    """One request = one ack. Returns a record dict with send/ack times;
    on refusal records why (HTTP status or exception)."""
    rec = {"payload": payload, "sent": time.monotonic(), "acked": None,
           "start": None, "end": None, "why": None}
    try:
        status, data = http_call(
            port, "POST", f"/v1/streams/{STREAM}/records",
            body={"records": [{"body": payload}]}, timeout=timeout)
    except OSError as e:
        rec["why"] = type(e).__name__
        return rec
    if status != 200:
        rec["why"] = f"HTTP {status} {data[:120]!r}"
        return rec
    ack = json.loads(data)
    rec["acked"] = time.monotonic()
    rec["start"], rec["end"] = ack["start"]["seq_num"], ack["end"]["seq_num"]
    return rec


def read_all(port, tail_seq):
    records = []
    cursor = 0
    for _ in range(10000):
        if cursor >= tail_seq:
            break
        status, data = http_call(
            port, "GET",
            f"/v1/streams/{STREAM}/records?seq_num={cursor}&count=1000",
            timeout=30)
        if status != 200:
            fail(3, f"read failed at seq {cursor}: HTTP {status} {data[:200]}")
        batch = json.loads(data).get("records", [])
        if not batch:
            fail(1, f"read returned no records at seq {cursor} < tail "
                    f"{tail_seq} — gap below tail",
                 inv=("stream_content", "dense-and-owned"))
        for rec in batch:
            records.append((rec["seq_num"], rec.get("body", "")))
        cursor = records[-1][0] + 1
    return records


def verify(a_pre, a_grey_acked, a_post_acked, a_grey_unacked, a_post_unacked,
           b_acked, boundary, readback, tail_seq):
    if os.environ.get("ORACLE_SELFTEST"):
        victim = b_acked.pop()  # relabel one persisted B write as post-T A ack
        a_post_acked.append(victim)
        log(f"ORACLE_SELFTEST: relabeled B-acked {victim['payload']!r} as a "
            f"post-takeover zombie ack")

    bodies = [b for _, b in readback]
    counts = {}
    for b in bodies:
        counts[b] = counts.get(b, 0) + 1

    seqs = [s for s, _ in readback]
    if seqs != list(range(tail_seq)):
        fail(1, f"read-back seqs not exactly 0..{tail_seq}: {len(seqs)} "
                f"records, first 20 = {seqs[:20]}",
             inv=("stream_content", "dense-and-owned"))

    # headline: persist-time, not ack-time. A post-T ack below the takeover
    # boundary is a late ack of a write B already recovered — allowed.
    late_acks, zombie_acks = [], []
    for z in a_post_acked:
        if z["end"] <= boundary:
            late_acks.append(z)
        else:
            zombie_acks.append(z)
    for z in late_acks:
        log(f"LATE ACK (allowed): {z['payload']} [{z['start']}, {z['end']}] "
            f"durable before takeover boundary {boundary}")
    for z in zombie_acks:
        if counts.get(z["payload"]):
            fail(1, f"zombie write persisted: {z['payload']!r} accepted by A "
                    f"beyond takeover boundary {boundary} as [{z['start']}, "
                    f"{z['end']}] and present in read-back",
                 inv=("no_zombie_persisted", "no-post-takeover-persist"))
        log(f"ZOMBIE ACK LIED (not a finding): {z['payload']} [{z['start']}, "
            f"{z['end']}] acked beyond boundary {boundary}, absent from "
            f"read-back")
    invariant("no_zombie_persisted", "no-post-takeover-persist", True,
              f"no A-accepted record beyond takeover boundary {boundary} in "
              f"read-back ({len(late_acks)} late ack(s) of pre-takeover "
              f"writes, {len(zombie_acks)} beyond-boundary ack(s) all absent)")

    for m in b_acked:
        n = counts.get(m["payload"], 0)
        if n != 1:
            fail(1, f"B-acked record {m['payload']!r} appears {n} times",
                 inv=("b_acked_present", "b-acked-exactly-once"))
    invariant("b_acked_present", "b-acked-exactly-once", True,
              f"all {len(b_acked)} B-acked records present exactly once")

    # contents: A-pre-takeover writes ∪ B-acked, at most once each; a write
    # first sent after T persisting unacked is a zombie persist too
    allowed = ({m["payload"] for m in a_pre}
               | {m["payload"] for m in a_grey_acked}
               | {m["payload"] for m in a_grey_unacked}
               | {m["payload"] for m in late_acks}
               | {m["payload"] for m in b_acked})
    post_unacked = {m["payload"] for m in a_post_unacked}
    for b, n in counts.items():
        if b in post_unacked:
            fail(1, f"unacked zombie write persisted: {b!r} first sent to A "
                    f"after takeover (T), never acked, present in read-back",
                 inv=("stream_content", "dense-and-owned"))
        if b not in allowed:
            fail(1, f"read-back contains a record never sent by A-pre-T or "
                    f"B: {b!r}", inv=("stream_content", "dense-and-owned"))
        if n > 1:
            fail(1, f"record duplicated: {b!r} appears {n} times",
                 inv=("stream_content", "dense-and-owned"))

    # pre-stop acked records may only be missing as a truncated suffix
    a_missing = [i for i, m in enumerate(a_pre) if not counts.get(m["payload"])]
    if a_missing and a_missing != list(range(a_missing[0], len(a_pre))):
        fail(1, f"A pre-stop acked records missing with holes (not a "
                f"suffix): indexes {a_missing[:10]} of {len(a_pre)}",
             inv=("stream_content", "dense-and-owned"))
    trunc = f", A suffix truncated: {len(a_missing)}" if a_missing else ""
    invariant("stream_content", "dense-and-owned", True,
              f"{len(bodies)} records dense in [0, {tail_seq}), contents are "
              f"A-pre-T ∪ B-acked, at most once each{trunc}")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "sigstop-takeover"
    if mode != "sigstop-takeover":
        fail(3, f"mode {mode!r} not implemented")
    seed = derive_seed()
    rng = random.Random(seed)
    n_a = 10 + seed % 21
    n_in = 4                                # in-flight on A across the STOP
    n_b1 = 5 + (seed >> 5) % 11
    n_b2 = 10
    n_z = 25
    b_delay = ((seed >> 8) % 2000) / 1000.0     # STOP -> B start
    t_cont = ((seed >> 12) % 8000) / 1000.0     # B start -> SIGCONT sweep
    log(f"mode={mode} seed={seed} a={n_a}+{n_in}inflight b1={n_b1} b2={n_b2} "
        f"z={n_z} b_delay={b_delay:.3f}s t_cont={t_cont:.3f}s")
    os.makedirs(DATA_DIR, exist_ok=True)

    server_a = start_server(PORT_A, "a")
    wait_health(PORT_A)
    create_basin()

    a_pre = []
    for i in range(n_a):
        rec = append_timed(PORT_A, f"s{seed}-A-{i:04d}")
        if rec["acked"] is None:
            fail(3, f"healthy A refused append {i} ({rec['why']}) — setup")
        a_pre.append(rec)
    log(f"A pre-stop: {len(a_pre)} acked")

    # leave appends mid-flight on A's open connections, then freeze it
    a_late = []          # in-flight + zombie attempts, classified later
    a_late_lock = threading.Lock()

    def inflight(i):
        rec = append_timed(PORT_A, f"s{seed}-I-{i:04d}", timeout=150)
        with a_late_lock:
            a_late.append(rec)

    in_threads = [threading.Thread(target=inflight, args=(i,))
                  for i in range(n_in)]
    for t in in_threads:
        t.start()
    time.sleep(0.02)     # let requests reach A's socket
    server_a.send_signal(signal.SIGSTOP)
    t_stop = time.monotonic()
    log("SIGSTOP A (in-flight appends frozen mid-request)")

    cont_state = {"at": None}

    def cont_timer(delay):
        time.sleep(delay)
        server_a.send_signal(signal.SIGCONT)
        cont_state["at"] = time.monotonic()
        log(f"SIGCONT A at +{cont_state['at'] - t_stop:.3f}s after stop")

    time.sleep(b_delay)
    threading.Thread(target=cont_timer, args=(t_cont,), daemon=True).start()
    server_b = start_server(PORT_B, "b")
    wait_health(PORT_B)
    takeover_tail = wait_stream_ready(PORT_B)
    log(f"B serving; tail at first readability = {takeover_tail}")

    b_acked = []
    t_first_b = None
    for i in range(n_b1):
        rec = append_timed(PORT_B, f"s{seed}-B-{i:04d}")
        if rec["acked"] is None:
            fail(3, f"healthy B refused append {i} ({rec['why']}) — setup")
        if t_first_b is None:
            t_first_b = rec["acked"]
        b_acked.append(rec)
    log(f"B phase 1: {len(b_acked)} acked; T fixed")

    def zombie():
        # wait for the CONT timer, then push through A's listener
        while cont_state["at"] is None:
            time.sleep(0.01)
        for i in range(n_z):
            rec = append_timed(PORT_A, f"s{seed}-Z-{i:04d}", timeout=3)
            with a_late_lock:
                a_late.append(rec)
            time.sleep(rng.random() * 0.02)

    b_refused = []  # sys.exit inside a thread only kills the thread

    def b_writer():
        for i in range(n_b1, n_b1 + n_b2):
            rec = append_timed(PORT_B, f"s{seed}-B-{i:04d}")
            if rec["acked"] is None:
                b_refused.append(f"append {i}: {rec['why']}")
                return
            b_acked.append(rec)
            time.sleep(rng.random() * 0.02)

    tz = threading.Thread(target=zombie)
    tb = threading.Thread(target=b_writer)
    tz.start(); tb.start()
    tz.join(); tb.join()
    for t in in_threads:
        t.join()
    if b_refused:
        fail(3, f"healthy B refused during zombie phase ({b_refused[0]}) "
                f"— setup")

    T = t_first_b
    cont_at = cont_state["at"]
    a_grey_acked = [r for r in a_late if r["acked"] and r["acked"] <= T]
    a_post_acked = [r for r in a_late if r["acked"] and r["acked"] > T]
    a_grey_unacked = [r for r in a_late if not r["acked"] and r["sent"] <= T]
    a_post_unacked = [r for r in a_late if not r["acked"] and r["sent"] > T]
    whys = {}
    for r in a_late:
        if not r["acked"]:
            whys[r["why"]] = whys.get(r["why"], 0) + 1
    log(f"A after stop: {len(a_grey_acked)} acked<=T, {len(a_post_acked)} "
        f"acked>T, {len(a_grey_unacked)}+{len(a_post_unacked)} unacked "
        f"(reasons: {whys})")
    for r in a_post_acked[:10]:
        log(f"POST-T A ACK: {r['payload']} [{r['start']}, {r['end']}] "
            f"+{r['acked'] - T:.3f}s after T")

    # gate: the zombie must observably reach A's write path post-CONT —
    # an ack or an HTTP-level storage rejection counts, a dead socket not
    reached = [r for r in a_late
               if cont_at and r["sent"] >= cont_at
               and (r["acked"] or (r["why"] or "").startswith("HTTP"))]
    if not reached and not os.environ.get("ORACLE_SELFTEST"):
        fail(3, f"no post-CONT attempt got an HTTP response from A "
                f"(t_cont +{t_cont:.2f}s after B start) — zombie never "
                f"reached the write path, trial void")

    tail_seq = wait_stream_ready(PORT_B)
    readback = read_all(PORT_B, tail_seq)
    verify(a_pre, a_grey_acked, a_post_acked, a_grey_unacked, a_post_unacked,
           b_acked, takeover_tail, readback, tail_seq)

    for p in (server_a, server_b):
        try:
            p.kill()
        except OSError:
            pass
    log("VERDICT: GREEN")


main()
PYEOF
