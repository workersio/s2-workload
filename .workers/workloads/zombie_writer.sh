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
# Mode double-kill-mid-recovery:
#   sigstop-takeover meets kill-during-recovery: A SIGSTOPped mid-stream
#   (zombie: sockets open, SlateDB handle live); B takes over the same
#   root, writes, and is SIGKILLed mid-stream; C starts, and DURING C's
#   lazy first-access recovery (start_streamer -> load_persisted_stream_tail
#   -> assert_no_records_following_tail) A is SIGCONTed and spams appends
#   through its still-open listener — the zombie races the tail rebuild
#   itself. Oracle = sigstop-takeover family plus:
#   - b_unacked (B's in-flight at its kill) allowed present-or-absent, once
#   - availability clause: C persistently failing recovery (guard tripping
#     or stream unrecoverable after bounded C retries) is RED
#     (recovery_available), not void — a zombie must not brick recovery
#   - anti-vacuous: >=1 zombie attempt gets an HTTP response from A with
#     send-time before C's first successful check-tail, else VOID
#
# Mode live-overlap-double-start:
#   NO SIGNALS. A serves and keeps actively appending (3-thread writer
#   pool) while B starts on the SAME root, sleeps through its time-based
#   fencing window (one manifest_poll_interval, server.rs:186-198), takes
#   over, and writes. In both other modes the prior instance is frozen
#   during the successor's boot, so the elapsed-time fencing assumption is
#   never contested — here A's write path is live through B's entire boot
#   (operator double-start / supervisor restart-while-hung). A keeps
#   writing until its handle self-fences or the post-T tail window ends.
#   Oracle = the persist-time-boundary family above
#   (no_zombie_persisted / b_acked_present / stream_content) with two
#   mode-scoped tightenings (test-reviewer, executor #16):
#   - NO truncation allowance: every A-acked record must be in read-back
#     (no freeze exists to excuse a missing durable ack — a hole is
#     acked-data loss across the live takeover);
#   - successor_available: B failing to serve on the live root (2
#     attempts) or persistently refusing appends (3-retry bound) is RED,
#     not void — the sleep-removal regression would present exactly like
#     this; on mid-schedule refusal, readback+verify still run first so a
#     data red outranks the availability red.
#   Anti-vacuous gate (critic, producer #8): >=1 A attempt whose full
#   round-trip lands inside B's boot window (spawn -> first check-tail
#   200) AND >=1 A attempt sent after T (B's first ack), both reaching
#   A's write path (ack or HTTP-level response); else VOID. Verify runs
#   before the vacuity exit.
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
PORT_C = 8082
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
           "start": None, "end": None, "why": None, "resp_at": None}
    try:
        status, data = http_call(
            port, "POST", f"/v1/streams/{STREAM}/records",
            body={"records": [{"body": payload}]}, timeout=timeout)
    except OSError as e:
        rec["resp_at"] = time.monotonic()
        rec["why"] = type(e).__name__
        return rec
    rec["resp_at"] = time.monotonic()
    if status != 200:
        rec["why"] = f"HTTP {status} {data[:120]!r}"
        return rec
    ack = json.loads(data)
    rec["acked"] = rec["resp_at"]
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


def dump_server_log(tag, n_bytes=500):
    """Print a server log tail so a real recovery_available RED is
    triageable from stdout (bind failure vs guard abort)."""
    try:
        with open(os.path.join(WORK_DIR, f"server-{tag}.log"), "rb") as f:
            data = f.read()[-n_bytes:]
        log(f"server-{tag} log tail: {data.decode(errors='replace')!r}")
    except OSError as e:
        log(f"server-{tag} log unreadable: {e!r}")


def wait_serving(proc, port, deadline_s=90):
    """'serving' once /health is 200, 'exited' if proc dies first, 'timeout'
    otherwise — never exits the process; caller decides what failure means."""
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return "exited"
        try:
            status, _ = http_call(port, "GET", "/health", timeout=2)
            if status == 200:
                return "serving"
        except OSError:
            pass
        time.sleep(0.05)
    return "timeout"


def verify(a_pre, a_grey_acked, a_post_acked, a_grey_unacked, a_post_unacked,
           b_acked, boundary, readback, tail_seq, b_unacked=(),
           allow_pre_truncation=True):
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
               | {m["payload"] for m in b_acked}
               | {m["payload"] for m in b_unacked})
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

    # pre-stop acked records may only be missing as a truncated suffix —
    # and in live-overlap mode (no freeze, every ack durability-gated from
    # a live A) not even that: any missing A-acked record is acked-data
    # loss across the takeover (reviewer-required, executor #16)
    a_missing = [i for i, m in enumerate(a_pre) if not counts.get(m["payload"])]
    if a_missing and not allow_pre_truncation:
        fail(1, f"A-acked record(s) missing from read-back after live "
                f"takeover — acked-data loss: indexes {a_missing[:10]} of "
                f"{len(a_pre)} (no freeze exists to excuse truncation)",
             inv=("stream_content", "dense-and-owned"))
    if a_missing and a_missing != list(range(a_missing[0], len(a_pre))):
        fail(1, f"A pre-stop acked records missing with holes (not a "
                f"suffix): indexes {a_missing[:10]} of {len(a_pre)}",
             inv=("stream_content", "dense-and-owned"))
    trunc = f", A suffix truncated: {len(a_missing)}" if a_missing else ""
    invariant("stream_content", "dense-and-owned", True,
              f"{len(bodies)} records dense in [0, {tail_seq}), contents are "
              f"A-pre-T ∪ B-acked, at most once each{trunc}")


def main_double_kill(seed, rng):
    n_a = 40 + seed % 80
    n_in = 4
    b_sched = 25 + (seed >> 5) % 15           # B append schedule
    b_kill_after = 5 + (seed >> 9) % 15       # B acks before SIGKILL B
    b_delay = ((seed >> 8) % 1500) / 1000.0   # STOP -> B start
    cont_lead = ((seed >> 14) % 30) / 1000.0  # CONT -> first C probe lead
    n_z_cap = 400
    log(f"mode=double-kill-mid-recovery seed={seed} a={n_a}+{n_in}inflight "
        f"b_sched={b_sched} b_kill_after={b_kill_after} b_delay={b_delay:.3f}s "
        f"cont_lead={cont_lead * 1000:.0f}ms")
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
    a_late = []
    a_late_lock = threading.Lock()

    def inflight(i):
        rec = append_timed(PORT_A, f"s{seed}-I-{i:04d}", timeout=150)
        with a_late_lock:
            a_late.append(rec)

    in_threads = [threading.Thread(target=inflight, args=(i,))
                  for i in range(n_in)]
    for t in in_threads:
        t.start()
    time.sleep(0.02)
    server_a.send_signal(signal.SIGSTOP)
    log("SIGSTOP A (in-flight appends frozen mid-request)")

    time.sleep(b_delay)
    server_b = start_server(PORT_B, "b")
    wait_health(PORT_B)
    takeover_tail = wait_stream_ready(PORT_B)
    log(f"B serving; takeover boundary = {takeover_tail}")

    b_acked, b_unacked = [], []
    b_state = {"in_flight": False, "done": False}

    def b_writer():
        for i in range(b_sched):
            b_state["in_flight"] = True
            rec = append_timed(PORT_B, f"s{seed}-B-{i:04d}", timeout=15)
            b_state["in_flight"] = False
            (b_acked if rec["acked"] else b_unacked).append(rec)
            if rec["acked"] is None:
                break  # B is gone
            time.sleep(rng.random() * 0.01)
        b_state["done"] = True

    tb = threading.Thread(target=b_writer)
    tb.start()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if len(b_acked) >= b_kill_after or b_state["done"]:
            break
        time.sleep(0.002)
    spin = time.monotonic() + 2
    while time.monotonic() < spin and not b_state["in_flight"]:
        time.sleep(0.0005)
    server_b.send_signal(signal.SIGKILL)
    log(f"SIGKILL B after {len(b_acked)} acks "
        f"(in_flight={b_state['in_flight']})")
    tb.join(timeout=30)
    server_b.wait(timeout=30)
    if not b_acked:
        fail(3, "B acked nothing before its kill — vacuous")
    if b_state["done"] and not b_unacked:
        fail(3, "B finished its whole schedule before the kill — not "
                "mid-stream, void")
    T = b_acked[0]["acked"]

    # C starts; the zombie is CONTed and spams A across C's lazy
    # first-access recovery window (sub-ms — spam, don't fire once)
    zomb = {"stop": False}
    n_z_threads = 3  # rejected attempts take ~100ms each; parallel
                     # connections keep the sub-ms..ms window under fire

    def zombie(z):
        i = 0
        while not zomb["stop"] and i < n_z_cap:
            rec = append_timed(PORT_A, f"s{seed}-Z{z}-{i:04d}", timeout=1)
            with a_late_lock:
                a_late.append(rec)
            i += 1

    t_cont = None
    t_first_ct = None
    tz = []
    server_c = None
    c_attempts = 0
    while c_attempts < 4 and t_first_ct is None:
        c_attempts += 1
        server_c = start_server(PORT_C, f"c{c_attempts}")
        st = wait_serving(server_c, PORT_C)
        if st != "serving":
            log(f"C attempt {c_attempts}: {st} before serving "
                f"(rc={server_c.poll()})")
            dump_server_log(f"c{c_attempts}")
            continue
        if t_cont is None:
            server_a.send_signal(signal.SIGCONT)
            t_cont = time.monotonic()
            tz = [threading.Thread(target=zombie, args=(z,), daemon=True)
                  for z in range(n_z_threads)]
            for t in tz:
                t.start()
            log("SIGCONT A; zombie storm begins")
            time.sleep(cont_lead)
        # force + poll the lazy first-access recovery
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if server_c.poll() is not None:
                log(f"C attempt {c_attempts}: exited rc={server_c.returncode} "
                    f"during zombie-raced recovery")
                dump_server_log(f"c{c_attempts}")
                break
            try:
                stt, _ = http_call(
                    PORT_C, "GET", f"/v1/streams/{STREAM}/records/tail",
                    timeout=2)
                if stt == 200:
                    t_first_ct = time.monotonic()
                    break
            except OSError:
                pass
            time.sleep(0.005)
        if t_first_ct is None and server_c.poll() is None:
            log(f"C attempt {c_attempts}: alive but check-tail never 200 "
                f"in 60s")
            server_c.kill()
            server_c.wait(timeout=30)
    zomb["stop"] = True
    for t in tz:
        t.join(timeout=30)
    for t in in_threads:
        t.join()

    if t_first_ct is None:
        fail(1, f"stream unrecoverable after {c_attempts} C attempt(s) with "
                f"a zombie racing recovery — recovery bricked",
             inv=("recovery_available", "zombie-cannot-brick-recovery"))
    invariant("recovery_available", "zombie-cannot-brick-recovery", True,
              f"C served check-tail on attempt {c_attempts}"
              + (f" ({c_attempts - 1} prior attempt(s) died/stalled "
                 f"mid-recovery — logged, not persistent)"
                 if c_attempts > 1 else ""))

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

    n_z_attempts = sum(1 for r in a_late if f"s{seed}-Z" in r["payload"])
    # airtight witness: the whole request (send AND response) inside the
    # un-served window — a response is proof the request traversed A's path
    reached = [r for r in a_late
               if t_cont and r["sent"] >= t_cont
               and (r["resp_at"] or 0) <= t_first_ct
               and (r["acked"] or (r["why"] or "").startswith("HTTP"))]
    log(f"zombie: {n_z_attempts} attempts, {len(reached)} reached A's "
        f"write path before C's first check-tail "
        f"(window {(t_first_ct - t_cont) * 1000:.0f}ms)")

    # verify BEFORE the vacuity exit: a vacuous race must not mask a
    # B-durability or zombie-persist red the run still witnessed
    tail_seq = wait_stream_ready(PORT_C)
    readback = read_all(PORT_C, tail_seq)
    verify(a_pre, a_grey_acked, a_post_acked, a_grey_unacked, a_post_unacked,
           b_acked, takeover_tail, readback, tail_seq, b_unacked=b_unacked)
    if not reached and not os.environ.get("ORACLE_SELFTEST"):
        fail(3, "no zombie attempt got an HTTP response from A before C's "
                "first successful check-tail — recovery never raced, void "
                "(oracle clean on everything else)")

    for p in (server_a, server_c):
        try:
            p.kill()
        except OSError:
            pass
    log("VERDICT: GREEN")


def main_live_overlap(seed, rng):
    n_pre = 10 + seed % 20                       # serial acks before the pool
    n_w = 3                                      # live writer pool on A
    overlap_lead = 0.2 + ((seed >> 6) % 800) / 1000.0   # pool -> B spawn
    n_b = 15 + (seed >> 5) % 15                  # B appends after takeover
    post_t_tail = 1.0 + ((seed >> 10) % 2000) / 1000.0  # keep A firing past T
    per_thread_cap = 500
    log(f"mode=live-overlap-double-start seed={seed} pre={n_pre} pool={n_w} "
        f"overlap_lead={overlap_lead:.3f}s b={n_b} "
        f"post_t_tail={post_t_tail:.3f}s")
    os.makedirs(DATA_DIR, exist_ok=True)

    server_a = start_server(PORT_A, "a")
    wait_health(PORT_A)
    create_basin()

    a_pre_serial = []
    for i in range(n_pre):
        rec = append_timed(PORT_A, f"s{seed}-A-{i:04d}")
        if rec["acked"] is None:
            fail(3, f"healthy A refused append {i} ({rec['why']}) — setup")
        a_pre_serial.append(rec)
    log(f"A serial pre-phase: {len(a_pre_serial)} acked")

    # live writer pool: A's write path stays under fire through B's whole
    # boot — the thing neither sigstop mode ever exercised
    a_all = []
    a_lock = threading.Lock()
    pool = {"stop": False}

    def writer(w):
        i = 0
        while not pool["stop"] and i < per_thread_cap:
            rec = append_timed(PORT_A, f"s{seed}-W{w}-{i:04d}", timeout=5)
            with a_lock:
                a_all.append(rec)
            i += 1
            time.sleep(rng.random() * 0.01)

    tw = [threading.Thread(target=writer, args=(w,), daemon=True)
          for w in range(n_w)]
    for t in tw:
        t.start()
    time.sleep(overlap_lead)

    # successor availability is an oracle clause, not a setup hedge
    # (reviewer-required, executor #16): a double-start bricking B on a
    # live root is RED — the sleep-removal regression would look exactly
    # like this, and VOID would mask it forever
    t_b_spawn = time.monotonic()
    server_b = None
    b_attempts = 0
    while b_attempts < 2:
        b_attempts += 1
        server_b = start_server(PORT_B, f"b{b_attempts}" if b_attempts > 1
                                else "b")
        st = wait_serving(server_b, PORT_B)
        if st == "serving":
            break
        log(f"B attempt {b_attempts}: {st} before serving "
            f"(rc={server_b.poll()})")
        dump_server_log(f"b{b_attempts}" if b_attempts > 1 else "b")
        try:
            server_b.kill()
            server_b.wait(timeout=30)
        except OSError:
            pass
        server_b = None
    if server_b is None:
        pool["stop"] = True
        fail(1, f"B never served on the live root across {b_attempts} "
                f"attempt(s) — a double-start bricked the successor",
             inv=("successor_available", "double-start-cannot-brick-successor"))
    takeover_tail = wait_stream_ready(PORT_B)
    t_b_ready = time.monotonic()
    log(f"B serving on live root; takeover boundary = {takeover_tail}; "
        f"boot window = {(t_b_ready - t_b_spawn) * 1000:.0f}ms "
        f"(attempt {b_attempts})")

    b_acked = []
    b_refusal = None
    for i in range(n_b):
        rec = None
        for _ in range(3):                      # bounded per-append retry
            rec = append_timed(PORT_B, f"s{seed}-B-{i:04d}", timeout=15)
            if rec["acked"] is not None:
                break
            time.sleep(0.3)
        if rec["acked"] is None:
            b_refusal = f"append {i}: {rec['why']}"
            break
        b_acked.append(rec)
        time.sleep(rng.random() * 0.01)
    if b_refusal and not b_acked:
        # no T exists — classification impossible; availability verdict
        pool["stop"] = True
        dump_server_log("b")
        fail(1, f"healthy-looking B persistently refused every append on "
                f"the live root ({b_refusal}) — successor dysfunction",
             inv=("successor_available", "double-start-cannot-brick-successor"))
    T = b_acked[0]["acked"]
    log(f"B: {len(b_acked)} acked; T fixed"
        + (f"; PERSISTENT REFUSAL mid-schedule: {b_refusal}" if b_refusal
           else ""))

    time.sleep(post_t_tail)                       # A keeps firing past T
    pool["stop"] = True
    for t in tw:
        t.join(timeout=60)

    a_pre = a_pre_serial + [r for r in a_all if r["acked"] and r["acked"] <= T]
    a_pre.sort(key=lambda r: (r["start"] if r["start"] is not None else 1 << 60))
    a_post_acked = [r for r in a_all if r["acked"] and r["acked"] > T]
    a_grey_unacked = [r for r in a_all if not r["acked"] and r["sent"] <= T]
    a_post_unacked = [r for r in a_all if not r["acked"] and r["sent"] > T]
    whys = {}
    for r in a_all:
        if not r["acked"]:
            whys[r["why"]] = whys.get(r["why"], 0) + 1
    last_ok = max((r["acked"] for r in a_all if r["acked"]), default=None)
    log(f"A pool: {len(a_all)} attempts — {len(a_pre) - len(a_pre_serial)} "
        f"acked<=T, {len(a_post_acked)} acked>T, {len(a_grey_unacked)}+"
        f"{len(a_post_unacked)} unacked (reasons: {whys})")
    if last_ok is not None:
        log(f"A's last successful ack at T{last_ok - T:+.3f}s "
            f"(boundary read at T{t_b_ready - T:+.3f}s)")
    for r in a_post_acked[:10]:
        log(f"POST-T A ACK: {r['payload']} [{r['start']}, {r['end']}] "
            f"+{r['acked'] - T:.3f}s after T")

    # witnesses (critic): A's write path provably live (a) inside B's boot
    # window — full round-trip within [spawn, first check-tail 200] — and
    # (b) after T; an ack or an HTTP-level response both count, dead
    # sockets do not
    reached_boot = [r for r in a_all
                    if r["sent"] >= t_b_spawn
                    and (r["resp_at"] or 0) <= t_b_ready
                    and (r["acked"] or (r["why"] or "").startswith("HTTP"))]
    reached_post = [r for r in a_all
                    if r["sent"] >= T
                    and (r["acked"] or (r["why"] or "").startswith("HTTP"))]
    log(f"witness: {len(reached_boot)} attempt(s) through A inside B's boot "
        f"window, {len(reached_post)} after T")

    # verify BEFORE the vacuity exit — and BEFORE any availability
    # verdict: a data red witnessed by this trial outranks both
    if b_refusal:
        try:
            tail_seq = wait_stream_ready(PORT_B, deadline_s=15)
            readback = read_all(PORT_B, tail_seq)
            verify(a_pre, [], a_post_acked, a_grey_unacked, a_post_unacked,
                   b_acked, takeover_tail, readback, tail_seq,
                   allow_pre_truncation=False)
        except SystemExit as e:
            if e.code == 1:
                raise                     # a real data red wins
            log("readback/verify unavailable on the refusing B — "
                "availability verdict follows")
        dump_server_log("b")
        fail(1, f"B persistently refused mid-schedule on the live root "
                f"({b_refusal}) after {len(b_acked)} acks — successor "
                f"dysfunction (data invariants verified where reachable)",
             inv=("successor_available", "double-start-cannot-brick-successor"))
    tail_seq = wait_stream_ready(PORT_B)
    readback = read_all(PORT_B, tail_seq)
    verify(a_pre, [], a_post_acked, a_grey_unacked, a_post_unacked,
           b_acked, takeover_tail, readback, tail_seq,
           allow_pre_truncation=False)
    invariant("successor_available", "double-start-cannot-brick-successor",
              True, f"B served on attempt {b_attempts} and acked all "
                    f"{len(b_acked)} appends on the live root")
    if ((not reached_boot or not reached_post)
            and not os.environ.get("ORACLE_SELFTEST")):
        fail(3, f"anti-vacuity: boot-window={len(reached_boot)} "
                f"post-T={len(reached_post)} — the live overlap never "
                f"contested the fencing window, void (oracle clean on "
                f"everything else)")

    for p in (server_a, server_b):
        try:
            p.kill()
        except OSError:
            pass
    log("VERDICT: GREEN")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "sigstop-takeover"
    if mode == "double-kill-mid-recovery":
        seed = derive_seed()
        main_double_kill(seed, random.Random(seed))
        return
    if mode == "live-overlap-double-start":
        seed = derive_seed()
        main_live_overlap(seed, random.Random(seed))
        return
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
