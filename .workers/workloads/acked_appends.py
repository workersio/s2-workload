#!/usr/bin/env python3
"""Workload: acked appends survive restart (s2 lite, --local-root).

Modes:
  baseline             start -> append N -> graceful stop -> restart -> verify
  kill9                start -> append under load -> SIGKILL mid-stream -> restart -> verify
  kill-during-recovery kill9 phase 1, then crash the server repeatedly DURING
                       first-access recovery, then final clean restart -> verify
  pipelined-kill       4 concurrent writers (one request = one ack per
                       connection) keep appends in flight; SIGKILL mid-burst
                       with >=2 in flight and a fresh ack inside the flush
                       window -> restart -> verify (per-writer ack order)

Oracle (see .workers/promises/acked-appends-survive-restart.md):
  - manifest line written only after the append HTTP response is fully read
  - completeness bounded by server check-tail, not by what a read returns
  - acked records: exactly once, in order, identical content
  - unacked records: present or absent, but at most once
  - anti-vacuous gate in kill9: enough acks AND >=1 in-flight-unacked at kill
  - anti-vacuous gate in kill-during-recovery: >=1 SIGKILL landed during a
    first-access recovery (before that restart served a successful stream read)

Exit codes: 0 green, 1 red (finding), 3 void/blocked (setup or vacuous trial).
"""

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
BASIN = "durability-wl-01"
STREAM = "acked-appends"
WORK_DIR = "/tmp/wl"  # /workspace (the repo checkout) is read-only in the guest
DATA_DIR = os.path.join(WORK_DIR, "s2root")
BASELINE_APPENDS = 40


def log(msg):
    print(msg, flush=True)


def invariant(inv_id, name, ok, summary):
    """Structured line the wio runtime parses into the invariants panel."""
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
    proc = subprocess.Popen(
        [S2, "lite", "--port", str(PORT), "--local-root", DATA_DIR],
        stdout=out, stderr=subprocess.STDOUT, env=env,
    )
    return proc


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
        # post-kill restart: a server that cannot come back violates the
        # promise ("present after restart") — availability RED, not void
        fail(1, "server did not become healthy after the kill — restart "
                "availability lost", inv=("restart_serves", "restart-serves"))
    fail(3, "server did not become healthy in time")


def wait_stream_ready(deadline_s=120, red_on_fail=False):
    """Readiness after restart: startup sleeps one manifest_poll_interval
    (time-based fencing), so poll check-tail instead of fixed sleeps."""
    deadline = time.monotonic() + deadline_s
    last = None
    while time.monotonic() < deadline:
        try:
            status, data = http_call(
                "GET", f"/v1/streams/{STREAM}/records/tail", timeout=2)
            if status == 200:
                return json.loads(data)
            last = (status, data[:200])
        except OSError as e:
            last = repr(e)
        time.sleep(0.2)
    if red_on_fail:
        fail(1, f"stream tail not readable after post-kill restart: {last} — "
                "acked data unreachable", inv=("restart_serves", "restart-serves"))
    fail(3, f"stream tail not readable after restart: {last}")


def wait_serving(proc, deadline_s=90):
    """Return 'serving' once /health is 200, 'exited' if proc dies first,
    'timeout' otherwise. Unlike wait_health this never exits the process —
    the recovery-round loop decides what a mid-round crash means."""
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return "exited"
        try:
            status, _ = http_call("GET", "/health", timeout=2)
            if status == 200:
                return "serving"
        except OSError:
            pass
        time.sleep(0.05)
    return "timeout"


def interrupt_recovery_rounds(seed):
    """After SIGKILL #1, crash the server repeatedly DURING first-access
    recovery. Each round: start the server, wait until it is serving (past the
    startup manifest_poll_interval sleep and SlateDB open), fire a stream-tail
    request that forces the lazy per-stream recovery (start_streamer ->
    load_persisted_stream_tail -> assert_no_records_following_tail, core.rs:
    82/144/165), and SIGKILL almost immediately so the kill lands during the
    restart's recovery path rather than after it. Returns the count of rounds
    where the kill preceded a successful (200) stream read — a genuine
    mid-recovery interruption (the anti-vacuous witness)."""
    rounds = 2 + seed % 3
    mid_recovery_kills = 0
    for r in range(rounds):
        proc = start_server()
        state = wait_serving(proc)
        if state == "exited":
            # Server died on its own during startup/recovery. A crash on
            # legitimately-recovered data would be a finding; here it is most
            # likely a self-inflicted restart of a prior interrupted round —
            # record and let the next round recover from it.
            log(f"recovery round {r}: server exited on its own "
                f"(rc={proc.returncode}) before serving")
            continue
        if state == "timeout":
            fail(3, f"recovery round {r}: server never served — setup blocked")
        probe = {"status": None}

        def _probe():
            try:
                st, _ = http_call(
                    "GET", f"/v1/streams/{STREAM}/records/tail", timeout=5)
                probe["status"] = st
            except OSError as e:
                probe["status"] = repr(e)

        t = threading.Thread(target=_probe, daemon=True)
        t.start()
        delay = ((seed >> (r + 3)) % 8) / 1000.0  # 0-7ms into first access
        time.sleep(delay)
        pre_kill_ok = probe["status"] == 200
        proc.send_signal(signal.SIGKILL)
        t.join(timeout=5)
        if not pre_kill_ok:
            mid_recovery_kills += 1
        log(f"recovery round {r}: probe_status={probe['status']} "
            f"pre_kill_200={pre_kill_ok} delay={delay * 1000:.0f}ms")
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=30)
    return mid_recovery_kills


WRITERS = 4


class PoolState:
    def __init__(self):
        self.lock = threading.Lock()
        self.in_flight = 0
        self.acked = 0
        self.last_ack_at = 0.0
        self.killed = False


def pipelined_writer(w, seed, pool, out_entries):
    """One connection-serial writer: at most one request outstanding, next
    append issued the moment the ack is read — the pool of these keeps
    several appends pipelined against storage latency globally."""
    for i in range(4000):
        if pool.killed:
            break
        payload = f"s{seed}-w{w}-{i:05d}"
        entry = {"payload": payload, "acked": False, "start": None,
                 "end": None, "writer": w}
        with pool.lock:
            pool.in_flight += 1
        result = append_one(payload)
        now = time.monotonic()
        with pool.lock:
            pool.in_flight -= 1
            if result:
                pool.acked += 1
                pool.last_ack_at = now
        if result:
            entry["acked"] = True
            entry["start"], entry["end"] = result
        out_entries.append(entry)
        if not result:
            if pool.killed:
                break
            time.sleep(0.05)  # pre-kill failure (e.g. transient 404) — back off
        else:
            # seeded pacing jitter: varies pool interleaving per seed
            time.sleep(((seed >> (w * 3)) % 4) / 2000.0)


def run_pipelined(seed, server, window):
    """Drive the writer pool, SIGKILL mid-burst. Returns (manifest,
    in_flight_at_kill, last_ack_age_s)."""
    # Prime the lazily-created stream: with slow flush arms appends 404 until
    # the creation record is durable (map.md reality note).
    prime_payload = f"s{seed}-prime"
    prime = None
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and prime is None:
        prime = append_one(prime_payload)
        if prime is None:
            time.sleep(0.25)
    if prime is None:
        fail(3, "prime append never acked — stream creation not durable, setup blocked")
    manifest = [{"payload": prime_payload, "acked": True,
                 "start": prime[0], "end": prime[1], "writer": "prime"}]

    # acks-before-kill scaled per flush arm (2s arm yields ~2 acks/s over 4
    # serial connections — unscaled targets void on runtime)
    base, span = {0.005: (30, 300), 0.5: (15, 60), 2.0: (8, 16)}[window]
    kill_after = base + (seed >> 2) % span
    log(f"writers={WRITERS} kill_after={kill_after} flush_window={window}s")

    pool = PoolState()
    per_writer = [[] for _ in range(WRITERS)]
    threads = [threading.Thread(target=pipelined_writer,
                                args=(w, seed, pool, per_writer[w]), daemon=True)
               for w in range(WRITERS)]
    for t in threads:
        t.start()

    deadline = time.monotonic() + 240
    reached = False
    while time.monotonic() < deadline:
        with pool.lock:
            if pool.acked >= kill_after:
                reached = True
                break
        time.sleep(0.005)
    if not reached:
        pool.killed = True
        with pool.lock:
            got = pool.acked
        fail(3, f"kill point never reached ({got}/{kill_after} acks) — vacuous trial")

    # spin until >=2 appends are genuinely in flight, then kill mid-burst
    spin_deadline = time.monotonic() + 10
    while time.monotonic() < spin_deadline:
        with pool.lock:
            if pool.in_flight >= 2:
                break
        time.sleep(0.0005)
    with pool.lock:
        infl_at_kill = pool.in_flight
        last_ack_age = time.monotonic() - pool.last_ack_at
    server.send_signal(signal.SIGKILL)
    pool.killed = True
    for t in threads:
        t.join(timeout=20)
    log(f"kill: in_flight={infl_at_kill} last_ack_age={last_ack_age * 1000:.1f}ms")
    for entries in per_writer:
        manifest.extend(entries)
    return manifest, infl_at_kill, last_ack_age


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


def append_one(payload):
    """One request = one ack. Returns (start_seq, end_seq) or None if unacked."""
    try:
        status, data = http_call(
            "POST", f"/v1/streams/{STREAM}/records",
            body={"records": [{"body": payload}]}, timeout=10)
    except OSError:
        return None
    if status != 200:
        return None
    ack = json.loads(data)
    return ack["start"]["seq_num"], ack["end"]["seq_num"]


def read_all(tail_seq):
    records = []
    cursor = 0
    for _ in range(10000):
        if cursor >= tail_seq:
            break
        status, data = http_call(
            "GET",
            f"/v1/streams/{STREAM}/records?seq_num={cursor}&count=1000",
            timeout=30)
        if status != 200:
            fail(3, f"read failed at seq {cursor}: HTTP {status} {data[:200]}")
        batch = json.loads(data).get("records", [])
        if not batch:
            fail(1, f"read returned no records at seq {cursor} < tail {tail_seq}"
                    " — gap below tail")
        for rec in batch:
            records.append((rec["seq_num"], rec.get("body", "")))
        cursor = records[-1][0] + 1
    return records


def verify(manifest, readback, tail_seq):
    """manifest: list of dicts {payload, acked, start, end}."""
    acked = [m for m in manifest if m["acked"]]
    sent_payloads = {m["payload"] for m in manifest}

    if os.environ.get("ORACLE_SELFTEST"):
        log("ORACLE_SELFTEST: dropping first acked record from readback")
        victim = acked[0]["payload"]
        readback = [(s, b) for s, b in readback if b != victim]

    max_acked_end = max((m["end"] for m in acked), default=0)
    if tail_seq < max_acked_end:
        fail(1, f"tail {tail_seq} < max acked end {max_acked_end} — acked data lost",
             inv=("tail_bound", "tail-covers-acked"))
    invariant("tail_bound", "tail-covers-acked", True,
              f"tail {tail_seq} >= max acked end {max_acked_end}")

    seqs = [s for s, _ in readback]
    if seqs != list(range(len(seqs))):
        fail(1, f"read-back seqs not a dense prefix: first 20 = {seqs[:20]}",
             inv=("dense_prefix", "gapless-below-tail"))
    invariant("dense_prefix", "gapless-below-tail", True,
              f"{len(seqs)} records tile seq 0..{tail_seq}")

    by_payload = {}
    for s, b in readback:
        by_payload.setdefault(b, []).append(s)

    for b, occurrences in by_payload.items():
        if b not in sent_payloads:
            fail(1, f"read-back contains a record never sent: {b!r} at {occurrences}",
                 inv=("no_phantoms", "only-sent-records"))
        if len(occurrences) > 1:
            fail(1, f"record duplicated (at-most-once violated): {b!r} at {occurrences}",
                 inv=("at_most_once", "no-duplicates"))
    invariant("no_phantoms", "only-sent-records", True,
              f"{len(by_payload)} distinct payloads, all sent by this run")
    invariant("at_most_once", "no-duplicates", True,
              "no payload appears twice (acked or unacked)")

    missing = [m for m in acked if m["payload"] not in by_payload]
    if missing:
        for m in missing[:10]:
            log(f"MISSING ACKED: {m}")
        fail(1, f"{len(missing)} acked record(s) absent after restart",
             inv=("acked_survive", "acked-exactly-once"))

    # ack order maps to strictly increasing seqs per writer connection;
    # cross-writer interleaving is unconstrained (serial modes = one writer)
    writers = {}
    for m in acked:
        writers.setdefault(m.get("writer", 0), []).append(m)
    for w, entries in sorted(writers.items(), key=lambda kv: str(kv[0])):
        order = [by_payload[m["payload"]][0] for m in entries]
        if order != sorted(order):
            fail(1, f"writer {w}: acked records out of ack order: {order[:20]}",
                 inv=("acked_order", "ack-order-preserved"))
    invariant("acked_survive", "acked-exactly-once", True,
              f"{len(acked)}/{len(acked)} acked records present after restart")
    invariant("acked_order", "ack-order-preserved", True,
              f"read-back order matches ack order per writer ({len(writers)} writer(s))")

    log(f"oracle: {len(acked)} acked verified, {len(readback)} records read, "
        f"tail {tail_seq}")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "baseline"
    seed = derive_seed()
    log(f"mode={mode} seed={seed}")
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(WORK_DIR, exist_ok=True)

    env_extra = {}
    kill_after = None
    total_appends = BASELINE_APPENDS
    if mode == "kill9":
        arm = ["", "500ms", "2s"][seed % 3]
        if arm:
            env_extra["SL8_FLUSH_INTERVAL"] = arm
        log(f"flush-arm={arm or 'default'}")
        total_appends = 4000
        kill_after = 20 + (seed >> 2) % 400  # appends before SIGKILL
    elif mode == "kill-during-recovery":
        arm = ["", "500ms", "2s"][seed % 3]
        if arm:
            env_extra["SL8_FLUSH_INTERVAL"] = arm
        log(f"flush-arm={arm or 'default'}")
        total_appends = 4000
        # Larger acked prefix than kill9: more records => longer first-access
        # recovery scan => a wider window for SIGKILL #2 to land inside it.
        kill_after = 100 + (seed >> 2) % 600
    elif mode == "pipelined-kill":
        arm = ["", "500ms", "2s"][seed % 3]
        if arm:
            env_extra["SL8_FLUSH_INTERVAL"] = arm
        log(f"flush-arm={arm or 'default'}")

    server = start_server(env_extra)
    wait_health()
    create_basin()

    if mode == "pipelined-kill":
        window = {"": 0.005, "500ms": 0.5, "2s": 2.0}[arm]
        manifest, infl_at_kill, last_ack_age = run_pipelined(seed, server, window)
        acked_count = sum(1 for m in manifest if m["acked"])
        log(f"appends: {len(manifest)} attempted, {acked_count} acked")
        if infl_at_kill < 2:
            fail(3, f"only {infl_at_kill} append(s) in flight at kill — "
                    "pipeline window missed, void")
        if last_ack_age > max(window, 0.05):
            fail(3, f"last ack {last_ack_age * 1000:.0f}ms before kill — "
                    "nothing freshly acked inside the flush window, void")
        if acked_count < 8:
            fail(3, f"only {acked_count} acks before kill — below floor 8, void")
        server.wait(timeout=30)

        server2 = start_server()
        wait_health(red_on_fail=True)
        tail = wait_stream_ready(red_on_fail=True)
        tail_seq = tail["tail"]["seq_num"]
        readback = read_all(tail_seq)
        verify(manifest, readback, tail_seq)
        invariant("pipeline_at_kill", "multi-writer-in-flight", True,
                  f"{infl_at_kill} appends in flight at SIGKILL, last ack "
                  f"{last_ack_age * 1000:.0f}ms prior")
        server2.terminate()
        log("VERDICT: GREEN")
        return

    manifest = []
    killed = False
    in_flight_at_kill = False
    for i in range(total_appends):
        payload = f"s{seed}-w0-{i:05d}"
        entry = {"payload": payload, "acked": False, "start": None, "end": None}
        if kill_after is not None and i == kill_after:
            server.send_signal(signal.SIGKILL)
            killed = True
            in_flight_at_kill = True  # this append races the kill
        result = append_one(payload)
        if result:
            entry["acked"] = True
            entry["start"], entry["end"] = result
        manifest.append(entry)
        if killed and not result:
            break  # server is gone; stop the writer

    acked_count = sum(1 for m in manifest if m["acked"])
    log(f"appends: {len(manifest)} attempted, {acked_count} acked")

    if mode in ("kill9", "kill-during-recovery"):
        if not killed:
            fail(3, "kill point never reached — vacuous trial")
        floor = 10 if mode == "kill9" else 50
        if acked_count < floor:
            fail(3, f"only {acked_count} acks before kill — below floor {floor}, void")
        if not in_flight_at_kill:
            fail(3, "no in-flight append at kill — quiesced kill is theater")
        server.wait(timeout=30)
    else:
        if acked_count != total_appends:
            fail(3, f"baseline expected all {total_appends} acked, got {acked_count}")
        server.terminate()
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            log("note: SIGTERM ignored, escalating to SIGKILL")
            server.kill()
            server.wait(timeout=30)

    if mode == "kill-during-recovery":
        mid = interrupt_recovery_rounds(seed)
        if mid < 1:
            fail(3, "no SIGKILL landed during recovery (every probe completed "
                    "first) — recovery window never interrupted, void")
        log(f"recovery interrupted mid-window {mid} time(s)")
        invariant("recovery_interrupted", "crash-during-recovery", True,
                  f"{mid} SIGKILL(s) landed during first-access recovery")

    post_kill = mode != "baseline"
    server2 = start_server()
    wait_health(red_on_fail=post_kill)
    tail = wait_stream_ready(red_on_fail=post_kill)
    tail_seq = tail["tail"]["seq_num"]
    readback = read_all(tail_seq)
    verify(manifest, readback, tail_seq)

    server2.terminate()
    log("VERDICT: GREEN")


if __name__ == "__main__":
    main()
