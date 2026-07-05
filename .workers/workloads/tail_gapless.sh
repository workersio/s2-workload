#!/bin/sh
# Workload: tail is gapless and monotonic (s2 lite, --local-root).
#
# Modes:
#   baseline  concurrent writers, no faults; ack'd ranges must tile [0, tail)
#   restart   SIGKILL + restart the server between appender waves; sequence
#             assignment must resume exactly at the persisted tail — the
#             union of ack'd ranges across all waves must still tile
#             [0, tail): a seq assigned twice across a restart shows up as
#             an overlap, a hole across the boundary as a gap
#   straddle-at-kill  SIGKILL *during* a wave (writers have in-flight unacked
#             appends AT the kill), restart, then a post-restart wave. Acked
#             ranges must never overlap or double-assign a seq across the
#             boundary; read-back [0, tail) is dense with no duplicated body;
#             below-tail gaps must reconcile to in-flight-unacked payloads the
#             run sent; a crash on restart (assert guard) is a finding.
#
# Oracle (see .workers/promises/tail-is-gapless-and-monotonic.md):
#   - range_tiling: all ack'd (start,end) ranges, sorted, tile [0, tail)
#     exactly — no overlap, no gap
#   - readback_dense: full read from 0 yields one record per seq 0..tail
#   - content_ownership: records inside each ack'd range match exactly the
#     payloads of the batch that owned that range, in order
#
# Ack precision: raw HTTP, one request = one ack (CLI stderr acks are
# batched/deduped and unusable). One log file per writer under /tmp.
#
# Self-contained: sh wrapper + embedded python3, because injection layers a
# single file onto the prepared base image.
#
# Exit codes: 0 green, 1 red (finding), 3 void/blocked (setup or vacuous).
MODE="${1:-baseline}"
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
PORT = 8080
BASIN = "durability-wl-02"
STREAM = "tail-gapless"
WORK_DIR = "/tmp/wl-tail"  # /workspace (the repo checkout) is read-only
DATA_DIR = os.path.join(WORK_DIR, "s2root")


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
    return subprocess.Popen(
        [S2, "lite", "--port", str(PORT), "--local-root", DATA_DIR],
        stdout=out, stderr=subprocess.STDOUT, env=env,
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


def append_batch(payloads):
    """One request = one ack. Returns (start, end) seq nums or None."""
    try:
        status, data = http_call(
            "POST", f"/v1/streams/{STREAM}/records",
            body={"records": [{"body": p} for p in payloads]}, timeout=15)
    except OSError:
        return None
    if status != 200:
        return None
    ack = json.loads(data)
    return ack["start"]["seq_num"], ack["end"]["seq_num"]


def get_tail():
    status, data = http_call(
        "GET", f"/v1/streams/{STREAM}/records/tail", timeout=10)
    if status != 200:
        fail(3, f"check-tail failed: HTTP {status} {data[:200]}")
    return json.loads(data)["tail"]["seq_num"]


def wait_stream_ready(deadline_s=120):
    """Readiness after restart: startup sleeps one manifest_poll_interval
    (time-based fencing) — poll check-tail, never fixed sleeps."""
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
                    " — gap below tail",
                 inv=("readback_dense", "one-record-per-seq"))
        for rec in batch:
            records.append((rec["seq_num"], rec.get("body", "")))
        cursor = records[-1][0] + 1
    return records


def writer(wave, wid, seed, n_appends, acks, errors):
    """acks: shared list of dicts (appended under lock); one log per writer."""
    rng = random.Random(seed ^ (wid * 0x9E3779B9) ^ (wave * 0x85EBCA6B))
    path = os.path.join(WORK_DIR, f"writer-v{wave}-{wid}.log")
    with open(path, "w") as f:
        for k in range(n_appends):
            size = 1 + rng.randrange(3)
            payloads = [f"s{seed}-v{wave}-w{wid}-a{k:04d}-r{j}"
                        for j in range(size)]
            result = append_batch(payloads)
            if result is None:
                errors.append((wid, k))
                f.write(f"{k} UNACKED size={size}\n")
                return  # baseline is fault-free; one refusal voids the trial
            start, end = result
            acks.append(
                {"writer": wid, "k": k, "payloads": payloads,
                 "start": start, "end": end})
            f.write(f"{k} {start} {end} {' '.join(payloads)}\n")
            if rng.random() < 0.3:
                time.sleep(rng.random() * 0.002)


def normalize_ranges(acks):
    """Half-open [start, start+len). Ack `end` may be inclusive or exclusive
    depending on API convention — infer from the first ack, then require
    every ack to be self-consistent."""
    first = acks[0]
    span = first["end"] - first["start"]
    n = len(first["payloads"])
    if span == n:
        exclusive = True
    elif span == n - 1:
        exclusive = False
    else:
        fail(3, f"cannot infer ack end convention: batch of {n} acked as "
                f"[{first['start']}, {first['end']}]")
    for a in acks:
        expect = a["start"] + len(a["payloads"]) - (0 if exclusive else 1)
        if a["end"] != expect:
            fail(1, f"ack range width mismatch (writer {a['writer']} append "
                    f"{a['k']}): batch of {len(a['payloads'])} acked as "
                    f"[{a['start']}, {a['end']}], convention "
                    f"{'exclusive' if exclusive else 'inclusive'}",
                 inv=("range_tiling", "ranges-tile-tail"))
        a["hi"] = a["start"] + len(a["payloads"])  # half-open upper bound
    log(f"ack end convention: {'exclusive' if exclusive else 'inclusive'}")
    return acks


def verify(acks, tail_seq, n_writers):
    acks = normalize_ranges(acks)
    ordered = sorted(acks, key=lambda a: a["start"])

    if os.environ.get("ORACLE_SELFTEST"):
        victim = ordered[len(ordered) // 2]
        log(f"ORACLE_SELFTEST: dropping ack [{victim['start']}, "
            f"{victim['hi']}) of writer {victim['writer']} from manifest")
        ordered = [a for a in ordered if a is not victim]

    # 1. range_tiling — sorted ranges tile [0, tail) with no overlap, no gap
    cursor = 0
    for a in ordered:
        if a["start"] != cursor:
            kind = "overlap" if a["start"] < cursor else "gap"
            fail(1, f"{kind} at seq {min(cursor, a['start'])}: expected next "
                    f"range to start at {cursor}, got [{a['start']}, "
                    f"{a['hi']}) from writer {a['writer']}",
                 inv=("range_tiling", "ranges-tile-tail"))
        cursor = a["hi"]
    if cursor != tail_seq:
        fail(1, f"ranges tile [0, {cursor}) but tail is {tail_seq}",
             inv=("range_tiling", "ranges-tile-tail"))
    invariant("range_tiling", "ranges-tile-tail", True,
              f"{len(ordered)} acked ranges from {n_writers} writers tile "
              f"[0, {tail_seq}) exactly")

    # interleaving floor — all-contiguous writers means the trial proved
    # nothing about concurrent assignment
    switches = sum(1 for prev, cur in zip(ordered, ordered[1:])
                   if prev["writer"] != cur["writer"])
    if switches < n_writers:
        fail(3, f"only {switches} writer interleavings observed across "
                f"{len(ordered)} ranges — vacuous trial")
    log(f"interleaving: {switches} writer switches across {len(ordered)} ranges")

    # 2. readback_dense — one record per seq, exactly 0..tail
    readback = read_all(tail_seq)
    seqs = [s for s, _ in readback]
    if seqs != list(range(tail_seq)):
        fail(1, f"read-back seqs not exactly 0..{tail_seq}: "
                f"{len(seqs)} records, first 20 = {seqs[:20]}",
             inv=("readback_dense", "one-record-per-seq"))
    invariant("readback_dense", "one-record-per-seq", True,
              f"{len(seqs)} records, one per seq in [0, {tail_seq})")

    # 3. content_ownership — each range holds its own batch, in order
    bodies = [b for _, b in readback]
    for a in ordered:
        got = bodies[a["start"]:a["hi"]]
        if got != a["payloads"]:
            fail(1, f"range [{a['start']}, {a['hi']}) owned by writer "
                    f"{a['writer']} holds {got[:5]!r}, expected "
                    f"{a['payloads'][:5]!r}",
                 inv=("content_ownership", "range-content-matches-owner"))
    invariant("content_ownership", "range-content-matches-owner", True,
              f"all {len(ordered)} ranges hold exactly their owner's batch")


def run_wave(wave, seed, n_writers, n_appends, acks):
    errors = []
    threads = [
        threading.Thread(target=writer,
                         args=(wave, w, seed, n_appends, acks, errors))
        for w in range(n_writers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        fail(3, f"{len(errors)} append(s) refused mid-wave {wave} (writers "
                f"are fault-free; first: writer {errors[0][0]} append "
                f"{errors[0][1]}) — setup")


def writer_straddle(wave, wid, seed, n_appends, records, inflight, lock, stop):
    """Straddle writer: records EVERY sent append with its ack status (acked
    with range, or unacked/in-flight). Unlike the baseline writer, an unacked
    append is expected here (the mid-wave SIGKILL), not a void — record it and
    stop. `inflight` is a 1-cell list counting appends currently outstanding,
    read by the killer to prove writers were mid-flight."""
    rng = random.Random(seed ^ (wid * 0x9E3779B9) ^ (wave * 0x85EBCA6B))
    path = os.path.join(WORK_DIR, f"writer-v{wave}-{wid}.log")
    with open(path, "w") as f:
        for k in range(n_appends):
            if stop.is_set():
                return
            size = 1 + rng.randrange(3)
            payloads = [f"s{seed}-v{wave}-w{wid}-a{k:04d}-r{j}"
                        for j in range(size)]
            with lock:
                inflight[0] += 1
            result = append_batch(payloads)
            with lock:
                inflight[0] -= 1
                if result is None:
                    records.append(
                        {"writer": wid, "wave": wave, "k": k,
                         "payloads": payloads, "acked": False,
                         "start": None, "end": None})
                else:
                    records.append(
                        {"writer": wid, "wave": wave, "k": k,
                         "payloads": payloads, "acked": True,
                         "start": result[0], "end": result[1]})
            if result is None:
                f.write(f"{k} UNACKED size={size} {' '.join(payloads)}\n")
                return
            f.write(f"{k} {result[0]} {result[1]} {' '.join(payloads)}\n")
            if rng.random() < 0.3:
                time.sleep(rng.random() * 0.002)


def run_wave_straddle(wave, seed, n_writers, n_appends, records,
                      killer_arg=None):
    """Run one concurrent wave. If killer_arg=(server, delay) is given, fire
    SIGKILL `delay`s into the wave while writers are mid-flight and return the
    inflight count sampled at the kill. Otherwise run to completion."""
    lock = threading.Lock()
    inflight = [0]
    stop = threading.Event()
    box = {"inflight_at_kill": None}
    threads = [
        threading.Thread(target=writer_straddle,
                         args=(wave, w, seed, n_appends, records,
                               inflight, lock, stop))
        for w in range(n_writers)
    ]

    def killer():
        server, delay = killer_arg
        time.sleep(delay)
        with lock:
            box["inflight_at_kill"] = inflight[0]
        server.send_signal(signal.SIGKILL)
        stop.set()

    for t in threads:
        t.start()
    kt = None
    if killer_arg is not None:
        kt = threading.Thread(target=killer)
        kt.start()
    if killer_arg is None:
        # unkilled wave: writers self-terminate at n_appends
        stop_deadline = time.monotonic() + 120
        for t in threads:
            t.join(timeout=max(0.1, stop_deadline - time.monotonic()))
    for t in threads:
        t.join(timeout=30)
    if kt is not None:
        kt.join(timeout=10)
    return box["inflight_at_kill"]


def wait_ready_or_crash(server, deadline_s=120):
    """After restart, distinguish (a) 'ready' with the recovered tail, (b)
    'crashed' — the process EXITED before serving (assert_no_records_following_tail
    firing is a finding), (c) 'timeout'. Never exits the process itself."""
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if server.poll() is not None:
            return ("crashed", server.returncode)
        try:
            status, data = http_call(
                "GET", f"/v1/streams/{STREAM}/records/tail", timeout=2)
            if status == 200:
                return ("ready", json.loads(data)["tail"]["seq_num"])
        except OSError:
            pass
        time.sleep(0.2)
    return ("timeout", None)


def verify_straddle(records, tail_seq, inflight_at_kill):
    acked = [r for r in records if r["acked"]]
    unacked = [r for r in records if not r["acked"]]
    if not acked:
        fail(3, "no acked appends at all — setup")

    # ack end convention (exclusive vs inclusive), inferred + self-consistent
    first = acked[0]
    span = first["end"] - first["start"]
    n = len(first["payloads"])
    if span == n:
        exclusive = True
    elif span == n - 1:
        exclusive = False
    else:
        fail(3, f"cannot infer ack end convention: batch of {n} acked as "
                f"[{first['start']}, {first['end']}]")
    for a in acked:
        expect = a["start"] + len(a["payloads"]) - (0 if exclusive else 1)
        if a["end"] != expect:
            fail(1, f"ack range width mismatch (writer {a['writer']} wave "
                    f"{a['wave']} append {a['k']}): [{a['start']}, {a['end']}]",
                 inv=("no_double_assign", "no-overlap-monotonic"))
        a["hi"] = a["start"] + len(a["payloads"])  # half-open upper bound

    ordered = sorted(acked, key=lambda a: a["start"])

    if os.environ.get("ORACLE_SELFTEST") and len(ordered) >= 2:
        # Plant a double-assignment: force the 2nd range to overlap the 1st by
        # one seq (share its last seq), the exact corruption inv 2 must catch.
        a0, victim = ordered[0], ordered[1]
        width = victim["hi"] - victim["start"]
        victim["start"] = max(0, a0["hi"] - 1)
        victim["hi"] = victim["start"] + width
        log(f"ORACLE_SELFTEST: forcing writer {victim['writer']} range to "
            f"overlap writer {a0['writer']} at seq {victim['start']}")
        ordered = sorted(acked, key=lambda a: a["start"])

    # inv 1 + 2: no overlap, monotonic, no acked range at/beyond tail
    covered = {}  # seq -> owning acked record
    prev_hi = 0
    for a in ordered:
        if a["start"] < prev_hi:
            fail(1, f"OVERLAP / double-assignment: writer {a['writer']} wave "
                    f"{a['wave']} range [{a['start']}, {a['hi']}) starts below "
                    f"prev range end {prev_hi} — a seq is owned by two writers",
                 inv=("no_double_assign", "no-overlap-monotonic"))
        if a["hi"] > tail_seq:
            fail(1, f"acked range [{a['start']}, {a['hi']}) extends beyond tail "
                    f"{tail_seq} — acked data above the recovered tail",
                 inv=("no_double_assign", "no-overlap-monotonic"))
        for s in range(a["start"], a["hi"]):
            covered[s] = a
        prev_hi = a["hi"]
    invariant("no_double_assign", "no-overlap-monotonic", True,
              f"{len(ordered)} acked ranges, no overlap, none beyond tail "
              f"{tail_seq}; no seq owned by two writers")

    # inv 3: read-back [0, tail) dense, one record per seq, no dup content
    readback = read_all(tail_seq)
    seqs = [s for s, _ in readback]
    if seqs != list(range(tail_seq)):
        fail(1, f"read-back seqs not dense 0..{tail_seq}: {len(seqs)} records, "
                f"first 20 = {seqs[:20]}",
             inv=("readback_dense", "one-record-per-seq"))
    bodies = [b for _, b in readback]
    seen = {}
    for s, b in readback:
        if b in seen:
            fail(1, f"payload {b!r} appears at seq {seen[b]} AND {s} — recovery "
                    f"double-applied a record",
                 inv=("readback_dense", "one-record-per-seq"))
        seen[b] = s
    invariant("readback_dense", "one-record-per-seq", True,
              f"{len(seqs)} records dense in [0, {tail_seq}), no duplicated body")

    # inv 4: content ownership for acked ranges
    for a in ordered:
        got = bodies[a["start"]:a["hi"]]
        if got != a["payloads"]:
            fail(1, f"range [{a['start']}, {a['hi']}) owned by writer "
                    f"{a['writer']} holds {got[:4]!r}, expected "
                    f"{a['payloads'][:4]!r}",
                 inv=("content_ownership", "range-content-matches-owner"))
    invariant("content_ownership", "range-content-matches-owner", True,
              f"all {len(ordered)} acked ranges hold their owner's batch")

    # inv 5: gap reconciliation — every below-tail seq not covered by an acked
    # range must hold a payload this run sent as an in-flight-unacked append
    unacked_payloads = set()
    for r in unacked:
        unacked_payloads.update(r["payloads"])
    gaps = [s for s in range(tail_seq) if s not in covered]
    for s in gaps:
        b = bodies[s]
        if b not in unacked_payloads:
            fail(1, f"gap seq {s} holds {b!r}, not covered by any acked range "
                    f"and not a known in-flight-unacked payload — recovery "
                    f"invented or misplaced a record",
                 inv=("gap_reconciled", "gaps-are-inflight-payloads"))
    invariant("gap_reconciled", "gaps-are-inflight-payloads", True,
              f"{len(gaps)} below-tail gap seq(s) all reconcile to in-flight "
              f"unacked payloads sent by this run")

    # inv 6: anti-vacuous — a real straddle happened
    if len(unacked) < 1 and (inflight_at_kill or 0) < 1:
        fail(3, f"no in-flight-unacked append at the kill (unacked={len(unacked)}, "
                f"inflight_at_kill={inflight_at_kill}) — quiesced boundary, void")
    invariant("straddle_witness", "inflight-unacked-at-kill", True,
              f"{len(unacked)} unacked in-flight append(s) straddled the kill "
              f"(inflight sampled at kill: {inflight_at_kill})")


def run_straddle(seed):
    n_writers = 3 + seed % 3
    # Appends over raw HTTP are synchronous: the request blocks until the batch
    # is durable (ack released after durable_seq covers it). The straddle needs
    # appends sent-but-unacked AT the kill; with the default 5ms flush that
    # window is ~ms and never catches. Stretch SL8_FLUSH_INTERVAL so each append
    # blocks ~<interval>: older appends flushed+acked (a real prefix), the ones
    # blocking inside the last interval are in-flight-unacked at the kill.
    arm = ["500ms", "500ms", "2s"][seed % 3]  # bias to the faster arm
    env_extra = {"SL8_FLUSH_INTERVAL": arm}
    n_appends = 400  # more than any writer finishes pre-kill; writers get cut
    post_appends = 15 + (seed >> 7) % 20  # wave 1 runs to completion, keep small
    if arm == "2s":
        kill_delay = 5.0 + ((seed >> 5) % 3000) / 1000.0   # 5-8s
    else:
        kill_delay = 2.0 + ((seed >> 5) % 2500) / 1000.0   # 2-4.5s
    log(f"mode=straddle-at-kill seed={seed} writers={n_writers} "
        f"flush-arm={arm} kill_delay={kill_delay * 1000:.0f}ms "
        f"post_appends={post_appends}")
    os.makedirs(DATA_DIR, exist_ok=True)

    server = start_server(env_extra)
    wait_health()
    create_basin()
    primer = append_batch([f"s{seed}-primer"])
    if primer is None:
        fail(3, "primer append refused — setup")
    records = [{"writer": -1, "wave": -1, "k": 0, "payloads": [f"s{seed}-primer"],
                "acked": True, "start": primer[0], "end": primer[1]}]

    # wave 0: writers streaming; SIGKILL mid-wave
    inflight_at_kill = run_wave_straddle(
        0, seed, n_writers, n_appends, records, killer_arg=(server, kill_delay))
    try:
        server.wait(timeout=30)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait(timeout=30)
    acked0 = [r for r in records if r["wave"] == 0 and r["acked"]]
    unacked0 = [r for r in records if r["wave"] == 0 and not r["acked"]]
    log(f"wave 0: {len(acked0)} acked, {len(unacked0)} unacked-in-flight, "
        f"inflight_at_kill={inflight_at_kill}")
    if len(acked0) < 10:
        fail(3, f"only {len(acked0)} acks before kill — below floor, void")
    if len(unacked0) < 1:
        fail(3, f"no in-flight-unacked append at the kill (inflight sampled "
                f"{inflight_at_kill}, but all completed) — quiesced boundary, "
                f"void; a longer flush arm / later kill would straddle")

    # restart — a crash here (assert_no_records_following_tail) is a FINDING
    server = start_server(env_extra)
    state, val = wait_ready_or_crash(server)
    if state == "crashed":
        fail(1, f"server CRASHED on restart (rc={val}) — recovery guard "
                f"assert_no_records_following_tail likely fired on a "
                f"legitimately-durable stream",
             inv=("recovery_no_crash", "restart-recovers-cleanly"))
    if state == "timeout":
        fail(3, "server never served after restart — setup/timeout")
    invariant("recovery_no_crash", "restart-recovers-cleanly", True,
              f"restart recovered, tail={val}")

    # wave 1: post-restart writers — force new assignment above recovered tail,
    # the collision surface with any pre-kill in-flight persisted seq
    run_wave_straddle(1, seed, n_writers, post_appends, records)
    acked1 = [r for r in records if r["wave"] == 1 and r["acked"]]
    log(f"wave 1 (post-restart): {len(acked1)} acked")

    tail_seq = get_tail()
    verify_straddle(records, tail_seq, inflight_at_kill)
    server.terminate()
    log("VERDICT: GREEN")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "baseline"
    if mode == "straddle-at-kill":
        run_straddle(derive_seed())
        return
    if mode not in ("baseline", "restart"):
        fail(3, f"mode {mode!r} not implemented")
    seed = derive_seed()
    n_writers = 3 + seed % 3
    if mode == "restart":
        n_waves = 2 + (seed >> 16) % 3
        n_appends = 8 + (seed >> 3) % 9   # per writer per wave
    else:
        n_waves = 1
        n_appends = 25 + (seed >> 3) % 26  # per writer
    log(f"mode={mode} seed={seed} writers={n_writers} "
        f"appends/writer={n_appends} waves={n_waves}")
    os.makedirs(DATA_DIR, exist_ok=True)

    server = start_server()
    wait_health()
    create_basin()
    # prime the stream so concurrent first appends don't race auto-creation
    primer = append_batch([f"s{seed}-primer"])
    if primer is None:
        fail(3, "primer append refused — setup")
    acks = [{"writer": -1, "k": 0, "payloads": [f"s{seed}-primer"],
             "start": primer[0], "end": primer[1]}]

    for wave in range(n_waves):
        if wave > 0:
            # kill hard between waves; assignment must resume at the
            # persisted tail after recovery
            server.send_signal(signal.SIGKILL)
            server.wait(timeout=30)
            server = start_server()
            wait_health()
            tail_r = wait_stream_ready()
            hi = max(a["start"] + len(a["payloads"]) for a in acks)
            log(f"restart {wave}: tail after recovery = {tail_r}, "
                f"max acked hi = {hi}")
            if tail_r < hi:
                log(f"TAIL REGRESSION: recovered tail {tail_r} < acked hi "
                    f"{hi} — expecting the tiling oracle to go red")
        run_wave(wave, seed, n_writers, n_appends, acks)
        log(f"wave {wave}: cumulative acks = {len(acks)}")

    expected = 1 + n_waves * n_writers * n_appends
    if len(acks) != expected:
        fail(3, f"expected {expected} acks, got {len(acks)} — setup")
    log(f"appends: {len(acks)} acked across {n_writers} writers x "
        f"{n_waves} wave(s)")

    tail_seq = get_tail()
    verify(acks, tail_seq, n_writers)

    server.terminate()
    log("VERDICT: GREEN")


main()
PYEOF
