#!/bin/sh
# Workload: tail is gapless and monotonic (s2 lite, --local-root).
#
# Modes:
#   baseline  concurrent writers, no faults; ack'd ranges must tile [0, tail)
#   restart   (planned rung — not implemented yet; exits VOID)
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


def writer(wid, seed, n_appends, acks, errors):
    """acks: shared list of dicts (appended under lock); one log per writer."""
    rng = random.Random(seed ^ (wid * 0x9E3779B9))
    path = os.path.join(WORK_DIR, f"writer-{wid}.log")
    with open(path, "w") as f:
        for k in range(n_appends):
            size = 1 + rng.randrange(3)
            payloads = [f"s{seed}-w{wid}-a{k:04d}-r{j}" for j in range(size)]
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


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "baseline"
    if mode != "baseline":
        fail(3, f"mode {mode!r} not implemented — rung is planned, not ready")
    seed = derive_seed()
    n_writers = 3 + seed % 3
    n_appends = 25 + (seed >> 3) % 26  # per writer
    log(f"mode={mode} seed={seed} writers={n_writers} appends/writer={n_appends}")
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

    errors = []
    threads = [
        threading.Thread(target=writer, args=(w, seed, n_appends, acks, errors))
        for w in range(n_writers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        fail(3, f"{len(errors)} append(s) refused in a fault-free run "
                f"(first: writer {errors[0][0]} append {errors[0][1]}) — setup")
    expected = 1 + n_writers * n_appends
    if len(acks) != expected:
        fail(3, f"expected {expected} acks, got {len(acks)} — setup")
    log(f"appends: {len(acks)} acked across {n_writers} writers")

    tail_seq = get_tail()
    verify(acks, tail_seq, n_writers)

    server.terminate()
    log("VERDICT: GREEN")


main()
PYEOF
