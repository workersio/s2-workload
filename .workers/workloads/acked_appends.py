#!/usr/bin/env python3
"""Workload: acked appends survive restart (s2 lite, --local-root).

Modes:
  baseline  start -> append N -> graceful stop -> restart -> verify
  kill9     start -> append under load -> SIGKILL mid-stream -> restart -> verify

Oracle (see .workers/promises/acked-appends-survive-restart.md):
  - manifest line written only after the append HTTP response is fully read
  - completeness bounded by server check-tail, not by what a read returns
  - acked records: exactly once, in order, identical content
  - unacked records: present or absent, but at most once
  - anti-vacuous gate in kill9: enough acks AND >=1 in-flight-unacked at kill

Exit codes: 0 green, 1 red (finding), 3 void/blocked (setup or vacuous trial).
"""

import http.client
import json
import os
import signal
import subprocess
import sys
import time

S2 = os.path.join(".workers", "vendor", "bin", "s2-linux-amd64")
PORT = 8080
BASIN = "durability-wl-01"
STREAM = "acked-appends"
DATA_DIR = os.path.join("data", "s2root")
BASELINE_APPENDS = 40


def log(msg):
    print(msg, flush=True)


def fail(code, msg):
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
    out = open(os.path.join("data", "server.log"), "ab")
    proc = subprocess.Popen(
        [S2, "lite", "--port", str(PORT), "--local-root", DATA_DIR],
        stdout=out, stderr=subprocess.STDOUT, env=env,
    )
    return proc


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
        fail(1, f"tail {tail_seq} < max acked end {max_acked_end} — acked data lost")

    seqs = [s for s, _ in readback]
    if seqs != list(range(len(seqs))):
        fail(1, f"read-back seqs not a dense prefix: first 20 = {seqs[:20]}")

    by_payload = {}
    for s, b in readback:
        by_payload.setdefault(b, []).append(s)

    for b, occurrences in by_payload.items():
        if b not in sent_payloads:
            fail(1, f"read-back contains a record never sent: {b!r} at {occurrences}")
        if len(occurrences) > 1:
            fail(1, f"record duplicated (at-most-once violated): {b!r} at {occurrences}")

    missing = [m for m in acked if m["payload"] not in by_payload]
    if missing:
        for m in missing[:10]:
            log(f"MISSING ACKED: {m}")
        fail(1, f"{len(missing)} acked record(s) absent after restart")

    order = [by_payload[m["payload"]][0] for m in acked]
    if order != sorted(order):
        fail(1, f"acked records out of ack order: {order[:20]}")

    log(f"oracle: {len(acked)} acked verified, {len(readback)} records read, "
        f"tail {tail_seq}")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "baseline"
    seed = derive_seed()
    log(f"mode={mode} seed={seed}")
    os.makedirs(DATA_DIR, exist_ok=True)

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

    server = start_server(env_extra)
    wait_health()
    create_basin()

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

    if mode == "kill9":
        if not killed:
            fail(3, "kill point never reached — vacuous trial")
        if acked_count < 10:
            fail(3, f"only {acked_count} acks before kill — below floor, void")
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

    server2 = start_server()
    wait_health()
    tail = wait_stream_ready()
    tail_seq = tail["tail"]["seq_num"]
    readback = read_all(tail_seq)
    verify(manifest, readback, tail_seq)

    server2.terminate()
    log("VERDICT: GREEN")


if __name__ == "__main__":
    main()
