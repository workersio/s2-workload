#!/usr/bin/env python3
"""Workload: reads never lose observed records (s2 lite, --local-root).

Modes:
  baseline        no faults: follower tails over SSE from seq 0 while a writer
                  appends; every acked record must reach the follower exactly
                  once in order, and a final catch-up read must equal what the
                  follower observed.
  across-restart  follower tails mid-stream; SIGKILL the server while the
                  follower is behind the writer; restart on the same root;
                  post-restart Remote-durability read must contain every
                  record the follower already observed; resumed follow must
                  continue gap-free.

Follow transport (probed in source, recorded in map.md):
  GET /v1/streams/{stream}/records?seq_num=N with Accept: text/event-stream
  -> axum SSE. Events: `event: batch` (data = ReadBatch JSON, id =
  "seq,count,bytes"), `event: ping` (heartbeat), `event: error`, and a bare
  `data: [DONE]` terminator. Catch-up scan filters at DurabilityLevel::Remote
  (lite/src/backend/read.rs:127); live follow is durable_seq-gated broadcast
  (lite/src/backend/streamer.rs:607). The oracle diffs these two mechanisms.

Oracle (see .workers/promises/reads-never-lose-observed-records.md):
  - one observed-log entry per delivered record, appended only after the SSE
    event is fully read and parsed
  - follower stream gap-free, duplicate-free, in seq order (and across resume)
  - every observed record present in the post-restart Remote read, same seq,
    identical content — none absent, none moved
  - catch-up read [0, tail) dense and a superset of the observed prefix
  - anti-vacuous (across-restart): observed floor AND follower behind the
    writer at the kill (lag > 0)
  - a partial delivery with an internal gap/duplicate is RED (the loss
    shape), never VOID; only a clean dense prefix that stalls may be VOID

Oracle self-tests (each must exit 1 before a green is trusted):
  ORACLE_SELFTEST=1    drop one observed record from the readback
                       -> observed_survive FAIL
  ORACLE_SELFTEST=gap  follower silently drops one delivered seq
                       -> follow_wellformed FAIL via the partial-delivery path

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
BASIN = "reads-wl-01"
STREAM = "reads-tail"
WORK_DIR = "/tmp/wl"  # /workspace (the repo checkout) is read-only in the guest
DATA_DIR = os.path.join(WORK_DIR, "s2root")
BASELINE_APPENDS = 200


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


class Follower(threading.Thread):
    """Tails the stream over SSE. Appends (seq, body) to self.observed only
    after the batch event is fully read and parsed — that log is the oracle's
    'delivered to the reader' witness."""

    def __init__(self, start_seq, timeout=30, drop_seq=None):
        super().__init__(daemon=True)
        self.start_seq = start_seq
        self.timeout = timeout
        self.drop_seq = drop_seq  # ORACLE_SELFTEST=gap: drop this seq once
        self.observed = []  # list of (seq, body), append-only
        self.last_event_id = None
        self.status = None
        self.ended = None  # 'stopped' | 'server-gone' | 'done-event' | repr(err)
        self._conn = None
        self._halt = threading.Event()  # NB: name must not shadow Thread._stop

    def stop(self):
        self._halt.set()
        conn = self._conn
        if conn is not None:
            try:
                conn.close()  # unblocks a blocked readline
            except Exception:
                pass

    def run(self):
        try:
            self._conn = http.client.HTTPConnection(
                "127.0.0.1", PORT, timeout=self.timeout)
            self._conn.request(
                "GET",
                f"/v1/streams/{STREAM}/records?seq_num={self.start_seq}",
                headers={
                    "S2-Basin": BASIN,
                    "Authorization": "Bearer ignored",
                    "Accept": "text/event-stream",
                },
            )
            resp = self._conn.getresponse()
            self.status = resp.status
            if resp.status != 200:
                self.ended = f"HTTP {resp.status}: {resp.read()[:200]!r}"
                return
            event, data_lines = None, []
            while not self._halt.is_set():
                raw = resp.readline()
                if not raw:  # EOF — server closed (kill) or stream ended
                    self.ended = "server-gone"
                    return
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
                elif line.startswith("id:"):
                    self.last_event_id = line[3:].strip()
                elif line == "":
                    if data_lines:
                        self._dispatch(event, "\n".join(data_lines))
                        if self.ended:
                            return
                    event, data_lines = None, []
        except Exception as e:
            # a cross-thread close() surfaces as AttributeError on the dead
            # buffer, not OSError — treat any error after stop() as clean
            self.ended = "stopped" if self._halt.is_set() else repr(e)
        finally:
            self.ended = self.ended or "stopped"
            try:
                if self._conn is not None:
                    self._conn.close()
            except Exception:
                pass

    def _dispatch(self, event, data):
        if data == "[DONE]":
            self.ended = "done-event"
            return
        if event == "batch":
            batch = json.loads(data)
            for rec in batch.get("records", []):
                if self.drop_seq is not None and rec["seq_num"] == self.drop_seq:
                    log(f"ORACLE_SELFTEST: follower silently dropping "
                        f"delivered seq={self.drop_seq}")
                    self.drop_seq = None
                    continue
                self.observed.append((rec["seq_num"], rec.get("body", "")))
        elif event == "error":
            self.ended = f"error-event: {data[:200]}"
        # ping: heartbeat, ignore


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
    """Post-restart readiness: poll check-tail, never fixed sleeps (startup
    sleeps one manifest_poll_interval for time-based fencing)."""
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
    """One request = one ack. Returns acked start seq or None if unacked."""
    try:
        status, data = http_call(
            "POST", f"/v1/streams/{STREAM}/records",
            body={"records": [{"body": payload}]}, timeout=10)
    except OSError:
        return None
    if status != 200:
        return None
    return json.loads(data)["start"]["seq_num"]


def read_all(tail_seq):
    """Unary paginated catch-up read [0, tail) — the Remote-durability path."""
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
            fail(1, f"read returned no records at seq {cursor} < tail "
                    f"{tail_seq} — gap below tail",
                 inv=("readback_dense", "catchup-read-dense"))
        for rec in batch:
            records.append((rec["seq_num"], rec.get("body", "")))
        cursor = records[-1][0] + 1
    return records


def wait_observed(follower, target, base_seq=0, deadline_s=90):
    """Wait for `target` deliveries. Returns False early once an internal gap
    is provable — the highest delivered seq spans more slots than records
    observed (delivery continued past a hole) — so a real loss is not stuck
    behind the timeout; the caller's wellformedness check then goes RED."""
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        obs = follower.observed
        n = len(obs)  # read the length first: obs[n-1] is stable under
        if n >= target:  # concurrent appends, obs[-1] is not
            return True
        if n and (obs[n - 1][0] - base_seq + 1) > n:
            return False  # gap already provable, don't wait out the clock
        if not follower.is_alive():
            return len(follower.observed) >= target
        time.sleep(0.05)
    return False


def partial_delivery_verdict(follower, target, label, base_seq=0):
    """Timeout/gap path: a partial observed log with an internal gap or
    duplicate is the finding (RED via check_wellformed), not a setup issue.
    Only a clean dense prefix shorter than target may exit VOID."""
    observed = list(follower.observed)
    check_wellformed(observed, f"{label} (partial, {len(observed)}/{target})",
                     start_seq=base_seq)
    head = observed[0][0] if observed else None
    tail_ = observed[-1][0] if observed else None
    fail(3, f"{label}: follower delivered a clean dense prefix "
            f"{len(observed)}/{target} (seqs [{head}..{tail_}], "
            f"ended={follower.ended}) and stalled — transport/setup issue, "
            f"not a loss (a loss would have gapped)")


def check_wellformed(observed, label, start_seq=0):
    """Invariant 2: delivered stream gap-free, duplicate-free, in seq order."""
    seqs = [s for s, _ in observed]
    want = list(range(start_seq, start_seq + len(seqs)))
    if seqs != want:
        bad = next((i for i, (a, b) in enumerate(zip(seqs, want)) if a != b),
                   len(seqs))
        fail(1, f"{label}: delivered seqs not dense from {start_seq} at "
                f"index {bad}: got {seqs[max(0, bad - 3):bad + 4]}",
             inv=("follow_wellformed", "follow-gap-and-dup-free"))


def verify_observed_in_readback(observed, readback):
    """Invariant 1 (the durability finding): every record the follower
    observed appears in the catch-up read at the same seq with identical
    content."""
    by_seq = dict(readback)
    if os.environ.get("ORACLE_SELFTEST") == "1" and observed:
        victim = observed[len(observed) // 2]
        log(f"ORACLE_SELFTEST: dropping observed record seq={victim[0]} "
            f"from the readback")
        by_seq.pop(victim[0], None)
    lost, moved = [], []
    for seq, body in observed:
        got = by_seq.get(seq)
        if got is None:
            lost.append(seq)
        elif got != body:
            moved.append((seq, body, got))
    if lost:
        for s in lost[:10]:
            log(f"OBSERVED-THEN-LOST: seq={s} "
                f"body={dict(observed)[s]!r}")
        fail(1, f"{len(lost)} record(s) delivered to the follower are absent "
                f"from the post-restart Remote read: seqs {lost[:20]} — "
                f"dirty read across the crash",
             inv=("observed_survive", "observed-records-survive"))
    if moved:
        for s, want, got in moved[:10]:
            log(f"OBSERVED-THEN-MOVED: seq={s} follower={want!r} read={got!r}")
        fail(1, f"{len(moved)} record(s) changed content between follow "
                f"delivery and the post-restart read",
             inv=("observed_survive", "observed-records-survive"))
    invariant("observed_survive", "observed-records-survive", True,
              f"all {len(observed)} follower-observed records present and "
              f"identical in the Remote read")


def verify_readback_dense(readback, tail_seq, observed_len):
    """Invariant 3: catch-up read dense over [0, tail), superset of the
    observed prefix."""
    seqs = [s for s, _ in readback]
    if seqs != list(range(tail_seq)):
        fail(1, f"catch-up read not a dense prefix of tail {tail_seq}: "
                f"first 20 = {seqs[:20]}",
             inv=("readback_dense", "catchup-read-dense"))
    if len(readback) < observed_len:
        fail(1, f"catch-up read ({len(readback)}) shorter than what the "
                f"follower observed ({observed_len})",
             inv=("readback_dense", "catchup-read-dense"))
    invariant("readback_dense", "catchup-read-dense", True,
              f"{len(readback)} records tile seq 0..{tail_seq}, superset of "
              f"the {observed_len}-record observed prefix")


def run_baseline(seed):
    server = start_server()
    wait_health()
    create_basin()

    drop_seq = None
    if os.environ.get("ORACLE_SELFTEST") == "gap":
        drop_seq = BASELINE_APPENDS // 2
    follower = Follower(start_seq=0, drop_seq=drop_seq)
    follower.start()

    acked = []  # (seq, payload)
    for i in range(BASELINE_APPENDS):
        payload = f"s{seed}-r0-{i:05d}"
        seq = append_one(payload)
        if seq is None:
            fail(3, f"baseline append {i} not acked — setup issue")
        acked.append((seq, payload))
        if (seed >> (i % 24)) & 1:
            time.sleep(0.001)  # seed-jittered pacing

    if not wait_observed(follower, BASELINE_APPENDS):
        # RED if the partial log has an internal gap/dup; VOID only if it is
        # a clean dense prefix that stalled (genuine transport/setup issue)
        partial_delivery_verdict(follower, BASELINE_APPENDS, "baseline follow")
    follower.stop()
    follower.join(timeout=10)
    observed = list(follower.observed)
    log(f"follower: {len(observed)} records observed, ended={follower.ended}, "
        f"last_event_id={follower.last_event_id}")

    if len(observed) < BASELINE_APPENDS:
        fail(3, f"observed {len(observed)} < {BASELINE_APPENDS} — vacuous")
    invariant("nonvacuous", "observed-floor", True,
              f"follower observed {len(observed)} records (floor "
              f"{BASELINE_APPENDS})")

    check_wellformed(observed, "baseline follow")
    invariant("follow_wellformed", "follow-gap-and-dup-free", True,
              f"delivered stream tiles seq 0..{len(observed)} exactly once, "
              f"in order")

    # every ack must be in the observed stream with identical content
    obs_by_seq = dict(observed)
    for seq, payload in acked:
        got = obs_by_seq.get(seq)
        if got != payload:
            fail(1, f"acked seq {seq} ({payload!r}) delivered to follower as "
                    f"{got!r}",
                 inv=("acked_delivered", "acked-reach-follower"))
    invariant("acked_delivered", "acked-reach-follower", True,
              f"all {len(acked)} acked records delivered with identical "
              f"content")

    tail = wait_stream_ready()
    tail_seq = tail["tail"]["seq_num"]
    readback = read_all(tail_seq)
    verify_readback_dense(readback, tail_seq, len(observed))
    verify_observed_in_readback(observed, readback)

    server.terminate()
    log("VERDICT: GREEN")


def run_across_restart(seed):
    arm = ["", "500ms", "2s"][seed % 3]
    env_extra = {"SL8_FLUSH_INTERVAL": arm} if arm else {}
    log(f"flush-arm={arm or 'default'}")
    server = start_server(env_extra)
    wait_health()
    create_basin()

    follower = Follower(start_seq=0)
    follower.start()

    total_appends = 4000
    kill_after = 60 + (seed >> 2) % 400  # acks before SIGKILL
    manifest = []  # (seq, payload) acked
    state = {"in_flight": None, "stop": False}

    def writer():
        for i in range(total_appends):
            if state["stop"]:
                return
            payload = f"s{seed}-r0-{i:05d}"
            state["in_flight"] = payload
            seq = append_one(payload)
            state["in_flight"] = None
            if seq is None:
                return  # server gone
            manifest.append((seq, payload))

    wt = threading.Thread(target=writer, daemon=True)
    wt.start()

    # wait for the kill point, then SIGKILL while the follower is behind
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and len(manifest) < kill_after:
        if not wt.is_alive():
            fail(3, f"writer finished early ({len(manifest)} acks) before "
                    f"kill point {kill_after} — setup issue")
        time.sleep(0.002)
    if len(manifest) < kill_after:
        fail(3, f"kill point {kill_after} not reached in time "
                f"({len(manifest)} acks) — void")

    acked_at_kill = len(manifest)
    observed_at_kill = len(follower.observed)
    in_flight = state["in_flight"]
    server.send_signal(signal.SIGKILL)
    kill_lag = acked_at_kill - observed_at_kill
    log(f"SIGKILL at acked={acked_at_kill} observed={observed_at_kill} "
        f"lag={kill_lag} in_flight={in_flight!r}")
    state["stop"] = True
    server.wait(timeout=30)
    wt.join(timeout=30)
    follower.join(timeout=30)  # socket EOF/reset ends it
    observed = list(follower.observed)
    log(f"follower pre-kill: {len(observed)} observed, ended={follower.ended}")

    if observed_at_kill < 50:
        fail(3, f"only {observed_at_kill} observed before kill — below floor "
                f"50, void")
    if kill_lag <= 0 and in_flight is None:
        fail(3, "follower fully caught up and writer idle at kill — quiesced "
                "kill proves nothing, void")
    invariant("nonvacuous", "kill-while-behind", True,
              f"kill at observed={observed_at_kill} (floor 50), "
              f"lag={kill_lag}, in_flight={in_flight is not None}")

    check_wellformed(observed, "pre-kill follow")

    # restart on the same root
    server2 = start_server(env_extra)
    wait_health()
    tail = wait_stream_ready()
    tail_seq = tail["tail"]["seq_num"]
    log(f"post-restart tail={tail_seq}")

    # invariant 1 + 3: post-restart Remote read vs the observed prefix
    readback = read_all(tail_seq)
    verify_readback_dense(readback, tail_seq, len(observed))
    verify_observed_in_readback(observed, readback)

    # invariant 2 (resume leg): follow again from where the follower stopped;
    # the resumed stream must tile [K, tail) with no gap or duplicate
    resume_from = len(observed)
    if resume_from < tail_seq:
        f2 = Follower(start_seq=resume_from)
        f2.start()
        if not wait_observed(f2, tail_seq - resume_from,
                             base_seq=resume_from):
            # same guard as baseline: gap/dup in the partial resume = RED
            partial_delivery_verdict(f2, tail_seq - resume_from,
                                     "resumed follow", base_seq=resume_from)
        f2.stop()
        f2.join(timeout=10)
        resumed = list(f2.observed)[:tail_seq - resume_from]
        check_wellformed(resumed, "resumed follow", start_seq=resume_from)
        rb_by_seq = dict(readback)
        for seq, body in resumed:
            if rb_by_seq.get(seq) != body:
                fail(1, f"resumed follow seq {seq} content {body!r} != "
                        f"Remote read {rb_by_seq.get(seq)!r}",
                     inv=("follow_wellformed", "follow-gap-and-dup-free"))
        log(f"resumed follow delivered {len(resumed)} records "
            f"[{resume_from}, {tail_seq})")
    else:
        log("no records beyond the observed prefix — resume leg skipped")
    invariant("follow_wellformed", "follow-gap-and-dup-free", True,
              f"delivered stream dense 0..{len(observed)} pre-kill and "
              f"resume tiles up to tail {tail_seq}")

    server2.terminate()
    log("VERDICT: GREEN")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "baseline"
    seed = derive_seed()
    log(f"mode={mode} seed={seed}")
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(WORK_DIR, exist_ok=True)

    if mode == "baseline":
        run_baseline(seed)
    elif mode == "across-restart":
        run_across_restart(seed)
    else:
        fail(3, f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
