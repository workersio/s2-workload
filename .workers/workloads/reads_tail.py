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
  last-event-id-resume
                  same kill schedule as across-restart, but the reader
                  reconnects the way a real SSE client does: the new session
                  carries the Last-Event-Id header ("seq,count,bytes",
                  captured from the final fully-delivered event) and NO
                  seq_num param, exercising apply_last_event_id
                  (handlers/v1/records.rs:49-66): start = seq+1, count/bytes
                  budgets decremented by what the header says was consumed.
                  Budget arm (seed-selected): the resumed request re-sends a
                  count limit; the session must end ([DONE]) after exactly
                  limit-minus-consumed records — the decremented budget, not
                  the raw limit.
  slow-follower-lagged
                  no kill: stall the follower's socket while a writer pushes
                  128-256 KiB appends past FOLLOWER_MAX_LAG=25 batches,
                  forcing broadcast RecvError::Lagged and the session's
                  silent catch-up re-entry (read.rs:219-222). Witness = a
                  batch event missing `tail` after follow batches carried
                  it. Delivered stream must stay gap/dup-free across the
                  handoff and equal the final Remote readback.

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
  ORACLE_SELFTEST=1            drop one observed record from the readback
                               -> observed_survive FAIL
  ORACLE_SELFTEST=gap          follower silently drops one delivered seq
                               -> follow_wellformed FAIL (partial-delivery
                               path in baseline, check_wellformed in
                               across-restart)
  ORACLE_SELFTEST=lost-stream  (across-restart) poll the stream in a
                               nonexistent basin post-restart ->
                               observed_survive FAIL via the
                               serving-but-stream-denied path
  ORACLE_SELFTEST=shift        (last-event-id-resume) resume with the header
                               seq shifted +1 — the server then starts one
                               past the true boundary and the tiling oracle
                               must FAIL (the off-by-one this arm exists to
                               catch)
  ORACLE_SELFTEST=bad-id       (last-event-id-resume) perturb the committed
                               event id before the emission check ->
                               header_describes_delivery FAIL (a server that
                               misdescribes its own delivery is a finding,
                               never a harness void)

Exit codes: 0 green, 1 red (finding), 3 void/blocked (setup or vacuous trial).
"""

import hashlib
import http.client
import json
import os
import signal
import socket
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


def norm_body(body):
    """Large payloads (slow-follower-lagged uses 2-256 KiB records) are
    compared by digest on BOTH oracle sides — observed log and readback —
    so content equality is preserved while memory stays bounded. Small
    bodies (every other mode) pass through verbatim."""
    if len(body) <= 1024:
        return body
    return f"sha256:{hashlib.sha256(body.encode()).hexdigest()}"


def http_call(method, path, body=None, timeout=10, basin=None):
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    try:
        headers = {"S2-Basin": basin or BASIN,
                   "Authorization": "Bearer ignored"}
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

    def __init__(self, start_seq, timeout=30, drop_seq=None,
                 resume_header=None, count_limit=None, rcvbuf=None):
        super().__init__(daemon=True)
        self.start_seq = start_seq
        self.timeout = timeout
        self.drop_seq = drop_seq  # ORACLE_SELFTEST=gap: drop this seq once
        # resume_header: Last-Event-Id value — the request then carries NO
        # seq_num param; the server derives the start (records.rs:49-66)
        self.resume_header = resume_header
        self.count_limit = count_limit  # count= query param (session budget)
        # rcvbuf: cap SO_RCVBUF before connect (shrinks the negotiated TCP
        # window) so a stalled client stops absorbing server writes quickly
        # — required to force broadcast lag (slow-follower-lagged)
        self.rcvbuf = rcvbuf
        self.observed = []  # list of (seq, body), append-only
        self.batches = []  # per batch-event: {n, first, has_tail}
        self.last_event_id = None  # committed only when its event dispatches
        self.status = None
        self.ended = None  # 'stopped' | 'server-gone' | 'done-event' | repr(err)
        self._conn = None
        self._halt = threading.Event()  # NB: name must not shadow Thread._stop
        self._pause = threading.Event()  # stall: stop draining the socket

    def pause(self):
        self._pause.set()

    def unpause(self):
        self._pause.clear()

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
            params = []
            if self.resume_header is None:
                params.append(f"seq_num={self.start_seq}")
            if self.count_limit is not None:
                params.append(f"count={self.count_limit}")
            path = f"/v1/streams/{STREAM}/records"
            if params:
                path += "?" + "&".join(params)
            headers = {
                "S2-Basin": BASIN,
                "Authorization": "Bearer ignored",
                "Accept": "text/event-stream",
            }
            if self.resume_header is not None:
                headers["Last-Event-Id"] = self.resume_header
            self._conn = http.client.HTTPConnection(
                "127.0.0.1", PORT, timeout=self.timeout)
            if self.rcvbuf is not None:
                # set before connect so window scaling honors the cap
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                                self.rcvbuf)
                sock.settimeout(self.timeout)
                sock.connect(("127.0.0.1", PORT))
                self._conn.sock = sock
            self._conn.request("GET", path, headers=headers)
            resp = self._conn.getresponse()
            self.status = resp.status
            if resp.status != 200:
                self.ended = f"HTTP {resp.status}: {resp.read()[:200]!r}"
                return
            event, data_lines, pending_id = None, [], None
            while not self._halt.is_set():
                # stall support (slow-follower-lagged): while paused we stop
                # draining the socket entirely, so backpressure fills hyper's
                # write path + kernel buffers and the server-side broadcast
                # receiver falls behind
                while self._pause.is_set() and not self._halt.is_set():
                    time.sleep(0.01)
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
                    pending_id = line[3:].strip()
                elif line == "":
                    if data_lines:
                        self._dispatch(event, "\n".join(data_lines))
                        if self.ended:
                            return
                        # commit the id only once its event fully dispatched
                        # — a kill mid-event must not advance the resume
                        # cursor past what was actually delivered (this is
                        # what a real SSE client does with lastEventId)
                        if event == "batch" and pending_id is not None:
                            self.last_event_id = pending_id
                    event, data_lines, pending_id = None, [], None
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
            recs = batch.get("records", [])
            # `tail` is present on follow-path batches (read.rs:209-212) and
            # OMITTED from catch-up-scan batches (read.rs:169-182; JSON drops
            # None, json.rs:36-37) — the lagged-handoff witness
            self.batches.append({
                "n": len(recs),
                "first": recs[0]["seq_num"] if recs else None,
                "has_tail": "tail" in batch,
            })
            for rec in recs:
                if self.drop_seq is not None and rec["seq_num"] == self.drop_seq:
                    log(f"ORACLE_SELFTEST: follower silently dropping "
                        f"delivered seq={self.drop_seq}")
                    self.drop_seq = None
                    continue
                self.observed.append(
                    (rec["seq_num"], norm_body(rec.get("body", ""))))
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


def wait_stream_ready(deadline_s=120, observed=None, basin=None):
    """Post-restart readiness: poll check-tail, never fixed sleeps (startup
    sleeps one manifest_poll_interval for time-based fencing).

    Verdict routing (test-reviewer, executor #10): if the process is serving
    (HTTP responses arrive) but persistently denies the stream while we hold
    >=50 follower-observed records, that is the strongest form of the
    promised finding — every observed record is unreadable — and must be
    RED, not VOID. Connection-level failure (never healthy) stays VOID."""
    deadline = time.monotonic() + deadline_s
    last = None
    serving_denials = 0
    while time.monotonic() < deadline:
        try:
            status, data = http_call(
                "GET", f"/v1/streams/{STREAM}/records/tail", timeout=2,
                basin=basin)
            if status == 200:
                return json.loads(data)
            last = (status, data[:200])
            serving_denials += 1  # the server answered; the stream did not
        except OSError as e:
            last = repr(e)
            serving_denials = 0
        time.sleep(0.2)
    if observed is not None and len(observed) >= 50 and serving_denials >= 25:
        fail(1, f"server is serving but the stream is unreadable after "
                f"restart ({serving_denials} consecutive denials, last "
                f"{last}) while {len(observed)} follower-observed records "
                f"are held — observed records lost with the stream",
             inv=("observed_survive", "observed-records-survive"))
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


def append_one(payload, why=None):
    """One request = one ack. Returns acked start seq or None if unacked.
    `why`: optional dict to record the failure reason for triage."""
    try:
        status, data = http_call(
            "POST", f"/v1/streams/{STREAM}/records",
            body={"records": [{"body": payload}]}, timeout=10)
    except OSError as e:
        if why is not None:
            why["reason"] = repr(e)
        return None
    if status != 200:
        if why is not None:
            why["reason"] = f"HTTP {status}: {data[:200]!r}"
        return None
    return json.loads(data)["start"]["seq_num"]


def read_all(tail_seq, red_on_error=False):
    """Unary paginated catch-up read [0, tail) — the Remote-durability path.

    red_on_error (post-restart leg): a persistent non-200 below tail is a
    finding — the read path denies a region it advertises — not setup.
    Transient errors get a bounded retry either way."""
    records = []
    cursor = 0
    for _ in range(10000):
        if cursor >= tail_seq:
            break
        status, data = None, b""
        for attempt in range(10):
            try:
                status, data = http_call(
                    "GET",
                    f"/v1/streams/{STREAM}/records?seq_num={cursor}&count=1000",
                    timeout=30)
            except OSError as e:
                status, data = None, repr(e).encode()
            if status == 200:
                break
            time.sleep(1)
        if status != 200:
            msg = (f"read persistently failing at seq {cursor} < tail "
                   f"{tail_seq} after 10 retries: {status} {data[:200]}")
            if red_on_error:
                fail(1, msg + " — advertised region unreadable",
                     inv=("readback_dense", "catchup-read-dense"))
            fail(3, msg)
        batch = json.loads(data).get("records", [])
        if not batch:
            fail(1, f"read returned no records at seq {cursor} < tail "
                    f"{tail_seq} — gap below tail",
                 inv=("readback_dense", "catchup-read-dense"))
        for rec in batch:
            records.append((rec["seq_num"], norm_body(rec.get("body", ""))))
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


def pre_kill_phase(seed, arm, env_extra, drop_seq):
    """Shared kill schedule (across-restart and last-event-id-resume):
    prime the stream, follow from seq 0, run a 3-thread pipelined writer
    pool to an arm-scaled kill point, SIGKILL inside a sampled lag>0
    window, join everything, gate floor + lag + wellformedness. Returns
    (follower, observed) with the server dead."""
    server = start_server(env_extra)
    wait_health()
    create_basin()

    # Prime the stream: with slow flush arms the lazily-created stream 404s
    # appends (stream_not_found) until its creation record is durable —
    # retry-bounded first append (map reality note).
    prime_deadline = time.monotonic() + 90
    prime_seq, why = None, {}
    while time.monotonic() < prime_deadline:
        prime_seq = append_one(f"s{seed}-prime", why)
        if prime_seq is not None:
            break
        time.sleep(0.25)
    if prime_seq is None:
        fail(3, f"prime append never acked: {why.get('reason', 'unknown')}")

    follower = Follower(start_seq=0, drop_seq=drop_seq)
    follower.start()

    total_appends = 4000
    # kill point scaled by flush arm: ack throughput is ~flush-rate bound
    # (2s arm: ~1.5 acks/s with 3 writers), so slow arms need a lower cap
    # or the 120s deadline voids the trial before the kill
    arm_span = {"": 400, "500ms": 200, "2s": 80}[arm]
    kill_after = 60 + (seed >> 2) % arm_span  # writer acks before SIGKILL
    manifest = []  # (seq, payload) acked; append is atomic under the GIL
    state = {"stop": False}
    idx_lock = threading.Lock()
    idx = {"i": 0}

    def writer():
        while not state["stop"]:
            with idx_lock:
                i = idx["i"]
                if i >= total_appends:
                    return
                idx["i"] = i + 1
            payload = f"s{seed}-r0-{i:05d}"
            w = {}
            seq = append_one(payload, w)
            if seq is None:
                if not state["stop"]:
                    log(f"writer: append {i} unacked: "
                        f"{w.get('reason', 'unknown')}")
                return  # server gone (or a real pre-kill failure — logged)
            manifest.append((seq, payload))

    # Pipelined writer pool: concurrent appends keep acks landing while
    # deliveries are still in flight, so the follower is genuinely behind
    # (lag > 0) at the kill — the spec's anti-vacuous condition.
    writers = [threading.Thread(target=writer, daemon=True) for _ in range(3)]
    for t in writers:
        t.start()

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and len(manifest) < kill_after:
        if not any(t.is_alive() for t in writers):
            fail(3, f"writers finished early ({len(manifest)} acks) before "
                    f"kill point {kill_after} — setup issue")
        time.sleep(0.002)
    if len(manifest) < kill_after:
        fail(3, f"kill point {kill_after} not reached in time "
                f"({len(manifest)} acks) — void")

    # spin briefly for a genuine lag>0 window, then kill inside it
    lag_deadline = time.monotonic() + 10
    acked_at_kill = observed_at_kill = kill_lag = 0
    while time.monotonic() < lag_deadline:
        acked_at_kill = len(manifest) + 1  # + prime record
        observed_at_kill = len(follower.observed)
        kill_lag = acked_at_kill - observed_at_kill
        if kill_lag > 0:
            break
        time.sleep(0.0005)
    server.send_signal(signal.SIGKILL)
    log(f"SIGKILL at acked={acked_at_kill} (incl prime) "
        f"observed={observed_at_kill} lag={kill_lag}")
    state["stop"] = True
    server.wait(timeout=30)
    for t in writers:
        t.join(timeout=30)
    follower.join(timeout=30)  # socket EOF/reset ends it
    observed = list(follower.observed)
    log(f"follower pre-kill: {len(observed)} observed, ended={follower.ended}")

    if observed_at_kill < 50:
        fail(3, f"only {observed_at_kill} observed before kill — below floor "
                f"50, void")
    if kill_lag <= 0:
        fail(3, f"no lag>0 window found in 10s (last sampled lag="
                f"{kill_lag}; delivery bursts can outpace writer-side ack "
                f"bookkeeping at flush boundaries) — kill not proven to "
                f"threaten undelivered acks, void")
    invariant("nonvacuous", "kill-while-behind", True,
              f"kill at observed={observed_at_kill} (floor 50), "
              f"lag={kill_lag} (> 0)")

    check_wellformed(observed, "pre-kill follow")
    return follower, observed


def run_across_restart(seed):
    arm = ["", "500ms", "2s"][seed % 3]
    env_extra = {"SL8_FLUSH_INTERVAL": arm} if arm else {}
    log(f"flush-arm={arm or 'default'}")
    selftest = os.environ.get("ORACLE_SELFTEST")
    drop_seq = 30 if selftest == "gap" else None  # below the floor of 50
    follower, observed = pre_kill_phase(seed, arm, env_extra, drop_seq)

    # restart on the same root
    server2 = start_server(env_extra)
    wait_health()
    if selftest == "lost-stream":
        # red-prove the stream-denial path: a serving server that denies the
        # stream while we hold >=floor observed records must RED. A missing
        # *stream* is auto-created by create-stream-on-read (proven: the
        # first selftest shape came back 200/tail-0), so deny via a
        # nonexistent basin — basins are never auto-created.
        log("ORACLE_SELFTEST: polling the stream in a nonexistent basin "
            "post-restart")
        wait_stream_ready(deadline_s=20, observed=observed,
                          basin=BASIN + "-selftest-missing")
    tail = wait_stream_ready(observed=observed)
    tail_seq = tail["tail"]["seq_num"]
    log(f"post-restart tail={tail_seq}")

    # invariant 1 + 3: post-restart Remote read vs the observed prefix
    readback = read_all(tail_seq, red_on_error=True)
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


def start_header_resume(header, expected_start, count_limit=None):
    """Open the header-resumed SSE session. By the time this runs the server
    is proven serving (check-tail 200 + a full readback succeeded), so a
    persistent non-2xx here means the server rejects its own emitted
    Last-Event-Id — a parse/validation regression breaking every real SSE
    resume: RED, not setup (test-reviewer, executor #14). Connection-level
    failures (no HTTP status at all) stay VOID after bounded retries."""
    last = None
    for _ in range(3):
        f2 = Follower(start_seq=expected_start, resume_header=header,
                      count_limit=count_limit)
        f2.start()
        deadline = time.monotonic() + 15
        while (time.monotonic() < deadline and f2.status is None
               and f2.is_alive()):
            time.sleep(0.05)
        if f2.status == 200:
            return f2
        last = f2.status if f2.status is not None else f2.ended
        f2.stop()
        f2.join(timeout=5)
        time.sleep(1)
    if isinstance(last, int):
        fail(1, f"server persistently rejects its own emitted Last-Event-Id "
                f"{header!r} (HTTP {last}, 3 attempts) while proven serving "
                f"— header parse/validation regression",
             inv=("header_accepted", "server-accepts-own-event-id"))
    fail(3, f"header-resumed session never got an HTTP response: {last}")


def run_last_event_id_resume(seed):
    """Same kill schedule as across-restart; the resume leg reconnects with
    the Last-Event-Id header instead of a seq_num param, so the server's own
    header parse + start=seq+1 arithmetic + budget decrement
    (apply_last_event_id, handlers/v1/records.rs:49-66) sit under the tiling
    oracle. The expected resume boundary is computed from OUR observed log,
    never from the header — an off-by-one on the server shows up as a
    skipped or duplicated record at the boundary."""
    arm = ["", "500ms", "2s"][seed % 3]
    env_extra = {"SL8_FLUSH_INTERVAL": arm} if arm else {}
    budget_arm = bool((seed >> 8) & 1)
    log(f"flush-arm={arm or 'default'} budget-arm={budget_arm}")
    selftest = os.environ.get("ORACLE_SELFTEST")
    drop_seq = 30 if selftest == "gap" else None  # below the floor of 50
    follower, observed = pre_kill_phase(seed, arm, env_extra, drop_seq)

    header = follower.last_event_id
    if header is None:
        fail(3, "follower committed no Last-Event-Id before the kill — "
                "cannot resume by header")
    if selftest == "bad-id":
        parts = header.split(",")
        header = f"{int(parts[0]) - 1},{parts[1]},{parts[2]}"
        log(f"ORACLE_SELFTEST: perturbing the committed event id to "
            f"{header!r} — header_describes_delivery must FAIL")
    hdr_seq, hdr_count, hdr_bytes = header.split(",")
    hdr_seq, hdr_count = int(hdr_seq), int(hdr_count)
    # The committed id is the server's bytes verbatim, coupled to the same
    # dispatched event as the records we logged — if it misdescribes what
    # was delivered (dense from 0 in one session => seq = last observed,
    # count = len), that is the server misdirecting every real EventSource
    # resume: a finding, not setup (test-reviewer, executor #14).
    if drop_seq is None and (hdr_seq != observed[-1][0]
                             or hdr_count != len(observed)):
        fail(1, f"server-emitted Last-Event-Id {header!r} misdescribes its "
                f"own delivery (last delivered seq {observed[-1][0]}, "
                f"{len(observed)} records this session) — every real SSE "
                f"resume from this id lands wrong",
             inv=("header_describes_delivery",
                  "event-id-describes-delivery"))
    expected_start = observed[-1][0] + 1  # our witness, independent of header

    if selftest == "shift":
        header = f"{hdr_seq + 1},{hdr_count},{hdr_bytes}"
        log(f"ORACLE_SELFTEST: resuming with +1-shifted Last-Event-Id "
            f"{header!r} — tiling oracle must FAIL")

    # restart on the same root
    server2 = start_server(env_extra)
    wait_health()
    wait_stream_ready(observed=observed)

    # The follower keeps draining the kernel socket buffer after SIGKILL, so
    # its observed log routinely catches the durable tail even though lag>0
    # held at the kill (seen in shakeout: lag=1 at kill, observed=tail=79
    # after drain). Append fresh post-restart records so the header-resumed
    # session always has something beyond the boundary to deliver — exactly
    # what a real reconnecting SSE client sees.
    extra = 3 + seed % 6
    ok = 0
    extra_deadline = time.monotonic() + 90
    while ok < extra and time.monotonic() < extra_deadline:
        if append_one(f"s{seed}-post-{ok:05d}") is not None:
            ok += 1
        else:
            time.sleep(0.25)
    if ok < extra:
        fail(3, f"only {ok}/{extra} post-restart appends acked — setup issue")
    log(f"appended {extra} post-restart records beyond the resume boundary")

    tail = wait_stream_ready(observed=observed)
    tail_seq = tail["tail"]["seq_num"]
    log(f"post-restart tail={tail_seq}")

    # invariant 1 + 3: post-restart Remote read vs the observed prefix
    readback = read_all(tail_seq, red_on_error=True)
    verify_readback_dense(readback, tail_seq, len(observed))
    verify_observed_in_readback(observed, readback)

    available = tail_seq - expected_start
    if available <= 0:
        fail(3, f"no records beyond the observed prefix (tail {tail_seq}, "
                f"resume boundary {expected_start}) despite {extra} acked "
                f"post-restart appends — inconsistent, void")
    log(f"resume boundary {expected_start}, {available} records available, "
        f"header={header!r}")

    if budget_arm:
        # the request re-sends a count limit; apply_last_event_id must serve
        # limit - consumed records (records.rs:61-62), then end the session.
        # want < available keeps the over-delivery disambiguator live: a
        # budget-not-decremented regression then delivers past want instead
        # of coinciding with the end of the stream.
        want = 1 + (seed >> 10) % min(max(available - 1, 1), 40)
        limit = hdr_count + want
        log(f"budget arm: count={limit} re-sent, {hdr_count} consumed -> "
            f"session must deliver exactly {want} then [DONE]")
        f2 = start_header_resume(header, expected_start, count_limit=limit)
        f2.join(timeout=90)  # budget exhaustion ends the session ([DONE])
        resumed = list(f2.observed)
        if f2.is_alive() or f2.ended != "done-event":
            # a session that never terminates = the budget was NOT
            # decremented (raw limit > available records) — the bug shape
            f2.stop()
            f2.join(timeout=10)
            if len(resumed) > want:
                fail(1, f"budget arm: session delivered {len(resumed)} > "
                        f"{want} = limit - consumed — budget not decremented "
                        f"by the Last-Event-Id count",
                     inv=("budget_resume", "resume-budget-decremented"))
            fail(3, f"budget arm: session did not end in 90s "
                    f"(delivered {len(resumed)}/{want}, "
                    f"ended={f2.ended}) — transport/setup issue")
        if len(resumed) != want:
            fail(1, f"budget arm: session ended after {len(resumed)} "
                    f"records, expected exactly {want} = limit({limit}) - "
                    f"consumed({hdr_count})",
                 inv=("budget_resume", "resume-budget-decremented"))
        invariant("budget_resume", "resume-budget-decremented", True,
                  f"session delivered exactly {want} = {limit} - "
                  f"{hdr_count} records, then [DONE]")
    else:
        target = available
        f2 = start_header_resume(header, expected_start)
        if not wait_observed(f2, target, base_seq=expected_start):
            # gap/dup in the partial resume = RED (this is where the
            # +1-shift selftest trips: delivery starts one past the
            # boundary, provable immediately)
            partial_delivery_verdict(f2, target, "header-resumed follow",
                                     base_seq=expected_start)
        f2.stop()
        f2.join(timeout=10)
        resumed = list(f2.observed)[:target]

    check_wellformed(resumed, "header-resumed follow",
                     start_seq=expected_start)
    rb_by_seq = dict(readback)
    for seq, body in resumed:
        if rb_by_seq.get(seq) != body:
            fail(1, f"header-resumed follow seq {seq} content {body!r} != "
                    f"Remote read {rb_by_seq.get(seq)!r}",
                 inv=("follow_wellformed", "follow-gap-and-dup-free"))
    invariant("follow_wellformed", "follow-gap-and-dup-free", True,
              f"header resume started exactly at {expected_start} and "
              f"tiled {len(resumed)} records gap/dup-free")

    server2.terminate()
    log("VERDICT: GREEN")


def run_slow_follower_lagged(seed):
    """No kill: attack the broadcast-overflow seam. The per-stream follow
    channel holds FOLLOWER_MAX_LAG=25 BATCHES (broadcast::Sender::new,
    streamer.rs:266, mod.rs:27); a follower that stalls its socket while a
    writer pushes large appends forces RecvError::Lagged and the read
    session silently re-enters the catch-up scan (read.rs:219-222,
    continue 'session) whose empty-scan branch can advance start_seq_num
    to tail without yielding (read.rs:183-185). Witness that Lagged
    actually fired: a batch event MISSING `tail` arriving after follow
    batches that had it. Oracle: the delivered stream stays gap-free,
    duplicate-free, in seq order across >=1 forced handoff, and equals the
    final Remote readback. Payloads are sized 128-256 KiB (and the client
    rcvbuf capped) so stall-window bytes defeat hyper's write path + kernel
    + client buffering — 25 tiny batches never lag."""
    server = start_server()
    wait_health()
    create_basin()

    prime_deadline = time.monotonic() + 60
    prime_seq, why = None, {}
    while time.monotonic() < prime_deadline:
        prime_seq = append_one(f"s{seed}-prime", why)
        if prime_seq is not None:
            break
        time.sleep(0.25)
    if prime_seq is None:
        fail(3, f"prime append never acked: {why.get('reason', 'unknown')}")

    drop_seq = None
    if os.environ.get("ORACLE_SELFTEST") == "gap":
        drop_seq = 5  # inside the first live segment
    # 32 KiB rcvbuf: a stalled client then absorbs almost nothing, so the
    # server's writes back up into its own send buffer within a few batches
    # and the broadcast receiver falls behind for real (first shakeout: with
    # default buffers ~5 MB of stall bytes were absorbed and Lagged never
    # fired — 4/4 vacuous)
    follower = Follower(start_seq=0, drop_seq=drop_seq, rcvbuf=32768)
    follower.start()

    state = {"appends": 0}

    def burst(n, size, tag):
        for i in range(n):
            body = f"s{seed}-{tag}-{state['appends']:05d}-"
            body += "x" * max(0, size - len(body))
            w = {}
            deadline = time.monotonic() + 30
            seq = None
            while time.monotonic() < deadline:
                seq = append_one(body, w)
                if seq is not None:
                    break
                time.sleep(0.1)
            if seq is None:
                fail(3, f"{tag} append {i} never acked: "
                        f"{w.get('reason', 'unknown')}")
            state["appends"] += 1

    def wait_follow_mode(min_idx, deadline_s=60):
        """Wait until a batch WITH tail lands at index >= min_idx — proof
        the session (re-)entered live follow. Live follow only emits on new
        appends, so nudge while waiting."""
        deadline = time.monotonic() + deadline_s
        while time.monotonic() < deadline:
            if any(b["has_tail"] for b in list(follower.batches)[min_idx:]):
                return
            burst(1, 2048, "nudge")
            time.sleep(0.25)
        fail(3, f"session never (re-)entered live follow after batch index "
                f"{min_idx} — no tail-carrying batch, setup issue")

    burst(12, 2048, "live0")  # establish the stream + initial catch-up
    n_stalls = 1 + seed % 3
    payload = 131072 + ((seed >> 7) % 3) * 65536  # 128/192/256 KiB
    log(f"n_stalls={n_stalls} payload={payload}B")
    gate_idx = 0
    for s in range(n_stalls):
        wait_follow_mode(gate_idx)
        n_before = len(follower.batches)
        follower.pause()
        n_burst = 70 + ((seed >> (3 + 4 * s)) % 41)  # 70..110 large appends
        burst(n_burst, payload, f"stall{s}")
        time.sleep(0.5)  # all acked (durable) — let the channel churn rest
        follower.unpause()
        target = state["appends"] + 1  # + prime
        if not wait_observed(follower, target, deadline_s=120):
            # gap/dup in the partial drain = RED (the loss shape)
            partial_delivery_verdict(follower, target,
                                     f"post-stall-{s} drain")
        log(f"stall {s}: {n_burst} appends x {payload}B while stalled; "
            f"batches {n_before} -> {len(follower.batches)}")
        gate_idx = len(follower.batches)

    follower.stop()
    follower.join(timeout=10)
    observed = list(follower.observed)
    batches = list(follower.batches)
    log(f"follower: {len(observed)} records in {len(batches)} batches, "
        f"ended={follower.ended}")

    # verify BEFORE the vacuity exit — a real loss must never be masked by
    # a missing witness
    check_wellformed(observed, "lagged follow")
    invariant("follow_wellformed", "follow-gap-and-dup-free", True,
              f"delivered stream tiles seq 0..{len(observed)} exactly once "
              f"across {n_stalls} stall(s)")

    tail = wait_stream_ready()
    tail_seq = tail["tail"]["seq_num"]
    readback = read_all(tail_seq)
    verify_readback_dense(readback, tail_seq, len(observed))
    verify_observed_in_readback(observed, readback)
    if len(observed) != tail_seq:
        fail(1, f"follower drained {len(observed)} records but tail is "
                f"{tail_seq} — delivery stopped short of the stream",
             inv=("follow_wellformed", "follow-gap-and-dup-free"))

    # anti-vacuity: the handoff witness — a catch-up batch (no tail) after
    # follow batches (tail present) proves RecvError::Lagged re-entered the
    # scan. A no-witness trial proved nothing about the seam: VOID.
    seen_tail = False
    handoffs = 0
    for b in batches:
        if b["has_tail"]:
            seen_tail = True
        elif seen_tail:
            handoffs += 1
    if handoffs == 0:
        fail(3, f"no catch-up re-entry witnessed ({len(batches)} batches, "
                f"none tail-less after a tail-carrying one) — Lagged never "
                f"fired, vacuous")
    invariant("nonvacuous", "lagged-handoff-witnessed", True,
              f"{handoffs} catch-up batch(es) after follow handoff across "
              f"{n_stalls} stall(s)")

    server.terminate()
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
    elif mode == "last-event-id-resume":
        run_last_event_id_resume(seed)
    elif mode == "slow-follower-lagged":
        run_slow_follower_lagged(seed)
    else:
        fail(3, f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
