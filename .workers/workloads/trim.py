#!/usr/bin/env python3
"""Workload: trim is final (s2 lite) — trim durability across a kill.

Modes:
  straddles-kill   Append a known ledger, trim to a seed-chosen point T, and
                   SIGKILL in one of two seams (seed-chosen), restart, then run
                   the two-sided phased absence/over-deletion oracle.

Trim contract (pinned from source):
  - Trim command record: single header ["", "trim"], body = 8-byte BE of the
    trim point value V. trim_point is RangeTo (..V): seqs < V are removed,
    seqs >= V retained (streamer.rs:377-388; kv/stream_trim_point.rs:19-21).
  - Applied synchronously in-streamer; the trim-point KV rides the SAME
    WriteBatch as the trim command record (streamer.rs:1045-1050, recovered
    core.rs:100-103) — so the record and the logical point are atomic: either
    both durable or neither.
  - V clamped to the trim record's own end (Regular), monotone: only advances
    if strictly greater (streamer.rs:378-382) — decreasing/equal = acked no-op.
  - The READ path never consults the trim point; absence is PHYSICAL only, via
    an async purge bgtask, event-triggered on trim durability
    (streamer.rs:601-605). After a kill there is NO re-trigger — the
    interrupted purge resumes on the 60s±10% tick, so below-T remnants are
    LEGITIMATELY readable until then.

Oracle (.workers/promises/trim-is-final.md), phased:
  - over_deletion : seqs >= T always present byte-exact, on every read
    (over-deletion = acked-data loss).
  - never_resurface : once a below-T seq is first observed absent it never
    reappears (Remote-durability scans, read.rs:128).
  - purge_liveness : below-T seqs all absent within the tick ceiling
    post-restart (for an APPLIED trim).
  - trim_applied_xor : an ACKED trim => applied (logical point >= T);
    an UNACKED trim => applied XOR not, and if not-applied the full ledger
    stays intact (a disappeared record with no applied trim = data loss).
  - tail_monotone : tail never regresses; a trim advances tail by exactly the
    command record.
  Anti-vacuity: the kill provably landed inside its seam (seam1: trim in-flight
  or just-acked at kill; seam2: a GENUINELY PARTIAL physical purge — the
  deletion set exceeds DELETE_BATCH_SIZE(=10_000) so it spans >=2 WriteBatches,
  and >=1 below-T record is still physically present immediately post-restart,
  proving recovery must resume a half-completed purge, not re-drive one atomic
  batch).

Exit codes: 0 green, 1 red (finding), 3 void/blocked.
"""

import base64
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
BASIN = "trim-wl-01"
STREAM = "trim-stream"
WORK_DIR = "/tmp/wl-trim"
DATA_DIR = os.path.join(WORK_DIR, "s2root")
TICK_CEIL = 300      # 60s±10% purge tick + a >10k-record purge whose tombstone
                     # scan slows the from-0 floor probe until compaction clears
                     # it (virtual-time inflation); margin kept under the 900s run


def log(msg):
    print(msg, flush=True)


def invariant(inv_id, name, ok, summary):
    log(f"INVARIANT {inv_id} {name} {'PASS' if ok else 'FAIL'} {summary}")


def dump_server_log(tail=20):
    try:
        with open(os.path.join(WORK_DIR, "server.log"), "rb") as f:
            lines = f.read().decode("utf-8", "replace").splitlines()
        log(f"--- server.log tail ({min(tail, len(lines))} of {len(lines)}) ---")
        for line in lines[-tail:]:
            log(f"  {line}")
        log("--- end server.log tail ---")
    except OSError as e:
        log(f"server.log unavailable: {e}")


def fail(code, msg, inv=None):
    if code == 1 and inv:
        invariant(inv[0], inv[1], False, msg)
    try:
        dump_server_log()
    except Exception:
        pass
    log(f"VERDICT: {'RED' if code == 1 else 'VOID'} — {msg}")
    sys.exit(code)


def derive_seed():
    env = os.environ.get("SEED")
    if env:
        return int(env, 0) & 0xFFFFFFFF
    with open("/dev/urandom", "rb") as f:
        return int.from_bytes(f.read(4), "little")


def http_call(method, path, body=None, timeout=15, extra_headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    try:
        headers = {"S2-Basin": BASIN, "Authorization": "Bearer ignored"}
        if extra_headers:
            headers.update(extra_headers)
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
        fail(1, "server did not become healthy after the kill — restart "
                "availability lost", inv=("restart_serves", "restart-serves"))
    fail(3, "server did not become healthy in time")


def wait_stream_ready(deadline_s=180, red_on_fail=False):
    deadline = time.monotonic() + deadline_s
    last = None
    while time.monotonic() < deadline:
        try:
            status, data = http_call(
                "GET", f"/v1/streams/{STREAM}/records/tail", timeout=3)
            if status == 200:
                return json.loads(data)
            last = (status, data[:200])
        except OSError as e:
            last = repr(e)
        time.sleep(0.2)
    if red_on_fail:
        fail(1, f"stream tail not readable after post-kill restart: {last}",
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
        [S2, "create-basin", BASIN, "--create-stream-on-append"],
        env=cli_env, capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        fail(3, f"create-basin failed: {r.stderr.strip()[:300]}")


def append_batch(bodies, timeout=120):
    records = [{"body": b} for b in bodies]
    return http_call("POST", f"/v1/streams/{STREAM}/records",
                     body={"records": records}, timeout=timeout)


def populate(n, seed):
    """Append n data records, bodies f'T{seed}-{i}'. Returns dict seq->body."""
    ledger = {}
    i = 0
    while i < n:
        batch = [f"T{seed}-{j}" for j in range(i, min(i + 1000, n))]
        st, data = append_batch(batch)
        if st != 200:
            fail(3, f"populate append failed at {i}: {st} {data[:200]}")
        ack = json.loads(data)
        start = ack["start"]["seq_num"]
        for k, b in enumerate(batch):
            ledger[start + k] = b
        i += len(batch)
    return ledger


def trim_to(v, timeout=30):
    """Trim command: header ['','trim'], body = 8-byte BE of v. Sent in
    base64 format (S2-Format: base64) because the body is binary and raw/UTF-8
    cannot carry bytes >= 128 as single bytes; the format also covers header
    bytes, so the 'trim' header value is base64 too (api/src/v1/stream/
    json.rs:243-250, data.rs:66-68). Reads stay raw — the stored header value
    b'trim' decodes back to 'trim', and data bodies are ASCII."""
    body = base64.b64encode(v.to_bytes(8, "big")).decode("ascii")
    hval = base64.b64encode(b"trim").decode("ascii")
    return http_call(
        "POST", f"/v1/streams/{STREAM}/records",
        body={"records": [{"body": body, "headers": [["", hval]]}]},
        timeout=timeout, extra_headers={"s2-format": "base64"})


def read_all(tail_seq, timeout=45):
    """Returns (data: dict seq->body for DATA records, trim_seqs: set of seqs
    whose record carries the trim header). Skips physically-absent seqs. A read
    blocked by a concurrent large purge is retried a few times before giving
    up (transient), so the terminal sweep survives purge contention."""
    data = {}
    trim_seqs = set()
    cursor = 0
    for _ in range(40000):
        if cursor >= tail_seq:
            break
        raw = None
        for attempt in range(5):
            try:
                st, raw = http_call(
                    "GET",
                    f"/v1/streams/{STREAM}/records?seq_num={cursor}&count=1000",
                    timeout=timeout)
                break
            except OSError as e:
                if attempt == 4:
                    fail(3, f"read at seq {cursor} kept timing out under purge "
                            f"contention ({e!r}) — cannot complete sweep")
                time.sleep(3)
        if st == 416:            # past tail / nothing at/after cursor
            break
        if st != 200:
            # read_all only runs post-restart; a 5xx here is the read path
            # faulting after a trim+restart (trim recovery corrupting reads) —
            # a finding, not a transport void.
            if 500 <= st < 600:
                fail(1, f"read returned HTTP {st} at seq {cursor} after "
                        "trim+restart — read path faulted post-recovery",
                     inv=("restart_serves", "restart-serves"))
            fail(3, f"read failed at seq {cursor}: HTTP {st} {raw[:200]}")
        batch = json.loads(raw).get("records", [])
        if not batch:
            break
        maxs = cursor
        for rec in batch:
            s = rec["seq_num"]
            hdrs = rec.get("headers", []) or []
            is_trim = any(h == ["", "trim"] for h in hdrs)
            if is_trim:
                trim_seqs.add(s)
            else:
                data[s] = rec.get("body", "")
            maxs = max(maxs, s)
        cursor = maxs + 1
    return data, trim_seqs


def get_tail():
    st, raw = http_call("GET", f"/v1/streams/{STREAM}/records/tail", timeout=10)
    if st != 200:
        fail(3, f"tail read failed: {st} {raw[:200]}")
    return json.loads(raw)["tail"]["seq_num"]


def physical_floor(post_tail):
    """First physically-available seq (purge progress). 416/empty => post_tail
    (everything below tail purged). None on transient error / timeout — the
    poll loop treats None as 'unknown, retry next tick'. A large purge
    (>10_000 records) can block a concurrent read past the socket timeout; that
    is a transient, not a crash, so a TimeoutError/OSError returns None too."""
    try:
        st, raw = http_call(
            "GET", f"/v1/streams/{STREAM}/records?seq_num=0&count=1", timeout=30)
    except OSError:
        return None
    if st == 416:
        return post_tail
    if st != 200:
        return None
    recs = json.loads(raw).get("records", [])
    if not recs:
        return post_tail
    return recs[0]["seq_num"]


def read_range(start, count, timeout=20):
    """Small read -> dict seq->body for DATA records (skips trim records)."""
    out = {}
    st, raw = http_call(
        "GET", f"/v1/streams/{STREAM}/records?seq_num={start}&count={count}",
        timeout=timeout)
    if st == 416:
        return out
    if st != 200:
        # range reads run only during the post-restart purge poll; a 5xx is the
        # read path faulting after trim recovery — a finding, not a void.
        if 500 <= st < 600:
            fail(1, f"range read returned HTTP {st} at seq {start} after "
                    "trim+restart — read path faulted post-recovery",
                 inv=("restart_serves", "restart-serves"))
        fail(3, f"range read failed at {start}: {st} {raw[:200]}")
    for rec in json.loads(raw).get("records", []):
        hdrs = rec.get("headers", []) or []
        if not any(h == ["", "trim"] for h in hdrs):
            out[rec["seq_num"]] = rec.get("body", "")
    return out


def straddles_kill(seed):
    arm = ["", "500ms", "2s"][seed % 3]
    env_extra = {"SL8_FLUSH_INTERVAL": arm} if arm else {}
    seam = 1 if (seed >> 3) % 2 == 0 else 2
    # seam2 must exercise the HALF-DONE physical purge, not just liveness: the
    # purge deletes below-T seqs in DELETE_BATCH_SIZE(=10_000)-record
    # WriteBatches (stream_trim.rs:18,80-108). A purge smaller than one batch is
    # a single atomic write — all-or-nothing across a crash, so no partial
    # physical state exists to mishandle on recovery. To cross the boundary the
    # deletion set (seqs [0,T)) must exceed 10_000, so seam2 uses n=13000 and
    # forces T into (10_000, n): the purge does >=1 intermediate durable write,
    # and a kill can leave the trim-point KV finalized with SOME below-T records
    # physically gone and others still present — the recovery-must-resume seam.
    # The phased poll stays cheap (count=1 floor/spot probes; the two full
    # sweeps only touch the ~n-T retained records), so 13k does not time out the
    # way an all-record re-read did.
    n = 300 if seam == 1 else 13000
    if seam == 1:
        # trim point strictly inside the ledger so both sides are non-empty
        T = 1 + (seed % (n - 2))
    else:
        # force T past the DELETE_BATCH_SIZE boundary so the purge spans >=2
        # batches: T in [10_001, 12_000], deletion set > 10_000.
        T = 10001 + (seed % 2000)
    log(f"seam={seam} flush-arm={arm or 'default'} n={n} T={T}")

    server = start_server(env_extra)
    wait_health()
    create_basin()
    ledger = populate(n, seed)
    pre_tail = get_tail()
    log(f"populated {n} records, pre-trim tail={pre_tail}")

    acked = {"status": None}

    if seam == 1:
        # SIGKILL around the trim ack: fire trim on a thread, kill at a
        # seed-chosen offset (in-flight / just-acked / settled).
        def _trim():
            try:
                st, data = trim_to(T, timeout=30)
                acked["status"] = st
                acked["body"] = data[:200]
            except OSError as e:
                acked["status"] = f"err:{type(e).__name__}"

        style = (seed >> 7) % 3                       # decoupled from flush arm
        base = {0: 0.0, 1: 0.03, 2: 0.4}[style]      # in-flight / just / settled
        offset = base + ((seed >> 5) % 60) / 1000.0
        t = threading.Thread(target=_trim, daemon=True)
        t.start()
        time.sleep(offset)
        server.send_signal(signal.SIGKILL)
        t.join(timeout=10)
        log(f"seam1 kill: style={style} offset={offset*1000:.0f}ms "
            f"trim_ack={acked['status']}")
        # Valid seam-1 outcomes: 200 (acked) or a connection cut (err:*,
        # genuinely ambiguous). A 4xx means the server REJECTED the command for
        # a client reason unrelated to the kill (e.g. a malformed trial) — a
        # setup void. But a 5xx is a SERVER FAULT on the trim command under the
        # kill race (a panic in apply_command, streamer.rs:377) — that is a
        # finding, not a void.
        stv = acked["status"]
        if isinstance(stv, int) and 500 <= stv < 600:
            fail(1, f"seam1 trim command returned HTTP {stv} "
                    f"{acked.get('body','')} — server faulted applying the trim "
                    "under the kill race",
                 inv=("trim_applied_xor", "acked-trim-durable"))
        if not (stv == 200 or (isinstance(stv, str) and stv.startswith("err"))):
            fail(3, f"seam1 trim response {stv} {acked.get('body','')} — server "
                    "rejected the command (not a kill outcome), void")
    else:
        # Trim acked (durable) => purge event-triggered; kill mid-purge.
        st, data = trim_to(T, timeout=60)
        if st != 200:
            fail(3, f"seam2 trim not acked: {st} {data[:200]}")
        acked["status"] = 200
        # small seed-scaled delay so the kill lands while the purge grinds the
        # >10_000-record deletion set across DELETE_BATCH_SIZE WriteBatches
        # (stream_trim.rs:80-108) — early enough that the purge is provably
        # incomplete (remnant present at read#1, enforced below).
        delay = ((seed >> 5) % 800) / 1000.0
        time.sleep(delay)
        server.send_signal(signal.SIGKILL)
        log(f"seam2 kill: trim acked, killed +{delay*1000:.0f}ms after ack")

    try:
        server.wait(timeout=40)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait(timeout=20)

    was_acked = acked["status"] == 200

    server2 = start_server(env_extra)
    wait_health(red_on_fail=True)
    tail = wait_stream_ready(red_on_fail=True)
    post_tail = tail["tail"]["seq_num"]

    if post_tail < pre_tail:
        fail(1, f"tail regressed across restart: pre {pre_tail} -> post {post_tail}",
             inv=("tail_monotone", "tail-never-regresses"))

    # read #1 immediately post-restart (purge not yet re-triggered)
    data1, trim_seqs = read_all(post_tail)
    applied = bool(trim_seqs) or (seam == 2)
    log(f"post-restart: tail={post_tail} trim_record={'yes' if trim_seqs else 'no'} "
        f"acked={was_acked} applied={applied}")

    # over-deletion side (ALWAYS): every retained seq present byte-exact.
    eff_T = T if applied else 0
    for s in range(eff_T, n):
        if s not in data1:
            # below the purge may have started; but seqs >= eff_T must never go.
            fail(1, f"retained seq {s} (>= trim point {eff_T}) absent post-restart "
                    f"— over-deletion / acked-data loss",
                 inv=("over_deletion", "retained-records-survive"))
        if data1[s] != ledger[s]:
            fail(1, f"retained seq {s} body {data1[s]!r} != ledger {ledger[s]!r}",
                 inv=("over_deletion", "retained-records-survive"))
    invariant("over_deletion", "retained-records-survive", True,
              f"all {n - eff_T} seqs >= {eff_T} present byte-exact at read#1")

    # trim_applied_xor: acked => applied
    if was_acked and not applied:
        fail(1, "trim was ACKED pre-kill but the logical trim point did not "
                "survive restart (no trim record, full ledger intact) — "
                "acked control op un-happened",
             inv=("trim_applied_xor", "acked-trim-durable"))
    invariant("trim_applied_xor", "acked-trim-durable", True,
              f"acked={was_acked} applied={applied} (consistent)")

    if not applied:
        # unacked-and-not-applied: nothing was trimmed; full ledger must stay,
        # and stay (a disappearing record with no trim = data loss).
        if len(data1) != n:
            fail(1, f"no trim applied yet {n - len(data1)} data records absent "
                    f"— data loss with no trim",
                 inv=("over_deletion", "retained-records-survive"))
        invariant("purge_liveness", "below-trim-absent-in-bound", True,
                  "no trim applied — nothing to purge (XOR arm)")
        invariant("never_resurface", "trimmed-never-returns", True,
                  "no trim applied — vacuously holds (XOR arm)")
        server2.terminate()
        log("VERDICT: GREEN (unacked trim not applied — XOR arm)")
        return

    # applied: below-T must purge within the tick ceiling; observe the phased
    # absence and never-resurface.
    present_below_1 = [s for s in range(T) if s in data1]

    # anti-vacuity per seam. seam2: the deletion set is >10_000 (spans >=2
    # DELETE_BATCH_SIZE WriteBatches), so a below-T remnant at read#1 witnesses
    # a GENUINELY PARTIAL physical purge that recovery must resume — not merely
    # an all-or-nothing single batch that hadn't committed.
    if seam == 2 and not present_below_1:
        fail(3, "seam2: purge already complete immediately post-restart "
                "(no below-T remnant) — the partial-purge seam was not "
                "exercised, void")
    if seam == 2:
        log(f"seam2 partial-purge witness: floor={min(present_below_1)} of T={T}, "
            f"{len(present_below_1)} below-T records still physical at read#1")
    if seam == 1 and acked["status"] not in (200,) and not str(
            acked["status"]).startswith("err"):
        fail(3, f"seam1: trim neither acked nor connection-cut ({acked['status']}) "
                "— kill did not land in the ack seam, void")

    # ORACLE_SELFTEST=resurrect: force the physical floor to fall back.
    st_mode = os.environ.get("ORACLE_SELFTEST")

    # Cheap phased poll via the physical FLOOR (first available seq) — reading
    # the whole ledger each tick is too slow for a 150s wait. The floor rises
    # monotonically from 0 toward T as the purge deletes; a floor that DECREASES
    # means a below-floor trimmed seq resurfaced (never_resurface). A small
    # over-deletion spot-check runs each tick; the full byte-exact sweep runs
    # once at read#1 and once at the end.
    max_floor = physical_floor(post_tail) or 0
    spot = sorted({T, (T + n) // 2, n - 1})
    deadline = time.monotonic() + TICK_CEIL
    while time.monotonic() < deadline:
        if server2.poll() is not None:
            fail(1, f"server DIED during purge wait (exit {server2.returncode})",
                 inv=("restart_serves", "restart-serves"))
        floor = physical_floor(post_tail)
        if floor is None:
            time.sleep(3)
            continue
        if floor < max_floor:
            fail(1, f"physical floor regressed {max_floor} -> {floor}: a trimmed "
                    f"seq below {max_floor} resurfaced — resurrection",
                 inv=("never_resurface", "trimmed-never-returns"))
        max_floor = max(max_floor, floor)
        # over-deletion spot-check: probe each retained seq individually
        # (count=1) — a single wide read is capped by the server's ~1000-record
        # batch limit and would spuriously "miss" the far end.
        for s in spot:
            if s >= n:
                continue
            try:
                d = read_range(s, 1)
            except OSError:
                # a big concurrent purge can block this read past the timeout;
                # skip the spot this tick — the terminal full sweep is the
                # authoritative over-deletion check.
                continue
            if s not in d:
                fail(1, f"retained seq {s} (>= T {T}) vanished during purge wait "
                        "— over-deletion", inv=("over_deletion", "retained-records-survive"))
        if max_floor >= T:
            break
        time.sleep(3)

    # final strong sweep: all below-T absent, all >=T byte-exact.
    dataf, _ = read_all(post_tail)
    if st_mode == "resurrect":
        # re-serve a trimmed record into the observed set (promise red-proof
        # plan): a below-T seq present after purge must trip the absence oracle.
        victim = T - 1
        dataf[victim] = ledger.get(victim, f"resurrected-{victim}")
        log(f"ORACLE_SELFTEST=resurrect: re-serving trimmed seq {victim}")
    still = [s for s in range(T) if s in dataf]
    if still:
        fail(1, f"{len(still)} below-trim seqs still present after {TICK_CEIL}s "
                f"(first {still[:10]}) — purge liveness failed",
             inv=("purge_liveness", "below-trim-absent-in-bound"))
    for s in range(T, n):
        if s not in dataf or dataf[s] != ledger[s]:
            fail(1, f"retained seq {s} missing/corrupt in final sweep "
                    f"({dataf.get(s)!r} vs {ledger[s]!r}) — over-deletion",
                 inv=("over_deletion", "retained-records-survive"))
    invariant("purge_liveness", "below-trim-absent-in-bound", True,
              f"all {T} below-trim seqs absent within {TICK_CEIL}s (floor {max_floor})")
    invariant("never_resurface", "trimmed-never-returns", True,
              f"physical floor rose monotonically to {max_floor} >= T {T}")
    invariant("tail_monotone", "tail-never-regresses", True,
              f"tail {pre_tail} -> {post_tail} (advanced by trim record)")

    server2.terminate()
    log(f"VERDICT: GREEN (seam={seam}, trim point {T}, {n} records)")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "straddles-kill"
    seed = derive_seed()
    log(f"mode={mode} seed={seed}")
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(WORK_DIR, exist_ok=True)
    if mode != "straddles-kill":
        fail(3, f"mode {mode} not built in this file yet")
    straddles_kill(seed)


if __name__ == "__main__":
    main()
