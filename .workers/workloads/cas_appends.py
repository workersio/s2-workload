#!/usr/bin/env python3
"""Workload: CAS (match_seq_num) appends are exactly-once (s2 lite).

Modes:
  storm-across-kill   N concurrent writers race match_seq_num appends at the
                      contended tail; SIGKILL mid-storm (gated on >=2 in-flight
                      CAS + arm-scaled acks); restart; resolve every ambiguous
                      in-flight CAS by retrying it with its ORIGINAL
                      match_seq_num and payload, and reconstruct the ledger.

CAS contract (verified in source):
  POST /v1/streams/{s}/records {"records":[{"body":..}], "match_seq_num": N}
  200 -> {"start":{"seq_num":N,..},"end":{"seq_num":N,..}} (single record: the
         match won position N).
  412 -> {"seq_num_mismatch": K}  (expected next seq; positions [.., K) durable
         at delivery per the deferred-412 contract, append.rs:236-247)
      or {"fencing_token_mismatch": "<tok>"} (fencing precedes seq,
         streamer.rs:341 before :350). Tokenless data appends never hit this.

Oracle (.workers/promises/cas-appends-exactly-once.md):
  - cas_single_winner : per position exactly one 200 winner (record-time +
    read-back single-occupant).
  - acked_winner_durable : every pre-kill 200 winner present at its position
    post-restart with its exact payload (durability).
  - deferred_412_durable : every pre-kill 412's named K <= final tail — the
    state a delivered 412 promised was durable never evaporates across the
    crash.
  - no_double_apply : an ambiguous retry with the ORIGINAL match_seq_num that
    412s while its position read-back == the original payload proves the
    original won exactly once; a retry that 200s a position already holding the
    original payload = double-apply. at-most-once by payload also holds.
  - dense_prefix : read-back tiles seq 0..tail.
  - restart_serves : server serves after the kill.
  Anti-vacuity: >=1 ambiguous in-flight CAS at kill AND >=1 recorded 412 loss
  (real contention) AND enough winners.

  Fence fold-in (critic set-gap): a fraction of attempts are CAS-guarded FENCE
  command records (headers [["","fence"]], unique token body, tokenless so the
  fence-gate passes) — the canonical lock-takeover pattern. Same ledger rules
  by POSITION; content identity for fence-won positions is by the token body.

Exit codes: 0 green, 1 red (finding), 3 void/blocked.
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
BASIN = "cas-wl-01"
STREAM = "cas-appends"
WORK_DIR = "/tmp/wl-cas"
DATA_DIR = os.path.join(WORK_DIR, "s2root")
WRITERS = 6


def log(msg):
    print(msg, flush=True)


def invariant(inv_id, name, ok, summary):
    log(f"INVARIANT {inv_id} {name} {'PASS' if ok else 'FAIL'} {summary}")


def dump_server_log(tail=20):   # wio workloads-logs tails ~64 lines
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


def http_call(method, path, body=None, timeout=15):
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
        fail(1, f"stream tail not readable after post-kill restart: {last} — "
                "acked data unreachable", inv=("restart_serves", "restart-serves"))
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


def cas_append(pos, payload, fence=False, timeout=15):
    """One CAS attempt at match_seq_num=pos. Returns:
      ('win', seq)                 -> 200, occupied seq (== pos)
      ('loss', K)                  -> 412 seq_num_mismatch, expected next K
      ('fence412', tok)            -> 412 fencing_token_mismatch (unexpected)
      ('other', (status, body))    -> any other HTTP status
    Raises OSError on connection failure (ambiguous — server may have applied)."""
    rec = {"body": payload}
    if fence:
        rec["headers"] = [["", "fence"]]
    status, data = http_call(
        "POST", f"/v1/streams/{STREAM}/records",
        body={"records": [rec], "match_seq_num": pos}, timeout=timeout)
    if status == 200:
        ack = json.loads(data)
        return ("win", ack["start"]["seq_num"])
    if status == 412:
        body = json.loads(data)
        if "seq_num_mismatch" in body:
            return ("loss", body["seq_num_mismatch"])
        if "fencing_token_mismatch" in body:
            return ("fence412", body["fencing_token_mismatch"])
        return ("other", (status, data[:200]))
    return ("other", (status, data[:200]))


class Storm:
    def __init__(self):
        self.lock = threading.Lock()
        self.in_flight = 0
        self.wins = 0
        self.last_win_at = 0.0
        self.killed = False
        self.winners = {}   # pos -> {payload, writer, fence}


def storm_writer(w, seed, storm, out):
    """Connection-serial CAS writer. Tracks its own view of the contended
    position; on 412 it jumps to the returned next-seq. Records every attempt
    with its outcome; the request outstanding at the kill becomes 'ambiguous'."""
    pos = 0
    rnd = (seed ^ (w * 2654435761)) & 0xFFFFFFFF
    for i in range(20000):
        if storm.killed:
            break
        rnd = (rnd * 1103515245 + 12345) & 0xFFFFFFFF
        is_fence = (rnd >> 28) == 0            # ~1/16 attempts are fences
        payload = (f"F{seed}-w{w}-a{i:05d}" if is_fence
                   else f"D{seed}-w{w}-a{i:05d}")
        entry = {"writer": w, "attempt": i, "pos": pos, "payload": payload,
                 "fence": is_fence, "status": None, "expected": None}
        with storm.lock:
            storm.in_flight += 1
        try:
            kind, val = cas_append(pos, payload, fence=is_fence)
        except OSError:
            with storm.lock:
                storm.in_flight -= 1
            entry["status"] = "ambiguous"
            out.append(entry)
            break
        now = time.monotonic()
        with storm.lock:
            storm.in_flight -= 1
            if kind == "win":
                prior = storm.winners.get(val)
                if prior is not None:
                    # two 200s for the same position — caught live
                    entry["status"] = "double_win"
                    entry["conflict"] = prior
                    out.append(entry)
                    fail(1, f"position {val} acked to TWO winners: "
                            f"{prior} and {entry}",
                         inv=("cas_single_winner", "one-winner-per-position"))
                storm.winners[val] = {"payload": payload, "writer": w,
                                      "fence": is_fence}
                storm.wins += 1
                storm.last_win_at = now
        if kind == "win":
            entry["status"] = "win"
            entry["pos"] = val
            pos = val + 1
        elif kind == "loss":
            entry["status"] = "loss"
            entry["expected"] = val
            pos = val
        elif kind == "fence412":
            entry["status"] = "fence412"
            out.append(entry)
            fail(1, f"tokenless append hit fencing_token_mismatch {val!r} — "
                    f"contract says tokenless is always accepted",
                 inv=("cas_single_winner", "one-winner-per-position"))
        else:
            entry["status"] = "other"
            entry["detail"] = val
            if storm.killed:
                out.append(entry)
                break
            time.sleep(0.02)
        out.append(entry)
        time.sleep(((seed >> (w * 3)) % 4) / 4000.0)


def run_storm(seed, server, window):
    base, span = {0.005: (40, 300), 0.5: (18, 60), 2.0: (10, 20)}[window]
    kill_after = base + (seed >> 2) % span
    log(f"writers={WRITERS} kill_after={kill_after} flush_window={window}s")

    storm = Storm()
    per_writer = [[] for _ in range(WRITERS)]
    threads = [threading.Thread(target=storm_writer,
                                args=(w, seed, storm, per_writer[w]),
                                daemon=True)
               for w in range(WRITERS)]
    for t in threads:
        t.start()

    deadline = time.monotonic() + 240
    reached = False
    while time.monotonic() < deadline:
        with storm.lock:
            if storm.wins >= kill_after:
                reached = True
                break
        time.sleep(0.005)
    if not reached:
        storm.killed = True
        with storm.lock:
            got = storm.wins
        fail(3, f"kill point never reached ({got}/{kill_after} wins) — vacuous")

    spin_deadline = time.monotonic() + 10
    while time.monotonic() < spin_deadline:
        with storm.lock:
            if storm.in_flight >= 2:
                break
        time.sleep(0.0005)
    with storm.lock:
        infl_at_kill = storm.in_flight
        last_win_age = time.monotonic() - storm.last_win_at
    server.send_signal(signal.SIGKILL)
    storm.killed = True
    for t in threads:
        t.join(timeout=20)
    log(f"kill: in_flight={infl_at_kill} last_win_age={last_win_age*1000:.1f}ms")

    manifest = []
    for entries in per_writer:
        manifest.extend(entries)
    return manifest, infl_at_kill, last_win_age


def read_all(tail_seq):
    """Returns dict pos -> body over [0, tail_seq)."""
    out = {}
    cursor = 0
    for _ in range(20000):
        if cursor >= tail_seq:
            break
        status, data = http_call(
            "GET", f"/v1/streams/{STREAM}/records?seq_num={cursor}&count=1000",
            timeout=30)
        if status != 200:
            fail(3, f"read failed at seq {cursor}: HTTP {status} {data[:200]}")
        batch = json.loads(data).get("records", [])
        if not batch:
            fail(1, f"read returned no records at seq {cursor} < tail {tail_seq}"
                    " — gap below tail",
                 inv=("dense_prefix", "gapless-below-tail"))
        for rec in batch:
            out[rec["seq_num"]] = rec.get("body", "")
        cursor = max(out) + 1
    return out


def verify(manifest, readback, tail_seq, infl_at_kill):
    winners = {}   # pos -> entry
    losses = []
    ambiguous = []
    for m in manifest:
        st = m["status"]
        if st == "win":
            winners[m["pos"]] = m
        elif st == "loss":
            losses.append(m)
        elif st == "ambiguous":
            ambiguous.append(m)

    log(f"ledger: {len(winners)} winners, {len(losses)} losses, "
        f"{len(ambiguous)} ambiguous")

    # anti-vacuity
    if infl_at_kill < 2 or len(ambiguous) < 1:
        fail(3, f"only {infl_at_kill} in-flight / {len(ambiguous)} ambiguous at "
                "kill — no genuine in-flight CAS to resolve, void")
    if len(losses) < 1:
        fail(3, "no 412 loss recorded — no real per-position contention, void")
    if len(winners) < 8:
        fail(3, f"only {len(winners)} winners — below floor 8, void")

    # selftest red-proofs (forge a defect to prove the oracle bites)
    st_mode = os.environ.get("ORACLE_SELFTEST")
    if st_mode == "doubleapply":
        victim = winners[min(winners)]
        log(f"ORACLE_SELFTEST=doubleapply: duplicating winner {victim['payload']!r}")
        readback = dict(readback)
        readback[tail_seq] = victim["payload"]   # phantom 2nd copy
        tail_seq += 1
    elif st_mode == "phantom412":
        log("ORACLE_SELFTEST=phantom412: forging a 412 naming K beyond tail")
        losses = list(losses) + [{"pos": tail_seq + 100, "expected": tail_seq + 100,
                                  "payload": "phantom", "writer": -1}]
    elif st_mode == "retrydoubleapply":
        # forge the retry-200-double-apply escape: a synthetic ambiguous entry
        # whose position ALREADY holds its original payload, with the retry
        # forced to 200 (via _force_win) — must trip the no_double_apply
        # 200-guard, the real detector for the exploration's headline.
        data_winners = {p: w for p, w in winners.items() if not w["fence"]}
        vp = min(data_winners)
        vw = data_winners[vp]
        log(f"ORACLE_SELFTEST=retrydoubleapply: forging retry-200 on durable "
            f"pos {vp} holding {vw['payload']!r}")
        ambiguous = list(ambiguous) + [{
            "writer": vw["writer"], "attempt": -1, "pos": vp,
            "payload": vw["payload"], "fence": False, "status": "ambiguous",
            "expected": None, "_force_win": True}]

    # restart_serves already asserted by wait_stream_ready(red_on_fail);
    # dense prefix
    seqs = sorted(readback)
    if seqs != list(range(tail_seq)):
        fail(1, f"read-back not dense over 0..{tail_seq}: "
                f"first-gap around {next((i for i in range(tail_seq) if i not in readback), None)}",
             inv=("dense_prefix", "gapless-below-tail"))
    invariant("dense_prefix", "gapless-below-tail", True,
              f"{tail_seq} records tile seq 0..{tail_seq}")

    # at-most-once by payload. Both data (D) and fence (F) bodies round-trip as
    # the raw payload/token on read (Raw format default, api/src/data.rs:43;
    # common/src/record/mod.rs:90-118, json.rs:90-99) — so both are checked.
    by_body = {}
    for pos, body in readback.items():
        by_body.setdefault(body, []).append(pos)
    for body, at in by_body.items():
        if body[:1] in ("D", "F") and len(at) > 1:
            fail(1, f"run payload duplicated (at-most-once broken): "
                    f"{body!r} at {at}",
                 inv=("no_double_apply", "at-most-once"))

    # acked_winner_durable: every pre-kill 200 winner present at its position
    for pos, m in winners.items():
        if pos not in readback:
            fail(1, f"acked winner absent post-restart: pos {pos} "
                    f"payload {m['payload']!r} — durability lost",
                 inv=("acked_winner_durable", "acked-winner-survives"))
        if readback[pos] != m["payload"]:
            kind = "fence" if m["fence"] else "data"
            fail(1, f"position {pos} acked to {kind} winner {m['payload']!r} but "
                    f"read-back holds {readback[pos]!r} — overwrite/double-assign",
                 inv=("cas_single_winner", "one-winner-per-position"))
    invariant("acked_winner_durable", "acked-winner-survives", True,
              f"{len(winners)} pre-kill winners all present with their payloads")

    # deferred_412_durable: every pre-kill 412's named K <= final tail
    for m in losses:
        K = m["expected"]
        if K > tail_seq:
            fail(1, f"pre-kill 412 named next-seq {K} (attempt at pos {m['pos']}, "
                    f"writer {m['writer']}) but final tail is {tail_seq} — the "
                    f"durable state the 412 promised evaporated across the crash",
                 inv=("deferred_412_durable", "delivered-412-is-durable"))
    invariant("deferred_412_durable", "delivered-412-is-durable", True,
              f"all {len(losses)} pre-kill 412s name positions <= tail {tail_seq}")

    # no_double_apply: resolve each ambiguous by retrying its ORIGINAL
    # match_seq_num + payload; content identity discriminates double-apply.
    resolved = {"landed_before": 0, "landed_now": 0, "lost": 0}
    for m in ambiguous:
        pos, payload = m["pos"], m["payload"]
        pre = readback.get(pos)
        if m.get("_force_win"):        # selftest: forge the retry-200 escape
            kind, val = "win", pos
        else:
          try:
            kind, val = cas_append(pos, payload, fence=m["fence"])
          except OSError:
            time.sleep(2)
            try:
                kind, val = cas_append(pos, payload, fence=m["fence"])
            except OSError as e:
                fail(1, f"ambiguous retry pos {pos} could not reach server "
                        f"twice: {e}", inv=("restart_serves", "restart-serves"))
        if kind == "win":
            # position was empty -> our retry appended it. That is fine ONLY if
            # the original had not already landed durably (else read-back[pos]
            # would have held our payload and CAS would 412). Assert the payload
            # did not ALREADY exist elsewhere pre-retry (no double copy).
            if pre == payload:
                fail(1, f"ambiguous retry at pos {pos} returned 200 while "
                        f"read-back ALREADY held the original payload "
                        f"{payload!r} — double-apply",
                     inv=("no_double_apply", "retry-never-double-applies"))
            resolved["landed_now"] += 1
        elif kind == "loss":
            occ = pre
            if occ == payload:
                # original won exactly once pre-kill; retry correctly refused
                resolved["landed_before"] += 1
            else:
                # someone else occupies pos; original lost — legitimate
                resolved["lost"] += 1
        elif kind == "fence412":
            fail(1, f"ambiguous retry pos {pos} hit fencing mismatch — tokenless",
                 inv=("no_double_apply", "retry-never-double-applies"))
        else:
            fail(3, f"ambiguous retry pos {pos} unexpected: {val}")
    invariant("no_double_apply", "retry-never-double-applies", True,
              f"ambiguous resolved: {resolved['landed_before']} won-pre-kill "
              f"(retry 412, content-matched), {resolved['landed_now']} landed "
              f"on retry, {resolved['lost']} lost")

    log(f"oracle: {len(winners)} winners durable, {len(losses)} 412s durable, "
        f"{len(ambiguous)} ambiguous resolved, tail {tail_seq}")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "storm-across-kill"
    seed = derive_seed()
    log(f"mode={mode} seed={seed}")
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(WORK_DIR, exist_ok=True)

    if mode != "storm-across-kill":
        fail(3, f"mode {mode} not built in this file yet")

    arm = ["", "500ms", "2s"][seed % 3]
    env_extra = {"SL8_FLUSH_INTERVAL": arm} if arm else {}
    window = {"": 0.005, "500ms": 0.5, "2s": 2.0}[arm]
    log(f"flush-arm={arm or 'default'}")

    server = start_server(env_extra)
    wait_health()
    create_basin()

    manifest, infl_at_kill, last_win_age = run_storm(seed, server, window)
    wins = sum(1 for m in manifest if m["status"] == "win")
    log(f"attempts: {len(manifest)}, wins: {wins}")
    if last_win_age > max(window, 0.05) + 0.2:
        fail(3, f"last win {last_win_age*1000:.0f}ms before kill — nothing "
                "freshly acked inside the flush window, void")
    server.wait(timeout=30)

    server2 = start_server()
    wait_health(red_on_fail=True)
    tail = wait_stream_ready(red_on_fail=True)
    tail_seq = tail["tail"]["seq_num"]
    readback = read_all(tail_seq)
    verify(manifest, readback, tail_seq, infl_at_kill)
    invariant("restart_serves", "restart-serves", True,
              f"served after kill; tail {tail_seq}")
    server2.terminate()
    log("VERDICT: GREEN")


if __name__ == "__main__":
    main()
