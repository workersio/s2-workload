#!/bin/sh
# Workload: stream lifecycle — delete / purge / recreate (s2 lite).
#
# Mode doe-stale-deadline:
#   the source-visible leak (promises/stream-delete-recreate-resurrection
#   .md, doe-stale-deadline-across-recreate): finalize_trim deletes meta/
#   id-mapping/tail/fencing but NOT stream_doe_deadline keys
#   (stream_trim.rs:135-146); StreamId is a deterministic hash, so
#   incarnation 1's armed DOE deadline fires against incarnation 2 of the
#   same name. Streamer's eligibility check consults CURRENT config
#   (streamer.rs:458-461,497-501) — min_age None is Ineligible — so:
#     stream A: inc1 {age:1s, doe.min_age:1s} -> deadline ~= t0+602s
#               (doe_arm_delay = age + min_age + 600s, streamer.rs:59-63);
#               delete; recreate as inc2 {age:1s, doe.min_age:3600s},
#               kept EMPTY (its own deadline sits ~70min out; its
#               last_tail_write_timestamp = tail-key create_ts ~= recreate
#               time, core.rs:110-111/:161 — BELOW inc1's stale cutoff).
#     stream B: control — same inc1 shape; inc2 recreated with NO DOE.
#   Probe both every ~30s through inc1's deadline + tick + margin.
#   Invariants:
#   - wrongful_delete: inc2 of A must survive the whole window — it was
#     empty for ~10min against ITS OWN 60min floor, and its own schedule
#     cannot fire inside the window (t2+3600+600s). Deletion inside the
#     window = delete-on-empty honored a DEAD incarnation's deadline.
#   - control_survives: inc2 of B (no DOE) must survive — min_age None
#     is Ineligible (streamer.rs:460) even against the stale key.
#   - purge_liveness: recreate still gated 300s after delete = RED (the
#     purge is event-triggered at delete, streamer.rs:601-606; a wedged
#     gate on a healthy server is a product failure, not a void).
#   - post-window: a fresh append to inc2-A must be accepted (stream
#     not just present but serving).
#   BUILD CONSTRAINT (critic): never append to inc2-A during the wait —
#   one append bumps last_tail_write_timestamp past the stale cutoff and
#   neutralizes the trial. GET-config / check-tail probes are safe.
#   Basin is created WITHOUT create-stream-on-read/append: an auto-create
#   would silently resurrect the name under a probe and corrupt the
#   oracle.
#
# Self-contained: sh wrapper + embedded python3 (injection layers one file).
# Exit codes: 0 green, 1 red (finding), 3 void/blocked.
MODE="${1:-doe-stale-deadline}"
exec python3 - "$MODE" <<'PYEOF'
import http.client
import json
import os
import signal
import subprocess
import sys
import time

S2 = os.path.join(".workers", "vendor", "bin", "s2-linux-amd64")
PORT = 8080
BASIN = "lifecycle-wl-01"
WORK_DIR = "/tmp/wl-lifecycle"
DATA_DIR = os.path.join(WORK_DIR, "s2root")

ARM_PAD = 602          # doe_arm_delay(age=1, min_age=1) = 1 + 1 + 600
TICK_MAX = 66          # bgtask tick 60s +10%
MARGIN = 40


def log(msg):
    print(msg, flush=True)


def invariant(inv_id, name, ok, summary):
    log(f"INVARIANT {inv_id} {name} {'PASS' if ok else 'FAIL'} {summary}")


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
    # NO create-stream-on-read/append: auto-create under a probe would
    # resurrect the deleted name and corrupt the oracle
    cli_env = dict(
        os.environ,
        S2_ACCOUNT_ENDPOINT=f"http://127.0.0.1:{PORT}",
        S2_BASIN_ENDPOINT=f"http://127.0.0.1:{PORT}",
        S2_ACCESS_TOKEN="ignored",
    )
    r = subprocess.run(
        [S2, "create-basin", BASIN],
        env=cli_env, capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        fail(3, f"create-basin failed: {r.stderr.strip()[:300]}")


def create_stream(name, config, timeout=120):
    return http_call("POST", "/v1/streams",
                     body={"stream": name, "config": config},
                     timeout=timeout)


def create_stream_poll(name, config):
    """One gate-poll attempt; a timeout while the purge grinds counts as
    'still gated', not a crash."""
    try:
        return create_stream(name, config)
    except OSError as e:
        return None, str(e).encode()


def get_stream(name):
    return http_call("GET", f"/v1/streams/{name}", timeout=10)


def delete_stream(name, timeout=15):
    # deleting a 25k-record stream can take >>15s of virtual time in the
    # deterministic sim — callers with big streams pass a large timeout
    return http_call("DELETE", f"/v1/streams/{name}", timeout=timeout)


def append_one(name, body_str):
    return http_call("POST", f"/v1/streams/{name}/records",
                     body={"records": [{"body": body_str}]}, timeout=15)


def dump_server_log(tail=20):   # workloads-logs output tails ~64 lines
    try:
        with open(os.path.join(WORK_DIR, "server.log"), "rb") as f:
            lines = f.read().decode("utf-8", "replace").splitlines()
        log(f"--- server.log tail ({min(tail, len(lines))} of {len(lines)}) ---")
        for line in lines[-tail:]:
            log(f"  {line}")
        log("--- end server.log tail ---")
    except OSError as e:
        log(f"server.log unavailable: {e}")


def append_many(name, bodies, token=None, headers=None, timeout=120):
    records = []
    for b in bodies:
        rec = {"body": b}
        if headers is not None:
            rec["headers"] = headers
        records.append(rec)
    payload = {"records": records}
    if token is not None:
        payload["fencing_token"] = token
    return http_call("POST", f"/v1/streams/{name}/records",
                     body=payload, timeout=timeout)


def fence(name, new_token, current=None):
    return append_many(name, [new_token], token=current,
                       headers=[["", "fence"]], timeout=30)


def trim_cmd(name, seq):
    body = "".join(chr(b) for b in seq.to_bytes(8, "big"))
    return append_many(name, [body], headers=[["", "trim"]], timeout=30)


def get_tail(name):
    # first tail read on a recreated stream triggers lazy streamer
    # recovery — seconds-to-minutes of virtual time after a 25k purge
    st, data = http_call("GET", f"/v1/streams/{name}/records/tail",
                         timeout=120)
    if st != 200:
        return st, None, data.decode("utf-8", "replace")[:300]
    return st, json.loads(data)["tail"]["seq_num"], ""


def read_from(name, seq, count=1000):
    st, data = http_call(
        "GET", f"/v1/streams/{name}/records?seq_num={seq}&count={count}",
        timeout=120)
    body = data.decode("utf-8", "replace")
    if st != 200:
        return st, [], body[:300]
    try:
        recs = json.loads(body).get("records", [])
    except json.JSONDecodeError:
        return st, [], body[:300]
    return st, [(r.get("seq_num"), r.get("body", "")) for r in recs], ""


def fresh_identity_oracle(name, seed, old_probe_seqs, t1_token, t_recreate):
    """Shared inc2 oracle: tail 0, no old bodies, default token governs,
    fresh fence works, appends from 0. Any inc1 body (prefix A{seed}-)
    visible = RED resurrection. Returns None; exits via fail() on RED.
    Connection failure mid-oracle is a LABELED RED (lazy per-stream
    recovery aborts fire at first access — core.rs:113/165-196 — i.e.
    HERE, not in the restart health window)."""
    try:
        _fresh_identity_oracle(name, seed, old_probe_seqs, t1_token,
                               t_recreate)
    except OSError as e:
        fail(1, f"server connection failure mid-oracle on {name}: "
                f"{type(e).__name__}: {e} — lazy recovery abort or serving "
                f"death at first inc2 access (core.rs:113, 165-196)",
             inv=("restart_serves", "serves-through-oracle"))


def _fresh_identity_oracle(name, seed, old_probe_seqs, t1_token, t_recreate):
    selftest = bool(os.environ.get("ORACLE_SELFTEST"))
    old_prefix = f"A{seed}-"

    st, tail, err = get_tail(name)
    if st != 200:
        fail(3, f"inc2 tail read failed: {st} {err} — setup/transient")
    if tail != 0:
        fail(1, f"inc2 of {name} check-tail is {tail}, not 0 — old tail "
                f"state survived recreate",
             inv=("resurrection", "recreated-stream-fresh"))
    log(f"inc2 tail=0 confirmed at +{time.monotonic() - t_recreate:.1f}s")

    def probe_old(tag):
        for s in old_probe_seqs:
            st, recs, err = read_from(name, s)
            if st not in (200, 416):        # 416 = TailResponse empty class
                time.sleep(2)
                st, recs, err = read_from(name, s)
            if st not in (200, 416):
                fail(1, f"old-seq read@{s} on inc2 of {name} returned "
                        f"{st} {err} — not the empty class; a leak "
                        f"presenting as a server error must not pass",
                     inv=("resurrection", "recreated-stream-fresh"))
            if selftest and tag == "initial" and s == old_probe_seqs[0]:
                log(f"ORACLE_SELFTEST: forging read@{s} -> old body")
                recs = [(s, f"{old_prefix}0-forged")]
            leaked = [(sq, b) for sq, b in recs
                      if b.startswith(old_prefix)]
            if leaked:
                fail(1, f"inc1 records RESURFACED in inc2 of {name} "
                        f"({tag} probe, read@{s}): {leaked[:3]} — purge "
                        f"left old-incarnation records readable",
                     inv=("resurrection", "recreated-stream-fresh"))
            log(f"{tag} read@{s}: {st} {len(recs)} records, no old bodies"
                + (f" ({err})" if err else ""))
        # timestamp index must be empty of old records too
        st, data = http_call(
            "GET", f"/v1/streams/{name}/records?timestamp=1&count=100",
            timeout=120)
        if st not in (200, 416):
            fail(1, f"timestamp read on inc2 of {name} returned {st} — "
                    f"not the empty class",
                 inv=("resurrection", "recreated-stream-fresh"))
        if st == 200:
            recs = json.loads(data).get("records", [])
            leaked = [r for r in recs
                      if r.get("body", "").startswith(old_prefix)]
            if leaked:
                fail(1, f"inc1 records visible via TIMESTAMP read on inc2 "
                        f"of {name}: {leaked[:3]}",
                     inv=("resurrection", "recreated-stream-fresh"))
        log(f"{tag} timestamp-read: {st}, no old bodies")

    probe_old("initial")

    # default token must govern: never-valid token 412-discloses "" not T1
    st, data = append_many(name, [f"B{seed}-wrongtok"], token="never-valid-tok")
    body = data.decode("utf-8", "replace")[:300]
    if st == 200:
        fail(1, f"never-valid-token append ACCEPTED on inc2 of {name} — "
                f"governance not at default",
             inv=("resurrection", "recreated-stream-fresh"))
    if st == 412:
        disclosed = json.loads(body).get("fencing_token_mismatch")
        if disclosed == t1_token:
            fail(1, f"inc2 of {name} 412 disclosed OLD token "
                    f"{t1_token!r} — inc1's fencing token survived recreate",
                 inv=("resurrection", "recreated-stream-fresh"))
        if disclosed != "":
            fail(1, f"inc2 412 disclosed {disclosed!r}, want '' (default)",
                 inv=("resurrection", "recreated-stream-fresh"))
        log("wrong-token 412 discloses '' (default governs; not T1)")
    else:
        time.sleep(2)
        st, data = append_many(name, [f"B{seed}-wrongtok"],
                               token="never-valid-tok")
        body = data.decode("utf-8", "replace")[:300]
        if st != 412:
            fail(1, f"wrong-token probe returned {st} {body} twice — "
                    f"governance contract (412 + disclosure) not honored "
                    f"on the recreated stream",
                 inv=("resurrection", "recreated-stream-fresh"))
        disclosed = json.loads(body).get("fencing_token_mismatch")
        if disclosed != "":
            fail(1, f"inc2 412 disclosed {disclosed!r}, want '' (default)",
                 inv=("resurrection", "recreated-stream-fresh"))

    # tokenless append accepted, sequences from 0
    st, data = append_many(name, [f"B{seed}-first"])
    if st != 200:
        time.sleep(2)
        st, data = append_many(name, [f"B{seed}-first"])
    if st != 200:
        fail(1, f"tokenless append refused on inc2 of {name}: {st} "
                f"{data[:200]} — recreated stream not serving",
             inv=("resurrection", "recreated-stream-fresh"))
    ack = json.loads(data)
    start_seq = ack["start"]["seq_num"]
    if start_seq != 0:
        fail(1, f"first inc2 append acked at seq {start_seq}, not 0 — "
                f"old sequence state survived recreate",
             inv=("resurrection", "recreated-stream-fresh"))
    log("first inc2 append acked at seq 0")

    # fresh fence to T2 works; T2-governed append acks
    t2 = f"T2-{seed:08x}"
    st, data = fence(name, t2)
    if st != 200:
        fail(1, f"fresh fence on inc2 of {name} refused: {st} {data[:200]}",
             inv=("resurrection", "recreated-stream-fresh"))
    st, data = append_many(name, [f"B{seed}-post-t2"], token=t2)
    if st != 200:
        fail(1, f"T2-governed append refused after fresh fence: {st} "
                f"{data[:200]}",
             inv=("resurrection", "recreated-stream-fresh"))
    log(f"fresh fence {t2} + governed append acked")

    # end-of-trial re-probe: catches late-durable resurfacing
    probe_old("end-of-trial")
    st, recs, err = read_from(name, 0)
    bad = [(sq, b) for sq, b in recs if not b.startswith(f"B{seed}-")
           and b != t2]
    if bad:
        fail(1, f"inc2 read-from-0 shows non-inc2 records: {bad[:3]}",
             inv=("resurrection", "recreated-stream-fresh"))
    invariant("resurrection", "recreated-stream-fresh", True,
              f"inc2 of {name}: tail 0, {len(old_probe_seqs)}x2 old-seq "
              f"probes + timestamp probe clean, '' governs (T1 never "
              f"disclosed), appends from 0, fresh fence works")


def main_doe_stale_deadline(seed):
    a = f"doe-a-{seed:08x}"
    b = f"doe-b-{seed:08x}"
    probe_period = 25 + seed % 11          # 25-35s
    delete_delay = 2 + (seed >> 4) % 4     # 2-5s after inc1 append
    log(f"mode=doe-stale-deadline seed={seed} streams={a},{b} "
        f"probe_period={probe_period}s delete_delay={delete_delay}s")
    os.makedirs(DATA_DIR, exist_ok=True)

    server = start_server()
    wait_health()
    create_basin()

    inc1_cfg = {"retention_policy": {"age": 1},
                "delete_on_empty": {"min_age_secs": 1}}
    for name in (a, b):
        st, data = create_stream(name, inc1_cfg)
        if st not in (200, 201):
            fail(3, f"inc1 create {name} failed: {st} {data[:200]} — setup")
    t_arm = time.monotonic()   # deadline entries reference ~this wall point
    for name in (a, b):
        st, data = append_one(name, f"s{seed}-{name}-inc1")
        if st != 200:
            fail(3, f"inc1 append {name} failed: {st} {data[:200]} — setup")
    time.sleep(delete_delay)
    for name in (a, b):
        st, data = delete_stream(name)
        if st not in (200, 202, 204):  # 202 = deletion accepted, purge async
            fail(3, f"delete {name} failed: {st} {data[:200]} — setup")
    t_del = time.monotonic()
    log(f"inc1 created+appended+deleted; deadline window ~= "
        f"[{ARM_PAD - delete_delay:.0f}s, {ARM_PAD + TICK_MAX:.0f}s] from now")

    # recreate: purge is event-triggered — gate should lift in seconds;
    # >300s on a healthy server is the purge_liveness RED
    inc2_a_cfg = {"retention_policy": {"age": 1},
                  "delete_on_empty": {"min_age_secs": 3600}}
    inc2_b_cfg = {"retention_policy": {"age": 1}}
    recreated = {}
    for name, cfg in ((a, inc2_a_cfg), (b, inc2_b_cfg)):
        deadline = time.monotonic() + 300
        last = None
        while time.monotonic() < deadline:
            st, data = create_stream(name, cfg)
            if st in (200, 201):
                recreated[name] = time.monotonic()
                break
            last = (st, data[:200])
            time.sleep(2)
        else:
            fail(1, f"recreate {name} still gated 300s after delete: {last}"
                    f" — purge/finalize never released the name",
                 inv=("purge_liveness", "recreate-allowed-within-bound"))
        log(f"recreated {name} at +{recreated[name] - t_del:.1f}s after "
            f"delete (gate-lift latency)")
    t2 = max(recreated.values())

    # confirm inc2 configs read back as acked (attribution baseline)
    for name, want_doe in ((a, 3600), (b, None)):
        st, data = get_stream(name)
        if st != 200:
            fail(3, f"inc2 GET {name} failed right after recreate: {st} "
                    f"{data[:200]} — setup")
        cfg = json.loads(data)
        got = (cfg.get("delete_on_empty") or {}).get("min_age_secs")
        if got != want_doe and not (want_doe is None and got in (None, 0)):
            fail(3, f"inc2 {name} config readback mismatch: "
                    f"delete_on_empty={got!r} want {want_doe!r} — setup")
    log("inc2 configs confirmed: A doe.min_age=3600s (own deadline "
        "~70min out), B no DOE; both EMPTY and staying empty")

    # probe through inc1's stale-deadline window; NEVER append to inc2
    window_end = t_arm + ARM_PAD + TICK_MAX + MARGIN
    selftest = bool(os.environ.get("ORACLE_SELFTEST"))
    selftest_at = t_arm + ARM_PAD + 10
    gone = {}
    while time.monotonic() < window_end and len(gone) < 2:
        time.sleep(min(probe_period, max(1, window_end - time.monotonic())))
        now = time.monotonic()
        for name in (a, b):
            if name in gone:
                continue
            st, data = get_stream(name)
            if selftest and name == a and now >= selftest_at:
                log(f"ORACLE_SELFTEST: forging GET {a} -> stream-not-found "
                    f"at +{now - t_del:.0f}s")
                st, data = 404, b'{"code":"stream-not-found","forged":true}'
            if st == 200:
                cfg = json.loads(data)
                if "deleted" in json.dumps(cfg):
                    log(f"probe +{now - t_del:.0f}s {name}: 200 but "
                        f"deletion-marked: {json.dumps(cfg)[:200]}")
                continue
            gone[name] = (now, st, data.decode("utf-8", "replace")[:300])
            log(f"probe +{now - t_del:.0f}s {name}: GONE — {st} {gone[name][2]}")
    for name in (a, b):
        if name not in gone:
            log(f"probe: {name} alive through window end "
                f"(+{time.monotonic() - t_del:.0f}s)")

    if a in gone:
        t_gone, st, body = gone[a]
        empty_for = t_gone - t2
        fail(1, f"inc2 of {a} deleted at +{t_gone - t_del:.0f}s after "
                f"inc1's delete — inside inc1's stale-deadline window; "
                f"inc2 was empty for {empty_for:.0f}s against its OWN "
                f"min_age floor of 3600s, and its own deadline "
                f"(~{3600 + 600}s after recreate) cannot have fired; "
                f"GET: {st} {body}",
             inv=("wrongful_delete", "own-min-age-honored-across-recreate"))
    invariant("wrongful_delete", "own-min-age-honored-across-recreate", True,
              f"inc2 of {a} (DOE min_age 3600s, empty ~"
              f"{time.monotonic() - t2:.0f}s) survived inc1's deadline "
              f"window")
    if b in gone:
        t_gone, st, body = gone[b]
        fail(1, f"CONTROL inc2 of {b} (NO delete_on_empty) deleted at "
                f"+{t_gone - t_del:.0f}s — min_age None must be Ineligible "
                f"(streamer.rs:460) even against a stale deadline; GET: "
                f"{st} {body}",
             inv=("control_survives", "no-doe-config-never-doe-deleted"))
    invariant("control_survives", "no-doe-config-never-doe-deleted", True,
              f"inc2 of {b} (no DOE) survived the window")

    # post-window: A must not just exist but serve
    st, data = append_one(a, f"s{seed}-{a}-postwindow")
    if st != 200:
        fail(1, f"post-window append to inc2 of {a} refused: {st} "
                f"{data[:200]} — stream present but not serving",
             inv=("wrongful_delete", "own-min-age-honored-across-recreate"))
    log(f"post-window append to {a} acked")

    server.terminate()
    log("VERDICT: GREEN")


def start_server_arm(arm):
    env = {"SL8_FLUSH_INTERVAL": arm} if arm else None
    return start_server(env)


def populate_inc1(name, seed, n_records, n_per_batch=1000):
    done = 0
    while done < n_records:
        k = min(n_per_batch, n_records - done)
        bodies = [f"A{seed}-{i}" for i in range(done, done + k)]
        st, data = append_many(name, bodies, timeout=120)
        if st != 200:
            fail(3, f"inc1 populate batch@{done} failed: {st} "
                    f"{data[:200]} — setup")
        done += k
    return done


def main_fresh_identity(seed):
    # No kills; the resurrection oracle itself (depth 5)
    name = f"fid-{seed:08x}"
    n_records = 60 + seed % 61            # 60-120; trim point < 128
    trim_at = n_records // 2
    t1 = f"T1-{seed:08x}"
    log(f"mode=fresh-identity seed={seed} stream={name} "
        f"records={n_records} trim_at={trim_at}")
    if os.path.isdir(DATA_DIR):
        import shutil
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)
    server = start_server()
    wait_health()
    create_basin()

    cfg = {"retention_policy": {"age": 3600}}
    st, data = create_stream(name, cfg)
    if st not in (200, 201):
        fail(3, f"inc1 create failed: {st} {data[:200]} — setup")
    populate_inc1(name, seed, n_records)
    st, data = fence(name, t1)
    if st != 200:
        fail(3, f"inc1 fence T1 failed: {st} {data[:200]} — setup")
    st, data = trim_cmd(name, trim_at)
    if st != 200:
        fail(3, f"inc1 partial trim failed: {st} {data[:200]} — setup")
    log(f"inc1 full surface: {n_records} records + fence {t1} + "
        f"trim@{trim_at}")

    st, data = delete_stream(name)
    if st not in (200, 202, 204):
        fail(3, f"delete failed: {st} {data[:200]} — setup")
    t_del = time.monotonic()

    # purge is event-triggered at delete; gate should lift in seconds
    deadline = time.monotonic() + 300
    last = None
    while time.monotonic() < deadline:
        st, data = create_stream_poll(name, cfg)
        if st in (200, 201):
            break
        last = (st, data.decode("utf-8", "replace")[:200])
        time.sleep(2)
    else:
        fail(1, f"recreate still gated 300s after delete: {last}",
             inv=("purge_liveness", "recreate-allowed-within-bound"))
    t_recreate = time.monotonic()
    log(f"recreate gate lifted at +{t_recreate - t_del:.1f}s after delete")

    probe_seqs = [0, trim_at - 1, trim_at, n_records - 1, n_records + 1]
    fresh_identity_oracle(name, seed, probe_seqs, t1, t_recreate)
    server.terminate()
    log("VERDICT: GREEN")


def main_kill_mid_purge(seed):
    # The crash arm (depth 10): 25k records = 3 purge WriteBatches at
    # DELETE_BATCH_SIZE=10k; SIGKILL divides the delete/purge pipeline;
    # after crash there is NO re-trigger — resumption waits the 60s±10%
    # tick. Delay classes (critic-reshaped): racing the DELETE request /
    # post-ack [0,2s] sub-100ms / long post-finalize control.
    import threading
    name = f"kmp-{seed:08x}"
    arm = [None, "500ms", "2s"][seed % 3]
    t1 = f"T1-{seed:08x}"
    n_records = 25000
    cls_sel = seed % 10
    if cls_sel < 3:
        # in-flight: DELETE ack latency is seconds-scale in the sim, so
        # sweep the whole request window; adaptive kill (below) fires at
        # the ack if it lands first
        if (seed >> 12) % 3 == 0:   # 1/3 target the trim-durability window
            kill_class, delay_ms = "racing", (seed >> 4) % 3000       # 0-3s
        else:
            kill_class, delay_ms = "racing", ((seed >> 4) % 200) * 100  # 0-20s
    elif cls_sel == 9:
        kill_class, delay_ms = "control", 70000 + ((seed >> 4) % 10) * 1000
    else:
        kill_class, delay_ms = "post-ack", ((seed >> 8) % 21) * 100
    log(f"mode=kill-mid-purge seed={seed} stream={name} arm="
        f"SL8_FLUSH_INTERVAL={arm or '(default)'} class={kill_class} "
        f"delay={delay_ms}ms")
    if os.path.isdir(DATA_DIR):
        import shutil
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)
    server = start_server_arm(arm)
    wait_health()
    create_basin()

    cfg = {"retention_policy": {"age": 3600}}
    st, data = create_stream(name, cfg)
    if st not in (200, 201):
        fail(3, f"inc1 create failed: {st} {data[:200]} — setup")
    t0 = time.monotonic()
    populate_inc1(name, seed, n_records)
    st, data = fence(name, t1)
    if st != 200:
        fail(3, f"inc1 fence failed: {st} {data[:200]} — setup")
    log(f"inc1 populated: {n_records} records + fence in "
        f"{time.monotonic() - t0:.1f}s")

    if kill_class == "racing":
        result = {}

        def fire():
            try:
                s, d = delete_stream(name, timeout=120)
                result["outcome"] = (s, d.decode("utf-8", "replace")[:100])
            except OSError as e:
                result["outcome"] = ("conn-error", str(e)[:100])
        th = threading.Thread(target=fire)
        t_send = time.monotonic()
        th.start()
        # adaptive: kill at delay_ms into the request, or right at the
        # ack if it lands first (in-flight seams cluster near the ack)
        while (time.monotonic() - t_send) * 1000 < delay_ms \
                and "outcome" not in result:
            time.sleep(0.01)
        server.kill()
        server.wait()
        th.join(timeout=30)
        log(f"racing kill at +{(time.monotonic() - t_send) * 1000:.0f}ms "
            f"into DELETE (planned {delay_ms}ms); "
            f"delete outcome={result.get('outcome')}")
    else:
        t_send = time.monotonic()
        st, data = delete_stream(name, timeout=120)
        if st not in (200, 202, 204):
            fail(3, f"delete failed: {st} {data[:200]} — setup")
        log(f"delete acked in {time.monotonic() - t_send:.1f}s")
        time.sleep(delay_ms / 1000.0)
        server.kill()
        server.wait()
        log(f"kill at +{delay_ms}ms after delete ack "
            f"(ack->kill delay logged for anti-vacuity)")
    t_kill = time.monotonic()

    server = start_server_arm(arm)
    deadline = time.monotonic() + 60
    healthy = False
    while time.monotonic() < deadline:
        if server.poll() is not None:
            fail(1, f"server process DIED during recovery (exit "
                    f"{server.returncode}) — assert_no_records_following_"
                    f"tail or another recovery abort (core.rs:165-196)",
                 inv=("restart_serves", "post-kill-restart-serves"))
        try:
            st, _ = http_call("GET", "/health", timeout=2)
            if st == 200:
                healthy = True
                break
        except OSError:
            pass
        time.sleep(0.2)
    if not healthy:
        fail(1, "server not healthy 60s after restart from killed root",
             inv=("restart_serves", "post-kill-restart-serves"))
    invariant("restart_serves", "post-kill-restart-serves", True,
              f"healthy at +{time.monotonic() - t_kill:.1f}s after kill")

    # seam classification (anti-vacuity): create-probe + append-probe
    try:
        st_c, data_c = create_stream(name, cfg)
    except OSError:
        time.sleep(5)
        st_c, data_c = create_stream(name, cfg)
    code_c = ""
    if st_c not in (200, 201):
        try:
            code_c = json.loads(data_c).get("code", "")
        except json.JSONDecodeError:
            pass
    if st_c in (200, 201):
        seam = "POST-FINALIZE"      # gate already open: purge completed
        recreated_early = True
    elif code_c == "stream_deletion_pending":
        seam = "PRE-FINALIZE"       # marked, purge incomplete — the seam
        recreated_early = False
    elif code_c in ("resource_already_exists", "stream_already_exists"):
        st_a, data_a = append_many(name, [f"A{seed}-probe"])
        if st_a == 200:
            seam = "DELETE-UNHAPPENED"
            log("delete fully un-happened (racing kill before trim "
                "durability); re-issuing DELETE to exercise the purge")
            st, data = delete_stream(name, timeout=120)
            if st not in (200, 202, 204):
                fail(3, f"re-delete failed: {st} {data[:200]} — setup")
        else:
            try:
                code_a = json.loads(data_a).get("code", "")
            except json.JSONDecodeError:
                code_a = ""
            if code_a != "stream_deletion_pending":
                fail(3, f"unclassifiable probe pair: create already-exists "
                        f"but append {st_a} {code_a!r} {data_a[:150]}")
            seam = "DIVIDED"        # trim==MAX + deleted_at==None
            log(f"DIVIDED state witnessed (control-plane sibling's "
                f"corridor): create 409 already_exists, append {st_a} "
                f"{data_a[:150]}")
        recreated_early = False
    else:
        fail(3, f"unclassifiable post-restart create-probe: {st_c} "
                f"{data_c[:200]}")
    log(f"SEAM={seam} (create-probe {st_c} {code_c or 'created'})")

    if not recreated_early:
        deadline = time.monotonic() + 300
        last = None
        while time.monotonic() < deadline:
            if server.poll() is not None:
                fail(1, f"server DIED during post-kill gate wait (exit "
                        f"{server.returncode})",
                     inv=("restart_serves", "post-kill-restart-serves"))
            st, data = create_stream_poll(name, cfg)
            if st in (200, 201):
                break
            last = (st, data.decode("utf-8", "replace")[:200])
            time.sleep(2)
        else:
            fail(1, f"recreate still gated 300s after restart (seam "
                    f"{seam}): {last} — interrupted purge never resumed "
                    f"on the recovery tick",
                 inv=("purge_liveness", "recreate-allowed-within-bound"))
    t_recreate = time.monotonic()
    log(f"recreate gate open at +{t_recreate - t_kill:.1f}s after kill "
        f"(gate-latency for anti-vacuity)")
    invariant("purge_liveness", "recreate-allowed-within-bound", True,
              f"seam {seam}, gate at +{t_recreate - t_kill:.1f}s")

    probe_seqs = [0, 4999, 9999, 10000, 15000, 19999, 24999, 25001]
    fresh_identity_oracle(name, seed, probe_seqs, t1, t_recreate)
    server.terminate()
    log(f"VERDICT: GREEN (seam={seam})")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "doe-stale-deadline"
    seed = derive_seed()
    if mode == "doe-stale-deadline":
        main_doe_stale_deadline(seed)
        return
    if mode == "fresh-identity":
        main_fresh_identity(seed)
        return
    if mode == "kill-mid-purge":
        main_kill_mid_purge(seed)
        return
    fail(3, f"mode {mode!r} not implemented yet")


main()
PYEOF
