#!/bin/sh
# Workload: control-plane op durability (s2 lite).
#
# Mode delete-straddle:
#   the critic's counter-promotion (promises/control-plane-acked-ops-
#   durable.md, control-plane-delete-straddle-ensure-erased). Delete is
#   two-phase and non-atomic: terminal trim through the streamer
#   (streams.rs:338-358), THEN mark_stream_deleted in a separate txn
#   (:360-379). SIGKILL an in-flight DELETE between the phases and the
#   server restarts into: trim_point == MAX, deleted_at == None. In that
#   state (a) GET serves the stream LIVE (streams.rs:240-258 reads meta
#   only) while appends fail deletion-pending (core.rs:118-120) — the
#   incoherence is logged; (b) the kill shot: an Ensure on the name
#   passes the DeletionPending gate (streams.rs:104-112 checks only
#   deleted_at) and 200-acks — then the recovered purge's finalize_trim
#   unconditionally deletes meta/id-mapping/tail (stream_trim.rs:136-146,
#   guarded only by trim-point equality :123-134) and the ACKED ENSURE
#   UN-HAPPENS within the recovery tick (60s±10%; the event trigger died
#   with the process).
#
#   Trial: create + populate a stream; fire DELETE on a thread; SIGKILL
#   the server at a bisection-chosen offset into the request; restart;
#   classify the landed seam by GET + append:
#     seam EARLY   — GET 200 live + append 200: delete un-happened
#                    entirely (kill before trim durability). Healthy.
#     seam STRADDLE— GET 200 live + append refused deletion-pending
#                    class: the divided state. Run the kill shot.
#     seam LATE    — GET 404 / deletion-marked: normal deletion. Healthy.
#   The offset BISECTS between EARLY and LATE until a STRADDLE lands
#   (the two-phase window scales with SL8_FLUSH_INTERVAL — arms below).
#   (GET may itself return 409 stream_deletion_pending — that is a LATE
#   classification, not unclassifiable; shakeout v1 false-void.)
#   On a landed straddle: Ensure = PUT /v1/streams/{name} (ProvisionMode::
#   Ensure — POST is CreateOnly and 409s already-exists on the live meta,
#   which is NOT the gate under attack) with a distinct config value.
#   Invariants:
#   - acked_ensure_erased: if the Ensure 200-acks, its stream/config must
#     still be there after the recovery tick (~180s watch). 404 or config
#     loss = RED.
#   - straddle_wedged: if the gate refuses the Ensure too, the divided
#     state must RESOLVE within 300s (sibling purge_liveness contract):
#     either fully-deleted (name freed; recreate + append must then work)
#     or ensure-acked. A name permanently neither creatable nor usable =
#     RED.
#   - restart_serves: server not healthy after restart = RED (never
#     void — a crash on recovery IS a finding).
#   - anti-vacuity: a run that exhausts its trial budget without landing
#     a straddle exits VOID with the seam ladder logged (reshape data).
#   Arms: SL8_FLUSH_INTERVAL unset / 500ms / 2s (seed-chosen) widen the
#   inter-phase window; bisection self-tunes the offset per arm.
#
# Self-contained: sh wrapper + embedded python3 (injection layers one file).
# Exit codes: 0 green, 1 red (finding), 3 void/blocked.
MODE="${1:-delete-straddle}"
exec python3 - "$MODE" <<'PYEOF'
import http.client
import json
import os
import shutil
import subprocess
import sys
import threading
import time

S2 = os.path.join(".workers", "vendor", "bin", "s2-linux-amd64")
PORT = 8080
BASIN = "ctlplane-wl-01"
WORK_DIR = "/tmp/wl-ctlplane"
DATA_DIR = os.path.join(WORK_DIR, "s2root")
SERVER_LOG = os.path.join(WORK_DIR, "server.log")

TICK_MAX = 66          # bgtask tick 60s +10%
WATCH_S = 180          # erased-watch: recovery tick + margin per spec
WEDGE_S = 300          # wedge-resolution bound (sibling purge_liveness contract)
MAX_TRIALS = 12


def log(msg):
    print(msg, flush=True)


def invariant(inv_id, name, ok, summary):
    log(f"INVARIANT {inv_id} {name} {'PASS' if ok else 'FAIL'} {summary}")


def dump_server_log(tail=20):   # workloads-logs output tails ~64 lines total
    try:
        with open(SERVER_LOG, "rb") as f:
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
    dump_server_log()
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


def start_server(flush=None):
    env = dict(os.environ)
    if flush:
        env["SL8_FLUSH_INTERVAL"] = flush
    out = open(SERVER_LOG, "ab")
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
        fail(1, f"server not healthy {deadline_s}s after restart from the "
                f"killed root — recovery failure",
             inv=("restart_serves", "post-kill-restart-serves"))
    fail(3, "server did not become healthy in time — setup")


def create_basin():
    # NO create-stream-on-read/append: auto-create would resurrect names
    # under probes and corrupt seam classification
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


def create_stream(name, config):
    # POST = ProvisionMode::CreateOnly (streams.rs:116)
    return http_call("POST", "/v1/streams",
                     body={"stream": name, "config": config}, timeout=15)


def ensure_stream(name, config):
    # PUT /v1/streams/{stream} = ProvisionMode::Ensure — the gate under
    # attack checks ONLY meta.deleted_at (streams.rs:106-112) before the
    # (Some(existing), Ensure) -> Updated/Noop 200 arm (:133)
    return http_call("PUT", f"/v1/streams/{name}", body=config, timeout=15)


def get_stream(name):
    return http_call("GET", f"/v1/streams/{name}", timeout=10)


def append_one(name, body_str, timeout=15):
    return http_call("POST", f"/v1/streams/{name}/records",
                     body={"records": [{"body": body_str}]}, timeout=timeout)


def classify_get(name):
    """-> ('live'|'marked'|'gone'|'other', status, body)"""
    st, data = get_stream(name)
    body = data.decode("utf-8", "replace")[:600]
    if st == 200:
        try:
            if json.loads(body).get("deleted_at") is not None:
                return "marked", st, body
        except (json.JSONDecodeError, AttributeError):
            pass
        return "live", st, body
    if st == 404:
        return "gone", st, body
    if st == 409 and "deletion_pending" in body:
        return "marked", st, body   # GET itself surfaces the pending state
    return "other", st, body


def fire_delete(name, result):
    """DELETE on a thread; server may die mid-request."""
    try:
        st, data = http_call("DELETE", f"/v1/streams/{name}", timeout=15)
        result["outcome"] = ("acked", st, data.decode("utf-8", "replace")[:200])
    except OSError as e:
        result["outcome"] = ("conn-error", None, str(e)[:200])


def main_delete_straddle(seed):
    selftest = bool(os.environ.get("ORACLE_SELFTEST"))
    arm_flush = [None, "500ms", "2s"][seed % 3]
    flush_ms = {None: 100, "500ms": 500, "2s": 2000}[arm_flush]
    lo_ms, hi_ms = 0.0, flush_ms * 2.5 + 300
    n_records = 3 + seed % 5
    log(f"mode=delete-straddle seed={seed} arm=SL8_FLUSH_INTERVAL="
        f"{arm_flush or '(default)'} bisect=[{lo_ms:.0f},{hi_ms:.0f}]ms "
        f"records={n_records} selftest={selftest}")
    if os.path.isdir(DATA_DIR):
        shutil.rmtree(DATA_DIR)          # never reuse a dirty root
    os.makedirs(DATA_DIR)
    if os.path.exists(SERVER_LOG):
        os.remove(SERVER_LOG)            # log opens "ab"; keep per-run

    server = start_server(arm_flush)
    wait_health()
    create_basin()

    ladder = []          # (offset_ms, seam) — reshape data + witness trail
    straddle = None      # (trial_no, name, offset_ms)
    for trial in range(1, MAX_TRIALS + 1):
        name = f"ds-{seed:08x}-t{trial:02d}"
        st, data = create_stream(name, {"retention_policy": {"age": 3600}})
        if st not in (200, 201):
            fail(3, f"t{trial} create {name} failed: {st} {data[:200]} — setup")
        for i in range(n_records):
            st, data = append_one(name, f"s{seed}-{name}-r{i}")
            if st != 200:
                fail(3, f"t{trial} populate append failed: {st} {data[:200]}"
                        f" — setup")

        offset_ms = (lo_ms + hi_ms) / 2 if trial > 1 else min(
            flush_ms * 0.6, hi_ms * 0.5)   # first probe inside the window
        result = {}
        t = threading.Thread(target=fire_delete, args=(name, result))
        t_send = time.monotonic()
        t.start()
        time.sleep(offset_ms / 1000.0)
        server.kill()
        server.wait()
        t.join(timeout=20)
        log(f"t{trial} {name}: SIGKILL at +{offset_ms:.1f}ms into DELETE; "
            f"delete outcome={result.get('outcome')}")

        server = start_server(arm_flush)
        wait_health(red_on_fail=True)

        state, st, body = classify_get(name)
        ap_st, ap_data = append_one(name, f"s{seed}-{name}-postrestart")
        ap_body = ap_data.decode("utf-8", "replace")[:300]
        if selftest and straddle is None:
            log(f"ORACLE_SELFTEST: forging t{trial} append -> "
                f"deletion-pending class (was {ap_st})")
            state, ap_st, ap_body = "live", 409, '{"forged":"deletion_pending"}'

        if state == "live" and ap_st == 200:
            seam = "EARLY"       # delete un-happened; stream fully serving
        elif state == "live":
            seam = "STRADDLE"    # live meta, refused append — divided state
            log(f"t{trial} INCOHERENCE logged: GET 200 live but append "
                f"refused {ap_st} {ap_body} (trim_point==MAX + "
                f"deleted_at==None)")
        elif state in ("marked", "gone"):
            seam = "LATE"        # mark_stream_deleted landed; normal path
        else:
            fail(3, f"t{trial} unclassifiable GET {st} {body} — setup/transient")
        ladder.append((offset_ms, seam))
        log(f"t{trial} seam={seam} GET={st} append={ap_st}")

        if seam == "EARLY":
            lo_ms = offset_ms
        elif seam == "LATE":
            hi_ms = offset_ms
            # healthy arm consistency: refused append + eventual purge
            if ap_st == 200:
                fail(1, f"t{trial} post-mark append ACCEPTED on a "
                        f"deletion-marked/gone stream: GET {st} {body} but "
                        f"append 200 — delete un-happened after ack path",
                     inv=("acked_op_durable", "delete-holds-after-restart"))
        else:
            straddle = (trial, name, offset_ms)
            break
        if hi_ms - lo_ms < 2.0 and trial >= 4:
            break   # window narrower than our timer resolution

    if straddle is None:
        log(f"seam ladder: {[(round(o, 1), s) for o, s in ladder]}")
        fail(3, f"no straddle landed in {len(ladder)} trials (arm "
                f"{arm_flush or 'default'}); window < timer resolution — "
                f"reshape: try wider flush arm / finer offsets")

    trial_no, name, offset_ms = straddle
    log(f"STRADDLE-LANDED trial={trial_no} name={name} "
        f"offset={offset_ms:.1f}ms ladder="
        f"{[(round(o, 1), s) for o, s in ladder]}")

    # the kill shot: Ensure (PUT, ProvisionMode::Ensure) with a DISTINCT
    # config value through the deleted_at-only gate (streams.rs:106-112)
    marker_cfg = {"retention_policy": {"age": 7777}}
    st, data = ensure_stream(name, marker_cfg)
    if st not in (200, 201):
        time.sleep(1)
        st, data = ensure_stream(name, marker_cfg)   # one transient retry
    if st not in (200, 201):
        # gate refused the Ensure too — divided state is fully gated.
        # Remaining question: does it ever RESOLVE? A name stuck neither
        # deletable nor creatable past the recovery tick is a liveness
        # finding (same contract as the sibling promise's purge_liveness)
        log(f"ensure on straddled name refused: {st} "
            f"{data.decode('utf-8', 'replace')[:300]} — gate blocks Ensure "
            f"in the divided state; watching {WEDGE_S}s for resolution")
        t0 = time.monotonic()
        resolved = None
        while time.monotonic() < t0 + WEDGE_S:
            time.sleep(10)
            state, gst, gbody = classify_get(name)
            est, edata = ensure_stream(name, marker_cfg)
            now = time.monotonic() - t0
            log(f"wedge-watch +{now:.0f}s: GET={gst}({state}) PUT={est}")
            if est in (200, 201):
                resolved = ("ensure-acked", now, est)
                break
            if state == "gone":
                resolved = ("fully-deleted", now, gst)
                break
        if resolved is None:
            fail(1, f"straddled name still WEDGED {WEDGE_S}s after restart "
                    f"(>= recovery tick 66s): GET live/pending but Ensure "
                    f"and appends refused deletion-pending — the divided "
                    f"state (trim_point==MAX, deleted_at==None) never "
                    f"resolved; name is permanently unusable",
                 inv=("straddle_wedged", "divided-state-resolves-within-tick"))
        kind, at_s, code = resolved
        log(f"divided state resolved: {kind} at +{at_s:.0f}s ({code})")
        if kind == "ensure-acked":
            # resolution produced an acked ensure — fall through to the
            # erased-watch below with a fresh ack time
            pass
        else:
            invariant("straddle_wedged", "divided-state-resolves-within-tick",
                      True, f"resolved to fully-deleted at +{at_s:.0f}s")
            # name freed: a fresh create must now work and serve
            st, data = create_stream(name, marker_cfg)
            if st not in (200, 201):
                time.sleep(2)
                st, data = create_stream(name, marker_cfg)
            if st not in (200, 201):
                fail(1, f"name freed by finalize but recreate refused: {st} "
                        f"{data[:300]}",
                     inv=("straddle_wedged", "divided-state-resolves-within-tick"))
            ap_st, ap_data = append_one(name, f"s{seed}-{name}-postresolve")
            if ap_st != 200:
                fail(1, f"recreated-after-resolution stream refuses appends: "
                        f"{ap_st} {ap_data[:300]}",
                     inv=("straddle_wedged", "divided-state-resolves-within-tick"))
            log("post-resolution recreate + append acked")
            server.kill()
            log("VERDICT: GREEN")
            return
    t_ack = time.monotonic()
    log(f"ensure ACKED {st} on straddled name with marker age=7777; "
        f"watching {WATCH_S}s through the recovery tick "
        f"(tick<= {TICK_MAX}s, event trigger died with the process)")

    watch_end = t_ack + WATCH_S
    selftest_at = t_ack + 20
    while time.monotonic() < watch_end:
        time.sleep(8)
        now = time.monotonic()
        state, st, body = classify_get(name)
        if selftest and now >= selftest_at:
            log(f"ORACLE_SELFTEST: forging watch GET {name} -> 404 at "
                f"+{now - t_ack:.0f}s")
            state, st, body = "gone", 404, '{"code":"stream_not_found","forged":true}'
        if state == "live":
            got_age = None
            try:
                got_age = (json.loads(body if not selftest else "{}")
                           .get("retention_policy") or {}).get("age")
            except (json.JSONDecodeError, AttributeError):
                pass
            if got_age is not None and got_age != 7777:
                fail(1, f"acked ensure's config LOST at +{now - t_ack:.0f}s: "
                        f"retention age reads {got_age!r}, acked 7777 — "
                        f"finalize_trim replayed over the acked op",
                     inv=("acked_ensure_erased", "acked-ensure-survives-recovery"))
            continue
        # gone/marked: double-probe before verdict (transient-404 guard)
        time.sleep(2)
        state2, st2, body2 = classify_get(name)
        if selftest:
            state2, st2, body2 = state, st, body
        if state2 != "live":
            fail(1, f"200-ACKED ensure ERASED at +{now - t_ack:.0f}s after "
                    f"its ack: GET {st2} {body2} (double-probed) — the "
                    f"recovered purge's finalize_trim deleted the meta the "
                    f"gate had just re-acked (stream_trim.rs:136-146 guarded "
                    f"only by trim-point equality)",
                 inv=("acked_ensure_erased", "acked-ensure-survives-recovery"))
        log(f"watch +{now - t_ack:.0f}s: transient non-live GET {st} "
            f"recovered to live on re-probe")
    invariant("acked_ensure_erased", "acked-ensure-survives-recovery", True,
              f"acked ensure survived {WATCH_S}s (>= recovery tick) on the "
              f"straddled name")

    # post-watch: is the name actually serving, or wedged live-but-refusing?
    ap_st, ap_data = append_one(name, f"s{seed}-{name}-postwatch")
    if ap_st != 200:
        time.sleep(2)
        ap_st, ap_data = append_one(name, f"s{seed}-{name}-postwatch2")
    log(f"post-watch append: {ap_st} "
        f"{ap_data.decode('utf-8', 'replace')[:200]}"
        + ("" if ap_st == 200 else " — WEDGE observation: name live on GET, "
           "acked config held, but appends still refused after the recovery "
           "tick (logged per spec; not the RED clause)"))

    server.kill()
    log("VERDICT: GREEN")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "delete-straddle"
    seed = derive_seed()
    os.makedirs(WORK_DIR, exist_ok=True)
    if mode == "delete-straddle":
        main_delete_straddle(seed)
        return
    fail(3, f"mode {mode!r} not implemented yet")


main()
PYEOF
