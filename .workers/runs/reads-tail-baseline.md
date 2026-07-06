# reads-tail-baseline — run evidence

- target commit: (filled at publish; drafts injected at d74d897 tree + fixes)
- workload: `.workers/workloads/reads_tail.py baseline`
- transport probed: SSE (`Accept: text/event-stream` on the read path) — see
  map.md reality note (event format, Last-Event-Id resume, Remote-durability
  catch-up scan vs durable-gated follow broadcast).

## Drafts

| run | depth | outcome | note |
|-----|-------|---------|------|
| nd7bns898gc2xymqbvxzzzhkw58a05pa | 2 | failed (harness) | follower observed all 200 (transport works); two harness bugs: `_stop` Event shadowed `Thread._stop` breaking `join()`; cross-thread `close()` raises AttributeError not OSError |
| nd7dwyegrdfdgz1tgxnq8fw03x8a00xs | 2 | 2/2 green | all 5 invariants PASS; e.g. seed=737398806, last_event_id=199,200,5400 |
| nd78qxe6qpvkjtk3670vft2qkd8a0b30 | 1 | RED (expected) | ORACLE_SELFTEST=1 red-proof: planted drop of observed seq 100 -> observed_survive FAIL, exit 1 (seed=87556086) |
| nd779kr6j2j6s7vyvztz2yyfs58a1hy0 | 5 | 5/5 green | full-depth sweep, all invariants PASS (e.g. seed=818597727) |

## Test-reviewer gate (REDO -> fixed)

Verdict on the first shape: **REDO** — a real follow-delivery gap routed to
VOID (exit 3, "setup/transport issue"), never RED: `wait_observed` timed out
below target and `check_wellformed` was unreachable for the loss shape. The
SUT has exactly this seam (read.rs:183-185 empty-scan skip; the Lagged branch
read.rs:219-222 re-enters it), so the masked class is the one the promise
exists to catch. Secondary: the exactly-once leg had no demonstrated red.

Fixes applied:
1. `wait_observed` returns early once a gap is provable (observed span >
   observed count); the timeout path runs `check_wellformed` on the partial
   log (`partial_delivery_verdict`) — internal gap/dup = RED via
   follow_wellformed; only a clean stalled dense prefix may VOID (with
   head/tail triage logging). Same guard on the across-restart resume leg.
2. `ORACLE_SELFTEST=gap` plants a silent follower-side drop of one delivered
   seq — red-proves the exactly-once leg through the new partial path.
   (`ORACLE_SELFTEST=1` remains the readback-drop proof.)

## Post-fix runs

| run | depth | outcome | note |
|-----|-------|---------|------|
| nd74tap2p8ma82tc9g30yngmfx8a0ge5 | 1 | RED (expected) | ORACLE_SELFTEST=gap: follower drop of seq 100 -> follow_wellformed FAIL via the partial-delivery path, exit 1, no timeout stall (seed=2958128658) |
| nd75xzpbnbnv3vk49r4hkyfykn8a1b2j | 1 | RED (expected) | ORACLE_SELFTEST=1 re-run post-fix: observed_survive FAIL (seed=3164093376) |
| nd70kjg2kp2gmk3zm8g9z4tcgh8a0yad | 5 | 5/5 green | full-depth sweep post-fix, all 5 invariants PASS (e.g. seed=4097857263) |

Verdict: **done + green** (test-reviewer KEEP after the REDO fixes; both
oracle legs red-proven). `published: pending` — official fires at wrap-up
via publish.py against the pushed image.

## Interpretation

Baseline is bring-up by design (expectations demoted in the spec): it proves
the SSE follow transport delivers every acked record exactly once in order,
that the follower's observed log, the acks, and the Remote-durability
catch-up read all agree, and that the oracle can detect a planted loss.
No faults; the crash seam is attacked by `reads-tail-across-restart`.

Invariants: nonvacuous (observed floor 200), follow_wellformed (dense,
dup-free, in order), acked_delivered (acks reach the follower with identical
content), readback_dense (catch-up read tiles [0, tail)), observed_survive
(observed set present and identical in the Remote read).

## Official (2026-07-06, image @ f4a2b31)

nd7fsyab7bb1q721xar5mm3a2n8a1x0w, depth 5: 5/5 green, all invariants PASS.
