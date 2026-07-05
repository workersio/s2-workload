# Run evidence — tail-gapless-straddle-at-kill

Straddle arm of the tail-is-gapless-and-monotonic promise. restart-interleaved
kills *between* appender waves (quiesced); this arm kills *during* a wave —
writers have in-flight *unacked* appends at the SIGKILL — then restarts and runs
a post-restart wave, asking whether resume double-assigns a seq that a pre-kill
in-flight append also received.

Target: prepared image at a88afdc + `.workers/workloads/tail_gapless.sh` at
working tree (adds `straddle-at-kill` mode), injected via `--workload-file`.
Project kn712jhg9p7wqx3a0rwnh698vs89x7rt (prod).

## Producer oracle revision (2026-07-05)

Executor #7 flagged an inconsistency in the drafted oracle: old invariant 1
("acked ranges tile [0, tail) exactly — no gap") contradicts invariant 4
(in-flight unacked appends at the kill). The recovered tail is `stable_pos` =
last *durable* seq (load_persisted_stream_tail, core.rs:144); an append durable
just before SIGKILL sits below tail but, if its ack died with the process, the
client manifest marks it unacked — a legitimate below-tail seq covered by no
acked range. The corruption signals are OVERLAP / DOUBLE-ASSIGNMENT, not the
mere existence of a gap. Oracle revised (see promise spec): no-overlap +
no-double-assign + dense read-back (no duplicated body) + content-ownership +
gap-reconciliation (below-tail gaps must hold known in-flight-unacked payloads,
at most once, no phantom) + anti-vacuous straddle witness. A crash on restart
(assert_no_records_following_tail firing) is itself a finding.

## Workload design

`sh .workers/workloads/tail_gapless.sh straddle-at-kill`. Appends over raw HTTP
are synchronous (block until durable). To create real in-flight-unacked appends
at the kill, the flush interval is stretched (`SL8_FLUSH_INTERVAL` 500ms|2s,
seed-biased to 500ms): appends older than the interval flushed+acked (the
prefix), those blocking inside the last interval are in-flight-unacked. Kill
lands mid-wave after the prefix builds; restart with the same arm; a post-restart
wave forces new assignment above the recovered tail (the collision surface).

## Red-proof (oracle can go red) — GATE PASSED

- Draft `nd70ekpqfdzs6f7nyee8v7n62n89yhy7`, depth 1, workload
  `01KWR8JDMP7JSVZW5V3QADGVGP`, `ORACLE_SELFTEST=1`.
- seed 4111024420, 500ms arm, wave 0: 32 acked + 4 unacked-in-flight (genuine
  straddle), restart recovered (tail 66), wave 1: 132 acked.
- Self-test forced writer 0's range to overlap the previous range by one seq;
  oracle caught it: `INVARIANT no_double_assign no-overlap-monotonic FAIL`,
  `VERDICT: RED`, exit 1. The core double-assignment signal is load-bearing.

## Green bring-up — 9/10 GREEN, 1 VOID (no findings)

- Draft `nd726c2zx1k9369v7yzs0q4qq989z234`, depth 10, no self-test.
- 9 seeds green (exit 0). 1 seed (500254371) exit-3 VOID: its 3 in-flight
  appends completed in the microseconds before SIGKILL delivered (0 unacked),
  so the anti-vacuous gate refused to call a quiesced boundary a straddle. A
  void is neither red nor green — the trial did not run the attack; correctly
  excluded, not a finding, not a false green.
- Representative green: workload `01KWR8KF3D4BJ8NEZHTXQP2JBJ`, seed 3248286012,
  500ms arm, wave 0: 18 acked + 3 unacked-in-flight straddle; restart recovered
  (tail 36); wave 1: 87 acked. All 6 invariants PASS: recovery_no_crash,
  no_double_assign (106 ranges, no overlap, none beyond tail 196), readback_dense
  (196 records, no duplicated body), content_ownership, gap_reconciled (0
  below-tail gaps), straddle_witness (3 unacked in-flight).

## Interpretation

s2-lite assigns dense, monotonic sequence numbers across a mid-wave crash: no
seq was ever owned by two writers, no acked range overlapped or extended beyond
the recovered tail, read-back stayed dense with no duplicated body, and the
in-flight unacked appends left no durable trace below tail (gap_reconciled saw 0
gaps — consistent with the durability gate: unacked == not-yet-durable == not
below the recovered `stable_pos`). No double-assignment across the boundary; the
`assert_no_records_following_tail` guard never fired. GREEN — the promise holds
on the straddle.

## Publication status: PENDING

Same blocker as acked-appends-kill-during-recovery: the official run needs the
`straddle-at-kill` workload + the oracle revision on origin/main so
`wio projects prepare` can pin the image. Direct push to origin/main is denied
under the "run the workload harness" scope. `published: pending`; wrap-up
re-fires publish.py once the push is authorized.
