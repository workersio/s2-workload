---
key: tail-is-gapless-and-monotonic
area: durability
title: Tail is gapless and monotonic
claim: >-
  Sequence numbers assigned to acknowledged appends are dense and strictly
  increasing; concurrent appenders never receive overlapping or gapped
  sequence ranges, and a full read returns exactly seq 0..tail.
status: active
provenance: https://s2.dev/docs/concepts (streams are ordered; appends return sequence ranges); README design notes (streamer task serializes appends)
explorations:
  - key: tail-gapless-baseline
    title: Tail gapless baseline
    description: >-
      Several concurrent writers append batches to one stream with no
      faults; ack'd sequence ranges must tile [0, tail) with no overlap and
      no gap, and a full read must return exactly that dense range.
    status: done
    result: green
    reason: null
    workload: workloads/tail_gapless.sh
    command: sh .workers/workloads/tail_gapless.sh baseline
    faults: []
    depth: 5
    replay: null
    freshness: new-current
    reported: null
    published: nd7cetna3ce5fy7gawq8ce6t8d89z6ar
  - key: tail-gapless-restart-interleaved
    title: Tail gapless restart interleaved
    description: >-
      Kill and restart the server between appender waves; sequence
      assignment must resume exactly at the persisted tail — no reuse of
      already-assigned seq numbers, no holes across the restart boundary.
    status: done
    result: green
    reason: null
    workload: workloads/tail_gapless.sh
    command: sh .workers/workloads/tail_gapless.sh restart
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: nd77brq9cbbevt9t0qgtz65srh89y6n5
  - key: tail-gapless-straddle-at-kill
    title: Tail gapless straddle at kill
    description: >-
      restart-interleaved kills *between* appender waves — a quiesced
      boundary. This arm kills *during* a wave: writers have in-flight
      unacked appends AT the SIGKILL, then restart and resume. The tail is
      re-derived fresh from durable KV on every restart
      (load_persisted_stream_tail, core.rs:144, DurabilityLevel::Remote ->
      stable_pos, streamer.rs:265; next_assignable_pos returns stable_pos
      when pending is empty, streamer.rs:327-331), guarded by
      assert_no_records_following_tail (core.rs:165). The straddle asks: when
      a writer's append was in-flight at the kill, does resume assign the
      next writer a seq that a pre-kill in-flight append also received, or
      leave a hole — i.e. does the union of all writers' acked ranges still
      tile [0, tail) with no seq owned by two writers across the boundary.
    status: done
    result: green
    reason: null
    workload: workloads/tail_gapless.sh
    command: sh .workers/workloads/tail_gapless.sh straddle-at-kill
    faults: []
    depth: 10
    replay: >-
      green draft nd726c2zx1k9369v7yzs0q4qq989z234 (depth 10, 9 green + 1
      exit-3 void where the in-flight appends completed before SIGKILL — the
      anti-vacuous gate correctly refused a quiesced trial, not a finding;
      e.g. seed 3248286012: 18 acked + 3 unacked-in-flight straddle, restart
      recovered, all 6 invariants pass). Red-proof draft
      nd70ekpqfdzs6f7nyee8v7n62n89yhy7 (seed 4111024420, 500ms arm, 32 acked +
      4 unacked-in-flight, planted overlap -> no_double_assign FAIL, exit 1).
      Both via --workload-file injection on prod.
    freshness: new-current
    reported: null
    published: nd7cgyertsgh18hvrvdf57ab6989zm80
---

# Tail is gapless and monotonic

## Adversarial model

The per-stream `streamer` task owns the tail and serializes appends. The
no-fault baseline is bring-up only, and it is honest to say so: fault-free
concurrent appends are exactly what upstream's Antithesis/Porcupine setup
hammers, so its marginal value is proving our range-tiling oracle and
multi-writer plumbing — timeboxed, depth 5, expectations demoted. The
promise's real value is the restart variant: tail recovery from real
storage — if the tail is rebuilt from a stale snapshot while acks were
issued beyond it, the server double-assigns seq numbers, which is silent
data corruption for consumers keyed by seq. Promote
tail-gapless-restart-interleaved to ready immediately after kill9 runs.

## Oracle

Writers record each ack's (start, end) seq range via raw HTTP (one
request = one ack; the CLI's stderr ack output is batched/deduped and
unusable for machine-readable ranges), one log file per writer. Invariants:
1. Ranges from all writers, sorted, tile [0, tail) exactly — no overlap, no
   gap.
2. Read-back from 0 yields one record per seq with content matching the
   writer that owned that range.
3. (restart variant) The union across restart boundaries still tiles — a
   seq assigned twice across a restart is an immediate finding.

## Straddle-at-kill arm

Strategy-critic (2026-07-05, source-verified) corrected an earlier "N
restart boundaries" framing: the resume position is re-derived **fresh from
durable KV on every restart** (`load_persisted_stream_tail`, core.rs:144,
`DurabilityLevel::Remote` -> `stable_pos`, streamer.rs:265;
`next_assignable_pos` returns `stable_pos` when pending is empty,
streamer.rs:327-331), so no snapshot is carried across restarts and there
is no "increasingly stale snapshot" to drift — extra boundaries would just
repeat one mechanism (a seed sweep, not a new surface). The genuinely
distinct element is the **straddle**: writers with in-flight *unacked*
appends at the instant of the kill, versus restart-interleaved which kills
between waves (quiesced). One boundary is enough to exercise it.

Oracle invariants (extend the promise's tiling oracle; make the guards
explicit). Producer revision 2026-07-05 (executor #7 flagged an inconsistency
between the old invariant 1 "no gap" and invariant 4's in-flight-unacked
appends): the recovered tail is `stable_pos` = the last *durable* seq
(load_persisted_stream_tail, core.rs:144). An append that reached durability
microseconds before SIGKILL #1 occupies a seq below the recovered tail, but if
its ack response died with the process the *client* manifest marks it unacked.
So a seq in [0, tail) legitimately need not be covered by an acked range — it
may be covered by an in-flight-unacked payload this run sent. The corruption
signals are OVERLAP and DOUBLE-ASSIGNMENT, not the mere existence of a gap in
acked coverage. Revised invariants:
1. No overlap and monotonic: sorted acked (start,end) ranges never overlap,
   and no acked range extends at or beyond `tail`. (Gaps between acked ranges
   are permitted only if reconciled by invariant 5.)
2. No seq is owned by two distinct writers — a seq assigned to a pre-kill
   in-flight append and re-assigned to a post-restart writer (or two acked
   ranges sharing any seq) is an immediate finding. This is the arm's core
   target.
3. Read-back [0, tail) is dense: exactly one record per seq in [0, tail), no
   holes below the recovered tail, and the server never persisted a record
   beyond its own persisted tail (the `assert_no_records_following_tail`
   guard, core.rs:165, must never have fired — a crash on restart is a
   finding).
4. Content ownership: every seq covered by an acked range holds exactly that
   range's payloads, in order.
5. Gap reconciliation: every seq in [0, tail) NOT covered by an acked range
   holds a payload this run sent as an in-flight-unacked append — at most once,
   never a phantom the run never sent. (A gap seq holding an unknown or
   duplicated payload is a finding: recovery invented or double-applied data.)
6. Anti-vacuous: at least one writer had an append in-flight (sent, unacked)
   at the SIGKILL (a real straddle, not a quiesced boundary), and the run
   completed the restart; otherwise the trial is void.

## Replay plan

Seed drives writer interleaving and, in the restart variants, the kill
point(s) — one for restart-interleaved, N for multi-restart-straddle. Red
runs replay by recorded seed via --exploration.
