---
key: cas-appends-exactly-once
area: appends
title: CAS appends are exactly-once
claim: >-
  A match_seq_num append is an atomic compare-and-swap: for each stream
  position exactly one contender wins, every loser receives a 412 naming the
  true next sequence number, and a delivered 412 is durable truth — under
  pipelining and across crash-restart.
status: active
provenance: "api/src/v1/stream/mod.rs:374-375 (\"append atomically\" + match_seq_num contract), :434-437 (SeqNumMismatch returns expected seq); lite/src/backend/streamer.rs:350-358 (match checked against next_assignable_pos, :327-331 — includes pending unacked appends); lite/src/backend/append.rs:236-247 (412 deferred until durability dependency stable)"
explorations:
  - key: cas-appends-baseline
    title: CAS appends baseline
    description: >-
      No faults. N concurrent HTTP writers loop check-tail then append with
      match_seq_num=tail, retrying off the 412-returned expected seq. External
      ledger oracle: per position exactly one 200 whose start.seq_num matches
      its match_seq_num; every 412 body seq_num equals the position's eventual
      occupant count; final read-back is exactly the concatenation of winners
      byte-for-byte; no position double-assigned, none skipped. Proves the
      ledger oracle observes the CAS invariant at all. One precedence probe
      (critic set-gap): on a fenced stream, an append that is BOTH
      wrong-token and wrong-match_seq_num must 412 with the fencing body —
      the fencing check precedes the seq check (streamer.rs:341 before
      :350); pin which 412 wins.
    status: ready
    result: null
    reason: null
    workload: workloads/cas_appends.py
    command: python3 .workers/workloads/cas_appends.py baseline
    faults: []
    depth: 5
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: cas-deferred-412-pipelined
    title: CAS deferred 412 under pipelining
    description: >-
      Adversarial concurrency against the pipelining seam: match_seq_num is
      checked against next_assignable_pos (streamer.rs:327-331), which counts
      PENDING UNACKED batches, not just stable positions — so a CAS decided
      against in-flight state races that state's durability. Slow the flush
      window (SL8_FLUSH_INTERVAL arms) so a deep unacked pipeline exists, fire
      CAS appends into it, and exploit the deferred-412 contract
      (append.rs:236-247): the moment a 412 naming seq K is DELIVERED, the
      records it was judged against are durable — an immediate check-tail must
      show tail >= K, and a post-412 read of [0,K) must succeed at Remote
      durability. A 412 naming a position that is not yet durable when
      delivered is the finding. Ledger oracle from baseline also applies.
      (Critic-verified chain: durability_dependency = assigned seq,
      error.rs:216-224; deferral gate append.rs:238-247; check-tail returns
      stable_pos, the DURABLE tail, streamer.rs:684-685 — so the oracle is
      non-vacuous. Unary raw-HTTP appends have no session, append.rs:121-122,
      so no session-poisoning can stretch the dependency; K is exact.)
    status: ready
    result: null
    reason: null
    workload: workloads/cas_appends.py
    command: python3 .workers/workloads/cas_appends.py deferred-412-pipelined
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: cas-storm-across-kill
    title: CAS storm across kill
    description: >-
      Fault boundary. SIGKILL the server mid-CAS-storm (kill gated on
      in-flight CAS requests, arm-scaled like acked-appends-pipelined-kill),
      restart on the same root, then resolve every ambiguous in-flight CAS by
      retrying it with its ORIGINAL match_seq_num: if the original won
      pre-kill, the retry must 412 (a second 200 for the same position =
      double-apply, the classic retry/idempotency break); if it lost or never
      landed, the retry follows normal CAS rules. Post-restart ledger
      reconstruction: read-back positions are dense, each owned by exactly one
      winner (pre-kill acked winners all present — durability), and every
      pre-kill 412 still names a position now occupied. REQUIRED (critic):
      payloads are writer+attempt-unique, and on a retry-412 the oracle
      asserts the position's read-back content is the ORIGINAL winner's
      payload — content identity is the only discriminator between
      "double-apply" and "another writer occupied it". Cheap fold-in
      (critic set-gap): include CAS-guarded FENCE command records in the
      storm (fence/trim flow through sequence_records too — CAS-guarded
      fence is the canonical lock-takeover pattern); same ledger rules
      apply to the command positions. Red-proof plan: selftest arm relabels
      one winner's position to fake a double-apply.
    status: done
    result: green
    reason: >-
      GREEN — CAS exactly-once holds across the crash. 6 writers race
      match_seq_num at the contended tail (plus ~1/16 CAS-guarded fence
      commands), SIGKILL mid-storm across all three flush arms, restart,
      then every ambiguous in-flight CAS retried with its ORIGINAL
      match_seq_num. Verified: per-position single winner, every pre-kill
      200-winner durable with exact payload (data AND fence token
      round-trip), every pre-kill 412's named next-seq <= final tail (the
      deferred-412 durability promise never evaporates), no double-apply
      on retry, dense read-back, server serves post-kill. 7 GREEN across
      all 3 arms (shakeout 101 + 300/303/301/304/305 + confirm 88/300)
      + 1 honest anti-vacuity VOID (302: 5 in-flight but 0 ambiguous —
      SIGKILL async, all completed before teardown). Three red-proofs bite
      their exact invariants: doubleapply→at-most-once, phantom412→
      deferred_412_durable, retrydoubleapply→retry-never-double-applies
      (the headline 200-guard). Test-reviewer KEEP (required retry-200
      selftest applied + confirmed RED; fence content-identity enabled per
      review). Residual (reviewer-cleared, non-blocking): the natural
      landed_before bucket (retry-412 content-matched) is structurally
      where correct behavior lands, not a bug guard — the real detector is
      the retry-200 guard, now selftest-proven. Evidence:
      runs/cas-storm-across-kill-green.md.
    workload: workloads/cas_appends.py
    command: python3 .workers/workloads/cas_appends.py storm-across-kill
    faults: []
    depth: 10
    replay: "SEED=88 / 300 (greens); red-proofs 101 with ORACLE_SELFTEST=doubleapply|phantom412|retrydoubleapply"
    freshness: new-current
    reported: 2026-07-07
    published: nd78rde86tgv42np79eftanf4h8a2k1p
---

# CAS appends are exactly-once

## Adversarial model

`match_seq_num` is the API's only optimistic-concurrency primitive; every
client pattern that builds a ledger, a lock, or exactly-once ingestion on S2
leans on it. The mechanism has two seams the docs do not mention: (1) the
match is evaluated against `next_assignable_pos()` (streamer.rs:327-331),
which includes pending batches that are sequenced but NOT yet durable — so a
CAS outcome is decided by state that can still be lost in a crash; (2) the
412 for a rejected conditional is deliberately deferred until its durability
dependency is stable (append.rs:236-247), turning every delivered 412 into a
durability promise about OTHER writers' data. A crash between decision and
durability, or a retry of an ambiguous CAS after restart, is where
exactly-once breaks: the same position acked to two contenders, or a 412
naming state that evaporated.

## Oracle

External ledger, reconstructed client-side from every response: per stream
position at most one 200 (whose `start.seq_num` equals the winner's
match_seq_num), every 412 body `{"seq_num": K}` consistent with the
position's eventual occupant, final read-back dense and byte-identical to
the winner concatenation. The deferred-412 clause is independently
checkable: on 412 delivery, check-tail >= K and Remote read of [0,K)
succeeds. Across the kill: pre-kill acked winners survive (durability),
ambiguous retries with original match_seq_num never double-apply.

## Replay plan

Seeded writer schedules; the workload prints per-trial seed and the full
ledger on failure. Replay = same mode + seed via --workload-file draft; the
official run replays the recorded seed.
