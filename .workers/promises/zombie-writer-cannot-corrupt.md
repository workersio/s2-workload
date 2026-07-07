---
key: zombie-writer-cannot-corrupt
area: durability
title: Zombie writer cannot corrupt
claim: >-
  A superseded s2 lite instance on the same --local-root cannot acknowledge
  or persist appends once a newer instance has taken over; nothing a zombie
  accepts after takeover appears in the stream.
status: active
provenance: "lite/src/server.rs startup (sleeping to ensure prior instance fenced out — time-based fencing on one manifest_poll_interval); SlateDB manifest-epoch fencing"
explorations:
  - key: zombie-writer-sigstop-takeover
    title: Zombie writer sigstop takeover
    description: >-
      Start instance A and write through it; SIGSTOP A (not kill); start
      instance B on the same root and write through B; SIGCONT A and push
      appends through A's still-open connections. Any post-takeover append
      acked by A that survives in read-back is corruption; the two
      instances' accepted writes must never interleave into an inconsistent
      stream.
    status: done
    result: green
    reason: null
    workload: workloads/zombie_writer.sh
    command: sh .workers/workloads/zombie_writer.sh sigstop-takeover
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: nd7ab7890b264pgx8v5vmndyk98a2k07
  - key: zombie-live-overlap-double-start
    title: Zombie live overlap double start
    description: >-
      The floor rung strategy-critic required instead of certifying this
      promise 2-rung (producer #8): in both existing rungs the prior
      instance is FROZEN (SIGSTOPped or dead) during the successor's entire
      boot, so the time-based fencing assumption — the startup sleep of one
      manifest_poll_interval "to ensure prior instance fenced out"
      (server.rs:186-198) — is never actually contested. Here it is: no
      signals at all. Instance A serves and keeps ACTIVELY appending
      (steady writer pool) while instance B starts on the same root, sleeps
      through its fencing window, takes over, and serves writes — an
      operator double-start / supervisor-restart-while-hung, the promise's
      honest no-fault shape and its highest-novelty attack at once. A keeps
      writing throughout B's boot and after takeover until A's handle
      self-fences ("detected newer DB client") or A is torn down. Oracle is
      the existing persist-time-boundary family unchanged (invariants 1-4
      of this promise): nothing A persists after B's takeover boundary
      appears in read-back, B-acked records exactly once, dense read-back;
      anti-vacuity requires >=1 A-append attempt landing DURING B's fencing
      sleep and >=1 after B's first ack, both observably reaching A's write
      path. A-acks issued during B's boot that lie below the takeover
      boundary are legitimate (persist-time criterion, invariant 2).
      Availability clause (test-reviewer, executor #16): B failing to
      serve on the live root or persistently refusing appends is RED
      (successor_available) — a double-start must not brick the
      successor, and a fencing-sleep regression would present exactly as
      successor dysfunction; data verification still runs first where
      reachable. Mode-scoped oracle tightening: no truncation allowance —
      every A-acked record must appear in read-back (no freeze exists to
      excuse a hole; a missing durable ack is acked-data loss).
      Reuses workloads/zombie_writer.sh machinery minus signals.
    status: done
    result: green
    reason: null
    workload: workloads/zombie_writer.sh
    command: sh .workers/workloads/zombie_writer.sh live-overlap-double-start
    faults: []
    depth: 10
    replay: >-
      green sweep draft nd71ggcsn58jg65edhqs9kktzd8a0x88 (depth 10, 10/10
      green, every trial witnessed 69-80 boot-window + 76-151 post-T
      attempts, ZERO A-acks after T; e.g. seed 931885941). Shakeout
      nd7131887bhhs35hk8m22p11p58a02hq (4/4). Red-proofs: pre-REDO
      nd728vnm0wysjw0hrqqwtjj77x8a1qmb (2/2 RED, seeds 4095698169 /
      813245199 -> no_zombie_persisted); post-REDO
      nd7ah1zn37z0xkjg93atcpe11d8a0e5z (seed 4035185551 -> RED). Post-REDO
      confirm nd75z3nwechmrh278evxsqkwz98a0aas (3/3 green,
      successor_available PASS emitted). All via --workload-file injection.
    freshness: new-current
    reported: null
    published: nd7chgy4ps49fmkzd8qvd9nbg18a2fs7
  - key: zombie-double-kill-mid-recovery
    title: Zombie double kill mid recovery
    description: >-
      The sigstop-takeover arm meets kill-during-recovery: A is SIGSTOPped
      mid-stream (zombie, sockets open, SlateDB handle live); B takes over
      on the same root, writes, and is SIGKILLed; C starts, and DURING C's
      lazy first-access recovery (start_streamer -> load_persisted_stream_tail
      -> assert_no_records_following_tail) the zombie A is SIGCONTed and
      pushes appends through its still-open connections — the zombie races
      the tail rebuild itself rather than a settled instance. Oracle is the
      sigstop-takeover family: nothing A persisted after B's takeover
      boundary may appear in read-back, B/C acked records exactly once,
      dense readback; anti-vacuous requires >=1 zombie attempt reaching A's
      write path while C's recovery is provably un-served (before C's first
      successful check-tail) — otherwise void. Availability clause: C
      persistently failing recovery (assert_no_records_following_tail guard
      tripping, core.rs:113/165-189, or the stream otherwise unrecoverable
      after bounded retries) is a RED availability finding, NOT a void —
      recovery is an error/abort path and a zombie must not be able to
      brick it. Executor note: the recovery window is sub-ms–ms (Remote
      reads); expect a high void rate and spam zombie attempts across the
      whole window rather than firing once.
    status: done
    result: green
    reason: null
    workload: workloads/zombie_writer.sh
    command: sh .workers/workloads/zombie_writer.sh double-kill-mid-recovery
    faults: []
    depth: 10
    replay: >-
      green sweep draft nd7b0a5bdfp8p1ca2s3yj2z19n8a0q33 (depth 10, 10/10
      green, zero voids; 5-8 zombie attempts inside every 196-297ms un-served
      recovery window; C recovered on attempt 1 in all; e.g. seed 618423676).
      Red-proof draft nd7998hybtnc19wkthdhw1bdg58a17v6 (seed 1113152951,
      ORACLE_SELFTEST relabel -> no_zombie_persisted FAIL, exit 1).
      Post-hardening confirm nd70xg28j0xyjk4ew5hwx6gscs8a1e5t (3/3 green,
      send+response-in-window witness). All via --workload-file injection.
    freshness: new-current
    reported: null
    published: nd717br5hsc30k6bc0fxrwfwsn8a2wst
---

# Zombie writer cannot corrupt

## Adversarial model

The startup path *itself* flags this hazard: a new instance sleeps one
`manifest_poll_interval` "to ensure prior instance fenced out" — fencing by
elapsed time, not by proof. A SIGSTOP'd instance is the classic adversary
time-based fencing loses to: it holds open sockets and an initialized
SlateDB handle, wakes after the window, and writes as if it still owns the
root. The backstop is SlateDB's manifest-epoch CAS on the local filesystem
(`object_store` LocalFileSystem rename semantics) — whether that backstop
actually stops an already-initialized zombie's WAL writes on a local FS is
exactly the open question, unreachable by upstream's in-sim testing and
plausibly untested by Antithesis. Highest bug-likelihood attack in the
current set; slot it ahead of fencing-stale-across-restart when promoting.

Cheap add-on arm (same workload family, later): double-kill — SIGKILL the
restarting server mid-recovery and restart again.

## Oracle

Drivers keep separate ack manifests for A and B (raw HTTP, one request =
one ack). Let T = the moment B's first ack returns, and let the takeover
boundary = B's recovered tail, observed after B first serves check-tail
and before B's first append. Invariants:
1. Every B-acked record appears exactly once in read-back.
2. No append PERSISTED by A after takeover appears in read-back. The
   criterion is persist-time, not ack-time: an A ack returned after T
   whose seq range lies entirely below the takeover boundary is a late
   ack of a write that was durable before B took over (B recovered it and
   built on it) — allowed, not a finding. An A-accepted record at or
   beyond the boundary that appears in read-back is the finding, acked or
   not. (2026-07-05 draft evidence: the ack-time phrasing false-flagged
   in-flight-at-SIGSTOP appends whose records B had already recovered —
   seq below boundary, stream dense, no consumer-visible inconsistency.)
   An A ack after T for a record absent from read-back is a lie told to a
   client that should have been fenced — log it, but rejected/dropped/
   lost zombie appends are all fine, not findings.
3. Read-back [0, check-tail) is a dense, gapless seq range whose contents
   are exactly (A-acked-before-T set, possibly-truncated suffix aside) ∪
   (B-acked set), each at most once.
4. Anti-vacuous gate: the zombie's post-SIGCONT attempts must observably
   reach A's write path — at least one post-CONT attempt gets an HTTP
   response from A (an ack, or a storage-layer rejection such as
   "detected newer DB client"). Pure connection failures mean the zombie
   never really tried, and the trial is void.

Measurement limit (live-overlap rung; test-reviewer, executor #16): the
boundary is read at B's first check-tail 200, before B's first append, so
two blur windows are black-box-indistinguishable from pre-takeover
durable writes: (a) an A write persisted in [B DB-open, boundary-read]
that B recovers and builds on classifies as legitimate; (b) an A ack
after T whose range lies at/below the measured boundary classifies as a
"LATE ACK (allowed)" — those log lines are the triage hook if one ever
looks suspicious. Neither is consumer-visible corruption (the stream
stays dense and B-acked intact); empirically both are unreachable today
(A's handle is fenced at B's DB-open, ~3s before T, zero post-T A-acks
across 10/10 sweep trials).

## Replay plan

Seed drives the SIGSTOP point, B start delay, SIGCONT delay, and both
payload schedules. Red runs replay by recorded seed via --exploration.
