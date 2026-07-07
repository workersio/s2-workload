---
key: acked-appends-survive-restart
area: durability
title: Acked appends survive restart
claim: >-
  Every append acknowledged by s2 lite running with --local-root is present,
  with the same content and order, after the server process is killed and
  restarted on the same root.
status: active
provenance: README.md ("data is always durable on object storage before being acknowledged or returned to readers")
explorations:
  - key: acked-appends-baseline
    title: Acked appends baseline
    description: >-
      No faults, graceful lifecycle: start lite with --local-root, append a
      known sequence, stop the server cleanly, restart, read back. Proves the
      oracle (acked-manifest vs read-back diff) observes the invariant at all
      before any adversarial variant runs.
    status: done
    result: green
    reason: null
    workload: workloads/acked_appends.py
    command: python3 .workers/workloads/acked_appends.py baseline
    faults: []
    depth: 3
    replay: null
    freshness: new-current
    reported: null
    published: nd7e4vkb3jd8f9j1bq8qb5a85x8a2y0a
  - key: acked-appends-kill9-mid-stream
    title: Acked appends kill9 mid stream
    description: >-
      SIGKILL the server while a writer is streaming appends, with the seed
      also selecting an SL8_FLUSH_INTERVAL arm (default | 500ms | 2s) to
      widen the ack-to-flush window; restart on the same root; every append
      acknowledged before the kill must be present and in order. Unacked
      in-flight appends may or may not appear, but must not corrupt the
      acked prefix and must not be duplicated.
    status: done
    result: green
    reason: null
    workload: workloads/acked_appends.py
    command: python3 .workers/workloads/acked_appends.py kill9
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: nd78r3n7xjgadhahdzpwnxgaw58a2ymt
  - key: acked-appends-kill-during-recovery
    title: Acked appends kill during recovery
    description: >-
      Append and ack a prefix P while streaming, SIGKILL #1 mid-stream, then
      restart and SIGKILL #2 during the first post-restart stream access —
      the lazy per-stream recovery, not the inert startup sleep. s2-lite's
      real recovery is start_streamer -> load_persisted_stream_tail
      (core.rs:82,144, DurabilityLevel::Remote) -> assert_no_records_following_tail
      (core.rs:165), triggered by the first request touching the stream
      (the check-tail readiness probe forces the streamer spawn). SIGKILL #2
      lands mid-rebuild, before a successful check-tail returns; then restart
      a third time cleanly. Every record acked before SIGKILL #1 must be
      present, in order, exactly once after the final restart. This attacks
      the s2-lite tail-rebuild path being interrupted, which kill9 (single
      kill of the serving process) never reaches.
    status: done
    result: green
    reason: null
    workload: workloads/acked_appends.py
    command: python3 .workers/workloads/acked_appends.py kill-during-recovery
    faults: []
    depth: 10
    replay: >-
      green draft nd71w6wkxmtkw8cz8w0qm3680x89ywce (depth 10, 10/10 green, e.g.
      seed 925258047: 211 acked, recovery interrupted 2x, all 6 invariants
      pass); red-proof draft nd7eg0yedp6bmee03pb9c9erdd89zzjn (seed 4021235855,
      2s flush arm, 663 acked, recovery interrupted 4x, ORACLE_SELFTEST drop ->
      dense_prefix FAIL, exit 1). Both via --workload-file injection on prod.
    freshness: new-current
    reported: null
    published: nd7fc5ttcnx7yknz32jxntqwa98a32r2
  - key: acked-appends-pipelined-kill
    title: Acked appends pipelined kill
    description: >-
      Four concurrent writer connections keep several appends in flight at
      once (one request = one ack per connection); SIGKILL lands mid-burst
      with multiple appends in flight and freshly-acked records inside the
      flush window, seed also selecting the SL8_FLUSH_INTERVAL arm
      (default | 500ms | 2s); restart on the same root. Every record acked
      to ANY writer before the kill must be present exactly once with
      identical content; per-writer ack order maps to strictly increasing
      seqs; unacked in-flight appends appear at most once. Distinct from
      kill9: that arm kills a serial writer with exactly one in-flight
      append, so the acked-set at risk is one flush window of ONE
      pipelined stream — this arm is the multi-writer mid-pipeline kill
      that the write-side suite never fires (test-reviewer flag,
      2026-07-06).
    status: done
    result: green
    reason: null
    workload: workloads/acked_appends.py
    command: python3 .workers/workloads/acked_appends.py pipelined-kill
    faults: []
    depth: 10
    replay: >-
      green sweep draft nd76m3rw39zj15m78d07snee118a1zqq (depth 10, 10/10
      green, all non-vacuous: 2-4 in flight at kill, last ack 0.6-32ms prior;
      e.g. seed 130903179 default arm, 325 acked). Red-proof draft
      nd73ta7cztjngkpryvcxx8mc0d8a0ee8 (seed 3400598209, 500ms arm,
      ORACLE_SELFTEST drop -> dense_prefix FAIL, exit 1). Post-hardening
      confirm nd76s1w1crwy1zz5x430m7mryx8a0shh (2/2 green). All via
      --workload-file injection on prod.
    freshness: new-current
    reported: null
    published: nd7f7exc43qv3pk442fpt6ycv58a2bn2
---

# Acked appends survive restart

## Adversarial model

Ack IS durability-gated by design: the streamer submits to SlateDB with
`await_durable: false` but releases the client ack only after the
`durable_seq` watch covers the batch (`lite/src/backend/streamer.rs:571`,
`durability_notifier.rs`; unit-tested at streamer.rs:1549). The flush
interval for `--local-root` defaults to **5ms** (`lite/src/server.rs:96` —
50ms is S3-only), overridable via `SL8_FLUSH_INTERVAL`. The bug class this
promise hunts is therefore the *marginal* gating bug — the notifier firing
when the WAL write is issued rather than completed, or an ack path that
bypasses the notifier — whose window at 5ms is microseconds. To make the
attack honest, the kill9 exploration seed-derives both the kill delay AND
an `SL8_FLUSH_INTERVAL` arm (default | 500ms | 2s): correctly gated acks
merely slow down when the interval stretches; a leaking ack path loses
~1-2s of acked traffic on kill — near-certain red per seed.

SIGKILL is the right level: the memtable/WAL buffer lives in process
memory and the page cache survives process death, so the mechanism is
preserved without disk-level faults (those are a later axis).

Trigger discipline: the kill must land while writes are streaming —
in-flight-unacked > 0 asserted at kill time, no kill-after-quiesce theater.

## Oracle

The driver (never killed) appends via raw HTTP, one request per append —
one request = one ack = one manifest line, written only after the response
is fully read. (The s2 CLI is unusable for the ack manifest: it prints
acks to stderr, ANSI-colored, deduped per linger batch — ack granularity
!= record granularity. CLI is fine for the restart read-back.)

Payloads are unique per (seed, writer, index). After restart, poll
`check-tail` for readiness (startup sleeps one manifest_poll_interval as
time-based fencing — fixed sleeps give false reds), then require:
1. `tail >= max acked end seq` — completeness is bounded by the server's
   own tail, not by whatever a read happened to return.
2. Reading [0, tail): every acked record appears exactly once, in ack
   order, with identical content.
3. Dense seq prefix — no gaps below tail.
4. Unacked payloads may be present or absent, but at most once — a WAL
   double-apply on recovery must not pass just because the record was
   indeterminate.
5. Anti-vacuous gate: acked_count at kill >= floor AND >= 1 in-flight
   unacked at kill; otherwise the trial is void, not green.

Before the baseline goes green, the oracle must be shown able to go red
(mutate the manifest or drop a record from read-back once, manually) —
"diff passed" is only meaningful after that.

## Recovery-atomicity arm (kill-during-recovery)

kill9 kills the *serving* process once; this arm kills the *recovering*
process — but at the point where s2-lite's own recovery code actually runs.
Strategy-critic (2026-07-05, source-verified) corrected an earlier draft
that aimed SIGKILL #2 at the `sleep(manifest_poll_interval)` startup wait
(server.rs:188-198): that sleep is inert and killing during it mutates
nothing, and killing during SlateDB's `build()` only interrupts SlateDB's
own WAL/manifest replay (upstream/sim-tested). The s2-lite recovery is
**lazy and per-stream, on first access after serving begins**:
`start_streamer` (core.rs:82) calls `load_persisted_stream_tail`
(core.rs:144, `DurabilityLevel::Remote`) then the hard guard
`assert_no_records_following_tail` (core.rs:165, panics if any record sits
beyond the persisted tail). SIGKILL #2 must land *there* — during the first
stream access the check-tail readiness probe forces — so the tail-rebuild
is interrupted and must resume cleanly on the third start. The bug class: a
tail rebuilt inconsistently with what was durable, or the assert-guard
firing on a legitimately-recovered stream, dropping or duplicating records
acked before kill #1.

Oracle invariants (same acked-manifest-vs-read-back family as kill9):
1. `tail >= max acked-before-kill1 end seq` after the final restart.
2. Every record acked before SIGKILL #1 appears exactly once in read-back,
   in ack order, identical content.
3. Dense seq prefix — no gaps below tail.
4. Unacked / in-flight records may be present or absent, but at most once —
   a recovery double-apply must not pass as "indeterminate".
5. Anti-vacuous: SIGKILL #2 must land during first-access recovery — after
   the restarted process began accepting connections but before it returned
   a *successful* check-tail for the stream (assert no successful readiness
   response was observed before the kill); and acked-before-kill1 count >=
   floor. Otherwise the rebuild path was never interrupted and the trial is
   void.

## Replay plan

Seed drives kill delay, payload schedule, and flush-interval arm — all
three, or replay does not reproduce. The recovery arm additionally
seed-derives the SIGKILL #2 delay within the restart window. A red run replays its recorded seed
via `--exploration acked-appends-kill9-mid-stream`; evidence lands in
runs/ with the manifest, the read-back dump, and the diff.

## Adversarial model — pipelined-kill arm (producer #7, 2026-07-06)

kill9 proves a serial writer's acked prefix survives; its at-risk set at the
kill is one in-flight append. With concurrent writers the streamer pipelines
multiple batches against storage latency (per-stream task serializes
sequencing, but several submitted-unflushed batches coexist), and the
durability notifier must release each ack only after ITS batch is covered —
a notifier that fires on submission order rather than durable coverage, or
coalesces watch updates across interleaved batches, loses acked records
only in the multi-writer shape. Executor plan: reuse acked_appends.py
machinery; writer pool of 4 threads each with its own manifest; kill point
after a seed-chosen global ack count with >=2 appends in flight (spin for
the window like reads_tail does); oracle = existing verify() family with
the global-ack-order clause replaced by per-writer order (ack order within
one connection maps to strictly increasing seqs; cross-writer interleaving
is unconstrained). Anti-vacuous: >=2 in flight at kill AND >=1 ack returned
within the last flush window before the kill. Red-proof: ORACLE_SELFTEST
drops one acked record from the readback (existing selftest path).

## Replay plan — pipelined-kill

Seed drives writer pacing, kill point, flush arm, and pool interleaving.
Red runs replay by recorded seed via --exploration.
