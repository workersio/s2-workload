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
    published: nd722qkpc6sgah6k5t19jfkpg589x2hr
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
    published: nd7ac90f6g2yhyqkf8yktmyf3d89xzzw
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

## Replay plan

Seed drives kill delay, payload schedule, and flush-interval arm — all
three, or replay does not reproduce. A red run replays its recorded seed
via `--exploration acked-appends-kill9-mid-stream`; evidence lands in
runs/ with the manifest, the read-back dump, and the diff.
