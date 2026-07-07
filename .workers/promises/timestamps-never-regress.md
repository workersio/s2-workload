---
key: timestamps-never-regress
area: appends
title: Timestamps never regress
claim: >-
  Record timestamps are monotonically non-decreasing in sequence order — the
  service adjusts client timestamps up to the maximum observed — across all
  timestamping modes, capping, and crash-restart.
status: active
provenance: "api/src/v1/stream/mod.rs:320-323 (the monotonicity claim); lite/src/backend/streamer.rs:964-1008 (sequenced_records: ClientRequire missing ts -> 422, arrival-cap :993-995 unless uncapped, adjust-up :996-1000); streamer.rs:940-946,1057-1059 + core.rs:95 (max timestamp recovered only via the tail-position key — the restart seam)"
explorations:
  - key: timestamps-baseline
    title: Timestamps baseline
    description: >-
      No faults. Streams configured per timestamping mode (client_prefer,
      client_require, arrival; uncapped on/off), fed a seeded adversarial
      timestamp grid: decreasing, duplicate, far-future, zero, and missing
      client timestamps. Oracle: full read-back is non-decreasing in
      timestamp for every stream; tail.timestamp equals the last record's;
      client_require without a timestamp is rejected with a 4xx (pin the
      OBSERVED class with a probe rather than assuming 422 — critic) and the
      batch atomically absent; arrival mode ignores client timestamps
      entirely; capped
      records never exceed arrival time (bounded by response time), and
      far-future timestamps pass through only when uncapped. Ack
      end.timestamp agrees with check-tail.
    status: ready
    result: null
    reason: null
    workload: workloads/timestamps.py
    command: python3 .workers/workloads/timestamps.py baseline
    faults: []
    depth: 5
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: timestamps-across-restart
    title: Timestamps across restart
    description: >-
      Fault boundary. The in-memory max-observed-timestamp is recovered only
      from the persisted tail-position key (core.rs:95, streamer.rs:363) —
      if recovery seeds it low, a post-restart append with an older client
      timestamp regresses the order and silently corrupts every
      timestamp-indexed read. Drive a high-water client timestamp (uncapped,
      far ahead of arrival), SIGKILL at seed-chosen offsets around the ack
      (in-flight / just-acked / settled, across SL8_FLUSH_INTERVAL arms —
      reuse the straddle kill machinery), restart, then append with client
      timestamps BELOW the pre-kill high water. Oracle: read-back
      non-decreasing across the boundary; post-restart appends adjusted up
      to at least the recovered maximum; if the high-water record was acked
      pre-kill it must both survive (durability) and still dominate the
      order; timestamp-start reads resolve identically before and after
      restart. Red-proof plan: selftest arm perturbs one recorded timestamp
      downward in the observed ledger.
    status: ready
    result: null
    reason: null
    workload: workloads/timestamps.py
    command: python3 .workers/workloads/timestamps.py across-restart
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: null
---

# Timestamps never regress

## Adversarial model

The API commits to server-enforced monotonicity: "the service will always
ensure monotonicity by adjusting it up if necessary to the maximum observed
timestamp" (api/src/v1/stream/mod.rs:320-323). That maximum is in-memory
state recovered across restart solely from the tail-position key — not from
a scan — so the interesting attacks are (a) adversarial client input the
adjust-up must neutralize mode-by-mode, and (b) the crash boundary, where a
lost or stale recovery seed lets an old client timestamp slip under the
pre-kill high water. A regression is silent: appends still ack, reads still
serve — but the timestamp secondary index (which read-window timestamp
starts depend on) is now lying. The corpus has zero timestamp coverage and
upstream's sim has no timestamp-mode modeling.

## Oracle

Total-order invariant over the full ledger: for all i,
ts[i+1] >= ts[i] in seq order, checked over complete read-back after every
phase (and across the restart boundary). Mode-contract clauses: 422 on
client_require-without-timestamp, arrival-mode independence from client
input, cap-at-arrival unless uncapped. Consistency clauses: ack
end.timestamp == check-tail timestamp == last record's; timestamp-start
reads resolve to the same seq before and after restart.

## Replay plan

Seeded timestamp grids and kill offsets; failure prints seed, mode, kill
style, and the offending (seq, ts) pair. Replay = same mode + seed.
