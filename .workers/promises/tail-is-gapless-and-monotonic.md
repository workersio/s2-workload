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
    published: nd77b88r534pn650fxfzgnyh2189xdmv
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
    published: nd7dxn5nb5987v3x5977jsmgkx89wtx2
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

## Replay plan

Seed drives writer interleaving and (in the restart variant) the kill
point. Red runs replay by recorded seed via --exploration.
