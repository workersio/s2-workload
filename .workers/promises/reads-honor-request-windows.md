---
key: reads-honor-request-windows
area: reads
title: Reads honor request windows
claim: >-
  Every read returns exactly the records its request window describes — for
  any combination of seq_num, timestamp, or tail_offset start, clamp,
  count/bytes limits, and exclusive until — and an out-of-range request fails
  with a 416 carrying the true tail.
status: active
provenance: "lite/src/backend/read.rs:249-288 (start resolution: tail_offset saturating_sub at :260, clamp rescue :267-271, UnwrittenError :273-277); read.rs:290-327 (timestamp resolution via secondary index, unwrap_or(tail) at :284); lite/src/handlers/v1/records.rs:38-47 (timestamp>=until -> 422); handlers/v1/error.rs:296 (416 carries TailResponse); common/src/read_extent.rs:163-175 (until exclusive)"
explorations:
  - key: read-window-baseline
    title: Read window baseline
    description: >-
      No faults. Seed one stream with a known (seq, timestamp, body) ledger
      that deliberately contains duplicate-timestamp runs (client timestamps
      adjusted up create runs of equal ts, streamer.rs:996-1000). Sweep a
      seeded cross-product of (start: seq_num | timestamp | tail_offset,
      clamp on/off, count/bytes limits, until, wait) and assert exact slice
      equality against a locally computed model: timestamp-start with a
      duplicate-ts run resolves to the FIRST seq in the run; until is
      exclusive; limits truncate exactly; every 416 body's tail equals
      check-tail; mutually-exclusive start params rejected 422. Proves the
      window model matches the implementation before any adversarial state.
      Run the sweep SERIALLY (critic) so 416-tail == check-tail is
      race-free.
    status: ready
    result: null
    reason: null
    workload: workloads/read_windows.py
    command: python3 .workers/workloads/read_windows.py baseline
    faults: []
    depth: 5
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: read-window-trim-boundary
    title: Read window at the trim boundary
    description: >-
      Adversarial state: the same window sweep against a stream whose head
      has been trimmed away. tail_offset start is computed by
      tail.seq_num.saturating_sub(offset) (read.rs:260) with no floor at the
      trim point, and clamp only rescues start-beyond-tail (read.rs:267-271)
      — so windows whose computed start lands below the trim point exercise
      arithmetic upstream churned in the same family (#527 saturating_add on
      the SSE side). CRITIC REVISION (source fact): the read path never
      consults the trim point — zero trim references in read.rs; absence is
      purely physical via the async purge bgtask, so below-trim reads
      legitimately serve trimmed records until the purge lands. REQUIRED:
      after trimming, run an explicit purge-completion barrier (poll a
      below-trim read until absent, bounded by the 60s±10% tick ceiling)
      BEFORE the window sweep — sweeping earlier reds on healthy behavior.
      Post-purge model (critic-confirmed): a below-trim seq start scans
      forward to the first live key, no error. Sweep model: reads starting
      below the trim point return records from the first live seq; no
      phantom trimmed records, no mid-window gaps, no wrong-first-record,
      416/clamp behavior consistent between unary and SSE paths. Keep the
      bonus corner: timestamp-start below the trim point after purge
      exercises the unwrap_or(tail) branch (read.rs:284) on a purged index —
      unreachable any other way. Requires trim (command-record path) but no
      kill — trim durability itself belongs to trim-is-final.
    status: ready
    result: null
    reason: null
    workload: workloads/read_windows.py
    command: python3 .workers/workloads/read_windows.py trim-boundary
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: null
---

# Reads honor request windows

## Adversarial model

The read path resolves a request window through three independent start
mechanisms (seq_num, timestamp secondary-index scan, tail_offset
subtraction), two truncation budgets (count, bytes), one exclusive bound
(until), and a rescue flag (clamp) — a cross-product the unit suite samples
only pointwise. The corpus's reads promise proves delivered records are
durable; nothing yet proves the WINDOW is right — off-by-one at a
duplicate-timestamp run, a tail_offset that lands below the trim point, or
a 416 that misreports the tail are all silent wrong-answer bugs that no
durability oracle can catch. Upstream churn (#527, #374) shows this exact
arithmetic family has bitten before.

## Oracle

Model-based exact equality: the client holds the full (seq, ts, body)
ledger it wrote, computes the expected slice for every parameter
combination locally, and requires byte-exact agreement — plus contract
checks on the error surface (422 for invalid combinations, 416 whose
TailResponse equals check-tail). On the trim arm the model is re-derived
from the trim point; the pinned-contract probes make the oracle
self-documenting rather than guess-driven.

## Ladder note

Two rungs drafted (baseline + adversarial-state). The fault-boundary rung
(window sweep across a kill/restart) is deliberately deferred: the restart
read path is already exercised by reads-never-lose-observed-records, and a
window-across-restart rung should be added only if either arm here
surprises. Under the 3-rung floor this promise stays producible work until
strategy-critic certifies otherwise or the third rung is drafted.

## Replay plan

Seeded ledger + seeded parameter sweep; failure prints the parameter tuple,
expected slice, and received slice. Replay = same mode + seed.
