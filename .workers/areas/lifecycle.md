---
key: lifecycle
title: Stream lifecycle
description: >-
  Delete, purge, and recreate. StreamId is a deterministic hash of
  (basin, stream) (stream_id.rs:24-29), so delete -> recreate REUSES the
  entire KV keyspace; freshness of a recreated stream depends on the
  purge bgtask (backend/bgtasks/stream_trim.rs) having deleted every
  old-incarnation key — records, tail, fencing token, meta, id-mapping —
  and it provably does NOT delete stream_doe_deadline keys.
order: 60
---

# Stream lifecycle

Deletion is multi-stage: terminal-trim record through the streamer ->
mark_stream_deleted bare txn (deleted_at set; recreate gated) -> bgtask
purge on the 60s±10% tick (non-txn WriteBatches of 10k key-pairs,
DurabilityLevel::Remote scan filter) -> finalize_trim txn (deletes
trim-point, meta, id-mapping, tail, fencing — stream_trim.rs:135-146).
Every stage boundary is a kill seam, and the recovery path ends at the
process-aborting assert_no_records_following_tail (core.rs:165-196) if a
recreated stream ever sees leftover records beyond its tail.

Open (beyond the promoted promise): append-vs-delete ack atomicity
(backlog 600 — two-phase delete racing auto-create first-appends,
#469).
