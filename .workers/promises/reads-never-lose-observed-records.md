---
key: reads-never-lose-observed-records
area: reads
title: Reads never lose observed records
claim: >-
  A record delivered to a reader — over a tail/follow read or a committed
  read — is never absent, reordered, or duplicated afterward, including
  across a server kill and restart; a follower is never handed a record that
  a post-restart durable read then fails to return.
status: active
provenance: "README.md (\"durable on object storage before being acknowledged or returned to readers\"); lite/src/backend/streamer.rs:607 (follow_tx.send gated on durable_seq); lite/src/backend/read.rs:146-150 (catch-up read filters at DurabilityLevel::Remote)"
explorations:
  - key: reads-tail-baseline
    title: Reads tail baseline
    description: >-
      No faults: one writer appends a known sequence while a follower tails
      the stream from seq 0; every acked record must be delivered to the
      follower exactly once, in seq order, with identical content, and a
      final catch-up read [0, tail) must equal what the follower observed.
      Bring-up: proves the follow-vs-read oracle and the follow transport
      work at all before the restart attack. Expectations demoted — a green
      here is plumbing, not evidence about the crash seam.
    status: ready
    result: null
    reason: null
    workload: workloads/reads_tail.py
    command: python3 .workers/workloads/reads_tail.py baseline
    faults: []
    depth: 5
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: reads-tail-across-restart
    title: Reads tail across restart
    description: >-
      A follower tails the stream and records every delivered record up to
      some seq K; the server is SIGKILLed mid-stream and restarted on the
      same root; the reader resumes from K via a catch-up read at Remote
      durability. The follow gate (durable_seq, streamer.rs:607) and the read
      filter (DurabilityLevel::Remote, read.rs:146-150) must agree across the
      crash: no record the follower already observed may be absent,
      reordered, or duplicated after restart. A record handed to the follower
      that a post-restart Remote read does not return is a dirty/phantom read
      across a crash — a high-value durability finding.
    status: ready
    result: null
    reason: null
    workload: workloads/reads_tail.py
    command: python3 .workers/workloads/reads_tail.py across-restart
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: null
---

# Reads never lose observed records

## Adversarial model

Every promise so far is write-side: it checks what a reader sees *once*,
after everything settles. This promise attacks the live read path across a
crash. Two mechanisms meet at the crash boundary:

- **Follow delivery is durability-gated.** A follower receives a record only
  after `durable_seq` covers it — `follow_tx.send` fires inside the
  durable-seq-advanced path (`lite/src/backend/streamer.rs:607`). So in
  principle a followed record was already durable when the client saw it.
- **Catch-up reads filter at Remote durability** (`read.rs:146-150`). After
  restart the reader re-reads from where it left off, and the server returns
  only records durable at the Remote level.

The bug class: the two thresholds disagree across a kill. If a follower is
fed a record on the basis of an in-memory `durable_seq` watermark that had
advanced past what actually landed on the object store — or if recovery
rebuilds the tail below a seq the follower already consumed — then a post-
restart Remote read denies a record the client already observed. That is a
dirty read surfacing as durability loss, invisible to any write-only oracle
and to upstream's in-sim harness (which never kills the real process).

SIGKILL of the serving process is the right level: the memtable/WAL buffer
and the broadcast follow channel live in process memory, so a divergence
between "sent to follower" and "durable on disk" is exactly what the kill
exposes. No disk faults needed.

## Oracle

The follower consumes the stream (raw HTTP follow/tail; the executor's probe
run determines the concrete transport — SSE, long-poll, or chunked read —
and records it as a reality note). One log line per *delivered* record:
(seq, content), written only after the record is fully read from the
follow channel. Separately, a final catch-up read [0, tail) after restart.

Invariants:
1. Every record the follower observed before the kill appears in the
   post-restart catch-up read, at the same seq, with identical content —
   none absent (the durability finding), none moved.
2. The follower's delivered stream is itself gap-free and duplicate-free in
   seq order, before and across the restart resume.
3. The post-restart catch-up read [0, tail) is dense and gapless, and is a
   superset of what the follower observed (the reader may see *more* after
   restart — records that became durable — but never *less*).
4. Anti-vacuous: the follower must have observed at least one record whose
   seq is at or beyond the recovered tail region the kill could threaten —
   a kill after the follower already drained everything durable proves
   nothing. Assert observed_count >= floor AND the kill landed while the
   follower was actively behind the writer (lag > 0 at kill).

Before baseline goes green, the oracle must be shown able to go red: an
`ORACLE_SELFTEST=1` flag drops one record from the follower's observed log
(or the catch-up read) once, and the diff must FAIL.

## Replay plan

Seed drives the writer schedule, the follower's start offset, and the kill
point (which must land while the follower is behind). A red run replays its
recorded seed via `--exploration reads-tail-across-restart`; evidence lands
in runs/ with the follower's observed log, the post-restart catch-up dump,
and the diff.
