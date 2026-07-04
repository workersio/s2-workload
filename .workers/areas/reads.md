---
key: reads
title: Reads
description: What a reader observes — via a tail/follow read or a committed read — stays consistent with durability across process death and restart.
order: 20
---

# Reads

What this area covers: the read side of the durability contract. The
durability area proves acked *writes* survive a kill; this area proves a
*reader* never observes a record that a post-restart durable read then
denies. The README states the read-side half of the claim directly: data is
"always durable on object storage before being acknowledged **or returned to
readers**."

The seam this area attacks (strategy-critic, 2026-07-05, source-verified):
a follower is fed records only after `durable_seq` advances
(`lite/src/backend/streamer.rs:607`, `follow_tx.send`), while catch-up reads
filter at `DurabilityLevel::Remote` (`lite/src/backend/read.rs:146-150`).
Whether the follow gate and the read filter agree across a crash — i.e.
whether every record a follower already saw is still returned by a
post-restart Remote read — is untested by upstream's in-sim harness, which
does not kill the real process.

Boundaries:
- `--local-root` mode only (same as durability — in-memory has no durability
  claim).
- Kill semantics: SIGKILL of the serving process, no graceful shutdown.
- Trimming/retention interaction with reads is out of scope until a promise
  names it.

Harvested-vs-open: upstream's Porcupine model is write-and-linearizability
focused; real-binary follow-then-crash-then-catch-up against the local
object store is the open flank.
