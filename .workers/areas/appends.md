---
key: appends
title: Appends
description: Append-path contracts beyond raw durability — conditional (CAS) appends, timestamp sequencing, batch limits, and input hardening behave exactly as documented, including under concurrency and crash-restart.
order: 30
---

# Appends

What this area covers: the semantic contracts of the append path itself.
The durability area proves an ack survives a kill; this area proves the
*decision* to ack was correct — that a `match_seq_num` compare-and-swap
admits exactly one winner, that assigned timestamps never regress, that
batch caps reject atomically, and that malformed input cannot wedge the
streamer.

Why it is its own area: these are ordering/admission invariants, not
persistence invariants. Their failure mode is a wrong answer served with
full durability — a double-applied CAS, a regressed timestamp corrupting
the timestamp index — which no durability oracle can see.

Boundaries:
- `--local-root` mode for anything with a kill arm; in-memory acceptable
  for pure-concurrency arms only if the promise says so.
- Fencing token semantics live in the durability area
  (fencing-excludes-stale-writers); this area treats tokens only as
  incidental request fields.
- Encryption and s2s-session hardening are backlog corridors in this area,
  not yet promised.

Harvested-vs-open: upstream's Porcupine/linearizability sim covers
append/read/check-tail/fencing/match-seq-num *logic* under network faults
in-sim (sim/src/scenarios/linearizable.rs:52-56) but never crosses a real
process kill, and its sim has no timestamp-mode or caps modeling. The open
flank is these contracts on the real binary across crash-restart.
