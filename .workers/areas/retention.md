---
key: retention
title: Retention
description: Deliberate data removal — explicit trim, age-based TTL expiry, and delete-on-empty — removes exactly what was asked, never more, never less, and removal decisions hold across crash-restart.
order: 40
---

# Retention

What this area covers: the inverse of durability — data the system is
*supposed* to destroy. Three mechanisms, three different machines: explicit
trim (a command record applied synchronously in the streamer, with async
physical deletion by bgtask), per-record slatedb TTL fixed at append time
(age retention), and delete-on-empty (a deadline-armed bgtask that deletes
whole streams).

The two-sided oracle is what makes this area honest: under-deletion
(trimmed/expired records resurfacing — a resurrection) and over-deletion
(records beyond the trim point, or streams with live acked writes,
destroyed — acked-data loss). Both directions are findings.

Boundaries:
- `--local-root` mode only; every mechanism here persists state the
  restart must honor (trim-point KV, expire_ts, DOE deadline KV).
- Read-window semantics after a trim (what a `tail_offset` read returns
  near the trim point) belong to the reads area promise
  reads-honor-request-windows; this area owns whether the data is gone,
  not how the window arithmetic reports it.
- DOE wall-clock reality: every DOE deadline carries a 600s refresh pad
  (streamer.rs:57-63) — DOE explorations are long-budget by construction
  and stay in the backlog until a design amortizes that.

Harvested-vs-open: upstream unit-tests the DOE deadline logic heavily
(stream_doe.rs:327-802) and seeds trim bgtask tests with hand-written KV
rows, but nothing exercises trim/TTL/DOE against the real binary across a
kill, and the sim models none of the three mechanisms.
