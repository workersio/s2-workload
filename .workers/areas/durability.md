---
key: durability
title: Durability
description: Acknowledged data survives process death and restart, with order and stream integrity intact.
order: 10
---

# Durability

What this area covers: the contract between an append acknowledgement and
what a reader observes after the `s2 lite` process dies and comes back on the
same `--local-root`. The README states it directly: "data is **always
durable** on object storage before being acknowledged or returned to
readers — just like s2.dev."

Boundaries:
- Only `--local-root` mode is in scope — in-memory mode is documented as an
  emulator with no durability claim.
- Kill semantics: SIGKILL of the server process (no graceful shutdown path).
  Host/QEMU-level power loss is a later, separate axis (needs wenv disk
  faults, not just process kill).
- Trimming and retention are out of scope until a promise names them.

Harvested-vs-open: upstream runs turmoil/madsim deterministic sims plus
Antithesis with a Porcupine linearizability model
(github.com/s2-streamstore/s2-verification), so simulated-runtime bugs are
well harvested. Real-binary, real-filesystem kill/restart against SlateDB's
local object store is the open flank we attack here.
