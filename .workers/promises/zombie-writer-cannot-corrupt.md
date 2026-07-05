---
key: zombie-writer-cannot-corrupt
area: durability
title: Zombie writer cannot corrupt
claim: >-
  A superseded s2 lite instance on the same --local-root cannot acknowledge
  or persist appends once a newer instance has taken over; nothing a zombie
  accepts after takeover appears in the stream.
status: active
provenance: "lite/src/server.rs startup (sleeping to ensure prior instance fenced out — time-based fencing on one manifest_poll_interval); SlateDB manifest-epoch fencing"
explorations:
  - key: zombie-writer-sigstop-takeover
    title: Zombie writer sigstop takeover
    description: >-
      Start instance A and write through it; SIGSTOP A (not kill); start
      instance B on the same root and write through B; SIGCONT A and push
      appends through A's still-open connections. Any post-takeover append
      acked by A that survives in read-back is corruption; the two
      instances' accepted writes must never interleave into an inconsistent
      stream.
    status: done
    result: green
    reason: null
    workload: workloads/zombie_writer.sh
    command: sh .workers/workloads/zombie_writer.sh sigstop-takeover
    faults: []
    depth: 10
    replay: null
    freshness: new-current
    reported: null
    published: nd78k85sk1fkre1w7jjw7290kd89ypc0
---

# Zombie writer cannot corrupt

## Adversarial model

The startup path *itself* flags this hazard: a new instance sleeps one
`manifest_poll_interval` "to ensure prior instance fenced out" — fencing by
elapsed time, not by proof. A SIGSTOP'd instance is the classic adversary
time-based fencing loses to: it holds open sockets and an initialized
SlateDB handle, wakes after the window, and writes as if it still owns the
root. The backstop is SlateDB's manifest-epoch CAS on the local filesystem
(`object_store` LocalFileSystem rename semantics) — whether that backstop
actually stops an already-initialized zombie's WAL writes on a local FS is
exactly the open question, unreachable by upstream's in-sim testing and
plausibly untested by Antithesis. Highest bug-likelihood attack in the
current set; slot it ahead of fencing-stale-across-restart when promoting.

Cheap add-on arm (same workload family, later): double-kill — SIGKILL the
restarting server mid-recovery and restart again.

## Oracle

Drivers keep separate ack manifests for A and B (raw HTTP, one request =
one ack). Let T = the moment B's first ack returns, and let the takeover
boundary = B's recovered tail, observed after B first serves check-tail
and before B's first append. Invariants:
1. Every B-acked record appears exactly once in read-back.
2. No append PERSISTED by A after takeover appears in read-back. The
   criterion is persist-time, not ack-time: an A ack returned after T
   whose seq range lies entirely below the takeover boundary is a late
   ack of a write that was durable before B took over (B recovered it and
   built on it) — allowed, not a finding. An A-accepted record at or
   beyond the boundary that appears in read-back is the finding, acked or
   not. (2026-07-05 draft evidence: the ack-time phrasing false-flagged
   in-flight-at-SIGSTOP appends whose records B had already recovered —
   seq below boundary, stream dense, no consumer-visible inconsistency.)
   An A ack after T for a record absent from read-back is a lie told to a
   client that should have been fenced — log it, but rejected/dropped/
   lost zombie appends are all fine, not findings.
3. Read-back [0, check-tail) is a dense, gapless seq range whose contents
   are exactly (A-acked-before-T set, possibly-truncated suffix aside) ∪
   (B-acked set), each at most once.
4. Anti-vacuous gate: the zombie's post-SIGCONT attempts must observably
   reach A's write path — at least one post-CONT attempt gets an HTTP
   response from A (an ack, or a storage-layer rejection such as
   "detected newer DB client"). Pure connection failures mean the zombie
   never really tried, and the trial is void.

## Replay plan

Seed drives the SIGSTOP point, B start delay, SIGCONT delay, and both
payload schedules. Red runs replay by recorded seed via --exploration.
