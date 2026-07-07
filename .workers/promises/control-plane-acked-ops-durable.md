---
key: control-plane-acked-ops-durable
area: control-plane
title: Acked control-plane ops are durable
claim: >-
  A 200-acked stream create, reconfigure, or delete survives a SIGKILL
  issued immediately after the ack: on restart from the same root the
  created stream exists with its acked config, the reconfigured config
  reads back as acked, and the deleted stream stays deleted. An acked
  metadata op never silently un-happens.
status: active
provenance: "lite/src/backend/streams.rs:212 (ensure/create), :319 (reconfigure), :376 (mark_stream_deleted) — txn.commit() IS durability-gated (slatedb 0.13.1: db_transaction.rs:519-521 -> WriteOptions::default await_durable:true, config.rs:462-470; db.rs:362-366 blocks on durable_watcher — critic-verified against the crate source); the REAL exposure is the two-phase delete (streams.rs:338-358 terminal trim, :360-379 mark_stream_deleted — separate txns, kill-divisible)"
explorations:
  - key: control-plane-ack-then-kill
    title: Control plane ack then kill
    description: >-
      REGRESSION FLOOR, not a bug hunt (critic revision, producer #9):
      the drafted premise — bare commit with no durability gate — was
      REFUTED against slatedb 0.13.1 source: DbTransaction::commit
      defaults await_durable:true and blocks on the durable watcher
      (db_transaction.rs:519-521, config.rs:462-470, db.rs:362-366), so
      acked control ops are durable by construction; durability_notifier
      exists for data-plane PIPELINING, not because commit lacks
      durability. The arm stays as the cheap floor: a RED here means a
      slatedb durable-watcher or WAL-replay bug. Per trial: seed-chosen
      schedule of K=4-8 ops across distinct stream names (create with
      explicit config, reconfigure, delete), each verified
      read-your-writes pre-kill; SIGKILL immediately after the last 200;
      restart; every acked op must hold (creates exist with acked config
      + appendable; reconfigures exact; deletes stay deleted). One 2s
      SL8_FLUSH_INTERVAL trial as control (a slower flush must delay the
      200, never detach it). RED acked_op_durable / restart_serves.
    status: ready
    result: null
    reason: null
    workload: workloads/control_plane.sh
    command: sh .workers/workloads/control_plane.sh ack-then-kill
    faults: []
    depth: 4
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: control-plane-delete-straddle-ensure-erased
    title: Delete straddle erases acked ensure
    description: >-
      The critic's counter-promotion (producer #9) — the freed bug-hunt
      budget lands here; mechanism complete in source. Delete is
      two-phase and non-atomic: terminal trim through the streamer
      (streams.rs:338-358), THEN mark_stream_deleted in a separate txn
      (:360-379). SIGKILL an in-flight unacked DELETE after the terminal
      trim is durable but before mark_stream_deleted commits. Restart:
      trim_point == MAX but meta.deleted_at == None. In that window (a)
      GET config serves the stream as LIVE (streams.rs:240-258 reads
      meta only) while appends fail deletion-pending (core.rs:118-120) —
      log the incoherence, and (b) the kill shot: an Ensure/reconfigure
      on the name passes the DeletionPending gate (streams.rs:104-112
      checks only deleted_at) and 200-acks — then the recovered purge's
      finalize_trim unconditionally deletes meta/id-mapping/tail
      (stream_trim.rs:136-146, guarded only by trim-point equality
      :123-134) and THE ACKED ENSURE UN-HAPPENS within the recovery tick
      (~66s). Per trial: create + populate a stream; issue DELETE and
      SIGKILL at a seed-swept sub-100ms offset into the request
      (straddling terminal-trim durability vs mark_stream_deleted);
      restart; classify the landed seam (GET state + append behavior);
      if the straddle landed (GET live + append deletion-pending),
      issue Ensure with a distinct config value, require its 200, then
      poll GET through the recovery tick + margin (~180s). RED
      acked_ensure_erased if the 200-acked config/meta vanishes; RED
      restart_serves per convention. Trials where the kill lands
      before trim durability (delete fully un-happened, stream intact)
      or after mark_stream_deleted (normal deletion) are healthy
      classification arms, not voids — assert their respective
      consistent outcomes; anti-vacuity requires >=1 landed straddle
      per sweep (SL8_FLUSH_INTERVAL 500ms/2s arms widen the
      inter-phase window; reshape delays if a sweep lands none).
    status: done
    result: red
    reason: >-
      FINDING #2 — acked control-plane op silently un-happens. SIGKILL
      between the delete's two txns leaves trim_point==MAX +
      deleted_at==None; the provision gate checks only deleted_at
      (streams.rs:106-112) so PUT Ensure 200-acks fresh meta
      (:133-151), then the recovered purge's finalize_trim (guarded
      only by trim-point equality, stream_trim.rs:123-146) erases the
      acked meta at the first tick (+64-72s). 5/5 RED across all three
      flush arms + same-seed replay + fresh seed; divided-state
      GET-live/append-pending incoherence also witnessed and logged.
      Test-reviewer KEEP. Evidence:
      runs/control-plane-delete-straddle-ensure-erased.md.
    workload: workloads/control_plane.sh
    command: sh .workers/workloads/control_plane.sh delete-straddle
    faults: []
    depth: 10
    replay: "SEED=1000000 (500ms arm; also 999999 default / 1000001 2s / fresh 424242); drafts nd77c6bwpmh6ysnjp5bs70a5t98a11a9, nd78dbht4c0m1hmqwe27a5mqns8a0krp, nd708rsmkfnxtcas320c916tbh8a11xe, nd79bqvaqzcwar86vjfhyt27mx8a0yr3, nd7382tv4vamyxfr9tmgkwvjcs8a01fa"
    freshness: new-current
    reported: 2026-07-07
    published: nd7frym4zs530q180emq4shw618a3m72
  - key: control-plane-baseline
    title: Control plane baseline
    description: >-
      No faults; the floor rung. One server; seed-chosen op schedule over
      K streams: create with explicit non-default config, GET + LIST
      immediately (read-your-writes: created stream visible with exact
      config in both, no eventual-consistency lag tolerated — commits are
      local SerializableSnapshot txns); reconfigure and re-read;
      delete and verify deleted_at surfaced / not-found on GET, LIST
      census exact (start_after-chained walk returns exactly the live
      set, no dupes or omissions); recreate-after-delete of the SAME
      name observes the DeletionPending gate (streams.rs:104-112) — pin
      the observed rejection class while pending; the purge is
      EVENT-TRIGGERED at delete (streamer.rs:601-606 ->
      bgtasks/mod.rs:66-79), so the gate normally lifts in seconds —
      log actual lift latency, keep the 60s±10% tick as the ceiling.
      Critic addition: duplicate DELETE while pending must be
      idempotent (mark_stream_deleted no-ops on deleted_at.is_some(),
      streams.rs:373-377). Ladder note: floor certified at these rungs
      by the critic given the delete-straddle redesign; the OCC
      conflict storm (backlog 360) stays in the backlog.
    status: ready
    result: null
    reason: null
    workload: workloads/control_plane.sh
    command: sh .workers/workloads/control_plane.sh baseline
    faults: []
    depth: 5
    replay: null
    freshness: new-current
    reported: null
    published: null
---

# Acked control-plane ops are durable

## Adversarial model

REVISED at the critic gate (producer #9): the drafted premise — bare
commit, no durability gate — was refuted against slatedb 0.13.1 source
(commit defaults await_durable:true and blocks on the durable watcher;
db_transaction.rs:519-521 / config.rs:462-470 / db.rs:362-366). Single
acked ops are durable by construction; ack-then-kill is a regression
floor. The genuine exposure is COMPOSITE ops: delete is two txns
(terminal trim, then mark_stream_deleted) divisible by a kill. In the
divided state — trim_point == MAX, deleted_at == None — GET serves the
stream as live while appends fail deletion-pending, and an Ensure on
the name 200-acks only to have finalize_trim erase its meta within the
recovery tick: an acked op silently un-happening, with every link of
the mechanism visible in source.

## Oracle

Per-op acked state is the manifest; post-restart reads are the truth.
Divergence on any acked op = RED. Ops on distinct stream names prevent
cross-contamination; pre-kill read-your-writes verification pins that
the op was actually applied, not just acked. Restart failure is RED
restart_serves, never void.

## Replay plan

Seed drives the op schedule, config values, kill delay, and flush-arm
selection. Red runs replay by recorded seed via --exploration.
