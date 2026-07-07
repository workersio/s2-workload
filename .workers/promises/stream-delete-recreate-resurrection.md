---
key: stream-delete-recreate-resurrection
area: lifecycle
title: Recreated stream is genuinely fresh
claim: >-
  A stream recreated under a previously deleted name is a new stream in
  every observable way: it starts empty at seq 0, serves none of the old
  incarnation's records at any seq or timestamp, is governed by the
  default (empty) fencing token, carries no old trim state, and is never
  deleted by the old incarnation's delete-on-empty schedule. Crashing
  anywhere in the delete/purge pipeline never wedges the name or aborts
  the recovered process.
status: active
provenance: "lite/src/stream_id.rs:24-29 (deterministic StreamId => full keyspace reuse across recreate); backend/bgtasks/stream_trim.rs:60-149 (non-txn purge WriteBatches + finalize txn; finalize deletes trim-point/meta/id-mapping/tail/fencing but NOT stream_doe_deadline); core.rs:165-196 (process-aborting assert on records beyond tail); streams.rs:98-110 (DeletionPending gating); #526 26f7e96"
explorations:
  - key: delete-recreate-fresh-identity
    title: Delete recreate fresh identity
    description: >-
      No kills; the resurrection oracle itself. Incarnation 1 gets the
      full state surface: N records appended, a fencing token T1 set via
      fence, a partial trim issued. Delete; poll create until the
      DeletionPending gate lifts (purge tick 60s±10% — bounded wait,
      liveness clause below); recreate the SAME name. Fresh-identity
      oracle: check-tail is 0/MIN; reads at every old seq return empty
      or the empty-stream error class, never an old body; timestamp
      reads resolve against the new (empty) index; the default token
      governs — a never-valid-token append 412-discloses "" (NOT T1)
      and a tokenless append is accepted; a fresh fence to T2 works;
      appends sequence from 0. Any old-incarnation record body, token,
      or trim behavior visible in incarnation 2 = RED resurrection.
      Liveness: recreate still gated after 300s = RED purge_liveness
      (the purge bgtask wedged on a healthy server is a product
      failure, not a void; the purge is event-triggered at delete, so
      300s is generous). Critic addition (cheap): re-run the old-seq
      probes once more at end of trial — catches any late-durable
      resurfacing for free.
    status: done
    workload: workloads/lifecycle.sh
    command: sh .workers/workloads/lifecycle.sh fresh-identity
    faults: []
    depth: 5
    result: green
    reason: >-
      GREEN — no-kill resurrection oracle holds. inc2 starts at tail 0,
      serves no old body at any of the 5 probed seqs (double-probed) nor
      by timestamp, default '' governs (T1 never disclosed on the 412),
      tokenless append acked, fresh fence T2 works, appends sequence from
      0; recreate gate lifted immediately (event-triggered purge). 6/6
      GREEN across the shakeout + sweep + hardened post-fix confirm
      (SEED=88001); ORACLE_SELFTEST forges a leak -> RED (proof the oracle
      bites). Scope caveat: does NOT cover DOE-deadline freshness — that
      corridor is RED, carried by doe-stale-deadline-across-recreate.
      Test-reviewer KEEP. Evidence:
      runs/delete-recreate-resurrection-green-rungs.md.
    replay: "SEED=88001 (hardened confirm); also 5/5 sweep + selftest 77002"
    freshness: new-current
    reported: 2026-07-07
    published: nd71jqpg9vwvvneztgcmq1kpf18a35e0
  - key: delete-recreate-kill-mid-purge
    title: Delete recreate kill mid purge
    description: >-
      The crash arm (the 667 row's core): incarnation 1 gets ~25k
      records (3 non-txn purge WriteBatches at DELETE_BATCH_SIZE=10k,
      stream_trim.rs:18,99 — a real multi-batch window) plus fence T1.
      Delete; SIGKILL at a seed-chosen delay — CRITIC REVISION
      (producer #9): the purge is EVENT-TRIGGERED at trim durability
      (streamer.rs:601-606 -> bgtasks/mod.rs:66-79), i.e. it starts at
      or slightly BEFORE the delete ack, and at the 5ms local-FS flush
      the whole 3-batch purge + finalize completes well under a second
      — the originally drafted [0,75s] tick-weighted sweep would land
      ~every trial post-finalize (degenerate). Required shape:
      concentrate delays in [0, ~2s] at sub-100ms granularity including
      0, add a few kills racing the DELETE request itself (unacked
      delete — this opens the two-phase seam; log the post-restart
      GET-live/append-pending coherence classification for the
      control-plane sibling), run SL8_FLUSH_INTERVAL 500ms/2s arms so
      each purge batch-write's durability wait widens the window, and
      keep 1-2 long delays as post-finalize controls. After a crash
      there is NO re-trigger — purge resumption waits the 60s±10%
      tick. Restart on the same root. RED restart_serves if the process aborts or cannot
      serve (assert_no_records_following_tail, core.rs:165-196, is
      process-aborting — a crash on recovery IS the finding). Purge
      must then complete: recreate allowed within 300s or RED
      purge_liveness. Recreate and run the full fresh-identity oracle
      from the sibling rung — any old record/token resurfacing in
      incarnation 2 after a mid-purge crash = RED resurrection.
      Anti-vacuity: log delete-ack->kill delay and post-restart
      recreate-gate latency, and classify each trial's seam by whether
      the recreate gate was still closed immediately after restart
      (pre-finalize kill) or already open (post-finalize); a sweep
      where every trial lands post-finalize never exercised the
      mid-pipeline seams — require at least one pre-finalize trial per
      sweep, else the sweep is degenerate (void, rerun with reshaped
      delays).
    status: done
    workload: workloads/lifecycle.sh
    command: sh .workers/workloads/lifecycle.sh kill-mid-purge
    faults: []
    depth: 10
    result: green
    reason: >-
      GREEN — crash-mid-purge never wedges or resurrects. Across shakeout
      (all four seam classes) + a 10-seed sweep + hardened post-fix
      confirms: process re-serves after restart (restart_serves PASS,
      +16-24s), the interrupted purge resumes on the tick and lifts the
      recreate gate within bound (+106-148s; no event re-trigger after a
      crash), and the recreated inc2 passes the full fresh-identity oracle
      (8 old-seq probes double-probed + timestamp clean). Seam
      distribution observed: DELETE-UNHAPPENED, DIVIDED, PRE-FINALIZE,
      POST-FINALIZE. Notable green facts: DELETE ack 26-51s virtual;
      2-tick gate resumption. Defect ledger: a re-delete timeout bug
      (default 15s) was found + fixed, replay SEED=3963731212 GREEN
      seam=DELETE-UNHAPPENED. Scope caveat: these greens do NOT cover the
      DOE-deadline corridor (RED, carried separately). Test-reviewer KEEP
      (5 hardenings applied + reconfirmed GREEN). Evidence:
      runs/delete-recreate-resurrection-green-rungs.md.
    replay: "SEED=90000 (DIVIDED) / 88817 (PRE-FINALIZE) hardened confirms; sweep + 3963731212 fixed-replay"
    freshness: new-current
    reported: 2026-07-07
    published: nd7eb0tve3gf58ns0v2dtqpc758a2n22
  - key: doe-stale-deadline-across-recreate
    title: DOE stale deadline across recreate
    description: >-
      The source-visible leak, hunted directly: finalize_trim deletes
      meta, id-mapping, tail, and fencing keys but NOT
      stream_doe_deadline keys (stream_trim.rs:135-146), and StreamId
      is deterministic — so incarnation 1's armed DOE deadline fires
      against incarnation 2 under the same name. The streamer's
      condition check consults CURRENT config (streamer.rs:458-461,
      497-501): min_age None => Ineligible neutralizes the no-DOE case;
      the exposed case is incarnation 2 WITH DOE at a LONGER min_age.
      Trial: stream A inc1 with retention age=1s + DOE min_age=1s
      (deadline ~= t0+602s: doe_arm_delay = age + min_age + 600s,
      streamer.rs:56-63); append once; delete inc1; recreate name with
      retention age=1s + DOE min_age=3600s; leave inc2 EMPTY (its own
      deadline sits ~70min out). Control stream B: same inc1 shape,
      inc2 recreated WITHOUT DOE. Wait until inc1's deadline + tick +
      margin (~700s), probing every 30s. RED wrongful_delete if inc2 of
      A is deleted/not-found before its own min_age of emptiness could
      possibly have elapsed (it was empty for <15min against a 60min
      floor) — the stale cutoff (max over inc1 entries,
      stream_doe.rs:33-41) classifies the young-empty inc2 as
      long-empty. Control B must survive untouched (else the
      neutralization claim is wrong too). GREEN only if both survive
      the window AND a post-window append to A works. CRITIC-VERIFIED
      end-to-end (producer #9, all links source-cited): cutoff =
      deadline - min_age (kv/stream_doe_deadline.rs:17-21); inc2's
      last_tail_write_timestamp = the fresh tail key's slatedb
      create_ts (core.rs:110-111, :161) ~= recreate time < cutoff — no
      now() neutralization exists; DOE has NO event trigger (60s tick
      only); dormancy cannot save inc2 (stream_doe.rs:111-121 spawns
      the streamer itself). Near-certain deterministic RED. BUILD
      CONSTRAINT: never append to inc2 of A during the wait (one
      append bumps last_tail_write_timestamp past the cutoff and
      neutralizes the trial); GET/check-tail probes are safe.
      ~13min/trial — depth 3, the long-budget design the
      doe-wrongful-delete backlog row (400) was waiting for; this arm
      covers ONLY its across-recreate corridor (the same-incarnation
      disarm-reconfigure path stays on the 400 row — different firing
      path, pinned expected-stale by upstream test stream_doe.rs:757-801).
    status: done
    result: red
    reason: >-
      FINDING — wrongful stream deletion. finalize_trim leaves
      stream_doe_deadline keys (stream_trim.rs:135-146) and StreamId is
      deterministic, so inc1's armed DOE deadline fires against the
      recreated inc2 (presence-only min_age recheck,
      streamer.rs:457-461/494-505; empty inc2 has no tail key =>
      timestamp ZERO, always below cutoff). 3/3 deterministic in-window
      REDs (+617s/+628s/+645-673s vs window [~600,668]); control B
      (no-DOE) survived every trial. Test-reviewer KEEP. Evidence:
      runs/doe-stale-deadline-across-recreate.md.
    workload: workloads/lifecycle.sh
    command: sh .workers/workloads/lifecycle.sh doe-stale-deadline
    faults: []
    depth: 3
    replay: "SEED=2492750010 (fresh-seed confirm 555000333); drafts nd73hn9zcw85r0vcnnfern09x18a1cxw, nd72c44ybyknm6yv4c1h575x0x8a1atw, nd743p947k7tm6f7sfskrh99a58a15df"
    freshness: new-current
    reported: 2026-07-07
    published: nd754m0b03nttctn6yxwfzfg4h8a30f9
---

# Recreated stream is genuinely fresh

## Adversarial model

Delete->recreate under a deterministic StreamId means every KV family —
records, tail, trim point, fencing token, DOE deadlines — is REUSED by
the new incarnation. Freshness is not a property of creation; it is a
property of the purge having completed exactly. The purge is a
background task doing non-transactional deletes in 10k batches with a
Remote durability scan filter, finalized by a separate txn, driven by a
60s tick, recovered lazily after crashes — five seams, each of which can
leak old state into the new incarnation or abort the process
(core.rs:165-196). And one leak is already visible in source: DOE
deadline keys survive finalize.

## Oracle

Fresh-identity: tail 0, no old bodies at any seq/timestamp, default
token governs (412 discloses ""), fresh fence works, appends from 0.
Resurrection (any old state observable), restart_serves (recovery
aborts), purge_liveness (name wedged >300s), and wrongful_delete (inc2
deleted by inc1's schedule) are all RED. Setup-only failures void.

## Replay plan

Seed drives record counts, kill delay, and probe cadence. Red runs
replay by recorded seed via --exploration.
