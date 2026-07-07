---
key: fencing-excludes-stale-writers
area: durability
title: Fencing excludes stale writers
claim: >-
  Once a stream's fencing token is set, appends carrying a stale or
  never-valid token are rejected with the governing token disclosed, and
  leave no trace in the stream, including across server restarts.
  Tokenless appends are always accepted — the contract is cooperative
  (enforced only when a token is provided, streamer.rs:341).
status: active
provenance: "https://s2.dev/docs/api (append semantics: fencing_token); lite/src/backend/kv/stream_fencing_token.rs (persisted token); lite/src/backend/streamer.rs:341 (mismatch rejection)"
explorations:
  - key: fencing-baseline
    title: Fencing baseline
    description: >-
      No faults; the pure cooperative-contract rung the ladder floor
      requires (added producer #8 floor audit — the promise's two existing
      rungs are both kill-boundary attacks; stale-across-restart
      self-describes as "baseline and restart are one exploration" but its
      every trial crosses a SIGKILL, so the no-fault leg was never
      isolated). Establish T1 by fence append; assert, with the critic's
      four REQUIRED pins: (1) tokenless appends are ALWAYS ACCEPTED, even
      after a fence — the cooperative contract; rejection happens only when
      a token is provided (streamer.rs:341, `if let Some(provided_token)`) —
      pin it explicitly; (2) the 412 body equals the CURRENT GOVERNING
      token (handlers/v1/error.rs:256-257 serializes `actual`) — the
      disclosure contract: a wrong-token writer is handed the governing
      key; pin the body, do not just check the status; (3) governance flips
      atomically at the fence record's position (streamer.rs:344-347,
      368-376 applied_point): appends sequenced before the T2 fence record
      governed by T1, at-or-after by T2, no mixed window; (4) reject the
      full wrong-token class — stale (previously valid) AND never-valid
      tokens — 412 with no trace in read-back. Plus: T1-carrying appends
      accepted; read-back [0, tail) is exactly the accepted set in order.
      Proves the 412/trace oracle observes the invariant with zero fault
      noise.
    status: done
    result: green
    reason: null
    workload: workloads/fencing.sh
    command: sh .workers/workloads/fencing.sh baseline
    faults: []
    depth: 5
    replay: >-
      green sweep draft nd771wn0z15s2mh253jwt1bzxx8a102j (depth 5, 5/5
      green, all 4 pins + content_exact witnessed per trial; e.g. seed
      2779616147) + explicit-seed greens nd786qc1vycjce52xsp1z480t58a17ce
      (SEED=1111111111) / nd77ee7sgmaq85w0yn8xzk6gh18a0zyp (SEED=777000111).
      Post-REDO confirm nd76dpymzts9q95aw80ejk4fa58a13nv (3/3). Red-proofs
      ×3 arms: ORACLE_SELFTEST=1 nd7d9xnscszshdhsqfm75jfcgh8a09g8 (seed
      1026934166 -> wrong_token_rejected), =flip
      nd7dc7nc6fe1n3e6dj8e206pvx8a15r6 (-> atomic_flip), =disclose
      nd74rgzxzezn8yrxcfqfjfcfw58a04ms (-> disclosure_412). Observed 412
      body shape: {"fencing_token_mismatch": "<governing>"}, "" in the
      default regime. All via --workload-file injection.
    freshness: new-current
    reported: null
    published: nd72qb85f933arm1pkqcwzgjjs8a2d92
  - key: fencing-stale-across-restart
    title: Fencing stale across restart
    description: >-
      Fence with a new token, SIGKILL the server, restart on the same root,
      then attempt appends with the pre-fence token: the recovered token
      (deserialized from storage) must still reject the stale writer, and
      read-back must contain only accepted records. Pure same-process token
      rejection is upstream-harvested territory; the restart boundary — the
      token's persistence and recovery path — is the differentiated attack,
      so baseline and restart are one exploration.
    status: done
    result: green
    reason: null
    workload: workloads/fencing.sh
    command: sh .workers/workloads/fencing.sh stale-across-restart
    faults: []
    depth: 5
    replay: null
    freshness: new-current
    reported: null
    published: nd753409knf9j9zb3hfrw6ns158a2qvy
  - key: fencing-fence-ack-straddles-kill
    title: Fencing fence ack straddles kill
    description: >-
      The fence WRITE itself races the crash: FIRST establish T1 by its
      own fence append and prove it settled (durable and probe-verified: a
      T1 append accepted, a stale-TOKEN append 412-rejected; tokenless
      appends are always accepted under the cooperative contract,
      streamer.rs:341 — spec text corrected producer #8) — this is
      required because recovery of a MISSING token yields
      FencingToken::default() (empty, core.rs:116), so without a settled
      T1 the unacked-fence XOR branch conflates "T1 governs" with "default
      token governs". Then write under T1, issue the fence-to-T2 append
      (single header ["", "fence"], body = T2) and
      SIGKILL the server at a seed-chosen offset around its ack (in-flight,
      just-acked inside the flush window, or acked+settled), across
      SL8_FLUSH_INTERVAL arms; restart on the same root; probe with a
      T1-token append, a T2-token append, and a tokenless append. If the
      fence was ACKED pre-kill, the recovered token must be T2 (T1 rejected
      with 412, T2 accepted) — an acked fence that regresses to T1 is the
      exactly-one-writer primitive silently breaking. If the fence was
      unacked, either token may govern but exactly one must: acceptance of
      T1 XOR acceptance of T2, consistent across repeated probes, and the
      fence record appears in read-back iff T2 governs. All acked data
      appends under the governing token exactly once; readback dense.
      Distinct from stale-across-restart, which fences long before the kill
      and only tests recovery of a settled token — this arm attacks the
      token's own durability window (fence record + token KV ride the same
      WriteBatch, streamer.rs:1039-1044, recovered core.rs:96-99).
    status: done
    result: green
    reason: null
    workload: workloads/fencing.sh
    command: sh .workers/workloads/fencing.sh fence-ack-straddles-kill
    faults: []
    depth: 10
    replay: >-
      green sweep draft nd7dhyftys4k4rpyve85a0h65n8a0ypr (depth 10, 10/10
      green; 4 UNACKED fences all XOR-resolved to T1, 6 ACKED all recovered
      to T2; arms default/500ms/2s; e.g. unacked seed 914993503, acked seed
      1210285088). Red-proof drafts nd76sk3sveh2x7fvj2pe546ak18a1vdc (seed
      4210227959) and nd702wdcq55tv2e86faf0wvbvx8a1feh (seed 1961397883,
      post-REDO): forged T1 probes -> fence_durable FAIL, exit 1. Post-REDO
      confirm nd74c6he5m5f345jvh7byad8kn8a0e4w (4/4 green). All via
      --workload-file injection.
    freshness: new-current
    reported: null
    published: nd70x19g0bn3w5q8g709t899h18a31aq
---

# Fencing excludes stale writers

## Adversarial model

Fencing is implemented in lite: the token persists via
`lite/src/backend/kv/stream_fencing_token.rs` and the streamer rejects
mismatches (streamer.rs:341). It is cooperative — only enforced when an
append supplies a token — and strongly consistent per the CLI docs. Pure
token-logic rejection is modeled by upstream's own verification (the
Porcupine state carries the fencing token), so the attack worth running is
the restart boundary: fence → SIGKILL → restart → stale-token append. If
the token is rebuilt lazily, defaulted on recovery, or raced by the
startup path, the stale writer wins — the exactly-one-writer primitive
silently breaks, invalidating every downstream assumption.

## Oracle

Driver writes under token T1, fences to T2, kills and restarts the server,
then attempts appends under T1 and under no token (tokenless appends are
allowed by the cooperative model — assert whatever the pre-kill behavior
was is preserved post-restart, not a guess about intent). Invariants:
stale-token appends fail with an explicit rejection both before and after
restart; read-back [0, check-tail) contains exactly the accepted records;
acceptance behavior is identical across the restart boundary.

## Bounced direction: concurrent-fence-mid-stream (not drafted)

A "fence while appends are in-flight" concurrency arm was considered and
**bounced by strategy-critic** (2026-07-05, source-verified). All appends
and the fence flow through one mpsc channel into a single streamer task;
`sequence_records` (streamer.rs:341) checks the token and `apply_command`
(streamer.rs:371) applies the fence **synchronously within one invocation,
no await between check and apply** — there is no intra-streamer TOCTOU gap.
The HTTP "race" only decides channel arrival order; whichever message
arrives first wins deterministically and no stale-token append can survive
past the fence record's position. The oracle would be green by
construction, and pure token-application ordering under concurrency is
exactly what upstream's Porcupine/linearizability harness already models.
The one genuinely racy seam here would be a **generation handoff** — an old
streamer draining while a new one spawns after lease loss — which is a
different promise, not this one. Not drafted; recorded so a later producer
does not re-propose it.

## Replay plan

Seed drives the fence/append/kill schedule. Red runs replay by recorded
seed via --exploration.
