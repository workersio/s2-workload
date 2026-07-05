---
key: fencing-excludes-stale-writers
area: durability
title: Fencing excludes stale writers
claim: >-
  Once a stream's fencing token is set, appends carrying a stale or missing
  token are rejected and leave no trace in the stream, including across
  server restarts.
status: active
provenance: "https://s2.dev/docs/api (append semantics: fencing_token); lite/src/backend/kv/stream_fencing_token.rs (persisted token); lite/src/backend/streamer.rs:341 (mismatch rejection)"
explorations:
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
    published: nd70nakvpp3a779vwj2ypdnjkx89za3a
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
