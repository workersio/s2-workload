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
    status: done
    result: green
    reason: null
    workload: workloads/reads_tail.py
    command: python3 .workers/workloads/reads_tail.py baseline
    faults: []
    depth: 5
    replay: {run: nd70kjg2kp2gmk3zm8g9z4tcgh8a0yad, case: baseline, seed: 4097857263}
    freshness: new-current
    reported: null
    published: nd74pmw2vr7z8qf228fwmfffms8a3xsc
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
    status: done
    result: green
    reason: null
    workload: workloads/reads_tail.py
    command: python3 .workers/workloads/reads_tail.py across-restart
    faults: []
    depth: 10
    replay: {run: nd72rd95yjks58bywbh9pes1s58a0fvh, case: across-restart, seed: 3051447964}
    freshness: new-current
    reported: null
    published: nd755qhdaxyg4yr00hf9yn53498a3cht
  - key: reads-tail-last-event-id-resume
    title: Reads tail Last-Event-Id resume
    description: >-
      Same kill schedule as across-restart, but the reader reconnects the
      way a real SSE client does: the new session carries the
      Last-Event-Id header captured from the final delivered event
      ("seq,count,bytes"), exercising apply_last_event_id
      (handlers/v1/records.rs:49-66 — start becomes seq+1 and any
      count/bytes budgets are decremented) instead of the seq_num query
      param the across-restart arm uses. The header parse, the +1 start
      arithmetic, and the budget subtraction are all restart-facing code
      the param path never touches; an off-by-one here silently skips or
      replays exactly one record at the crash boundary. Oracle family
      unchanged: pre-kill observed log dense; resumed stream tiles
      [K, tail) exactly once with contents equal to the Remote readback;
      observed records all present post-restart; anti-vacuous floor and
      lag>0 at kill as in across-restart. Budget arm (seed-selected): the
      budget-decrement half of apply_last_event_id (records.rs:61-62) is
      a no-op unless the request carries a limit — in this arm the
      resumed session also sets a count (or bytes) limit and the oracle
      asserts the session terminates after delivering exactly
      limit - consumed records (the decremented budget), not the raw
      limit. Red-proof: a +1-shifted Last-Event-Id header must trip the
      tiling oracle (skipped or duplicated record at the boundary).
    status: done
    result: green
    reason: null
    workload: workloads/reads_tail.py
    command: python3 .workers/workloads/reads_tail.py last-event-id-resume
    faults: []
    depth: 10
    replay: >-
      green sweep draft nd74s6rgt1wd20hhykje3zj43s8a0hd3 (depth 10, 9/10
      green + 1 documented 2s-arm anti-vacuity void; budget and plain arms,
      all flush arms). Red-proofs: +1-shifted header
      nd73gccsjx37zw4wy7kgr1de2x8a10dc (2/2 exit-1 follow_wellformed, seeds
      3934514529 budget / 680046819 plain); readback-drop
      nd79hx999y1v5d0x0r8mpygk058a1s41 (seed 1108731994,
      observed_survive); perturbed event id
      nd7cktxkegj8tcf656crd3hbpx8a0h38 (seed 3761786860,
      header_describes_delivery). Post-REDO confirm
      nd72dzcywtx6amb7w08qvtk2eh8a15dc (4/4 green). All via
      --workload-file injection.
    freshness: new-current
    reported: null
    published: nd7d95rp17yp1354mk7zrjfzg98a2egf
  - key: reads-tail-slow-follower-lagged
    title: Reads tail slow follower lagged
    description: >-
      No kill: attack the broadcast-overflow seam. The per-stream follow
      channel holds FOLLOWER_MAX_LAG=25 batches (backend/mod.rs:27); a
      follower that stalls its socket while writers push far past 25
      batches forces tokio broadcast RecvError::Lagged, and the read
      session silently falls back to a fresh catch-up scan
      (read.rs:219-222, continue 'session) whose empty-scan branch can
      advance start_seq_num to tail without yielding (read.rs:183-185).
      The client sees one uninterrupted SSE stream across the transparent
      server-side handoff. Oracle: the delivered stream must remain
      gap-free, duplicate-free, in seq order across >=1 forced Lagged
      handoff, and equal the final Remote readback. Anti-vacuity witness:
      catch-up batches carry no `tail` field (read.rs:169-172, 178-182;
      JSON omits tail when None, api/src/v1/stream/json.rs:28,36-37)
      while follow batches carry `tail: Some` (read.rs:209-212) — a batch
      event MISSING `tail` arriving after follow batches that had it
      proves the session re-entered catch-up, i.e. Lagged fired; gate red
      eligibility on that witness, void otherwise. Do NOT use "delivery
      jump > channel capacity" as the witness — a jump IS the bug, not
      the trigger proof. Sizing: capacity is 25 BATCHES
      (broadcast::Sender::new(FOLLOWER_MAX_LAG), streamer.rs:266,
      mod.rs:27), and a stalled client only stops follow_rx polling once
      hyper's write path + kernel send buffer + client recv buffer fill —
      so per-append payloads must be large enough that stall-window bytes
      exceed ~1-4 MB across 25+slack appends per stall; 25 tiny batches
      never lag. Follower stall pacing, writer burst sizes, and stall
      count are seed-driven.
    status: done
    result: green
    reason: null
    workload: workloads/reads_tail.py
    command: python3 .workers/workloads/reads_tail.py slow-follower-lagged
    faults: []
    depth: 10
    replay: >-
      green sweep draft nd75mfsza3wndpve0k06gkzj0h8a06j0 (depth 10, 10/10
      green, every trial witnessed 9-71 catch-up batches after follow
      handoff across 1-3 stalls; payloads 128-256 KiB). Red-proofs: gap
      nd7ef5pthmwbz37vyqtvkqv3b18a1243 (seed 3348524698,
      follow_wellformed via partial_delivery_verdict); readback-drop
      nd7b8g8bvvwjff1vpn3k81h4j18a1j2p (seed 775902020,
      observed_survive). Shakeout v2 nd736afd7ncjat3yvep3dryjjx8a1q57
      (4/4 green). All via --workload-file injection.
    freshness: new-current
    reported: null
    published: nd76wbbdh4n0vmhtwf4shbdqxn8a2myk
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

## Evidence — reads-tail-baseline (executor #9, 2026-07-06)

GREEN at depth 5 (post-fix sweep nd70kjg2kp2gmk3zm8g9z4tcgh8a0yad, 5/5).
Transport probed and pinned: SSE (`Accept: text/event-stream` on the read
path), batch/ping/error events + `[DONE]`, `Last-Event-Id` resume — recorded
as a map.md reality note. Test-reviewer REDO fixed: a partial delivery with
an internal gap now goes RED (follow_wellformed) instead of VOID, and both
oracle legs are red-proven (`ORACLE_SELFTEST=1` -> observed_survive FAIL;
`ORACLE_SELFTEST=gap` -> follow_wellformed FAIL). The follower machinery and
partial-delivery guard are shared with the across-restart arm.

## Evidence — reads-tail-across-restart (executor #10, 2026-07-06)

GREEN across flush arms (default/500ms/2s): depth-3 pipelined draft 3/3,
depth-6 arm-scaled 6/6, depth-1 final-code sanity — every kill inside a
sampled lag>0 window (acked-but-undelivered records at SIGKILL), s2-lite
never denied a follower-observed record after restart; resumed follows tiled
[K, tail) exactly. Design notes: ack and follow delivery advance in the same
durable_seq event, so a serial writer can never lag — a 3-thread pipelined
writer pool plus a sampled kill window implements the spec's lag>0
anti-vacuity honestly. Test-reviewer REDO fixed: post-restart
serving-but-stream-denied now RED via observed_survive (was VOID-masked;
red-proven with a nonexistent-basin selftest — note check-tail auto-creates
a missing *stream* on create-stream-on-read basins, so stream-level denial
cannot be simulated), read_all retries then REDs below tail, and
ORACLE_SELFTEST=gap wired in-mode (red-proven pre-kill). Depth-10 sweep
voids on the 2s arm were throughput-bound kill points, fixed by arm-scaled
kill_after — anti-vacuity gate untouched. Reviewer flag for a future arm:
manifest ⊆ readback across this kill schedule would catch
ack-before-remote-durable for lag-window records (write-side promises never
kill mid-pipeline).

## Adversarial model — producer #7 arms (2026-07-06)

**last-event-id-resume.** The across-restart arm resumes by seq_num query
param — arithmetic our own workload controls. Real SSE clients resume with
the Last-Event-Id header, and the server applies it itself:
`apply_last_event_id` (handlers/v1/records.rs:49-66) parses
"seq,count,bytes", sets start = seq.saturating_add(1), and decrements any
count/bytes budgets by what was already consumed. That is restart-facing
parsing and off-by-one arithmetic the param path never executes; skipping
or replaying exactly one record at the crash boundary is the classic bug
shape. Executor plan: capture follower.last_event_id (already recorded by
the Follower class), reconnect post-restart with that header and NO seq_num
param, then run the identical tiling oracle. Red-proof: existing selftest
paths apply; additionally a synthetic header with seq shifted by +1 must
make the tiling check fail (proves sensitivity to exactly the off-by-one).

**slow-follower-lagged.** The only server-side seam in the live-follow path
that upstream's in-sim harness can model but plausibly never drives to
overflow against the real binary: FOLLOWER_MAX_LAG=25 (backend/mod.rs:27)
bounds the broadcast channel; Lagged receivers silently re-enter the
catch-up scan (read.rs:219-222) whose empty-yield branch advances
start_seq_num to tail without emitting (read.rs:183-185) — records between
scan-start and tail that the Remote filter excludes at that instant would
be skipped, not delivered. A stalled-then-resumed follower is the honest
client-side trigger (no server cooperation needed). Executor plan: follower
stops reading its socket for a seed-chosen pause while a writer pushes
>25*batch records, resumes, repeats 1-3 times, then drains to tail; oracle
is the existing wellformedness + readback-equality machinery; anti-vacuity
= observed delivery jump spanning > FOLLOWER_MAX_LAG batches across a
stall (client-side witness of the overflow). Red-proof: ORACLE_SELFTEST=gap
already proves the gap detector; add a stall that drops (not just delays)
one batch in the client to prove the equality leg if needed.

## Replay plan — producer #7 arms

Seed drives writer pacing/bursts, kill point and flush arm
(last-event-id-resume), stall schedule (slow-follower-lagged). Red runs
replay by recorded seed via --exploration.

## Evidence — reads-tail-last-event-id-resume (executor #14, 2026-07-06)

GREEN across flush arms and both budget/plain arms: depth-4 shakeout 4/4,
depth-10 sweep 9/10 (+1 documented 2s-arm anti-vacuity void), post-REDO
confirm 4/4. Boundary tiling exact in every trial (resume started exactly
at observed[-1]+1); budget sessions delivered exactly limit−consumed then
[DONE] (e.g. count=259 re-sent, 253 consumed → exactly 6). Design notes:
the follower keeps draining the kernel socket buffer after SIGKILL, so
observed routinely catches the durable tail despite lag>0 at the kill —
the mode appends 3-8 fresh post-restart records so the resumed session
always has records beyond the boundary (provenance-independent for the
header arithmetic under test; pre-kill durability still fully asserted by
observed_survive/readback_dense). Follower now commits last_event_id only
at event dispatch (WHATWG EventSource semantics) — a kill mid-event cannot
advance the resume cursor. Test-reviewer REDO fixed two VOID-masks: (1) a
server-emitted id that misdescribes its own delivery is now RED
header_describes_delivery (red-proven via ORACLE_SELFTEST=bad-id), was
"harness bug" void; (2) the server persistently rejecting its own emitted
header post-readback is now RED header_accepted (parse/validation
regression), was "transport" void. Residual (reviewer, non-gating): bytes
budget (records.rs:62) untested — count-only arm; want==available corner
eliminated by capping want ≤ available−1.

## Evidence — reads-tail-slow-follower-lagged (executor #15, 2026-07-06)

GREEN at depth 10 (10/10, every trial witnessed 9-71 catch-up batches
after follow handoff across 1-3 stalls). The Lagged handoff is physically
hard to force: with default buffers ~5 MB of stall bytes were absorbed by
server sndbuf + client rcvbuf and delivery stayed 100% follow-path (first
shakeout 4/4 vacuous, batch count == append count). Fix was physical, not
oracular: SO_RCVBUF capped at 32 KiB before connect (window scaling honors
it) + bursts of 70-110 × 128-256 KiB appends per stall; catch-up
coalescing (batch count << append count) then corroborates real Lagged.
New machinery: Follower pause/unpause socket stall, per-batch has_tail
metadata (the spec's witness — tail-less batch after tail-ful ones),
norm_body sha256 normalization on BOTH oracle sides for >1 KiB bodies
(memory-bounded equality; all other modes' payloads pass through
verbatim). Verify runs before the vacuity exit; per-stall drains RED on
provable gaps. Both legs red-proven in-mode (gap seed 3348524698,
readback-drop seed 775902020). Test-reviewer KEEP, no required action.
Residual for a future arm (reviewer): overlap the writer burst with the
unpause window so the post-Lagged catch-up races in-flight not-yet-Remote
appends — the schedule that could reach the empty-scan skip branch
(read.rs:183-185) itself; this green proves handoff resume correctness
over a quiescent durable range.
