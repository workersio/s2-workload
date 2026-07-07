# Map — S2 (s2-lite) workload harness

Static evidence index. Not a queue: no owners, no claims, no priorities.

## Target

| Fact | Value |
|------|-------|
| Target repo | `s2-streamstore/s2` (fork: `workersio/s2-workload`) |
| Pinned ref | `76bf34a8ceb226970a6a847eaeb72e5b1464c081` (main, 2026-07-03) |
| System under test | `s2 lite` — self-hostable S2 API server (SlateDB storage engine) |
| SUT binary | `.workers/vendor/bin/s2` — release `s2-cli-v0.38.0`, x86_64-linux-musl, embeds CLI + lite server |
| wio project | `kn712jhg9p7wqx3a0rwnh698vs89x7rt` ("S2 Workload", prod) |
| wio branch | `main` |
| Local wio binary | `/Users/viswa/code/workers/formal/packages/wio/target/release/wio` (has `--exploration` / `--workload-file`; npm 0.4.0 not yet released) |

## Reality notes

- `s2 lite --local-root <dir>` persists to local disk via SlateDB's filesystem
  object store — the mode for durability/kill promises. In-memory mode (no
  flags) is an emulator only; nothing survives restart by design.
- Documented durability claim (README): with a bucket/local-root, "data is
  always durable on object storage before being acknowledged or returned to
  readers." Verified in source: ack is durability-gated — streamer submits
  `await_durable: false` but releases the ack only after SlateDB's
  `durable_seq` covers the batch (`lite/src/backend/streamer.rs:571`,
  `durability_notifier.rs`).
- Per-stream `streamer` tokio task owns the tail, serializes appends,
  broadcasts to followers; appends are pipelined against storage latency.
- `SL8_`-prefixed env vars tune SlateDB. `SL8_FLUSH_INTERVAL` default is
  **5ms for local FS and in-memory; 50ms is S3-only** (`lite/src/server.rs:96`).
- Startup does time-based fencing: new instance sleeps one
  `manifest_poll_interval` "to ensure prior instance fenced out"
  (`lite/src/server.rs`) — poll `check-tail` for readiness after restart,
  never fixed sleeps; and the time-based assumption is itself an attack
  surface (zombie-writer promise). **Observed reality (executor #16,
  live-overlap double-start, 16 green trials):** the sleep is
  belt-and-suspenders on local FS — a live prior instance is fenced at the
  successor's slatedb **DB-open** (manifest-epoch CAS), ~2.7-3.2s BEFORE
  the successor's first ack and early inside its ~3.5-3.9s boot window.
  Every prior-instance attempt after DB-open fails HTTP 500 ("detected
  newer DB client", or "database closed while waiting for durability" for
  in-flight at the fence). Zero post-takeover acks or persists across all
  trials.
- `/dev/urandom` in the sim guest is deterministic **per-run**: separate
  `simulate create`s draw overlapping seed sequences (4 of 10 sweep seeds
  duplicated a shakeout's). When seed distinctness matters, pass explicit
  `SEED` values per trial instead of deriving in-guest.
- The s2 CLI prints append acks to **stderr**, ANSI-colored, deduped per
  linger batch — unusable as an ack manifest. Ack-precision paths use raw
  HTTP (one request = one ack); CLI is fine for read-back/check-tail.
- Fencing is implemented in lite: token persisted via
  `lite/src/backend/kv/stream_fencing_token.rs`, mismatch rejected at
  `streamer.rs:341`; cooperative (enforced only when appends carry a token).
- `/streams/*` requests need the `S2-Basin` header when hitting lite over raw
  HTTP; the CLI/SDKs handle it.
- Upstream runs turmoil/madsim-style deterministic sim tests (`mad-turmoil`);
  our axis is whole-process fault injection against the real binary (kill,
  disk, network at OS level), which sims do not cover.
- `s2-streamstore/s2-verification` (Go) holds their own correctness tooling —
  mine it for oracle ideas before drafting promises.
- `publish.py` must run from a checkout whose branch exists on origin (the
  CLI's `--exploration` git gate fetches `origin/<current-branch>`): run it
  from the main checkout on `main`, not from a local-only worktree branch.
  Drafts via `--workload-file` injection skip the gate and run from anywhere.
- Worker-side `--workload-file` injection now DELIVERS on prod (verified
  2026-07-05: two drafts ran kill-during-recovery code present only in c865b2d,
  absent from the a88afdc image, and executed correctly). The draft fast path
  is live — drafts no longer need commit -> prepare -> run; inject and go.
  Officials still require the committed+prepared image (publish.py does not
  inject, so published runs stay pinned to a pushed commit). Supersedes the
  earlier "injected file does not reach the guest on prod" note.
- Fencing over raw HTTP: fence = append one record with a single header
  `["", "fence"]`, body = new token; guarded appends carry `fencing_token`
  in AppendInput; mismatch = HTTP 412. Ack `end` is exclusive.
- Follow transport is **SSE**: `GET /v1/streams/{s}/records?seq_num=N` with
  `Accept: text/event-stream` (extract.rs routes on the Accept header ->
  `ReadRequest::EventStream`, handlers/v1/records.rs:210). Events:
  `event: batch` (data = same ReadBatch JSON as unary reads, `id` =
  `seq,count,bytes`), `event: ping` heartbeats, `event: error`, bare
  `data: [DONE]` terminator. `Last-Event-Id: seq,count,bytes` resumes at
  seq+1 (records.rs:49). Unbounded read (no count/bytes/until) = infinite
  wait -> live tail. Catch-up scan filters at `DurabilityLevel::Remote`
  (backend/read.rs:127); caught-up sessions hand off to the streamer's
  durable-gated broadcast (`client.follow`, backend/read.rs:190+,
  streamer.rs:607). Verified live from python http.client (chunked SSE
  parses fine via resp.readline()).
- With slow `SL8_FLUSH_INTERVAL` arms (500ms/2s) a lazily-created stream
  404s appends (`stream_not_found`) until its creation record is durable,
  even on a `--create-stream-on-append` basin — prime new streams with a
  retry-bounded append before precision phases.
- Ack and follow-delivery gate on the same durable_seq advance, so a serial
  one-request-one-ack writer never yields follower lag at a kill point;
  producing acked-but-undelivered records requires concurrent appenders.
- `check-tail` on a `--create-stream-on-read` basin auto-creates a missing
  stream (200, tail 0) — a stream-level denial cannot be simulated there;
  use a nonexistent basin (basins are never auto-created, 404
  basin_not_found) to exercise denial paths.
- Zombie writes are rejected at the storage layer with HTTP 500 "detected
  newer DB client" / "database closed while waiting for durability" —
  SlateDB self-fences an already-initialized superseded handle; each
  rejected attempt takes ~100ms at the zombie. C's lazy first-access
  recovery under zombie contention stays un-served for ~200-400ms (not
  sub-ms) — measured t_cont -> first successful check-tail (2026-07-06,
  double-kill arm).
- An SSE follower keeps draining the kernel socket buffer after the server
  is SIGKILLed, so its observed log routinely catches the durable tail even
  when lag>0 held at the kill instant — "records beyond the resume
  boundary" cannot be sourced from pre-kill lag; append fresh post-restart
  records instead (2026-07-06, last-event-id-resume arm: lag=1 at kill,
  observed=tail=79 after drain, 4/4 vacuous before the fix).
- Forcing broadcast Lagged on a stalled SSE client is physically hard in
  the guest: with default sockets ~5 MB of stall-window bytes are absorbed
  (server sndbuf + client rcvbuf autotuning) before the session task ever
  blocks, and the 25-batch channel never overflows. Cap the client's
  SO_RCVBUF (32 KiB) BEFORE connect (window scaling honors it) and burst
  ≥70 × ≥128 KiB appends per stall; catch-up coalescing (batch count <<
  append count) corroborates a real Lagged handoff (2026-07-06,
  slow-follower-lagged arm).

- Config/timing surface (cartographer fan-out 2026-07-06, source-cited):
  bgtasks (trim, DOE, basin-deletion) tick every 60s ±10% jitter and are
  also event-triggered (bgtasks/mod.rs:19-44); streamer DORMANT_TIMEOUT is
  a hardcoded 60s (streamer.rs:55); every delete-on-empty deadline carries
  a 600s DOE_DEADLINE_REFRESH_PERIOD pad (streamer.rs:57-63) — DOE
  deletions take ≥10min wall clock; default retention is Age(7 days)
  (config.rs:78-84, age=0 invalid) so long-lived fixtures silently TTL;
  DOE deadlines are second-granularity (allow ≥1s slack); `until` is
  exclusive; SIGTERM/SIGINT = graceful drain with a 10s budget and no
  explicit db close (server.rs:356-382); RECORD_BATCH_MAX = 1000 records /
  1 MiB metered (caps.rs:13-16, formula record/mod.rs:208-216);
  `--append-inflight-bytes` defaults 128MiB, clamped to min 1MiB
  (core.rs:58-63).
- The object store is a guest-drivable fault plane: `AWS_ENDPOINT_URL_S3`
  (+ AWS creds env) points slatedb at any endpoint and an `http://` URL
  auto-enables allow_http (server.rs:288-310) — dependency faults need
  only a local python S3 stub, no disk-fault models. `SL8_*` env reaches
  ANY slatedb Settings field, not just FLUSH_INTERVAL (server.rs:178-184);
  `SL8_MANIFEST_POLL_INTERVAL` also controls the startup fencing sleep.
- Read-start params seq_num | timestamp | tail_offset are mutually
  exclusive (422); 416 bodies carry the true tail as a TailResponse
  (handlers/v1/error.rs:296); 412 CAS bodies are externally-tagged
  (`{"seq_num":N}` / `{"fencing_token":...}`); a rejected conditional's
  412 is deferred until its durability dependency is stable
  (append.rs:236-247) — a delivered 412 is durable truth.

## Areas

| Key | Title | Spec |
|-----|-------|------|
| durability | Durability | areas/durability.md |
| reads | Reads | areas/reads.md |
| appends | Appends | areas/appends.md |
| retention | Retention | areas/retention.md |

## Promoted findings

| Date | Promise | Exploration | Evidence |
|------|---------|-------------|----------|

- FINDING (2026-07-07, executor #18, deterministic 3/3): stale DOE
  deadline survives delete→recreate — finalize_trim deletes
  meta/tail/mapping/fencing/trim-point but NOT stream_doe_deadline keys
  (stream_trim.rs:135-146); deterministic StreamId (stream_id.rs:24-29)
  reuses the keyspace, so inc1's armed deadline fires against inc2 and
  DELETES it (~10min after recreate) despite inc2's own min_age=3600s —
  the recheck is presence-only (streamer.rs:457-461, 494-505) and an
  empty inc2 has no tail key => timestamp ZERO, always below the cutoff.
  Replay SEED=2492750010. Evidence:
  runs/doe-stale-deadline-across-recreate.md.

- FINDING #2 (2026-07-07, executor #19, 5/5 across all flush arms):
  kill-divided stream delete lets an acked Ensure silently un-happen —
  delete is two txns (terminal trim streams.rs:338-358, then
  mark_stream_deleted :360-379); a SIGKILL between them leaves
  trim_point==MAX + deleted_at==None, where GET serves the stream live
  while appends 409 deletion-pending, and PUT Ensure 200-acks fresh
  meta (gate checks only deleted_at, streams.rs:106-112) — which the
  recovered purge's finalize_trim then erases at the first tick
  (+64-72s; guarded only by trim-point equality,
  stream_trim.rs:123-146). Replay SEED=1000000 (500ms flush arm).
  Evidence: runs/control-plane-delete-straddle-ensure-erased.md.
  Reality notes: POST /v1/streams is CreateOnly (409 already-exists on
  live meta); Ensure is PUT /v1/streams/{name} with the config as body;
  GET can itself 409 stream_deletion_pending; `wio workloads logs`
  tails ~64 lines — keep in-guest log dumps short.

- REALITY (executor #20, lifecycle green rungs — deterministic-sim
  timing/error contract on the delete/purge/recreate path):
  - A 25k-record stream's DELETE acks in **26-51s** of virtual time
    (3 purge WriteBatches at DELETE_BATCH_SIZE=10k, separately awaited).
  - After a mid-purge SIGKILL there is **no event re-trigger**; the
    interrupted purge resumes on the 60s±10% tick and the recreate gate
    lifts ~2 ticks out (**+106-148s** observed). Un-killed, the
    event-triggered purge lifts the gate in seconds.
  - The legitimate empty-read class on a fresh/recreated stream is
    **416** with a `{"tail":{...}}` body (not 404, not a bare 200) —
    old-seq resurrection probes should accept exactly 200/416 and treat
    any other status as leak-shaped.
  - Kill-mid-purge seam taxonomy by post-restart create-probe:
    201 created = POST-FINALIZE; 409 stream_deletion_pending =
    PRE-FINALIZE; 409 resource_already_exists + append acks =
    DELETE-UNHAPPENED; 409 resource_already_exists + append 409
    deletion_pending = DIVIDED.

- REALITY (executor #21, CAS/match_seq_num contract — confirmed against
  api/ + lite/ source and live):
  - CAS append: POST /v1/streams/{s}/records {"records":[..],
    "match_seq_num": N}. 200 -> {"start":{"seq_num":N},"end":{"seq_num":N}}.
  - 412 body is an externally-tagged snake_case enum:
    {"seq_num_mismatch": K} (K = expected next seq = next_assignable_pos,
    streamer.rs:350-358) or {"fencing_token_mismatch":"<tok>"}. The promise
    prose's {"seq_num": K} was imprecise — the real key is seq_num_mismatch.
  - Fencing check precedes the seq check (streamer.rs:341 before :350);
    tokenless/missing-token appends are ALWAYS accepted, so a CAS race with
    no fencing_token is governed purely by match_seq_num.
  - Deferred-412 is exact: SeqNumMismatch's durability dependency is ..K
    (error.rs:220-222) and the reject is delivered only once K <= stable_pos
    (append.rs:238) — a delivered 412 guarantees tail >= K (a durability
    promise about OTHER writers' data).
  - Fence command records round-trip on read as headers [["","fence"]] +
    body = the token string (Raw format default, api/src/data.rs:43;
    common/record/mod.rs:90-118; json.rs:90-99) — so a fence winner's token
    is content-checkable on read-back, same as a data body.

- REALITY (executor #22, trim contract + purge physics — confirmed against
  source and live):
  - Trim command body must be EXACTLY 8 bytes (BE of the point,
    common/record/command.rs:88-95). Raw/UTF-8 JSON cannot carry bytes >=128
    as single bytes (String::from_utf8_lossy), so a raw send corrupts and
    422s. Send the 8-byte body AND the "trim" header value base64-encoded
    under an `s2-format: base64` request header — the format applies to
    header name/value bytes too (data.rs:64-68, json.rs:243-250). Reads stay
    raw: stored b"trim" decodes back to the string "trim".
  - Single record reads cap at ~1000 records per batch
    (api/v1/stream/mod.rs:62-63). Wide non-paginated reads SILENTLY TRUNCATE
    (no error) — a false over-deletion RED if you assume one read returns the
    whole tail. Paginate by cursor = max_seq+1, or probe with count=1.
  - The physical purge deletes below-V records in DELETE_BATCH_SIZE(=10_000,
    stream_trim.rs:18)-record WriteBatches (:80-108); an intermediate db.write
    fires at 10_000 then the remainder + finalize. A deletion set <10_000 is
    a SINGLE atomic batch (all-or-nothing across a crash — no partial physical
    state); to exercise the half-done-purge recovery seam the deletion set
    must exceed 10_000 (n>10_000, point >10_000).
  - Reading "first available seq" from seq 0 during/right after a large purge
    is SLOW — the scan walks accumulated tombstones for 0..floor before the
    first live seq, and can exceed a 15-30s socket timeout until compaction
    clears them. Probes from seq >= point (retained range) are unaffected.
    Poll purge progress with retry-on-timeout, not one tight read.
  - Purge is event-triggered on trim durability (streamer.rs:601-605) and
    does NOT re-trigger after a kill — the interrupted purge resumes on the
    next 60s±10% tick, so below-point remnants are LEGITIMATELY readable
    until then. s2-lite resumes a half-completed purge correctly (floor rises
    monotonically to the point; no already-purged seq is re-served).
