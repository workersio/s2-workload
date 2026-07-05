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
  surface (zombie-writer promise).
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

## Areas

| Key | Title | Spec |
|-----|-------|------|
| durability | Durability | areas/durability.md |
| reads | Reads | areas/reads.md |

## Promoted findings

| Date | Promise | Exploration | Evidence |
|------|---------|-------------|----------|
