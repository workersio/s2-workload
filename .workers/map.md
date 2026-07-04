# Map — S2 (s2-lite) workload harness

Static evidence index. Not a queue: no owners, no claims, no priorities.

## Target

| Fact | Value |
|------|-------|
| Target repo | `s2-streamstore/s2` (fork: `workersio/s2-workload`) |
| Pinned ref | `76bf34a8ceb226970a6a847eaeb72e5b1464c081` (main, 2026-07-03) |
| System under test | `s2 lite` — self-hostable S2 API server (SlateDB storage engine) |
| SUT binary | `.workers/vendor/bin/s2` — release `s2-cli-v0.38.0`, x86_64-linux-musl, embeds CLI + lite server |
| wio project | (pending connect in web app) |
| wio branch | `main` |
| Local wio binary | `/Users/viswa/code/workers/formal/packages/wio/target/release/wio` (has `--exploration` / `--workload-file`; npm 0.4.0 not yet released) |

## Reality notes

- `s2 lite --local-root <dir>` persists to local disk via SlateDB's filesystem
  object store — the mode for durability/kill promises. In-memory mode (no
  flags) is an emulator only; nothing survives restart by design.
- Documented durability claim (README): with a bucket/local-root, "data is
  always durable on object storage before being acknowledged or returned to
  readers."
- Per-stream `streamer` tokio task owns the tail, serializes appends,
  broadcasts to followers; appends are pipelined against storage latency.
- `SL8_`-prefixed env vars tune SlateDB (e.g. `SL8_FLUSH_INTERVAL`, default
  50ms remote / 5ms in-memory).
- `/streams/*` requests need the `S2-Basin` header when hitting lite over raw
  HTTP; the CLI/SDKs handle it.
- Upstream runs turmoil/madsim-style deterministic sim tests (`mad-turmoil`);
  our axis is whole-process fault injection against the real binary (kill,
  disk, network at OS level), which sims do not cover.
- `s2-streamstore/s2-verification` (Go) holds their own correctness tooling —
  mine it for oracle ideas before drafting promises.

## Areas

| Key | Title | Spec |
|-----|-------|------|

## Promoted findings

| Date | Promise | Exploration | Evidence |
|------|---------|-------------|----------|
