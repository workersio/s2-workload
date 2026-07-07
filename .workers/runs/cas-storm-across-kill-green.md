# Run evidence — cas-storm-across-kill (GREEN)

Executor #21, 2026-07-07. Drafts via `--workload-file` injection on prod.
**GREEN** — CAS (`match_seq_num`) exactly-once holds across a mid-storm
SIGKILL. Test-reviewer verdict **KEEP** (required retry-200 selftest applied +
confirmed RED; fence content-identity enabled per review).

## Command

```
python3 .workers/workloads/cas_appends.py storm-across-kill
```

## What it attacks

`match_seq_num` is the API's only optimistic-concurrency primitive; every
client ledger / lock / exactly-once-ingestion pattern leans on it. Two seams:
the match is evaluated against `next_assignable_pos()` which counts pending
UNACKED batches (streamer.rs:327-331/350-358), and the 412 for a rejected
conditional is deferred until its durability dependency is stable
(append.rs:236-247) — so a delivered 412 is a durability promise about
another writer's data. A crash between decision and durability, or a retry of
an ambiguous CAS after restart, is where exactly-once would break.

6 connection-serial writers race `match_seq_num` at their local view of the
contended tail (start 0; on 412 jump to the returned next-seq); ~1/16 attempts
are CAS-guarded FENCE command records (the canonical lock-takeover pattern,
tokenless so the fence-gate passes and only the seq check governs). SIGKILL
gated on ≥2 in-flight + arm-scaled wins across SL8_FLUSH_INTERVAL arms
(default / 500ms / 2s). Restart same root. Every ambiguous in-flight CAS
(OSError, no response at kill) is retried with its ORIGINAL `match_seq_num` +
payload; **content identity** discriminates double-apply.

## CAS contract (verified in source, reviewer-corroborated)

- `POST /v1/streams/{s}/records` body `{"records":[{"body":..}], "match_seq_num": N}`.
- 200 → `{"start":{"seq_num":N},"end":{"seq_num":N}}` (single record).
- 412 body is an externally-tagged snake_case enum:
  `{"seq_num_mismatch": K}` (K = expected next seq = `next_assignable_pos`) or
  `{"fencing_token_mismatch":"<tok>"}` (api/src/v1/stream/mod.rs:428-437,
  handlers/v1/error.rs:255-262). Fencing check precedes seq (streamer.rs:341
  before :350); tokenless appends always accepted → data writers race purely
  on the seq.
- Deferred-412 is exact, not approximate: for `SeqNumMismatch` the durability
  dependency is `..K` (error.rs:220-222) and the reject is delivered only once
  `K <= stable_pos` (append.rs:238) — so a delivered pre-kill 412 guarantees
  `tail >= K`, and asserting `K <= final_tail` post-crash is the precise
  contract.
- Fence round-trip: a non-empty-token fence command reads back as headers
  `[["","fence"]]` + `body`=token (Raw format default, common/record/mod.rs:
  90-118, json.rs:90-99) — so fence winners' token IS content-checkable.

## Invariants

`cas_single_winner`, `acked_winner_durable`, `deferred_412_durable`,
`no_double_apply` (retry-200-while-position-holds-original-payload = double
apply; plus at-most-once by data+fence payload), `dense_prefix`,
`restart_serves`. Anti-vacuity: ≥2 in-flight AND ≥1 ambiguous AND ≥1 loss AND
≥8 winners AND a fresh win inside the flush window, else VOID.

## Runs — 7 GREEN / 1 honest VOID / 3 red-proofs

| kind | seed / env | arm | ledger (win/loss/ambig) | outcome |
|---|---|---|---|---|
| shakeout | 101 | 2s | 15 / 75 / 4 | **GREEN** |
| sweep | 300 | default | (green) | **GREEN** |
| sweep | 303 | default | | **GREEN** |
| sweep | 301 | 500ms | | **GREEN** |
| sweep | 304 | 500ms | | **GREEN** |
| sweep | 305 | 2s | | **GREEN** |
| sweep | 302 | 2s | 25 / 125 / **0** | **VOID** (anti-vacuity: 5 in-flight but 0 ambiguous — SIGKILL async, all outstanding requests completed before teardown; correctly voided) |
| confirm | 300 | default | 115 / 527 / 2 | **GREEN** (fence content-identity now enabled) |
| confirm | 88 | default | 40 / 200 / 4 | **GREEN** |
| red-proof | 101 + ORACLE_SELFTEST=doubleapply | 2s | | **RED** at-most-once ("D101-w0-a00000 at [0,15]") |
| red-proof | 101 + ORACLE_SELFTEST=phantom412 | 2s | | **RED** deferred_412_durable ("412 named next-seq 115 but tail 15") |
| red-proof | 101 + ORACLE_SELFTEST=retrydoubleapply | 2s | 15 / 75 / 3 | **RED** no_double_apply ("retry at pos 0 returned 200 while read-back ALREADY held D101-w0-a00000") |

Every green satisfied all six invariants non-vacuously (real per-position
contention: 75-527 losses per trial; ambiguous in-flight CAS resolved
1-4/trial). No trial double-applied, lost an acked winner, or named a 412
position that evaporated.

## Reviewer's required strengthening (applied)

The reviewer verified the headline double-apply escape is the **retry-200
guard** (retry returns 200 while `read-back[pos]==original payload`), and that
this branch was *executed but never asserted-to-fire* — the `doubleapply`
selftest only tripped the orthogonal at-most-once scan. Added
`ORACLE_SELFTEST=retrydoubleapply`, which forges a synthetic ambiguous entry
on an already-durable position with the retry forced to 200 → trips the guard.
Confirmed **RED** (run 01KWXGYFQX52S0AJZK2GPC5HWR). Also enabled fence
content-identity (the token round-trips; the prior skip was an unnecessary
under-check) and fixed the stale "fence bodies may not round-trip" comment —
both greens re-confirmed with fence content checked.

## Residual (reviewer-cleared, non-blocking)

No green naturally witnessed the `landed_before` bucket (retry 412 with
content-matched original). Per the reviewer this is structurally where correct
behavior lands — a same-`match_seq_num` retry on a durably-occupied position
MUST 412, it cannot double-apply — so it is an accounting confirmation, not a
bug guard. The real detector is the retry-200 guard, now selftest-proven.

## Interpretation

s2-lite's CAS is exactly-once across crash-restart on the record path and the
CAS-guarded command (fence) path: single winner per position, deferred-412 as
a kept durability promise, acked winners durable, ambiguous retries never
double-apply. No product finding. Official publication replays a recorded
green seed at wrap-up.
