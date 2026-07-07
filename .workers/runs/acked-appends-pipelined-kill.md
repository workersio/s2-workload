# Run evidence — acked-appends-pipelined-kill

Executor #11, 2026-07-06. Target image commit at draft time: 33f6396 (drafts
via `--workload-file` injection, so guest code = working tree).

## Command

```
python3 .workers/workloads/acked_appends.py pipelined-kill
```

4 connection-serial writer threads (one request = one ack each) keep 2-4
appends pipelined globally; SIGKILL fires after a seed-chosen global ack
count, gated on >=2 genuinely in flight; seed also picks the
SL8_FLUSH_INTERVAL arm (default | 500ms | 2s) with the acks-before-kill
target scaled per arm (2s arm ~2 acks/s over 4 serial connections —
unscaled targets void on runtime, the executor-#10 lesson). Stream is primed
with a retry-bounded append first (slow-flush arms 404 lazily-created
streams until the creation record is durable).

## Oracle

Existing acked_appends verify() family with the ack-order clause made
per-writer: within one connection, ack order maps to strictly increasing
read-back seqs; cross-writer interleaving unconstrained. Plus: tail covers
max acked end; dense prefix below tail; acked exactly once, identical
content; unacked at most once; no phantoms. Anti-vacuity: >=2 in flight at
kill AND last ack within max(2×flush-window, 50ms) of the kill AND >=8 acks.

## Runs (all drafts by injection, prod)

| exploration id | depth | purpose | outcome |
|---|---|---|---|
| nd7dnz6yaqwts9jsqb9xre1zhs8a013n | 3 | shape shakeout | 3/3 succeeded (arms hit: 2s ×2, default; in_flight at kill 3-4) |
| nd73ta7cztjngkpryvcxx8mc0d8a0ee8 | 1 | red-proof | FAILED as required — seed 3400598209, 500ms arm, in_flight=2 at kill, ORACLE_SELFTEST drop → dense_prefix FAIL, exit 1 |
| nd76m3rw39zj15m78d07snee118a1zqq | 10 | green sweep | 10/10 green, all non-vacuous: in_flight 2-4 at kill, last ack 0.6-32ms prior; arms default ×2, 500ms ×1, 2s ×7; e.g. seed 130903179 (default, 325 acked), seed 1852517057 (2s, 9 acked, 4 in flight) |
| nd76s1w1crwy1zz5x430m7mryx8a0shh | 2 | post-hardening confirm | 2/2 green (2s arm ×2) with tightened gates |

## Test-reviewer verdict (foreground gate)

KEEP. Confirmed distinct fault surface vs kill9: multi-batch durability
accounting in the streamer (`subscribe_durability` subscribes the queue
front; `on_db_durable_seq_advanced` pops every batch with db_seq <=
durable_seq) — an ack released on submission order or a coalesced-watch
off-by-one loses acked records only in this multi-writer shape. Oracle
verified able to catch loss (tail_bound/acked_survive), duplication
(at_most_once), per-writer reorder (acked_order). Two recommended
hardenings applied same-episode: fresh-ack vacuity gate tightened from
2×flush-window to 1×window (floor 50ms), and post-kill restart failure now
RED (`restart_serves` invariant) instead of VOID — fixes inherited
VOID-masking in kill9/kill-during-recovery/baseline-restart paths too.
Deferred (recorded, not blocking): a selftest variant that exercises
acked_survive directly (current drop-based selftest trips dense_prefix
first).

## Interpretation

s2-lite's durability-gated ack held under the multi-writer pipelined kill:
across 15 non-vacuous kill trials (3 shakeout + 10 sweep + 2 confirm) no
acked record was lost, duplicated, or re-ordered within any writer
connection, across all three flush arms. The at-risk set at kill (2-4
in-flight appends + freshly-acked records inside the flush window) was
non-empty in every trial.

