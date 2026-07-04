# Loop state
- budget: none
- counters: { episodes: 1, producer: 1, executor: 0, workloads: 0 }
- in-flight unit: none
- re-plan triggers: none
- publish-pending: []
- last episode summary: producer #1 — area durability; 4 promises
  (acked-appends-survive-restart, tail-is-gapless-and-monotonic,
  fencing-excludes-stale-writers, zombie-writer-cannot-corrupt), 6 named
  explorations, 3 ready (acked-appends-baseline, acked-appends-kill9-mid-stream,
  tail-gapless-baseline). Strategy-critic gated: kill9 reshaped (flush-interval
  arms, raw-HTTP ack manifest, anti-vacuous gate, check-tail bound), fencing
  folded to restart-boundary attack, zombie-writer promise added on critic's
  find. Next ready: acked-appends-baseline (oracle bring-up; prove it can go
  red before trusting green).
