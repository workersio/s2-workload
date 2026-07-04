# Loop state
- budget: none
- counters: { episodes: 3, producer: 1, executor: 2, workloads: 34 }
- in-flight unit: none
- re-plan triggers: none
- publish-pending: []
- last episode summary: executor #2 — acked-appends-kill9-mid-stream official
  10/10 green (flush arms default|500ms|2s, randomized kill delay); durability
  ack-gate holds under SIGKILL. Both acked-appends explorations done+green+
  published; page shows Durability / Acked appends survive restart passing.
  Next ready: tail-gapless-baseline (bring-up for restart-interleaved). Then
  promote tail-gapless-restart-interleaved and zombie-writer-sigstop-takeover.
