# Loop state
- budget: none
- counters: { episodes: 8, producer: 3, executor: 5, workloads: ~137 }
- in-flight unit: none
- re-plan triggers: none
- publish-pending: []
- last episode summary: executor #5 — tail-gapless-restart-interleaved
  done+green (restart mode in workloads/tail_gapless.sh: 2-4 appender
  waves, SIGKILL between waves, global range-tiling verify across the
  boundary). Recovered tail resumed exactly at the acked high-water mark
  in every trial; no seq reuse, no holes. Self-test red re-proven in
  restart mode. All durability-area rungs are now done+green+published:
  acked-appends (baseline, kill9), tail-gapless (baseline, restart),
  zombie-writer (sigstop-takeover). Next: producer #4 should promote
  fencing-stale-across-restart (workloads/fencing.sh, planned) — last
  drafted rung in the area — and consider new rungs: zombie double-kill
  arm (spec notes it), disk-full/io-error fault models, multi-stream
  contention.
