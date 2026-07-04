# Loop state
- budget: none
- counters: { episodes: 10, producer: 4, executor: 6, workloads: ~190 }
- in-flight unit: none
- re-plan triggers: none
- publish-pending: []
- last episode summary: executor #6 — fencing-stale-across-restart
  done+green (workloads/fencing.sh). The recovered fencing token
  (deserialized from storage after SIGKILL) still rejects stale-token
  appends with an identical HTTP 412 pre-kill and post-restart; tokenless
  cooperative behavior preserved; read-back exact. Every drafted rung in
  the durability area is now done+green+published: acked-appends
  (baseline, kill9), tail-gapless (baseline, restart-interleaved),
  zombie-writer (sigstop-takeover), fencing (stale-across-restart).
  Next producer episode should draft NEW rungs, candidates in order of
  bug-likelihood: (1) zombie double-kill arm — SIGKILL the restarting
  server mid-recovery and restart again (sketched in the zombie spec);
  (2) fence/append race — concurrent fence T2 while T1 writers are
  mid-stream (token application point vs pipelined appends);
  (3) disk-fault models (io-error/full on the local root) if the runtime
  exposes them; (4) a reads area (long-poll/SSE tail correctness under
  restart).
