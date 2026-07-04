# Loop state
- budget: session cap — 3 loops or 20 workloads (this session, started @ episodes:10/workloads:~190)
- session-baseline: { episodes: 10, workloads: ~190 }
- session-loops-used: 1 of 3  (loop 1 = producer #5, this episode)
- counters: { episodes: 11, producer: 5, executor: 6, workloads: ~190 }
- in-flight unit: none
- re-plan triggers: none
- publish-pending: []
- ready-queue (dispatcher order, oldest promise first):
    1. acked-appends-kill-during-recovery  (acked-appends promise)
    2. tail-gapless-straddle-at-kill        (tail-gapless promise)
    3. reads-tail-baseline                  (reads promise — new)
    4. reads-tail-across-restart            (reads promise — new)
- last episode summary: producer #5 (session loop 1) — drafted 3 new arms,
  ran the strategy-critic gate (source-verified), acted on all verdicts.
  BOUNCED fencing-concurrent-fence-midstream (streamer serializes append+
  fence in one non-awaiting invocation, streamer.rs:341/371 — no TOCTOU;
  safe by construction, not distinct from upstream Porcupine; recorded the
  bounce so it is not re-proposed). REVISED acked-appends-kill-during-recovery:
  retargeted SIGKILL #2 off the inert startup sleep onto the real lazy
  per-stream recovery (start_streamer -> load_persisted_stream_tail ->
  assert_no_records_following_tail, core.rs:82/144/165) triggered by the
  first post-restart stream access. REVISED tail-gapless (renamed
  multi-restart-straddle -> straddle-at-kill): tail is re-derived fresh from
  durable KV each restart (no carried snapshot, so N boundaries add nothing),
  so foregrounded the distinct element — in-flight-unacked appends AT the
  kill vs restart-interleaved's quiesced boundary. ADDED highest-value flank
  the critic surfaced: new `reads` area + promise reads-never-lose-observed-
  records (baseline + tail-across-restart) — the corpus was write-side-only;
  attacks the follow-gate (durable_seq, streamer.rs:607) vs catch-up-read
  filter (DurabilityLevel::Remote, read.rs:146-150) agreement across a crash
  (a dirty read that a post-restart Remote read denies = durability finding).
  Needs a new workload workloads/reads_tail.py (executor probes follow
  transport). 4 ready explorations queued.
- PRIOR (executor #6) summary: fencing-stale-across-restart
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
