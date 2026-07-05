# Loop state
- budget: session cap — 3 loops or 20 workloads (this session, started @ episodes:10/workloads:~190)
- session-baseline: { episodes: 10, workloads: ~190 }
- session-loops-used: 3 of 3 done (loop 3 = producer oracle-revision + executor #8, straddle-at-kill GREEN)
- STOP CONDITION MET: session-workloads = 24 (>= 20-workload cap) AND 3 loops done. Wrap-up ran.
- counters: { episodes: 14, producer: 6, executor: 8, workloads: ~214, session-workloads: 24 }
- in-flight unit: none
- re-plan triggers: none
- publish-pending (both GREEN, blocked on origin/main push under "run the
  workload harness" scope — default-branch push DENIED; wrap-up re-fires
  publish.py once authorized; draft evidence durable in runs/):
    - acked-appends-kill-during-recovery — red-proof nd7eg0yedp6bmee03pb9c9erdd89zzjn
      RED, green nd71w6wkxmtkw8cz8w0qm3680x89ywce 10/10.
    - tail-gapless-straddle-at-kill — red-proof nd70ekpqfdzs6f7nyee8v7n62n89yhy7
      RED, green nd726c2zx1k9369v7yzs0q4qq989z234 9/10 green + 1 anti-vacuous void.
- ready-queue (dispatcher order — untouched, next session resumes here):
    1. reads-tail-baseline                  (reads promise — new)
    2. reads-tail-across-restart            (reads promise — new)
- last episode summary: loop 3 = executor #8 + inline producer #6 oracle
  revision. tail-gapless-straddle-at-kill GREEN. Building the straddle exposed a
  spec inconsistency (invariant 1 "no gap" vs invariant 4 "in-flight unacked at
  kill" — a durable-but-unacked append legitimately sits below tail with no acked
  range over it); producer revised the oracle to no-overlap + no-double-assign +
  dense-readback + content-ownership + gap-reconciliation + straddle-witness, a
  crash-on-restart being a finding. Built straddle-at-kill mode (SL8_FLUSH_INTERVAL
  stretched so acks lag -> real in-flight-unacked at kill; post-restart wave =
  collision surface). Red-proof RED via planted overlap; green 9/10 (1 correctly
  voided non-straddle). s2-lite never double-assigned a seq across the crash;
  unacked appends left no durable trace below tail. Official publish blocked
  (published: pending). 13 draft workloads this arm (3 red-proof iterations + 10
  green); 24 session total.
- PRIOR: executor #7 (session loop 2) — acked-appends-
  kill-during-recovery GREEN. Injection fast path proven live on prod (drafts
  ran c865b2d-only code absent from the a88afdc image — map reality updated).
  Red-proof (ORACLE_SELFTEST) went RED via dense_prefix, exit 1. Depth-10 green
  bring-up 10/10, non-vacuous (recovery interrupted 2-4x/run), all 6 invariants
  pass — s2-lite tail-rebuild survives repeated mid-recovery SIGKILL. Official
  publish BLOCKED on origin/main push (published: pending). 11 draft workloads
  this session (1 red-proof depth-1 + 10 green depth-10).
- PRIOR: producer #5 (session loop 1) — drafted 3 new arms,
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
