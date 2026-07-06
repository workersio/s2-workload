# Loop state
- budget: session cap — 1 loop (producer #7 only, per coordinator directive
  2026-07-06 post-wrap-up; drafts queue for the next session, no execution).
  BUDGET EXHAUSTED — session wrapped 2026-07-06.
- session-baseline: { episodes: 16, workloads: ~263 }
- session-loops-used: 1 of 1 — STOP (producer #7 complete; next session
  starts at the ready-queue below)
- counters: { episodes: 17, producer: 7, executor: 10, workloads: ~263, session-workloads: 0 }
- in-flight unit: none
- re-plan triggers: none
- publish-pending: [] (PRIOR session 2026-07-06 wrapped clean: stop condition
  hit at 49 session-workloads >= 40 cap after executor #9 + #10; publish.py
  re-fired all 10 done explorations @ f4a2b31, every `published:` field
  rewritten. Both new reads arms official on the committed image:
    - reads-tail-baseline       -> nd7fsyab7bb1q721xar5mm3a2n8a1x0w (5/5 green)
    - reads-tail-across-restart -> nd71vb3mh6kbdhj6vxq49b593d8a062m (8 green /
      2 honest 2s-arm VOIDs — negative-lag samples, zero invariant violations)
  Prior officials all re-fired green at the same image; ids in promise
  frontmatter, committed @ 1650708.)
- ready-queue (dispatcher order): all 5 critic-gated ACCEPT (strategy-critic
  2026-07-06, verdict archived in the producer #7 episode summary below;
  required spec fixes applied to the promise frontmatter before ready):
  1. acked-appends-pipelined-kill      (acked-appends-survive-restart)
  2. zombie-double-kill-mid-recovery   (zombie-writer-cannot-corrupt)
  3. fencing-fence-ack-straddles-kill  (fencing-excludes-stale-writers)
  4. reads-tail-last-event-id-resume   (reads-never-lose-observed-records)
  5. reads-tail-slow-follower-lagged   (reads-never-lose-observed-records)
  Executor notes carried from the critic: #2 expect high void rate — spam
  zombie attempts across the sub-ms recovery window; C failing recovery is
  RED (availability), not void. #3 must establish+settle T1 before the T2
  straddle (missing token recovers to FencingToken::default(), core.rs:116).
  #4 include the count/bytes-limited seed arm (budget decrement,
  records.rs:61-62). #5 gate anti-vacuity on the tail-field-absent witness
  and size payloads to ~1-4 MB per stall window (capacity is 25 BATCHES).
- candidate directions for producer batch #8 (critic set-question, do NOT
  execute before drafting + critic gate): TRIM DURABILITY — new promise
  "trimmed records stay trimmed; trim never exceeds its acked point".
  CommandRecord::Trim applied synchronously (streamer.rs:377-388), trim-point
  KV rides the same WriteBatch (streamer.rs:1045-1050), recovered
  core.rs:100-103, terminal trim = deletion-pending gate (core.rs:118-120),
  plus async background deletion on durability (BgtaskTrigger::StreamTrim,
  streamer.rs:601-605). Arms: trim-straddles-kill (acked-trim regression =
  resurrection), kill-mid-trim-bgtask (partial physical deletion remnants),
  over-trim. Reuses fencing.sh/acked_appends machinery. Also still open:
  manifest ⊆ readback across the kill schedule; disk-fault models NOT
  available (no .workers/fault/ — verified 2026-07-06, do not re-propose
  without runtime evidence).
- last episode summary: producer #7 (this session's only loop). Drafted 5
  named explorations from the carried candidate directions across 4 promises
  (acked-appends-pipelined-kill; zombie-double-kill-mid-recovery;
  fencing-fence-ack-straddles-kill; reads-tail-last-event-id-resume;
  reads-tail-slow-follower-lagged), each with source-cited fault windows.
  Strategy-critic (foreground gate): ALL FIVE ACCEPT — none a wrapper or
  seed sweep — with 4 required spec fixes, all applied: (2) zombie arm
  gained the RED-availability clause for persistent recovery failure
  (assert_no_records_following_tail is an abort path, core.rs:113/165-189)
  + spam-the-window guidance; (3) fence-straddle now requires T1
  established+settled before the T2 straddle (default-token conflation,
  core.rs:116; fence record + token KV share a WriteBatch,
  streamer.rs:1039-1044 / core.rs:96-99); (4) Last-Event-Id gained the
  count/bytes-limited budget arm (records.rs:61-62 is a no-op without a
  limit) with exact limit-minus-consumed termination oracle; (5)
  slow-follower's anti-vacuity witness replaced — delivery jump IS the bug;
  the witness is a batch event missing `tail` after follow batches had it
  (catch-up: read.rs:169-182 tail None; follow: read.rs:209-212 tail Some;
  JSON omission json.rs:28,36-37) — plus payload sizing to defeat
  hyper/kernel/client buffering (~1-4 MB per stall window; capacity is 25
  BATCHES, streamer.rs:266). Critic also named trim durability the top
  batch-#8 candidate (recorded above, not drafted). Zero workloads run
  (producer episode; execution forbidden this session).
- PRIOR: session loop 2 (prev session) = executor #10. reads-tail-
  across-restart done+GREEN. Pipelined 3-writer pool +
  sampled lag>0 kill window (serial writers can never lag — ack and delivery
  share the durable_seq event); prime-append fix for slow-flush 404s;
  arm-scaled kill_after killed the 2s-arm void storm (depth-6: 6/6 green,
  zero voids). Test-reviewer REDO fixed (VOID-masking: serving-but-stream-
  denied post-restart and read_all non-200 now RED; gap selftest wired
  in-mode). Red-proofs: ORACLE_SELFTEST=1 across the real kill RED
  (seed=1837281636), gap in-mode RED (seed=290419235), lost-stream via
  nonexistent basin RED (seed=4028217004; check-tail auto-creates missing
  streams on create-stream-on-read basins — map note). 32 draft workloads
  this arm (49 session total).
- PRIOR: session loop 1 (prev session) = executor #9. reads-tail-baseline
  done+GREEN. Built
  workloads/reads_tail.py (both modes). Probed the
  follow transport: SSE on the read path (Accept: text/event-stream), batch/
  ping/error events + [DONE], Last-Event-Id resume; catch-up scan filters
  DurabilityLevel::Remote (read.rs:127), caught-up sessions hand off to the
  streamer's durable-gated broadcast — map.md reality note added.
  Test-reviewer REDO (real follow-gap routed to VOID not RED; exactly-once
  leg unproven) fixed: gap-aware wait_observed + partial_delivery_verdict
  (gap/dup in partial delivery = RED, only a clean stalled dense prefix may
  VOID), ORACLE_SELFTEST=gap red-proof added. Both legs red-proven
  (gap seed=2958128658, drop seed=3164093376); post-fix depth-5 sweep 5/5
  green (replay seed=4097857263). 17 draft workloads this arm.
- PRIOR: loop 3 (prev session) = executor #8 + inline producer #6 oracle
  revision. tail-gapless-straddle-at-kill GREEN. Building the straddle exposed a
  spec inconsistency (invariant 1 "no gap" vs invariant 4 "in-flight unacked at
  kill" — a durable-but-unacked append legitimately sits below tail with no acked
  range over it); producer revised the oracle to no-overlap + no-double-assign +
  dense-readback + content-ownership + gap-reconciliation + straddle-witness, a
  crash-on-restart being a finding. Built straddle-at-kill mode (SL8_FLUSH_INTERVAL
  stretched so acks lag -> real in-flight-unacked at kill; post-restart wave =
  collision surface). Red-proof RED via planted overlap; green 9/10 (1 correctly
  voided non-straddle). s2-lite never double-assigned a seq across the crash;
  unacked appends left no durable trace below tail. 13
  draft workloads this arm (3 red-proof iterations + 10 green); 24 session total.
- PRIOR: executor #7 (session loop 2) — acked-appends-
  kill-during-recovery GREEN. Injection fast path proven live on prod (drafts
  ran c865b2d-only code absent from the a88afdc image — map reality updated).
  Red-proof (ORACLE_SELFTEST) went RED via dense_prefix, exit 1. Depth-10 green
  bring-up 10/10, non-vacuous (recovery interrupted 2-4x/run), all 6 invariants
  pass — s2-lite tail-rebuild survives repeated mid-recovery SIGKILL. 11
  draft workloads that session (1 red-proof depth-1 + 10 green depth-10).
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
- PRIOR (executor #6) summary: fencing-stale-across-restart
  done+green (workloads/fencing.sh). The recovered fencing token
  (deserialized from storage after SIGKILL) still rejects stale-token
  appends with an identical HTTP 412 pre-kill and post-restart; tokenless
  cooperative behavior preserved; read-back exact. Every drafted rung in
  the durability area was then done+green+published: acked-appends
  (baseline, kill9), tail-gapless (baseline, restart-interleaved),
  zombie-writer (sigstop-takeover), fencing (stale-across-restart).
