# Loop state
- rails: { loops: 100, workloads: 250 }   # session 2026-07-06 (run-until-exhaustion); supersedes the prior session's 1-loop budget cap, which that session honored and wrapped on
- no-new-info: { streak: 0, K: 5 }
- session-baseline: { episodes: 17, workloads: ~263 }
- session-loops-used: 13 of 100
- counters: { episodes: 31, producer: 9, executor: 22, workloads: ~510, session-workloads: ~245 }
- in-flight unit: none
- STOP (2026-07-07, loop 14): WORKLOAD SAFETY RAIL approaching — ~245 of 250
  session-workloads used. Coverage is NOT exhausted (ready queue #6-#15
  below remain, all under-floor/low-L). The next ready unit
  (#6 control-plane-ack-then-kill) is a full executor (shakeout + sweep +
  red-proof, 4-8 runs) that cannot complete inside the remaining ~5-workload
  headroom without leaving an incomplete in-flight unit — worse than a clean
  stop. Wrapped here per the rail. What remains queued is listed in
  ready-queue; next session resumes at row 5 (#6).
- re-entry: trim-straddles-kill → switch (GREEN, executor #22) — trim is
  final across BOTH kill seams; the partial-purge recovery seam is now
  genuinely exercised (deletion set > DELETE_BATCH_SIZE, half-done purge
  resumed correctly) and the resurrect selftest bites at that scale. The one
  remaining trim rung is trim-baseline (#15, no-fault ladder floor, low-L);
  no deepening of the kill seam left that adds bug-likelihood. Switch.
- PRIOR re-entry: cas-storm-across-kill → switch (GREEN, executor #21) — CAS
  exactly-once holds across the crash on both the record and CAS-guarded
  fence-command paths; all three headline detectors are selftest-proven.
  The remaining cas rungs (baseline #8, deferred-412-pipelined #9) attack
  different seams and stay queued.
- PRIOR re-entry: delete-recreate-kill-mid-purge + delete-recreate-fresh-identity
  → switch (both GREEN, executor #20) — the non-DOE freshness surface and
  crash-recovery are certified across all four kill seams; the one leak in
  this area is DOE-deadline key survival, already RED on its own rung. No
  deepening left here that the doe rung doesn't already carry.
- PRIOR re-entry: control-plane-delete-straddle-ensure-erased → switch — the
  kill shot landed 5/5 and is recorded; the adjacent seams are already
  queued (kill-mid-purge attacks the same divided state from the purge
  side; ack-then-kill is the regression floor). Reviewer's forward
  requirement noted in the runs file: add a wedge-branch forge before
  this workload serves as the post-fix regression floor.
- PRIOR re-entry: doe-stale-deadline-across-recreate → switch — corridor
  deterministically red 3/3 (finding recorded + reported); DOE firing is
  one-shot per stale entry so deepening the same corridor adds nothing;
  same-incarnation DOE corridors stay on backlog row 400.
- FINDING #2 (2026-07-07, executor #19): control-plane-delete-straddle-
  ensure-erased RED — 200-acked PUT Ensure erased by the recovered
  purge's finalize_trim ~64s after its ack (kill-divided two-phase
  delete). Replay SEED=1000000; test-reviewer KEEP; evidence
  runs/control-plane-delete-straddle-ensure-erased.md; published:
  pending.
- FINDING #1 (2026-07-07, executor #18): doe-stale-deadline-across-recreate
  RED — wrongful deletion of a recreated stream by its dead predecessor's
  DOE schedule. Replay SEED=2492750010; test-reviewer KEEP; evidence
  runs/doe-stale-deadline-across-recreate.md; published: pending.
- USER DIRECTIVE (2026-07-07, standing): "go towards finding bugs" —
  prioritize highest-L backlog rows and fault-boundary arms over
  remaining baselines; ladder-floor completeness is secondary until the
  high-score corridors are attacked. Queue order below is re-prioritized
  accordingly; row-1 exhaustion still respects the floor.
- carried note: RESOLVED producer #8 — floor audit done for both promises.
  Fencing: fencing-baseline rung added (critic: honest work — the straddle
  spec itself misstated the tokenless contract; 4 required pins written
  in). Zombie: critic REFUSED the 2-rung certification and required
  zombie-live-overlap-double-start (time-based fencing sleep never
  contested while prior instance is live — server.rs:186-198); rung added,
  ready. Residual fencing corner still open for a future rung:
  durable-but-unacked fence (T2-governs-unacked) never observed across 18
  unacked-capable trials.
- batch-#9 hard-commit (critic counter-promotion, producer accepted):
  control-plane-acked-ops-durable (480) row 1 +
  stream-delete-recreate-resurrection (667) row 2 — do NOT re-litigate at
  the next producer episode; promote them first.
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
- ready-queue (RE-PRIORITIZED per user directive: bug-likelihood first;
  entries 1-6 gated by strategy-critic 2026-07-07 producer #9 (verdicts
  + required fixes applied to frontmatter), 7-15 carried from producer
  #8's gate):
  1.  ~~doe-stale-deadline-across-recreate~~ DONE+RED (executor #18 — FINDING #1)
  2.  ~~control-plane-delete-straddle-ensure-erased~~ DONE+RED (executor #19 — FINDING #2)
  3.  ~~delete-recreate-kill-mid-purge~~ DONE+GREEN (executor #20 — all 4 seam classes, oracle clean; DOE corridor RED separately)
  4.  ~~delete-recreate-fresh-identity~~ DONE+GREEN (executor #20 — resurrection oracle holds; selftest bites)
  5.  ~~cas-storm-across-kill~~ DONE+GREEN (executor #21 — CAS exactly-once across kill; 3 selftests bite; reviewer KEEP)
  6.  control-plane-ack-then-kill      (control-plane-acked-ops-durable) — regression floor, depth cut 10→4 (commit IS durable, critic-refuted premise) — NEXT
  7.  control-plane-baseline           (control-plane-acked-ops-durable)
  8.  cas-appends-baseline             (cas-appends-exactly-once)
  9.  cas-deferred-412-pipelined       (cas-appends-exactly-once)
  10. timestamps-across-restart        (timestamps-never-regress)
  11. ~~trim-straddles-kill~~ DONE+GREEN (executor #22 — trim final across both kill seams; partial-purge recovery exercised; selftest bites; reviewer REDO→KEEP)
  12. read-window-trim-boundary        (reads-honor-request-windows)
  13. read-window-baseline             (reads-honor-request-windows)
  14. timestamps-baseline              (timestamps-never-regress)
  15. trim-baseline                    (trim-is-final)
  Executor notes carried from the critic: #1 anti-vacuity needs >=1 A
  append DURING B's fencing sleep + >=1 after B's first ack, both reaching
  A's write path; A-acks below the takeover boundary are legitimate
  (persist-time criterion). #2 must pin the four required clauses
  (tokenless-always-accepted, 412-body==governing-token, atomic governance
  flip at fence position, never-valid-token class) — without them it is
  padding. #3-#5: writer+attempt-unique payloads; retry-412 content
  identity is the double-apply discriminator; run the fencing-vs-seq 412
  precedence probe; fold CAS-guarded fence records into the storm. #6 run
  the sweep serially (race-free 416-tail). #7 purge-completion barrier
  BEFORE the sweep (below-trim reads are healthy until purge lands; tick
  ceiling 60s±10%); keep the ts-start-on-purged-index corner
  (read.rs:284). #8 pin the observed 4xx class for
  client_require-missing-ts. #10-#11 phased two-sided oracle (immediate =
  over-deletion side only; absence only after barrier / "never regress
  after first observed absence" + tick-ceiling liveness); tail advances by
  exactly the trim command record; over-trim clamps
  (streamer.rs:378-381), decreasing trims are acked no-ops (:382).
- workload-file plan (execution-shape rule): cas_appends.py, read_windows.py,
  timestamps.py, trim.py are NEW files; fencing.sh gains `baseline` mode;
  zombie_writer.sh gains `live-overlap-double-start` mode.
- still-open non-batch directions: manifest ⊆ readback across the kill
  schedule; disk-fault models NOT available (no .workers/fault/ — verified
  2026-07-06, do not re-propose without runtime evidence).
- last episode summary: executor #22 (loop 14). trim-straddles-kill
  done+GREEN. Built NEW trim.py straddles-kill: append a known ledger, trim
  to a seed point, SIGKILL in one of two seams, restart, run the two-sided
  phased absence/over-deletion oracle. Trim contract verified in source FIRST
  (8-byte BE body; RangeTo ..V; trim-point KV atomic with the trim record;
  read path never consults the point — absence is PHYSICAL via async purge,
  event-triggered on durability, no re-trigger after kill). FOUR real bugs in
  my own harness caught + fixed during bring-up: (1) trim body 422 — raw JSON
  can't carry bytes>=128; fixed with base64 body+header value + s2-format
  header (also unmasked a false seam1 "green" that had 422'd benignly); (2)
  seam2 25k TIMEOUT — purge-poll re-read the whole stream; fixed with cheap
  physical_floor + count=1 spot probes, n cut to 3000; (3) false over-deletion
  RED — a wide read truncated at the ~1000-record cap; fixed with count=1
  probes; (4) selftest didn't bite — floor-forge never fired; fixed by
  injecting a below-T seq into the final read set. Then TEST-REVIEWER
  REDO with three blockers, ALL fixed + re-confirmed KEEP: (#1, the big one)
  at n=3000 the deletion set (<DELETE_BATCH_SIZE=10_000) is a SINGLE atomic
  WriteBatch, so the "half-done physical purge" safety seam was NEVER
  exercised — only liveness; fixed by seam2 n=13000, T in [10001,12000] so
  the purge spans >=2 batches → SEED=1500 GREEN witnessed a genuinely partial
  purge (floor=0, 11501 below-T remnants at read#1) and recovery resumed it
  correctly (floor 0→11501, all invariants PASS); selftest RED at that scale
  (seq 11500). (#2) seam1 trim 5xx now RED not VOID (server fault). (#3)
  post-restart read 5xx now RED not VOID (read-path corruption). Supporting
  hardening (transient-fault tolerance, not oracle weakening): physical_floor
  OSError→None + poll-retry (the from-0 floor probe scans ~10k tombstones
  during a big purge and can time out), read_all 5×retry-then-VOID, spot-check
  OSError-skip, TICK_CEIL 150→300. Runs: seam1 258 GREEN (×2 incl. post-
  refactor); seam2 partial 1500 GREEN; selftests 258/1500 RED. No product
  finding (green — s2-lite trim recovery is correct). ~33 draft workloads.
  Evidence: runs/trim-straddles-kill-green.md. published: pending.
- PRIOR: executor #21 (loop 13). cas-storm-across-kill
  done+GREEN. Built NEW cas_appends.py storm-across-kill mode: 6 writers
  race match_seq_num at the contended tail (+~1/16 CAS-guarded fence
  commands), SIGKILL mid-storm across all 3 flush arms, restart, resolve
  every ambiguous in-flight CAS by retrying with its ORIGINAL match_seq_num;
  content identity discriminates double-apply. CAS contract verified in
  source FIRST (412 body {"seq_num_mismatch":K}, fencing precedes seq,
  tokenless always accepted, deferred-412 dependency ..K) — shakeout passed
  clean on the FIRST try, no shape bug. 7 GREEN across all arms (101 +
  300/303/301/304/305 + confirm 88/300) + 1 honest anti-vacuity VOID (302:
  5 in-flight but 0 ambiguous). Test-reviewer KEEP with one required
  strengthening: the headline retry-200 double-apply guard was executed but
  never asserted-to-fire — added ORACLE_SELFTEST=retrydoubleapply → confirmed
  RED; also enabled fence content-identity (token round-trips, Raw format)
  and fixed a stale comment; both greens re-confirmed. 3 red-proofs bite
  their exact invariants (doubleapply→at-most-once, phantom412→
  deferred_412_durable, retrydoubleapply→no_double_apply). Residual
  (reviewer-cleared): natural landed_before bucket is where correct behavior
  lands, not a bug guard. No product finding. ~12 draft workloads.
  Evidence: runs/cas-storm-across-kill-green.md. published: pending.
- PRIOR: executor #20 (loop 12). Closed BOTH remaining
  lifecycle rungs GREEN: delete-recreate-fresh-identity and
  delete-recreate-kill-mid-purge. Built fresh_identity_oracle +
  main_fresh_identity + main_kill_mid_purge in lifecycle.sh. fresh-identity:
  6/6 GREEN + ORACLE_SELFTEST (77002) RED. kill-mid-purge: 4 shakeout GREENs
  (one per seam class DELETE-UNHAPPENED/DIVIDED/PRE-FINALIZE/POST-FINALIZE) +
  9/10 sweep GREEN + defect-fix replay 3963731212 GREEN. Test-reviewer KEEP
  ×2 with 5 hardenings — ALL APPLIED and re-confirmed GREEN post-fix (SEED
  88001 fresh-identity; 90000 DIVIDED + 88817 PRE-FINALIZE kill-mid-purge,
  runs nd76xhy2/nd70w53r/nd74gpsb). Hardenings: crash-during-oracle labeled
  RED restart_serves+dump; old-seq reads accept only 200/416 else retry→RED;
  wrong-token probe retry→RED (was VOID); DELETE-UNHAPPENED probe body
  A{seed}-prefixed; two-band racing delays; DIVIDED requires explicit
  stream_deletion_pending code; server.poll() in gate-poll loop. Notable
  green facts recorded: 25k-record DELETE acks 26-51s virtual; interrupted
  purge resumes ~2 ticks out (+106-148s, no event re-trigger). SCOPE CAVEAT
  (recorded in evidence + promise): these greens do NOT cover DOE-deadline
  freshness — that corridor is FINDING #1's RED. Zero VOIDs. ~34 draft
  workloads. Evidence: runs/delete-recreate-resurrection-green-rungs.md.
  published: pending (both rungs). No new product finding (green rungs).
- PRIOR: executor #19 (loop 11). control-plane-delete-
  straddle-ensure-erased done+RED — **FINDING #2**. Built
  control_plane.sh delete-straddle mode: DELETE fired on a thread,
  SIGKILL at a BISECTION-chosen offset (self-tuning per flush arm —
  lands the straddle by trial 2-3 on every arm incl. the 5ms default),
  seam classification EARLY/STRADDLE/LATE by GET+append, then the kill
  shot: PUT Ensure (ProvisionMode::Ensure) with marker config, 180s
  erased-watch; ensure-refused path gets a 300s wedge-resolution watch
  (RED straddle_wedged). Shakeout v1 exposed two of MY bugs: POST is
  CreateOnly (409 already-exists — not the gate; Ensure is PUT with
  config body) and GET-409 deletion_pending was "unclassifiable"
  (false VOID) — both fixed. v2: 3/3 RED across default/500ms/2s arms;
  same-seed replay + fresh-seed 424242 both RED = 5/5. Acked-Ensure
  erased at +64-72s — exactly the first jittered purge tick (no
  startup tick; event trigger died with the process). Test-reviewer
  KEEP: no coherent linearization exists; Created-on-finalized-meta
  excluded (200-not-201 + timing + one-shot guard); LATE-seam clause
  sound. Forward req: forge the wedge branch before post-fix
  regression-floor duty. ~8 draft workloads. published: pending.
  Bug reported to user.
- PRIOR: executor #18 (loop 10). doe-stale-deadline-
  across-recreate done+RED — **FIRST PRODUCT FINDING**. Built lifecycle.sh
  doe-stale-deadline mode (A: inc1 DOE min_age=1s+append+delete, inc2
  min_age=3600s left EMPTY; control B: inc2 no-DOE; GET-only probes;
  basin without auto-create). Shakeout v1 VOID caught my 202-vs-200/204
  DELETE-ack assumption (delete is async). Shakeout v2 RED seed
  2492750010 (A dead in (645,673], window [597,668], B alive +703s);
  same-seed replay RED (+617s) and fresh-seed 555000333 RED (+628s) —
  3/3 deterministic, control-discriminated. Test-reviewer KEEP: all
  harness-artifact alternatives excluded by source (retention is TTL on
  record keys only; Ensure-erased corridor structurally too early;
  auto-create incompatible with persistent 404); precision fix applied
  (empty inc2 has NO tail key => timestamp ZERO); hardening notes
  recorded (server.log dump on RED, DATA_DIR hygiene, double GONE probe,
  append retry) — non-blocking. control_survives/purge_liveness red legs
  honestly unexercised. 4 draft workloads. published: pending (official
  replays seed 2492750010 at wrap-up). Bug reported to user.
- PRIOR: producer #9 (loop 9, bug-hunt pivot). Promoted
  batch #9 hard-commits: stream-delete-recreate-resurrection (667) -> 3
  rungs + NEW lifecycle area; control-plane-acked-ops-durable (480) -> 3
  rungs (one critic counter-promotion) + NEW control-plane area. Also
  triaged the #17 bounce-back (promise claim fixed: tokenless always
  accepted). Strategy-critic (foreground, source-verifying, fetched
  slatedb 0.13.1 crate source): 3 ACCEPT / 2 REVISE, all fixes applied.
  KEY REFUTATION: slatedb txn.commit defaults await_durable:true
  (db_transaction.rs:519-521, config.rs:462-470, db.rs:362-366) — bare
  control-plane commits ARE durability-gated; ack-then-kill demoted to
  regression floor (depth 10→4). COUNTER-PROMOTION accepted:
  control-plane-delete-straddle-ensure-erased (depth 10) — two-phase
  delete divisible by kill => trim_point==MAX + deleted_at==None =>
  GET-live/append-pending incoherence AND an Ensure that 200-acks then
  gets erased by finalize_trim within the recovery tick. KEY
  CONFIRMATION: doe-stale-deadline-across-recreate verified end-to-end
  (cutoff math, create_ts-based last-write, no event trigger, no
  neutralization) — "near-certain deterministic RED"; build constraint:
  no appends to inc2-A during the wait. kill-mid-purge delay
  distribution reshaped ([0,2s] sub-100ms + delete-racing + SL8 arms —
  purge is event-triggered at delete, NOT tick-scheduled; drafted shape
  was degenerate). Second-corridor idea (Remote-filter purge misses)
  DISPROVEN — not added. Backlog: 2 rows promoted+archived (active 18,
  top 600), row-400 annotation corrected (disarm-reconfigure path NOT
  covered, stays). Zero workloads run.
- PRIOR: executor #17 (loop 8). fencing-baseline
  done+GREEN — the ladder-floor no-fault rung. Built `baseline` mode in
  fencing.sh: three governance regimes in series (default "" -> T1 -> T2),
  all 4 critic pins enforced non-vacuously. Shakeout v1 caught my own
  412-body-key bug (actual shape: {"fencing_token_mismatch": "<governing>"},
  "" in default regime — disclosure parse path proven live). Shakeout v2
  3/3; red-proofs ×3 ARMS all RED (=1 stale-relabel seed 1026934166 ->
  wrong_token_rejected; =flip -> atomic_flip; =disclose -> disclosure_412);
  sweep 5/5 + 2 explicit-SEED greens (1111111111, 777000111).
  Test-reviewer REDO fixed same-episode (2 VOID-maskings unique to a
  no-fault mode): correct-token/fence 412 refusal now RED (fence acked
  but never applied would have voided); dropped connection now
  server.poll()-gated RED (crash-on-append). Post-REDO confirm 3/3.
  No product finding. Spec-note bounced to producer: promise CLAIM still
  says "stale or missing token are rejected" — missing/tokenless is
  ALWAYS accepted (streamer.rs:341); propagate the #8 correction.
  18 draft workloads. published: pending.
  Evidence: runs/fencing-baseline.md.
- PRIOR: executor #16 (loop 7). zombie-live-overlap-
  double-start done+GREEN — the critic-required third rung: NO signals; A
  serves under a live 3-thread writer pool while B double-starts on the
  same root through its fencing sleep and takes over; A keeps firing
  through B's boot + 1-3s past T. Built main_live_overlap in
  zombie_writer.sh (conjunctive anti-vacuity: >=1 A round-trip inside B's
  boot window AND >=1 attempt after T). Shakeout 4/4; red-proof 2/2 RED
  (seeds 4095698169/813245199 -> no_zombie_persisted); depth-10 sweep
  10/10 green, every trial witnessed (69-80 boot-window, 76-151 post-T),
  ZERO A-acks after T. Test-reviewer REDO fixed same-episode (2
  VOID-masking defects): mode-scoped allow_pre_truncation=False (missing
  A-acked record = RED acked-data loss — no freeze to excuse a hole) and
  RED successor_available (B never serving / persistent append refusal
  would mask a fencing-sleep regression; readback+verify still run
  first). Post-REDO confirm 3/3 green (successor_available PASS emitted),
  post-REDO red-proof RED (seed 4035185551). NEW REALITY (map note): A is
  fenced at B's slatedb DB-open (epoch CAS), ~3s before B's first ack —
  the time-based sleep is belt-and-suspenders on local FS. Also: guest
  /dev/urandom repeats across runs; pass explicit SEEDs when distinctness
  matters. Evidence: runs/zombie-live-overlap-double-start.md. 20 draft
  workloads. published: pending. Residual (carried, unchanged):
  durable-but-unacked fence corner on the fencing promise.
- PRIOR: producer #8 (loop 6). Cartographer fan-out: 5
  foreground scouts (docs, tests, commits, runtime/config, API surface) →
  35 shards deduped to 23 backlog rows (4 promoted, 19 active + 1 critic
  seam row = 20 active, top 667); map.md gained the config/timing surface
  note, the object-store-fault-plane note (AWS_ENDPOINT_URL_S3 +
  allow_http = guest-drivable dependency faults), and the
  error-contract note (mutually-exclusive read starts, 416 TailResponse,
  deferred 412). Promoted 4 corridors into 4 NEW promises + 2 new areas
  (appends, retention): cas-appends-exactly-once (3 rungs — full ladder),
  reads-honor-request-windows (2), timestamps-never-regress (2),
  trim-is-final (2); + floor-audit rungs fencing-baseline and (critic-
  required) zombie-live-overlap-double-start = 11 ready entries.
  Strategy-critic (foreground, source-verifying): 8 ACCEPT / 2 REVISE
  (read-window-trim-boundary: read path never consults trim point — purge
  barrier required; trim-baseline: phased two-sided oracle, tail advances
  by the trim record) — all fixes applied; floor certifications: zombie
  REFUSED (third rung required + added), fencing-baseline ACCEPTED with 4
  required pins (and it caught the straddle spec's tokenless misstatement,
  corrected). Ranking audit: doe-wrongful-delete C 3→4 (533→400), s2s
  L 4→3 (427→320), sse-session-termination-taxonomy added (216), top-3
  provenance verified, stale-DOE-deadline-across-recreate ammunition
  recorded on the 667 row. Counter-promotion resolved: batch #9 rows 1-2
  hard-committed (control-plane-acked-ops-durable,
  stream-delete-recreate-resurrection). Zero workloads run (producer
  episode). Skips recorded on the 667 row.
- PRIOR: executor #15 (loop 5). reads-tail-slow-follower-
  lagged done+GREEN — the ready queue's last entry. Built the no-kill
  broadcast-overflow mode (Follower pause/unpause stall, per-batch
  has_tail witness metadata, SO_RCVBUF 32KiB pre-connect cap, norm_body
  sha256 equality for >1KiB bodies on both oracle sides). Shakeout v1 4/4
  VOID — default buffers absorbed ~5MB of stall bytes, Lagged never fired
  (map note); physical fix (rcvbuf cap + 70-110 × 128-256KiB bursts), not
  an oracle downgrade. Red-proofs ×2 in-mode: gap (3348524698 ->
  follow_wellformed) and readback-drop (775902020 -> observed_survive).
  Depth-10 sweep 10/10, every trial witnessed 9-71 catch-up batches after
  handoff. Test-reviewer KEEP, no required action; residual future arm:
  burst overlapping unpause so catch-up races in-flight appends (the
  empty-scan skip branch read.rs:183-185 itself). 20 draft workloads.
  published: pending.
- PRIOR: executor #14 (loop 4). reads-tail-last-event-id-
  resume done+GREEN. Built the header-resume mode (Follower gained
  resume_header/count_limit + commit-at-dispatch for last_event_id —
  WHATWG semantics; pre_kill_phase extracted, reviewer-verified
  diff-equivalent for published across-restart). Shakeout v1 4/4 VOID
  exposed socket-buffer drain closing lag post-SIGKILL (map note); fixed
  with 3-8 post-restart appends. Red-proofs ×3: +1-shift (2/2, seeds
  3934514529/680046819 -> follow_wellformed), readback-drop (1108731994 ->
  observed_survive), bad-id (3761786860 -> header_describes_delivery).
  Depth-10 sweep 9/10 + 1 honest 2s-arm void. Test-reviewer REDO fixed
  same-episode: (1) server id misdescribing its own delivery RED (was
  "harness bug" void); (2) server rejecting its own emitted header RED
  header_accepted (was "transport" void); want capped ≤ available−1.
  Post-REDO confirm 4/4. Budget decrement exact in every green (e.g.
  259 re-sent, 253 consumed -> exactly 6 then [DONE]). Residual: bytes
  budget (records.rs:62) untested — count-only arm. 28 draft workloads.
  published: pending.
- PRIOR: executor #13 (loop 3). fencing-fence-ack-straddles-
  kill done+GREEN. Built the straddle mode (settle-T1 precondition; kill
  styles in-flight/just-acked/settled, style-0 delay swept [0,window) and
  2×-weighted after two shakeouts produced zero unacked fences). Red-proof
  RED ×2 (seeds 4210227959, 1961397883 — forged T1 probes -> fence_durable
  FAIL). Depth-10 sweep 10/10 green, 4 unacked + 6 acked outcomes.
  Test-reviewer REDO fixed same-episode: post-kill restart failure RED
  restart_serves (also retrofitted to published stale-across-restart mode);
  probes classify by code (412-only governs; transients retry->void;
  unresolved bodies at-most-once). 33 draft workloads. published: pending.
- PRIOR: executor #12 (loop 2). zombie-double-kill-mid-recovery
  done+GREEN. Built double-kill mode (A SIGSTOP mid-stream -> B takeover +
  SIGKILL mid-in-flight -> C lazy recovery raced by 3-thread zombie storm).
  Red-proof RED (seed 1113152951, relabel -> no_zombie_persisted FAIL).
  Depth-10 sweep 10/10 green ZERO voids (expected high void rate never
  materialized — un-served window is ~200-300ms, not sub-ms; storage-layer
  rejections "detected newer DB client" prove SlateDB self-fencing works).
  Test-reviewer KEEP + 3 hardenings applied (response-timestamp witness,
  verify-before-vacuity-exit, C log-tail dump) + 3/3 confirm. 18 draft
  workloads. published: pending. New reality: zombie rejections take ~100ms
  each at A; C recovery under zombie contention ~200-400ms.
- PRIOR: executor #11 (loop 1). acked-appends-pipelined-kill done+GREEN. Built pipelined-kill mode
  (4 connection-serial writers, kill gated on >=2 in flight + fresh ack,
  arm-scaled kill_after), per-writer acked_order clause in verify(). Red-proof
  RED (seed 3400598209, drop -> dense_prefix FAIL). Depth-10 sweep 10/10 green
  non-vacuous. Test-reviewer KEEP; applied its two hardenings (fresh-ack gate
  1×window; post-kill restart failure now RED restart_serves, not VOID —
  fixes inherited VOID-masking in all acked_appends modes) + 2/2 confirm.
  16 draft workloads. published: pending (officials batch at wrap-up).
- PRIOR: producer #7 (prev session's only loop). Drafted 5
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
