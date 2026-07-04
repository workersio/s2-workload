# Loop state
- budget: none
- counters: { episodes: 7, producer: 3, executor: 4, workloads: 100+ }
- in-flight unit: none
- re-plan triggers: none
- publish-pending: []
- last episode summary: executor #4 — zombie-writer-sigstop-takeover
  done+green (workloads/zombie_writer.sh). SlateDB's manifest-epoch fence
  stops an already-initialized zombie's writes on local FS: 25/25 post-CONT
  appends per trial rejected with "detected newer DB client", across a
  seeded SIGCONT sweep of the whole takeover window — the spec's open
  question answers YES at ack level. Producer #3 refined the oracle with
  draft evidence: corruption keys on persist-time (takeover boundary =
  B's recovered tail), not ack-time — first encoding false-flagged late
  acks of pre-takeover in-flight writes (3/4 seeds); vacuity gate is now
  "zombie must reach A's write path". Next ready:
  tail-gapless-restart-interleaved (extends tail_gapless.sh with restart
  mode; kill/restart between appender waves, tail must resume at persisted
  tail with no seq reuse). After that: consider promoting
  fencing-excludes-stale-writers or a double-kill arm on zombie_writer.
