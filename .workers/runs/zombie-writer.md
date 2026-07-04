# Run evidence — zombie-writer-sigstop-takeover

- Workload: `.workers/workloads/zombie_writer.sh` — A writes, in-flight
  appends frozen mid-request by SIGSTOP, B takes over the same root on
  port 8081, SIGCONT at a seeded offset sweeping the whole takeover window
  (0-8s from B start), zombie pushes 25 appends through A's still-open
  listener while B keeps writing. Classification: time vs T (B's first
  ack) AND seq vs the takeover boundary (B's recovered tail read before
  B's first append).
- Oracle self-test: draft 01KWQC2KD052EKB9WHGFSFAAZC RED — relabeled
  B-ack detected as beyond-boundary persist (`no_zombie_persisted FAIL`).
- Bring-up: nd75hm0wf4nv7shyyrqnt1qqmd89xj5r — 4/4 green across the CONT
  sweep. In every trial the zombie's post-CONT writes were refused by
  SlateDB's manifest-epoch fence: HTTP 500 `detected newer DB client`
  (in-flight durability waits die with `database closed while waiting
  for durability`). The spec's open question — does the epoch CAS stop an
  already-initialized zombie's writes on local FS — answers YES at ack
  level, in 25/25 attempts per trial.
- Oracle refinement (producer #3, evidence-driven): first encoding used
  ack-time — "no A ack after T may persist" — and false-flagged 3/4
  seeds (exploration nd731y4wva3yav82ree67jt7cs89x6g6): the flagged
  records were in-flight-at-SIGSTOP appends already durable before B took
  over (their seqs sat below B's recovered tail; B built on them; stream
  dense, no dup). Spec oracle now keys corruption on PERSIST-time via the
  takeover boundary; late acks of pre-takeover writes are allowed and
  logged. Vacuity gate refined to "zombie must reach A's write path"
  (HTTP response, ack or storage rejection) — pure dead-socket trials
  void.
- Official exploration: see frontmatter `published:` (depth 10).
