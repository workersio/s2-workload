---
key: trim-is-final
area: retention
title: Trim is final
claim: >-
  An acknowledged trim is final: records at or below the trim point never
  reappear on any read path, trim never removes records beyond its requested
  point, and both directions hold across crash-restart and the asynchronous
  physical deletion.
status: active
provenance: "lite/src/backend/streamer.rs:377-388 (CommandRecord::Trim applied synchronously in-streamer); streamer.rs:1045-1050 (trim-point KV rides the same WriteBatch as the trim record); core.rs:100-103 (trim point recovered on restart), core.rs:118-120 (terminal trim = deletion-pending gate); streamer.rs:601-605 (BgtaskTrigger::StreamTrim — async physical deletion on durability); lite/src/backend/bgtasks/stream_trim.rs:80-149 (non-transactional record purge, finalize txn)"
explorations:
  - key: trim-baseline
    title: Trim baseline
    description: >-
      No faults. Append a known ledger, issue trim command records
      (single header ["", "trim"], seeded points including repeated,
      decreasing, and equal-to-previous trims). CRITIC-REVISED PHASED
      ORACLE (the read path never consults the trim point — absence is
      physical-only via the async purge): IMMEDIATELY after a trim ack
      assert only the over-deletion side (every record >= point still
      readable byte-exact; below-trim records MAY still be served — that is
      healthy); then run a purge-completion barrier (the purge bgtask is
      event-triggered on trim durability, streamer.rs:601-605, so usually
      fast; ceiling = 60s±10% tick) and assert the absence side: below-trim
      seqs absent on every read path (unary, SSE catch-up), and once
      absence is first observed it never regresses (reads scan at Remote
      durability, read.rs:128 — observed absence cannot un-happen). Tail
      contract (critic fix): tail is NOT unaffected — the trim command
      record is itself an append; assert tail never regresses and advances
      by exactly the command record per trim. Two source-confirmed pins:
      over-trim (point beyond tail) is clamped to the trim record's own
      position (streamer.rs:378-381, min with new_applied_point.end), and
      decreasing/equal trim points are acked no-ops (streamer.rs:382
      monotone guard). Proves the two-sided oracle (resurrection AND
      over-deletion) observes the invariant at all.
    status: ready
    result: null
    reason: null
    workload: workloads/trim.py
    command: python3 .workers/workloads/trim.py baseline
    faults: []
    depth: 5
    replay: null
    freshness: new-current
    reported: null
    published: null
  - key: trim-straddles-kill
    title: Trim straddles kill
    description: >-
      Fault boundary, two seams in one arm. Seam 1 — the trim ACK: SIGKILL
      at seed-chosen offsets around the trim ack (in-flight / just-acked /
      settled, SL8_FLUSH_INTERVAL arms; the fencing straddle pattern —
      trim record and trim-point KV share a WriteBatch, streamer.rs:1045-50,
      recovered core.rs:100-103). If the trim was ACKED pre-kill, the
      recovered LOGICAL trim point must be >= the acked point — observable
      post-restart as: below-point records eventually absent (purge resumes
      on tick), and a re-trim below the point acks as a no-op; an acked
      trim whose point regresses is resurrection. If unacked,
      applied-XOR-not, consistent across repeated probes. Seam 2 — the
      PHYSICAL purge: kill while the stream-trim bgtask is mid-purge
      (records deleted in non-transactional batches, stream_trim.rs:80-108),
      restart. CRITIC CORRECTION: after restart there is NO event trigger
      for the interrupted purge — resumption waits for the 60s±10% tick, so
      remnants below the trim point are LEGITIMATELY readable until then.
      Phased oracle: (1) records above the point intact and byte-exact,
      always; (2) "never resurface" = once absence of a below-trim seq is
      first observed it never regresses (Remote-durability scans,
      read.rs:128); (3) liveness: below-trim records absent within the tick
      ceiling post-restart. Anti-vacuity: a trial counts only if the kill
      provably landed inside its seam (in-flight trim at kill, or purge
      demonstrably incomplete at kill). REVIEWER-REQUIRED (REDO→KEEP): the
      seam-2 deletion set MUST exceed DELETE_BATCH_SIZE(=10_000,
      stream_trim.rs:18) so the purge spans >=2 WriteBatches — below that it
      is a single atomic batch (all-or-nothing across a crash) and no partial
      physical state exists to mishandle; seam2 therefore uses n=13000 with
      trim point in [10001,12000]. Also: a 5xx on the trim command (seam1) or
      on any post-restart read is a finding (server fault / read-path
      corruption), not a void. Red-proof plan: selftest arm re-serves a
      trimmed record into the observed set.
    status: done
    result: green
    reason: >-
      GREEN — a durable trim is final across the kill on BOTH seams. Seam1
      (trim ack straddle, n=300): acked ⟺ applied, retained byte-exact,
      below-point purged within the tick, tail advances by the trim record.
      Seam2 (physical purge, n=13000, T∈[10001,12000] so the deletion set
      crosses DELETE_BATCH_SIZE and the purge spans >=2 WriteBatches): the
      headline GREEN (SEED=1500, T=11501) killed +46ms after ack witnessed a
      GENUINELY PARTIAL purge — floor=0, all 11501 below-T records still
      physical at read#1 — and recovery correctly RESUMED the half-completed
      purge: floor rose monotonically 0→11501, all below-T absent within
      300s, 1499 retained byte-exact, tail 13000→13001. All five invariants
      PASS non-vacuously. No resurrection, no over-deletion, no acked-trim
      un-happening. Selftest (ORACLE_SELFTEST=resurrect) RED on
      purge_liveness at both scales (seq 258 at n=3000, seq 11500 at
      n=13000). Test-reviewer REDO→KEEP: the REDO caught that n=3000 left the
      partial-purge safety seam unexercised (single atomic batch) and two
      5xx VOID-masks — all three fixed and re-confirmed. No product finding.
      Evidence: runs/trim-straddles-kill-green.md.
    workload: workloads/trim.py
    command: python3 .workers/workloads/trim.py straddles-kill
    faults: []
    depth: 10
    replay: "SEED=1500 (seam2 partial-purge GREEN, n=13000 T=11501); SEED=258 (seam1 GREEN); red-proofs 1500 / 258 with ORACLE_SELFTEST=resurrect"
    freshness: new-current
    reported: 2026-07-07
    published: pending
---

# Trim is final

## Adversarial model

Trim is a write that promises an ABSENCE — the hardest promise to keep
across a crash. It is applied synchronously in the streamer, persisted as a
trim-point KV in the same WriteBatch as the trim command record, recovered
at startup, and then made physical by an asynchronous bgtask that deletes
records in non-transactional batches. That gives two distinct crash seams:
the trim's own durability window (ack vs kill — the fencing-straddle shape)
and the partially-executed physical purge (a restart mid-purge must honor
the LOGICAL trim point even though the physical state is half-done). Both
directions matter: resurrection (trimmed data served again — a contract
break clients cannot detect) and over-deletion (purge exceeding the point —
acked-data loss, the corpus's core theme). Strategy-critic named this the
top batch-#8 candidate (producer #7); explicitly distinct from age-TTL
retention (different mechanism, backlog row retention-ttl-expiry-boundary)
and from read-window arithmetic near the trim point (reads area,
read-window-trim-boundary).

## Oracle

Two-sided set oracle against the client-held ledger: for every read path,
served seqs == { s : s >= effective trim point } exactly — nothing below
(resurrection), everything at-or-above byte-exact (over-deletion), tail
never regressed by trim. Across kills: acked trim => recovered trim point
>= acked point; unacked trim => XOR, stable across probes. Purge
completion is a liveness clause with a bounded window (event-triggered
bgtask + 60s tick ceiling).

## Replay plan

Seeded trim points and kill offsets; failure prints seed, seam, kill style,
trim point, and the offending seq. Replay = same mode + seed. Reuses the
straddle/kill machinery proven in fencing.sh and acked_appends — new file
because the state model (two-sided absence oracle) and oracle family are
new.
