# Run evidence — tail-gapless-baseline

- Official exploration: `nd73rgzhky72vswbhxayvhrjeh89wge0` (2026-07-05,
  depth 5, 5/5 green) @ 6407e05.

- Workload: `.workers/workloads/tail_gapless.sh` — self-contained sh wrapper +
  embedded python3 (injection layers exactly one file onto the base image).
  Concurrent writer threads append 1-3-record batches via raw HTTP (one
  request = one ack), one log per writer under /tmp/wl-tail. Ack `end`
  convention (inclusive vs exclusive) inferred from the first ack, then
  enforced per-ack; ranges normalized to half-open.
- Oracle proven non-vacuous first: draft 01KWQAY18ZTRT2TM1N8B8TC8Z9 with
  ORACLE_SELFTEST=1 (drops one mid-manifest ack) went RED —
  `range_tiling FAIL gap at seq 103`, exit 1.
- Draft bring-up: 01KWQAY2EYJERC59B6PNEFA2PQ green (seed 282610292,
  5 writers × 33 appends, 166 acked ranges tile [0, 337) exactly,
  162 writer switches, read-back dense, every range holds exactly its
  owner's batch). All 3 invariants PASS: range_tiling, readback_dense,
  content_ownership.
- Vacuity floor: a trial voids (exit 3) unless writer interleaving is
  actually observed (switches >= writer count) and every append in the
  fault-free run was acked.
- Per the promise spec this rung is bring-up only (depth 5, expectations
  demoted): fault-free concurrent appends are upstream's home turf; the
  value here is the proven range-tiling oracle + multi-writer plumbing for
  the restart-interleaved rung.

# Run evidence — tail-gapless-restart-interleaved

- Restart mode added to the same workload file (same setup/oracle family,
  per the work item): 2-4 appender waves, SIGKILL + restart between waves,
  check-tail polled for readiness (startup fencing sleep), acked ranges
  accumulated across all waves into the one global tiling verify — seq
  reuse across a restart surfaces as an overlap, a hole as a gap; the
  workload logs TAIL REGRESSION if the recovered tail undercuts the acked
  high-water mark.
- Restart-mode self-test: draft 01KWQCHNHBSR452YRFVF12HBVT RED (dropped
  ack detected as tiling gap).
- Bring-up: nd74a20tcv4cw3kmd3nt49frdh89wjg4 — 3/3 green; e.g. seed
  2212099588: recovered tail = acked hi = 90 exactly, 89 ranges from 4
  writers tile [0, 169) across the kill boundary.
- Official exploration: see frontmatter `published:` (depth 10).
