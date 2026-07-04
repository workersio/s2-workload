# Run evidence — tail-gapless-baseline

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
