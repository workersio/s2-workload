# Run evidence — zombie-double-kill-mid-recovery

Executor #12, 2026-07-06. Drafts via `--workload-file` injection on prod
(guest code = working tree; image commit at draft time 33f6396).

## Command

```
sh .workers/workloads/zombie_writer.sh double-kill-mid-recovery
```

A SIGSTOPped mid-stream with 4 in-flight appends frozen; B takes over the
same root, writes, SIGKILLed mid-in-flight (spin-verified); C starts and
during its lazy first-access recovery the zombie A is SIGCONTed — 3
parallel zombie connections spam A across the whole un-served window
(rejected attempts take ~100ms each, so serial spam starves the window).
Bounded C retries (4); readback through C.

## Oracle

sigstop-takeover verify() family + `b_unacked` (B's in-flight at kill,
at-most-once) + `recovery_available` (zombie must not brick recovery —
persistent C failure is RED availability, single crash+retry logged) +
anti-vacuity witness: >=1 zombie attempt whose send AND response both land
before C's first successful check-tail, with an HTTP-level (storage-layer)
response. Vacuity exit runs AFTER verify so a vacuous race cannot mask a
B-durability red.

## Runs (all drafts by injection, prod)

| exploration id | depth | purpose | outcome |
|---|---|---|---|
| nd7f25zh1m61qyt6k530w66has8a10vn | 4 | shape shakeout | 4/4 green non-vacuous; B in_flight=True at kill in all; zombie rejected at storage layer (HTTP 500 "detected newer DB client" / "database closed while waiting for durability") |
| nd7998hybtnc19wkthdhw1bdg58a17v6 | 1 | red-proof | FAILED as required — seed 1113152951, ORACLE_SELFTEST relabel → no_zombie_persisted FAIL, exit 1 |
| nd7b0a5bdfp8p1ca2s3yj2z19n8a0q33 | 10 | green sweep | 10/10 green, zero voids; 5-8 zombie attempts reached A's write path inside every 196-297ms un-served window; C recovered on attempt 1 in all |
| nd70xg28j0xyjk4ew5hwx6gscs8a1e5t | 3 | post-hardening confirm | (see below) |

## Test-reviewer verdict (foreground gate)

KEEP. Confirmed distinct from published sigstop-takeover (that arm races an
established live B and reads back through the acker; this one crash-tests
B-ack durability through a recovered C and races the tail-rebuild itself).
Confirmed the late-ack allowance cannot mask a real zombie persist: A is
frozen, never restarted, so post-CONT assignments are necessarily >= its
durable tail = takeover boundary. Three hardenings applied same-episode:
(1) response timestamps on rejected attempts; witness now requires send AND
response inside the un-served window; (2) verify() runs before the vacuity
exit (vacuous race can't mask a red); (3) C server-log tail dumped on any C
exit/stall for recovery_available triage.

## Interpretation

SlateDB's manifest-epoch fencing held against an already-initialized zombie
racing C's tail rebuild: every zombie attempt was rejected at the storage
layer, nothing A accepted post-takeover appeared in read-back, all B-acked
records survived B's mid-in-flight SIGKILL exactly once, and the zombie
never bricked recovery (C attempt 1 success in all trials). The observed
un-served recovery window is ~200-300ms (not the sub-ms the spec guessed) —
plenty of room for the storm, witnessed by storage-layer rejections inside
the window.
