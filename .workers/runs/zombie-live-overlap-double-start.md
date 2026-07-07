# Run evidence — zombie-live-overlap-double-start

Executor #16, 2026-07-06. Drafts via `--workload-file` injection on prod.

## Command

```
sh .workers/workloads/zombie_writer.sh live-overlap-double-start
```

No signals: A serves under a live 3-thread writer pool while B starts on
the same `--local-root` mid-traffic, sleeps through its time-based fencing
window (one manifest_poll_interval, server.rs:186-198), takes over, and
writes. A keeps firing through B's entire boot and for a seeded 1-3s past
T (B's first ack). The rung strategy-critic required instead of a 2-rung
certification: both older modes freeze the prior instance during the
successor's boot, so the elapsed-time fencing assumption was never
contested by a live writer until this.

## Oracle

Shared persist-time-boundary family (no_zombie_persisted /
b_acked_present / stream_content) with two mode-scoped tightenings from
the test-reviewer REDO, fixed same-episode:
- NO truncation allowance: every A-acked record must appear in read-back
  (no freeze exists to excuse a hole — a missing durable ack is
  acked-data loss across live takeover; was silently green-lit before).
- successor_available: B failing to serve on the live root (2 attempts)
  or persistently refusing appends (3-retry bound) is RED, not void — a
  fencing-sleep regression would present exactly as successor
  dysfunction; on mid-schedule refusal readback+verify still run first so
  a data red outranks.
Anti-vacuity (conjunctive): >=1 A round-trip fully inside B's boot window
(spawn -> first check-tail 200) AND >=1 A attempt sent after T, both with
an HTTP-level response. Verify runs before the vacuity exit.

## Runs (all drafts by injection, prod)

| exploration id | depth | purpose | outcome |
|---|---|---|---|
| nd7131887bhhs35hk8m22p11p58a02hq | 4 | shakeout | 4/4 GREEN first pass — witnesses 69-76 boot-window / 104-135 post-T per trial |
| nd728vnm0wysjw0hrqqwtjj77x8a1qmb | 2 | red-proof (ORACLE_SELFTEST relabel) | 2/2 RED as required — seeds 4095698169, 813245199 -> no_zombie_persisted FAIL, exit 1 |
| nd71ggcsn58jg65edhqs9kktzd8a0x88 | 10 | green sweep | 10/10 GREEN, all witnessed (69-80 boot-window, 76-151 post-T), zero A-acks after T |
| nd75z3nwechmrh278evxsqkwz98a0aas | 3 | post-REDO confirm | 3/3 GREEN, successor_available PASS emitted |
| nd7ah1zn37z0xkjg93atcpe11d8a0e5z | 1 | post-REDO red-proof | RED as required — seed 4035185551 |

## Test-reviewer verdict (foreground gate)

REDO -> fixed same-episode -> revalidated. Findings: (1) suffix-truncation
allowance in shared verify() masked an acked-data-loss red this mode can
uniquely produce (B recovering less than a live A durably acked) —
mode-scoped `allow_pre_truncation=False`; (2) successor dysfunction
VOIDed, masking the most plausible regression (fencing sleep
removed/shortened) — RED successor_available, mirroring double-kill's
recovery_available; (3) doc-only: boundary measurement blur windows
recorded in the promise Oracle section ("LATE ACK (allowed)" lines are
the triage hook). Reviewer confirmed the mode is a genuine new failure
surface (live writer pool through boot; new anti-vacuity pair), not a
wrapper; existing sweep evidence stands for the unchanged invariants.
Recommendation (not gating): pass explicit distinct SEEDs per trial —
/dev/urandom repeats across runs in the deterministic sim (4 of 10 sweep
seeds duplicated the shakeout's).

## Interpretation

Time-based fencing held under live contestation: across 16 green trials
(shakeout + sweep + confirm, ~1500 A-attempts per-run average 220), A's
handle was fenced at B's slatedb DB-open — A's last successful ack lands
~2.7-3.2s BEFORE B's first ack, early inside B's ~3.5-3.9s boot window —
and every subsequent A attempt failed HTTP 500 ("detected newer DB
client" ~90%, "database closed while waiting for durability" for in-flight
at the fence). Zero A-acks after T in every trial; zero zombie persists;
B-acked always exactly-once; readback always dense with no A-acked holes.
The elapsed-time sleep is belt-and-suspenders in practice — the epoch CAS
at DB-open is what actually fences on local FS. 20 draft workloads this
arm.
