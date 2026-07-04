# Run evidence — acked-appends-baseline

- Official exploration: `nd722qkpc6sgah6k5t19jfkpg589x2hr` (2026-07-04, depth 3, all green)
  - workloads: 01KWPE8RVJ6JBNM816C8XRWWHC, 01KWPE8RVJ2EJ1FFR24PJ9T9JF, 01KWPE8RVJBTMX5RM3ZC7P8A49
  - page: Durability / Acked appends survive restart — pass @ f2281bf
- Oracle proven non-vacuous first: draft 01KWPE4T98Z9D3SGE0KJZ2B5N0 with
  ORACLE_SELFTEST=1 went RED (dense-prefix violation detected, exit 1).
- Draft bring-up: 01KWPE3H6VGN3QM9TM957TW6JN green (40/40 acked, verified
  across graceful restart).
- Guest reality: /workspace read-only (state under /tmp); python3 + wget +
  busybox present, no curl/jq; SIGTERM on lite exits cleanly.

# Run evidence — acked-appends-kill9-mid-stream

- Official exploration: `nd7ac90f6g2yhyqkf8yktmyf3d89xzzw` (2026-07-04, depth 10,
  10/10 green). Seeds swept kill delay (20-420 appends) and SL8_FLUSH_INTERVAL
  arms (default 5ms | 500ms | 2s). Acked records survived SIGKILL in every
  trial, including 2s-flush arms with hundreds of acked-then-killed records —
  the durability gate (ack after durable_seq) holds under real process death.
- Draft bring-up: nd7c9zcv70c2cd522hfyzggxh589wpfz (seed 2009485763, 2s arm,
  260 acked verified, green).

# 2026-07-04 (later) — INVARIANT protocol re-publish

Workload now emits one `INVARIANT <id> <name> PASS|FAIL <summary>` line per
oracle clause (6 clauses). Verified: runtime parses them into structured
`invariants` on the workload record; self-test draft shows
`hasInvariantViolation: true` with the violated clause (dense_prefix).
Officials re-fired with the new workload (idempotent upsert):
- baseline: nd7a2x0yk3jw2zcjqbdwsbp14589wp0q — 3/3 green
- kill9:    nd70mmyfffbhy56swm643s8vwx89x2qe — 10/10 green, e.g. 396 acked
  verified after SIGKILL
