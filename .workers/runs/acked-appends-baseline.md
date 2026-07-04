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
