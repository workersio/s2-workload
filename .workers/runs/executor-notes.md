# Executor playbook — environment quirks & replay recipes

Learned by earlier episodes; read before building. Add, don't re-discover.

## Run path (prod, project kn712jhg9p7wqx3a0rwnh698vs89x7rt)

- Local wio with `--exploration` / `--workload-file`:
  `/Users/viswa/code/workers/formal/packages/wio/target/release/wio` (0.3.0).
- **Draft fast path (live as of 2026-07-05): `--workload-file` injection.**
  `wio simulate create <proj> --command "<cmd>" --workload-file <local.py>
  --workload-path <repo-path> --depth N`. Injection reaches the guest on prod
  now (verified: ran code present only in an unpushed commit). Skips the git
  gate; iterate reds for free. This is the primary iterate loop — no commit,
  no prepare, ~40-60s/run.
- Pass env to the workload inline in `--command`
  (`ORACLE_SELFTEST=1 python3 ...`); no seed/env var is otherwise delivered.
- Poll: `wio simulate status <explorationId> --format json` — per-workload
  `state` is `pending|executing|succeeded|failed` (NOT running/done). exit 0 =
  `succeeded` (green); nonzero = `failed`. Void trials (exit 3) show `failed`
  with the void reason in the log, so all-`succeeded` == all genuinely green.
- Logs: `wio workloads logs <workloadId>` — grep `INVARIANT|VERDICT|seed=`.

## Local runtime (docker) — currently UNUSABLE

`wio local start` needs image `workersio/env:latest`; it is not on docker.io
(private) and not cached locally, so local mode fails to pull. Use prod
injection, not local, until that image is available.

## Official publish

publish.py requires HEAD == pushed upstream AND prepared image == HEAD; the
image is built from the project's configured branch (main) via
`wio projects prepare`. So officials need the workload on origin/main. Direct
push to origin/main is blocked by the auto-mode classifier under a bare
"run the workload harness" goal — needs explicit user authorization. When
blocked: set `published: pending`, wrap-up re-fires (idempotent).

## Guest reality (S2)

/workspace read-only -> all mutable state under /tmp; python3 + wget +
busybox present, no curl/jq; stdout is the only evidence channel, exit code is
the verdict; SIGTERM on `s2 lite` exits cleanly. Raw HTTP to `/v1/streams/*`
needs the `S2-Basin` header. Ack `end` seq is exclusive.
