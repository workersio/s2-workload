# Run evidence — acked-appends-kill-during-recovery

Recovery-atomicity arm of the acked-appends-survive-restart promise. kill9
kills the *serving* process once; this arm kills the *recovering* process,
repeatedly, at the point where s2-lite's own lazy per-stream recovery runs
(start_streamer -> load_persisted_stream_tail -> assert_no_records_following_tail,
core.rs:82/144/165), forced by the first post-restart stream-tail access.

Target: prepared image at a88afdc + `.workers/workloads/acked_appends.py` at
c865b2d (kill-during-recovery mode) injected via `--workload-file`. Project
kn712jhg9p7wqx3a0rwnh698vs89x7rt (prod), branch main.

## Red-proof (oracle can go red) — GATE PASSED

- Draft `nd7eg0yedp6bmee03pb9c9erdd89zzjn`, depth 1, workload
  `01KWR7JJBAB3AKG40D6YTA61TT`, command
  `ORACLE_SELFTEST=1 python3 .workers/workloads/acked_appends.py kill-during-recovery`.
- seed 4021235855, 2s flush arm, 664 attempted / 663 acked, in-flight at kill.
- Anti-vacuous witness fired: `recovery interrupted mid-window 4 time(s)` —
  4 SIGKILLs landed during first-access recovery (every probe hit
  ConnectionResetError before a 200, i.e. mid-rebuild).
- ORACLE_SELFTEST dropped the first acked record from read-back; oracle caught
  it: `INVARIANT dense_prefix gapless-below-tail FAIL`, `VERDICT: RED`, exit 1.
  Proves the diff is load-bearing before trusting any green.

## Green bring-up — 10/10 GREEN

- Draft `nd71w6wkxmtkw8cz8w0qm3680x89ywce`, depth 10, no self-test. All 10
  seeds `succeeded` (exit 0). None vacuous (a void trial exits 3 -> `failed`).
- Representative: workload `01KWR7N8XS0R0NX0SG1ERE4JQ9`, seed 925258047,
  default (5ms) flush arm, 212 attempted / 211 acked, `recovery interrupted
  mid-window 2 time(s)`. All six invariants PASS:
  recovery_interrupted, tail_bound, dense_prefix, no_phantoms, at_most_once,
  acked_survive, acked_order (tail 211, 211/211 acked present in ack order).

## Interpretation

s2-lite's tail-rebuild path survives being SIGKILLed mid-recovery: every
record acked before SIGKILL #1 is present exactly once, in ack order, dense
below the recovered tail, after crashing the recovering process 2-4 times and
a final clean restart. The `assert_no_records_following_tail` guard did not
misfire on a legitimately-recovered stream, and no double-apply duplicated
records. GREEN — the promise holds on this arm.

## Publication status: PENDING

Official publish requires c865b2d (the kill-during-recovery workload) on
origin/main so `wio projects prepare` can pin the image to it. Direct push to
origin/main is denied by the auto-mode classifier under the current
"run the workload harness" authorization (scoped out as a default-branch push).
`published: pending`; wrap-up re-fires publish.py once the push is authorized
(or the workload otherwise lands on main). The green verdict + red-proof above
are the durable evidence; publication is replay-confirmation of the same seeds.

## Map-reality update

**Worker-side `--workload-file` injection now delivers on prod.** Both drafts
above ran injected code (kill-during-recovery mode is only in c865b2d, absent
from the a88afdc image) and executed correctly. The earlier note that injected
files "do not reach the guest on prod" is superseded — the draft fast path
(commit-free `--workload-file`) is live; drafts no longer need
commit -> prepare -> run. Officials still require the committed image (publish.py
does not inject, by design — published results stay bound to a pushed commit).
