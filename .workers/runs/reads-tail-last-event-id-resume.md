# Run evidence — reads-tail-last-event-id-resume

Executor #14, 2026-07-06. Drafts via `--workload-file` injection on prod.

## Command

```
python3 .workers/workloads/reads_tail.py last-event-id-resume
```

Same kill schedule as across-restart (shared `pre_kill_phase`: prime,
follow from 0, 3-thread pipelined writer pool, arm-scaled kill point,
SIGKILL inside a sampled lag>0 window), but the resume leg reconnects the
way a real SSE client does: Last-Event-Id header ("seq,count,bytes",
committed only when its event fully dispatched — WHATWG EventSource
semantics) and NO seq_num param, exercising `apply_last_event_id`
(handlers/v1/records.rs:49-66): start = seq+1, count/bytes budgets
decremented by consumed. Seed-selected budget arm re-sends a count limit;
plain arm follows unlimited. 3-8 fresh post-restart appends guarantee
records beyond the boundary (socket-buffer drain after SIGKILL routinely
lets observed catch the durable tail — shakeout v1 was 4/4 vacuous
without this).

## Oracle

header_describes_delivery (the server's own event id must equal the
observed witness: last delivered seq + session count — RED, never
"harness bug" void); header_accepted (server persistently rejecting its
own emitted header while proven serving is a parse regression — RED);
follow_wellformed (resume starts exactly at observed[-1]+1 — computed
from OUR log, never the header — and tiles gap/dup-free with contents
equal to the Remote readback); budget_resume (session ends [DONE] after
exactly limit−consumed records; want ≤ available−1 keeps the
over-delivery disambiguator live); plus the inherited observed_survive /
readback_dense / nonvacuous (floor 50 + lag>0 at kill) family.

## Runs (all drafts by injection, prod)

| exploration id | depth | purpose | outcome |
|---|---|---|---|
| nd7c5bh5099gvvs8ge9cmse1e98a0vhw | 4 | shakeout v1 | 4/4 VOID — observed caught tail post-drain, available=0 → led to post-restart appends |
| nd79qq5cxchfr95mfzhqfnvn7n8a0ksw | 4 | shakeout v2 | 4/4 green — budget+plain, boundary exact, [DONE] after exactly limit−consumed |
| nd73gccsjx37zw4wy7kgr1de2x8a10dc | 2 | red-proof +1-shift | 2/2 FAILED as required — follow_wellformed "not dense at index 0" (seeds 3934514529 budget, 680046819 plain) |
| nd79hx999y1v5d0x0r8mpygk058a1s41 | 1 | red-proof readback-drop | FAILED as required — seed 1108731994, observed_survive FAIL |
| nd74s6rgt1wd20hhykje3zj43s8a0hd3 | 10 | green sweep | 9/10 green + 1 VOID (2s-arm lag window, documented anti-vacuity void) |
| nd79d7r1gkznjkkch201xstkfn8a14j0 | 1 | red-proof bad-id v1 | VOID (2s-arm lag window before the check) — rerun |
| nd7cktxkegj8tcf656crd3hbpx8a0h38 | 2 | red-proof bad-id v2 | FAILED as required — seed 3761786860, header_describes_delivery FAIL, exit 1 |
| nd72dzcywtx6amb7w08qvtk2eh8a15dc | 4 | post-REDO confirm | 4/4 green on final code |

## Test-reviewer verdict (foreground gate)

REDO → fixed same-episode → effectively KEEP. Value confirmed high:
restart-facing header parse + seq+1 arithmetic + budget decrement the
param path never executes; originality gate passed (new failure surface,
new budget_resume oracle, new shift selftest). Commit-at-dispatch,
post-restart appends, and the independent boundary witness all endorsed;
pre_kill_phase extraction verified diff-equivalent for the published
across-restart mode. Required fixes applied: (1) server id misdescribing
its own delivery → RED header_describes_delivery, red-proven via
ORACLE_SELFTEST=bad-id (was fail(3) "harness id-commit bug" —
VOID-masking the exact emission bug class this arm exists to catch);
(2) persistent non-2xx on the header-resumed session after the server is
proven serving → RED header_accepted (was "transport" void; a parser
rejecting its own Display output breaks every real resume). Non-gating
suggestion also taken: want capped at available−1. Residual: bytes budget
(records.rs:62) untested — count-only arm.

## Interpretation

apply_last_event_id held across all arms: the resumed session always
started exactly one past the last fully-delivered event, tiled gap- and
duplicate-free with contents identical to the Remote readback, and the
count budget was correctly decremented by the header's consumed count
(sessions ended [DONE] after exactly limit−consumed). The server-emitted
event id described its own delivery exactly in every green trial. 28
draft workloads this arm.
