# Run evidence — reads-tail-slow-follower-lagged

Executor #15, 2026-07-06. Drafts via `--workload-file` injection on prod.

## Command

```
python3 .workers/workloads/reads_tail.py slow-follower-lagged
```

No kill: the follower stalls its socket (pause between readlines) while a
writer bursts 70-110 × 128-256 KiB appends, overflowing the per-stream
broadcast channel (FOLLOWER_MAX_LAG=25 batches, streamer.rs:266,
mod.rs:27) so the read session hits RecvError::Lagged and silently
re-enters the catch-up scan (read.rs:219-222). Client SO_RCVBUF capped at
32 KiB before connect — with default buffers ~5 MB of stall bytes were
absorbed and Lagged never fired. 1-3 stalls per trial, seed-driven; live
follow re-established between stalls via nudge appends.

## Oracle

follow_wellformed (delivered stream gap/dup-free, in order, dense from 0
across every handoff; per-stall drains RED on provable gaps via
partial_delivery_verdict); observed == readback (norm_body sha256
equality on both sides for >1 KiB bodies); readback_dense;
len(observed) == tail (delivery must not stop short); nonvacuous witness
= a batch event MISSING `tail` (catch-up, read.rs:169-182, JSON omits
None) arriving AFTER follow batches that carried it (read.rs:209-212) —
the only in-loop path back to catch-up is the Lagged arm. Verify runs
BEFORE the vacuity exit.

## Runs (all drafts by injection, prod)

| exploration id | depth | purpose | outcome |
|---|---|---|---|
| nd7a0f5qtdj1wesvgq6pxex17x8a18jb | 4 | shakeout v1 | 4/4 VOID — default buffers absorbed the stall (batch count == append count, all follow-path); led to rcvbuf cap + 70-110 × 128-256 KiB bursts |
| nd736afd7ncjat3yvep3dryjjx8a1q57 | 4 | shakeout v2 | 4/4 green — witnesses 16-71 catch-up batches; coalescing visible (105 appends -> 43 batches) |
| nd7ef5pthmwbz37vyqtvkqv3b18a1243 | 1 | red-proof gap | FAILED as required — seed 3348524698, follow_wellformed "not dense from 0 at index 5", exit 1 |
| nd7b8g8bvvwjff1vpn3k81h4j18a1j2p | 1 | red-proof readback-drop | FAILED as required — seed 775902020, observed_survive FAIL, exit 1 |
| nd75mfsza3wndpve0k06gkzj0h8a06j0 | 10 | green sweep | 10/10 green, every trial witnessed (9-71 catch-up batches after handoff, 1-3 stalls) |

## Test-reviewer verdict (foreground gate)

KEEP, no required action. Witness verified sound against the SUT source
(tail Some only on follow batches; the only in-loop return to catch-up
after a tail-ful batch is Lagged); verify-before-vacuity real; anti-
vacuity empirically honest (v1's 4/4 VOID drove a physical redesign, not
a witness downgrade); norm_body audit clean (identical both sides; no
published oracle weakened — all other modes' payloads <1024 B pass
verbatim; the one asymmetric comparison fails RED-safe). Originality:
new failure surface (broadcast overflow, no kill), new adversarial class
(socket stall + TCP window collapse), new witness channel (has_tail
batch metadata), new data shape (large-record SSE framing). Residual for
a future arm: overlap the burst with unpause so the catch-up scan races
in-flight appends — could reach the empty-scan skip branch
(read.rs:183-185) itself; this green covers handoff resume over a
quiescent durable range. Cosmetic docstring drift fixed same-episode.

## Interpretation

The transparent Lagged handoff never corrupted the stream: across 14
green trials (shakeout + sweep) with 1-3 forced overflows each, delivery
stayed gap-free, duplicate-free, in order, drained exactly to tail, and
matched the Remote readback content-exactly (digest-compared for large
bodies). 20 draft workloads this arm.
