# Run evidence — fencing-fence-ack-straddles-kill

Executor #13, 2026-07-06. Drafts via `--workload-file` injection on prod.

## Command

```
sh .workers/workloads/fencing.sh fence-ack-straddles-kill
```

T1 established AND settled first (fence acked + bogus-token 412 + T1 append
accepted — a missing token recovers to FencingToken::default(), so an
unsettled T1 conflates "T1 governs" with "default governs"). Then the fence
to T2 races SIGKILL: styles in-flight-sweep (2× weighted; delay swept
[0, flush-window) — a wider delay never lands before the ack), just-acked,
acked+settled, across SL8_FLUSH_INTERVAL arms. Post-restart T1/T2/tokenless
probes ×2 rounds.

## Oracle

t1_settled; fence_durable (acked fence must recover to T2); governs_xor
(unacked fence: exactly one token governs — both-rejected = default-token
conflation, both-accepted = fencing broken); governs_consistent (identical
probe outcomes across rounds); fence_record (T2 fence record in readback
iff T2 governs); content_exact (dense; exactly the accepted bodies,
indeterminate unacked fence + unresolved probe attempts at most once);
restart_serves (post-kill recovery failure is RED, not void). Probes
resolve only to ack or explicit 412 — transients retry bounded then VOID.

## Runs (all drafts by injection, prod)

| exploration id | depth | purpose | outcome |
|---|---|---|---|
| nd7ad9cer8zp587dksq58fvdd58a09zg | 4 | shakeout v1 | 4/4 green, but ALL fences acked — delay overshot ack latency |
| nd798yrsk1vkhtarxznv39fheh8a0bmh | 6 | shakeout v2 (delay ≤ window) | 6/6 green, still all acked (no style-0 seeds drawn) |
| nd7acgsehxr12c7t8beb5ajchh8a134k | 8 | shakeout v3 (style 0 ×2 weight) | 8/8 green — 3 UNACKED (→T1, consistent, fence absent) + 5 ACKED (→T2) |
| nd76sk3sveh2x7fvj2pe546ak18a1vdc | 1 | red-proof | FAILED as required — seed 4210227959, forged T1 probes → fence_durable FAIL |
| nd7dhyftys4k4rpyve85a0h65n8a0ypr | 10 | green sweep | 10/10 green, 4 UNACKED + 6 ACKED, all arms |
| nd74c6he5m5f345jvh7byad8kn8a0e4w | 4 | post-REDO confirm | 4/4 green |
| nd702wdcq55tv2e86faf0wvbvx8a1feh | 1 | post-REDO red-proof | FAILED as required — seed 1961397883, fence_durable FAIL, exit 1 |

## Test-reviewer verdict (foreground gate)

REDO → fixed same-episode → effectively KEEP. Confirmed distinct fault
model (the fence write's own durability window; fence record + token KV
share one WriteBatch, streamer.rs:1039-1044/1061-1065; missing token →
default, core.rs:116) vs stale-across-restart (settled-token recovery
only). Required fixes applied: (1) post-kill restart/read failure now RED
restart_serves (recovery choking on the crash-consistent half-state is the
most plausible real manifestation — was VOID-masked; also applied to the
published stale-across-restart mode's post-kill sites); (2) probe
rejections classified by code — only 412 counts as "token governs";
transients retry then VOID; unresolved attempt bodies tolerated
at-most-once in content_exact. Residual (logged per-trial, not a defect):
the durable-but-unacked fence corner (T2-governs-unacked) has not been
observed — all UNACKED outcomes resolved to T1.

## Interpretation

The fence-to-T2 write straddling SIGKILL never corrupted token state: acked
fences always recovered to T2 (T1 412-rejected), unacked fences always
resolved cleanly to T1 with the fence record absent — never a default-token
conflation, never flapping, content exact across all arms. 33 draft
workloads this arm.
