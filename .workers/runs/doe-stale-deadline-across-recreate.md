# Run evidence — doe-stale-deadline-across-recreate

Executor #18, 2026-07-07. Drafts via `--workload-file` injection on prod.
**FINDING: RED — first product bug of the harness.** Test-reviewer verdict
KEEP (genuine product finding; oracle honest; all harness-artifact
explanations excluded by source + timeline).

## Command

```
sh .workers/workloads/lifecycle.sh doe-stale-deadline
```

## The bug

`finalize_trim` deletes the trim-point, meta, id-mapping, tail, and
fencing keys of a purged stream but NOT its `stream_doe_deadline` keys
(stream_trim.rs:135-146). StreamId is a deterministic hash of
(basin, stream) (stream_id.rs:24-29), so a recreated stream reuses the
dead incarnation's keyspace — including the orphaned armed DOE deadline.
When the stale deadline comes due, the DOE scan fires against the NEW
incarnation and deletes it, even though the new stream's own
delete-on-empty floor (min_age 3600s) is nowhere near elapsed.

**Customer impact:** delete a stream that had delete-on-empty configured,
recreate the same name with a 60-minute empty-floor — the new stream is
silently deleted ~10 minutes later. Wrongful, config-defying stream
deletion; data-plane resource loss with no error surfaced anywhere.

## Mechanism (every link source-cited)

1. inc1 (retention age=1s, DOE min_age=1s) + one append arms a deadline
   at t_arm + 602s: `doe_arm_delay = age + min_age + 600s`
   (streamer.rs:59-63).
2. Delete inc1 → event-triggered purge → `finalize_trim` erases
   meta/tail/mapping/fencing/trim-point but leaves the deadline key
   (stream_trim.rs:135-146). No second deadline confounds:
   `doe_deadline_maybe` is rate-limited by
   DOE_DEADLINE_REFRESH_PERIOD=600s (streamer.rs:557-568) so the
   terminal-trim append arms nothing, and `arm_doe_on_full_trim` skips
   full deletes (stream_trim.rs:67-68).
3. Recreate the name → same StreamId → the stale deadline now points at
   inc2.
4. At the deadline (checked only on the 60s±10% tick — DOE has no event
   trigger), the fire path spawns the streamer itself
   (stream_doe.rs:106-129); the eligibility recheck consults CURRENT
   config but only for min_age **presence** — the value 3600 is never
   compared against the stale cutoff (streamer.rs:457-461, 494-505).
5. Cutoff = deadline − min_age (kv/stream_doe_deadline.rs:17-21). For a
   never-appended inc2 there is no tail key at all (tail written only on
   append, streamer.rs:1057-1060) so `last_tail_write_timestamp =
   TimestampSecs::ZERO` (core.rs:110-111) — maximally below the cutoff.
   Verdict: eligible → terminal trim → inc2 deleted.
6. One-shot nuance: `process_stream_doe` clears the stale keys after
   firing (stream_doe.rs:126), so each stale entry fires at most once —
   the control stream's stale key is consumed silently the same way.

## Trial shape

Stream A: inc1 {age:1, doe.min_age:1} + 1 append, delete (202 ack),
recreate {age:1, doe.min_age:3600}, LEAVE EMPTY (own deadline ~+4210s).
Control stream B: same inc1 shape, inc2 recreated WITHOUT DOE
(min_age None → Ineligible escape, streamer.rs:460). Basin has no
auto-create flags. GET-only probes (safe: cannot bump
last_tail_write_timestamp) through window t_arm+602+66+40.
RED wrongful_delete if A's inc2 404s inside the window; RED
control_survives if B dies; GREEN only if both survive AND a post-window
append to A works.

## Runs (all drafts by injection, prod)

| exploration id | run | seed | purpose | outcome |
|---|---|---|---|---|
| (shakeout v1) | — | — | shakeout | VOID — my bug: DELETE /v1/streams returns 202 (async), check wanted 200/204; fixed |
| nd73hn9zcw85r0vcnnfern09x18a1cxw | — | 2492750010 | shakeout v2 | **RED wrongful_delete** — A last alive +645s, GONE by +673s (window [597,668]); B alive through +703s |
| nd72c44ybyknm6yv4c1h575x0x8a1atw | 01KWWE4WK9A90ZB8EA3Q69SATY | 2492750010 | same-seed replay confirm | **RED wrongful_delete** — A GONE at +617s probe (window [597,668]); B alive through +703s |
| nd743p947k7tm6f7sfskrh99a58a15df | 01KWWE4X36X1G5BBY6QAPZF5VX | 555000333 | fresh-seed confirm | **RED wrongful_delete** — A GONE at +628s probe (window [600,668]); B alive through +706s |

3/3 in-window REDs. Exact 404 body (identical shape in all three):
`{"code":"stream_not_found","message":"stream `doe-a-…` in basin
`lifecycle-wl-01` not found"}`. Detection lands at probe granularity
(28-33s) over a wall-clock ±10% tick, so the death instant varies within
the window across runs of the same seed; the window itself held every
time.

## Timing attribution (reviewer-verified sound)

Deadline = t_arm + 602s; first eligible tick in (602, 668] after t_arm.
Observed deaths at +617/+628/(645,673] — all inside the band. No other
stream-deleting mechanism operates there: retention is slatedb TTL on
record keys only (`Ttl::ExpireAfter` in db_submit_append,
streamer.rs:1021-1037 — meta/tail/mapping never TTL'd, and inc2-A had
zero appends so zero record keys); the acked-Ensure-erased corridor would
kill inc2 within ~1-66s of recreate, not +617s (trim-point key consumed
at the original finalize, stream_trim.rs:135); auto-create is excluded by
the persistent 404 itself. The control asymmetry is the clincher: B
(min_age None → Ineligible, streamer.rs:460) survived every trial while A
(presence-only check, value never consulted) died — exactly the
source-predicted discrimination.

## Test-reviewer verdict (foreground gate)

**KEEP.** Q1 harness-artifact explanations all dead (see above). Q2
timing attribution sound; one precision fix applied to this record
(never-appended inc2 → no tail key → timestamp ZERO, not create_ts). Q3
green path falsifiable (a fixed server → both survive → post-window
append → GREEN). Q4 evidence gaps recommendation-level, NOT redo: the
black-box case stands without server logs; server.log excerpt not
retrievable post-run (guest /tmp; the mode did not dump on RED — noted as
hardening below). Q5 selftest superseded — the real RED exercised the
identical `gone[a] → fail(1, wrongful_delete)` path; `control_survives`
and `purge_liveness` red legs remain unexercised (honest note).

Non-blocking hardenings for future lifecycle trials (do not re-run for
these alone): dump server.log tail on RED/VOID exit; wipe/assert-empty
DATA_DIR at start (exist_ok=True reuses a dirty root; server.log opens
"ab" and accumulates); second GONE probe before verdict; one retry on the
post-window append.

## Interpretation

Deterministic, replayable, control-discriminated wrongful stream
deletion. Fix directions visible in source: (a) `finalize_trim` should
delete `stream_doe_deadline` keys alongside the other five families,
and/or (b) the DOE recheck should compare the CURRENT min_age value
against emptiness duration rather than presence-only. Covers ONLY the
across-recreate corridor of backlog row 400; the same-incarnation
disarm-reconfigure path (streams.rs:304-306) remains open there.
~4 draft workloads this arm (~52 min of trial wall-clock). Official
publication replays seed 2492750010 at wrap-up.
