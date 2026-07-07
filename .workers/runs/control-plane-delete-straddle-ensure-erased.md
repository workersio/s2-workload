# Run evidence — control-plane-delete-straddle-ensure-erased

Executor #19, 2026-07-07. Drafts via `--workload-file` injection on prod.
**FINDING #2: RED — acked control-plane op silently un-happens.**
Test-reviewer verdict KEEP (genuine; all artifact hypotheses excluded
from source + timeline).

## Command

```
sh .workers/workloads/control_plane.sh delete-straddle
```

## The bug

Stream delete is two-phase and non-atomic: terminal trim through the
streamer (streams.rs:338-358), THEN `mark_stream_deleted` in a separate
txn (:360-379). SIGKILL between the phases leaves the divided state:
`trim_point == ..MAX` durable, `deleted_at == None`. In that state the
provision gate — which checks ONLY `meta.deleted_at`
(streams.rs:106-112), not the durably-persisted trim point — lets a
PUT Ensure through: it commits fresh meta and **200-acks with
`deleted_at: None` in the body** (the `(Some(existing), Ensure)` arm,
streams.rs:133-151, :221-226). ~64s later the recovered purge's first
tick runs `finalize_trim`, guarded only by trim-point equality
(stream_trim.rs:123-134), and erases the just-acked meta, id-mapping,
and tail (:136-146) with no re-check of `deleted_at` or meta freshness.

**Customer impact:** a client gets 200 on `PUT /v1/streams/{name}`,
reads its config back, and the stream 404s a minute later with no
client-visible cause — an acked metadata op silently un-happening,
exactly the promise's claim violated verbatim.

Also logged (spec clause a): the divided state itself is incoherent —
GET serves the stream as live (200, config body) while appends refuse
409 `stream_deletion_pending` (core.rs:118-120).

## Runs (all drafts by injection, prod) — 5/5 RED

| exploration id | run | seed | arm | outcome |
|---|---|---|---|---|
| (shakeout v1, 3 runs) | — | 999999/1000000/1000001 | all | 1 false-VOID + 2 false-GREEN — my bugs: used POST (CreateOnly, 409 already-exists — NOT the gate) and classified GET-409 deletion_pending "unclassifiable"; both fixed |
| nd77c6bwpmh6ysnjp5bs70a5t98a11a9 | 01KWWFDJXA113CSCZ8PADJNXF2 | 999999 | default (5ms) | **RED** — straddle t03; erased at **+64s** |
| nd78dbht4c0m1hmqwe27a5mqns8a0krp | 01KWWFDKDFA2XY2YF48HWJST2M | 1000000 | 500ms | **RED** — straddle t02 @925ms; erased at **+72s** |
| nd708rsmkfnxtcas320c916tbh8a11xe | 01KWWFDKX5YTMKEGW0AWS8AJT0 | 1000001 | 2s | **RED** — straddle t02; erased at **+64s** |
| nd79bqvaqzcwar86vjfhyt27mx8a0yr3 | 01KWWFHWZH97C23YWQW6AX6CYZ | 1000000 (replay) | 500ms | **RED** — same shape: ladder [(300ms EARLY), (925ms STRADDLE)], ensure ACKED 200 marker age=7777, erased at **+64s** (double-probed) |
| nd7382tv4vamyxfr9tmgkwvjcs8a01fa | 01KWWFHXRFBS7FB524143SK1S2 | 424242 (fresh) | — | **RED** — straddle t03; erased at **+64s** |

Replay-run timeline (seed 1000000, 500ms arm), representative:
t1 SIGKILL +300ms into DELETE → seam EARLY (delete un-happened; GET 200
+ append 200). t2 SIGKILL +925ms → STRADDLE: GET 200 live config body,
append 409 `{"code":"stream_deletion_pending",...}`; PUT Ensure
**ACKED 200** with marker retention age=7777; watch: GONE at **+64s**
after the ack — GET 404 `{"code":"stream_not_found",...}`, double-probed
2s apart. Erasure instants (+64/+72/+64/+64/+64s) sit exactly in the
first jittered purge tick (54-66s — no startup tick, bgtasks/mod.rs:
20-27, :100-115; the event trigger died with the killed process).

## Exclusions (reviewer-verified)

- **"Just the original DELETE completing late":** the server's own model
  refuses provisioning in deletion-pending (streams.rs:106-112) and a
  delete effect must transit the observable marked state
  (mark_stream_deleted, :373-377) — the purge path skips it. No coherent
  linearization of {GET 200 live, append 409 pending, Ensure 200
  Updated, GET 404} exists.
- **"Created on already-finalized meta":** Created maps to 201 + header
  `created`; Updated/Noop map to 200 (handlers/v1/streams.rs:242-246) —
  runs recorded 200, so meta existed at ack. Timing: finalize cannot
  have run pre-Ensure (no startup tick; classification + PUT completed
  seconds after restart). Guard consumption: finalize deletes the
  trim-point key in its own txn (stream_trim.rs:135) — one-shot.
- **Auto-create resurrection:** basin owns defaults
  (create_stream_on_append=false, common/src/config.rs:324);
  corroborated by LATE trials' appends refusing.
- **Flake:** 5/5 across three flush arms and three seeds,
  tick-consistent erasure instants.

## Test-reviewer verdict (foreground gate)

**KEEP.** Oracle honest under both fix shapes: Fix A (gate also refuses
on trim==MAX) → ensure 409 → wedge-resolution watch → first tick
purges → recreate+append asserted → GREEN; Fix B (finalize re-checks
deleted_at) → ack survives the 180s watch → GREEN (post-watch append
wedge logged, not RED — correct scoping). LATE-seam append clause
sound (valid while the workload owns basin config). ORACLE_SELFTEST
does forge the acked_ensure_erased path; `straddle_wedged` and
`restart_serves` legs unforged — note-level, but ADD A WEDGE-BRANCH
FORGE before trusting this workload as the post-fix regression floor
(under Fix A the wedge branch becomes the primary path). Hardening
note: classify_get OSError in the watch loop unhandled (server crash
mid-watch would traceback-exit without a clean invariant line).

## Interpretation

Deterministic (mechanism-level: bisection lands the straddle by trial
2-3 on every arm; replay contract is "mechanism reproduces", not
bit-identical trial count). Fix directions visible in source: (A) the
provision gate should treat a durable `trim_point == MAX` as
deletion-pending, and/or (B) `finalize_trim` should re-check
`deleted_at`/meta freshness before erasing — and must also resolve the
orphan trim point or the name stays append-wedged. Related open seam
(same divided state, different victim): the GET-live/append-pending
incoherence is now witnessed and logged per spec. ~8 draft workloads
this arm. Official publication replays seed 1000000 at wrap-up.
