# Run evidence — recreated-stream-fresh: the two green rungs

Executor #20, 2026-07-07. Two rungs of `stream-delete-recreate-resurrection`
that came back **GREEN** and stayed green under a hardened oracle:
`delete-recreate-fresh-identity` (no kill) and
`delete-recreate-kill-mid-purge` (SIGKILL mid-purge). Test-reviewer KEEP ×2
with 5 hardenings, applied and re-confirmed green post-fix. These greens are
scoped: they do **not** cover the DOE-deadline corridor, which is RED and
carried by `doe-stale-deadline-across-recreate` (FINDING #1).

## Commands

```
sh .workers/workloads/lifecycle.sh fresh-identity
sh .workers/workloads/lifecycle.sh kill-mid-purge
```

## What the rungs assert

**fresh-identity (no kill).** inc1 gets N records + fence T1 + a partial
trim, then delete → gate-poll → recreate the same name. Oracle on inc2:
tail 0; reads at every old seq return 200-empty or the **416 empty class**
(`{"tail":{...}}`), never an old body — double-probed (initial + end of
trial); timestamp read resolves against the empty index; a never-valid-token
append 412-discloses `""` (NOT T1); a tokenless append is accepted; a fresh
fence to T2 works; appends sequence from 0. Any old body/token/trim behavior
in inc2 = RED resurrection.

**kill-mid-purge (crash arm).** inc1 gets ~25k records (multi-batch purge
window) + fence T1. Delete, then SIGKILL at a seed-chosen offset (two-band
racing delays: 1/3 in [0,3s] targeting the trim-durability window, the rest
0-20s; plus post-ack controls). Restart on the same root:
- **restart_serves**: the recovered process must serve
  (`assert_no_records_following_tail` is process-aborting, core.rs:165-196 —
  a crash on recovery IS the finding).
- **purge_liveness**: interrupted purge has NO event re-trigger — it resumes
  on the 60s±10% tick; recreate gate must lift within 300s.
- **resurrection**: the recreated inc2 must pass the full fresh-identity
  oracle.
Each trial classifies its landed seam by the create-probe.

## Runs — all GREEN

| exploration id | run | seed | rung | outcome |
|---|---|---|---|---|
| nd76xhy2939h15fatskm54g1nd8a2j6e | 01KWXFAPNM8P145FNRGSRWSPFT | 88001 | fresh-identity | **GREEN** — oracle clean, gate lifted +0.0s |
| nd70w53rfhpvf7c6hdjeed090s8a2r76 | 01KWXFAQFRE9XDFFV99FP551D5 | 90000 | kill-mid-purge | **GREEN** seam=DIVIDED — restart_serves +16.0s, gate +147.0s, oracle clean |
| nd74gpsbve83y2z6er3we659eh8a3k5z | 01KWXFARH7NGTG03BB8KAJK8A0 | 88817 | kill-mid-purge | **GREEN** seam=PRE-FINALIZE — restart_serves +23.7s, gate +127.3s, oracle clean |

Prior (pre-hardening) shakeout + sweep, same rungs: fresh-identity 1 shakeout
+ 5/5 sweep GREEN + ORACLE_SELFTEST (SEED=77002) RED (leak forge bites);
kill-mid-purge 4 shakeout GREENs (one per seam class) + 9/10 sweep GREEN
(4 DIVIDED + 5 PRE-FINALIZE) + fixed-bug replay GREEN.

## Seam distribution (kill-mid-purge, across all runs)

| seam | create-probe signature | witnessed |
|---|---|---|
| DELETE-UNHAPPENED | 409 resource_already_exists + append acks | yes (incl. 3963731212) |
| DIVIDED | 409 resource_already_exists + append 409 stream_deletion_pending | yes (SEED=90000) |
| PRE-FINALIZE | 409 stream_deletion_pending | yes (SEED=88817) |
| POST-FINALIZE | 201 created | yes (shakeout) |

Anti-vacuity satisfied: no sweep was all-post-finalize; ≥1 pre-finalize per
sweep, else the run VOIDs by design. Zero VOIDs in the recorded runs.

## Notable green facts (recorded from the green logs)

- **DELETE ack latency**: a 25k-record stream's DELETE acks in **26-51s** of
  virtual time in the deterministic sim (3 purge batches, separately awaited
  writes).
- **Interrupted-purge resumption**: after a mid-purge crash there is no event
  re-trigger; the gate lifts ~2 ticks out (**+106-148s** observed; +147/+127s
  in the hardened confirms).
- **Legit empty-read class is 416** with a `{"tail":{...}}` body (not 404,
  not a 200 with zero records only) — the oracle now accepts exactly 200/416
  and treats any other status on an old-seq probe as a leak-shaped RED.

## ORACLE_SELFTEST proof

`ORACLE_SELFTEST=1` forges an old body at the first probed seq → the oracle
fires RED resurrection (proof it bites). Recorded seed 77002.

## Defect ledger (my bugs, all fixed)

- `set -- $spec` in zsh does not word-split → guest ran a malformed command
  (exit 127) ×4. Fixed by explicit per-line commands.
- Virtual-time inflation → TimeoutError cascade: delete_stream 15s→120s,
  get_tail/read_from/timestamp 10-20s→120s, append_many 60→120s, gate-poll
  create wrapped to treat OSError as "still gated". One re-delete
  (DELETE-UNHAPPENED branch) still at default 15s → fixed to 120s; **replay
  SEED=3963731212 → GREEN seam=DELETE-UNHAPPENED** (run
  01KWWH5NZX34T3YCVRJRHX08K7).

## Hardenings applied (test-reviewer's 5, then re-confirmed green)

1. Crash-during-oracle now labeled **RED restart_serves** + server.log dump
   (was an unlabeled traceback exit): `_fresh_identity_oracle` wrapped,
   OSError → `fail(1, …, inv=("restart_serves","serves-through-oracle"))`.
2. Old-seq reads accept only **200/416**; anything else retries once then RED
   resurrection (a leak presenting as a server error must not pass).
3. Wrong-token probe retries once then **RED** (was VOID) — governance
   contract (412 + `""` disclosure) enforced.
4. DELETE-UNHAPPENED append-probe body carries the `A{seed}-` prefix (so a
   resurfaced probe body is attributable).
5. Two-band racing delays (1/3 in [0,3s]); DIVIDED classifier requires an
   explicit `stream_deletion_pending` code else VOID; `server.poll()` death
   check inside the gate-poll loop.

All three post-fix confirms above ran with these in place → GREEN.

## Scope caveat (important)

These greens certify the **records/tail/token/trim/liveness** freshness
surface and crash-recovery. They do **not** certify **DOE-deadline**
freshness — the build constraint explicitly forbids appending to inc2 during
the DOE window, and this rung keeps inc2 short-lived. The DOE corridor is a
confirmed product bug (FINDING #1), carried by
`doe-stale-deadline-across-recreate` (replay SEED=2492750010). A green here
is not a green there.

## Interpretation

The purge pipeline's crash-recovery and the recreate freshness contract hold
across all four kill seams; the one real leak in this area is the DOE-deadline
key survival, isolated to its own rung. This rung is a healthy regression
floor for the non-DOE surface. Official publication replays the recorded
seeds at wrap-up.
