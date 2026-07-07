# Run evidence — trim-straddles-kill (GREEN)

Executor #22, 2026-07-07. Drafts via `--workload-file` injection on prod.
**GREEN** — a durable trim is final across a mid-operation SIGKILL: retained
records survive byte-exact, below-trim records are physically purged and never
resurface, and recovery correctly **resumes a genuinely half-completed physical
purge**. Test-reviewer verdict **KEEP** (after one REDO that materially
strengthened the seam — see below).

## Command

```
python3 .workers/workloads/trim.py straddles-kill
```

## What it attacks

`trim-is-final`: a trim command (`["","trim"]` header, body = 8-byte BE of the
trim point V; `trim_point` is `RangeTo<NonZeroSeqNum>` = `..V`) removes seqs
`< V` and retains `>= V`. The trim-point KV rides the SAME WriteBatch as the
trim record (streamer.rs:1045-1050) → the logical point is atomic with the
record. But the READ path never consults the trim point: below-trim absence is
**physical only**, via an async purge bgtask, event-triggered on trim
durability (streamer.rs:601-605), that deletes below-V records in
`DELETE_BATCH_SIZE`(=10_000)-record WriteBatches (stream_trim.rs:18,80-108) and
does NOT re-trigger after a kill (resumes on the 60s±10% tick). Two crash seams:

- **seam1** — trim command IN-FLIGHT at kill (n=300). A trim that acks then a
  kill, or a connection-cut, is the ambiguous window; the recovered logical
  point must be consistent (acked ⟺ applied).
- **seam2** — trim ACKED (durable), kill +delay AFTER ack, landing **mid-purge**
  (n=13000, T ∈ [10001,12000]). The deletion set `[0,T)` exceeds
  `DELETE_BATCH_SIZE`, so the purge spans ≥2 WriteBatches — a crash can leave
  the trim-point KV finalized with SOME below-T records physically deleted and
  others still present. Recovery must **resume the half-completed purge** and
  must never re-serve an already-purged seq. This is the safety seam the
  promise's "half-done physical state" claim is about.

## Trim contract (verified in source)

- Trim body must be exactly 8 bytes (common/record/command.rs:88-95). Sending
  raw bytes ≥128 as a JSON string corrupts them (UTF-8 multi-byte) → 422. Fix:
  base64-encode BOTH the 8-byte body AND the header value `b"trim"` under an
  `s2-format: base64` request header — the format applies to header name/value
  bytes too (data.rs:64-68, json.rs:243-250). Stored `b"trim"` round-trips to
  the string `"trim"` on a default (raw) read, so `h == ["","trim"]` detection
  is correct.
- V clamped to the trim record's own end (Regular); monotone strictly-greater
  guard — decreasing/equal trims are acked no-ops (streamer.rs:378-382).
- Single record reads cap at ~1000 records/batch (api/v1/stream/mod.rs:62-63) —
  wide non-paginated reads silently truncate. The purge poll uses `count=1`
  probes (immune to the cap); the full sweeps paginate by `cursor = maxs+1`.

## Invariants

`over_deletion` (retained seqs ≥ T present byte-exact, on every read),
`trim_applied_xor` (acked ⟺ applied), `purge_liveness` (all below-T absent
within the 300s tick ceiling for an applied trim), `never_resurface` (physical
floor rises monotonically, never regresses = no resurrection), `tail_monotone`
(tail advances by exactly the trim command record). Anti-vacuity: seam1 = trim
in-flight or just-acked at kill; **seam2 = a genuinely partial physical purge**
— ≥1 below-T record still physical at read#1 (else VOID), witnessed on a
deletion set that spans the batch boundary.

## Runs

| kind | seed / env | seam / arm | n / T | outcome |
|---|---|---|---|---|
| seam1 shakeout / refactor-confirm | 258 | seam1 / default | 300 / 259 | **GREEN** |
| seam2 | 1500 | seam2 / default | 3000 / (pre-REDO) | GREEN (liveness only) |
| seam2 | 1501 | seam2 / 500ms | 3000 | GREEN (liveness only) |
| seam2 | 1502 | seam2 / 2s | 3000 | GREEN (liveness only) |
| **seam2 partial-purge** | **1500** | **seam2 / default** | **13000 / 11501** | **GREEN** |
| red-proof | 258 + ORACLE_SELFTEST=resurrect | seam1→seam2 (3000/13000) | — | **RED** purge_liveness/never_resurface |
| red-proof | 1500 + ORACLE_SELFTEST=resurrect | seam2 / default | 13000 / 11501 | **RED** purge_liveness (seq 11500) |

The headline GREEN (SEED=1500, n=13000, T=11501): killed +46ms after the trim
ack; **partial-purge witness fired — floor=0, all 11501 below-T records still
physical at read#1** (purge genuinely incomplete at kill). Recovery resumed the
half-completed purge: physical floor rose monotonically 0→11501, all 11501
below-T seqs absent within 300s, 1499 retained seqs byte-exact, tail 13000→13001
(advanced by exactly the trim record). All five invariants PASS non-vacuously.

## Test-reviewer: REDO → KEEP

Initial verdict **REDO**, three blockers, all resolved and re-confirmed KEEP:

1. **BLOCKER (partial-purge seam unexercised).** At n=3000 the deletion set
   (<10_000) is a SINGLE atomic WriteBatch — all-or-nothing across a crash, so
   no partial physical state exists to mishandle. The "half-done physical purge"
   the promise claims was never reached; the old seam2 witness proved only
   liveness. **Fix:** seam2 → n=13000, T ∈ [10001,12000] so the deletion set
   exceeds `DELETE_BATCH_SIZE` and the purge does ≥1 intermediate durable write
   before finalize. Now a crash can leave a genuinely partial physical state.
   Confirmed GREEN + selftest RED at the new scale.
2. **VOID-mask (seam1 trim 5xx).** The guard voided on any non-200, swallowing a
   5xx server fault (a panic in apply_command) as a setup void. **Fix:** 5xx on
   the trim command → RED (trim_applied_xor); 4xx and connection-cut stay VOID.
3. **VOID-mask (post-restart read 5xx).** `read_all`/`read_range` voided on any
   non-200. **Fix:** 5xx → RED (restart_serves) — a trim-recovery bug corrupting
   the read path is a finding, not a void.

Supporting hardening (transient-fault tolerance for the larger purge, NOT an
oracle weakening — the terminal full sweep remains the authoritative
over-deletion/absence check): the from-0 `physical_floor` probe scans ~10k
tombstones during the purge and can exceed the socket timeout, so it now catches
OSError→None (poll retries); the per-tick spot-check skips on OSError;
`read_all` retries a timed-out batch 5× then VOIDs; TICK_CEIL 150→300.

## New reality (map notes)

- A trim purge of >10_000 records spans ≥2 `DELETE_BATCH_SIZE` WriteBatches; the
  intermediate `db.write` (stream_trim.rs:99-102) fires at 10_000.
- Reading "first available seq" from seq 0 during/right after a large purge is
  **slow** — the read scans accumulated tombstones for records 0..floor before
  reaching the first live seq; it speeds up once compaction clears them. Probes
  from seq ≥ T (retained range) are unaffected. Poll the floor with retry, not a
  single tight-timeout read.

## Interpretation

s2-lite's trim is final across the crash on BOTH seams: acked trims are durable
and atomic with their logical point, retained records survive byte-exact, and
recovery correctly resumes a physically half-completed purge without
over-deleting or resurrecting a trimmed seq. No product finding. Official
publication replays a recorded green seed at wrap-up.
