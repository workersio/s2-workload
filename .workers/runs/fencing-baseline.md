# Run evidence — fencing-baseline

Executor #17, 2026-07-07. Drafts via `--workload-file` injection on prod.
Command: `sh .workers/workloads/fencing.sh baseline` (no faults).

One server, one seed, three governance regimes in series: default (empty
token) -> fence T1 -> fence T2. The critic's four required pins, all
non-vacuously enforced: tokenless always accepted (all three regimes);
full wrong-token class (stale + never-valid) 412 in every regime with no
trace; 412 body == current governing token (observed shape
`{"fencing_token_mismatch": "<governing>"}`, `""` under the default
token); governance flips at the fence record's position (T1-accepted
strictly inside (P1,P2), T2-accepted after P2, fence bodies at their
acked positions, first post-fence-ack stale attempt already 412).

| exploration id | depth | purpose | outcome |
|---|---|---|---|
| nd7dskf2d56exs8wx4t402vdp18a17ef | 2 | shakeout v1 | 2/2 RED — MY bug: asserted body key `fencing_token`; actual is snake_cased variant `fencing_token_mismatch`. Proves the disclosure parse path is live. |
| nd7a7x4efrsfm8aeg63r1aechh8a1c0j | 3 | shakeout v2 | 3/3 GREEN, 5/5 invariants per trial |
| nd7d9xnscszshdhsqfm75jfcgh8a09g8 | 1 | red-proof =1 (stale relabel) | RED seed 1026934166 -> wrong_token_rejected, exit 1 |
| nd771wn0z15s2mh253jwt1bzxx8a102j | 5 | sweep | 5/5 GREEN (3 seeds duplicated shakeout's — guest urandom reuse) |
| nd76dpymzts9q95aw80ejk4fa58a13nv | 3 | post-REDO confirm | 3/3 GREEN |
| nd7dc7nc6fe1n3e6dj8e206pvx8a15r6 | 1 | red-proof =flip (P2 perturbed) | RED -> atomic_flip |
| nd74rgzxzezn8yrxcfqfjfcfw58a04ms | 1 | red-proof =disclose (forged body) | RED -> disclosure_412 |
| nd786qc1vycjce52xsp1z480t58a17ce / nd77ee7sgmaq85w0yn8xzk6gh18a0zyp | 1+1 | explicit-SEED top-up greens | GREEN (1111111111, 777000111) |

Test-reviewer: REDO -> fixed same-episode -> revalidated. Gating: (1)
correct-token/fence 412 refusals voided — in a quiescent no-fault run a
412 on the governing token IS the finding (fence acked but never
applied); now RED atomic_flip. (2) dropped connections voided — on
localhost with no faults that means the server died on the attempt;
now `server.poll()`-gated RED. Non-gating recs all applied: =flip and
=disclose selftest arms (both proven RED), explicit SEED greens,
single-record fence-ack guard on P1/P2 derivation. Reviewer confirmed
not-a-wrapper vs stale-across-restart's pre-kill phase (default-regime
probing, 412-body pinning, never-valid class, and the flip-position
check exist nowhere else) and verified all source claims.

Interpretation: the pure cooperative contract holds — no product
finding. Spec-note for producer: the promise-level `claim` still says
"stale or MISSING token are rejected"; missing (tokenless) is always
accepted per streamer.rs:341 and this rung's pin — claim text needs the
producer #8 correction propagated.
