# Run evidence — fencing-stale-across-restart

- Workload: `.workers/workloads/fencing.sh` — write under T1, fence to T2
  (command record: single header `["", "fence"]`, body = token — verified
  in source, `common/src/record/mod.rs` try_from_parts), stale (T1) /
  tokenless / control (T2) attempts pre-kill, SIGKILL + restart, same
  attempts post-restart. Controls must ack on both sides (else void), and
  every attempt must get an HTTP response.
- Oracle self-test: draft 01KWQD4EG2FPNMKS71DMNP9VW5 RED — a stale append
  faked as accepted post-restart trips `stale_rejected FAIL`.
- Bring-up: nd7ahmrdjp9myynhrga997f7nh89xbdh — 3/3 green. Stale appends
  rejected HTTP 412 identically pre-kill and post-restart (recovered
  token, deserialized from storage, still enforces); tokenless appends
  accepted on both sides (cooperative model preserved); read-back dense,
  exactly the accepted set (fence command records included), no stale
  payload leaked.
- Official exploration: see frontmatter `published:` (depth 5).
