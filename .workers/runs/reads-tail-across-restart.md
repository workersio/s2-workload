# reads-tail-across-restart — run evidence

- workload: `.workers/workloads/reads_tail.py across-restart`
- attack: follower tails over SSE while a pipelined writer pool (3 threads)
  appends; SIGKILL the server inside a sampled lag>0 window (acks landed
  that the follower has not yet been handed); restart on the same root;
  diff the pre-kill observed log against the post-restart Remote read;
  resume the follow from K and require it to tile [K, tail) gap-free.

## Drafts

| run | depth | outcome | note |
|-----|-------|---------|------|
| nd7eetc4d36j1dc6ngv9xhbez18a1y2p | 2 | 2 VOID (setup) | 500ms/2s flush arms: first append 404 stream_not_found — lazily-created stream not yet durable; exposed by slow arms |
| nd79mn6tjr9s6vnr989tfeh9bd8a0ncs | 3 | 2 green, 1 VOID | failure logging added; default-arm trials green but lag=0 at kill (serial writer lets the follower catch up between acks) |
| nd73vy3q17y9rmf7p98exrzrfd8a1d54 | 3 | 3/3 green | prime-append fix (retry-bounded first append) + pipelined writer pool + sampled lag>0 kill window: lag=1 at kill on all arms incl 500ms; kill landed amid in-flight deliveries (observed advanced past the sampled count before socket death) |
| nd778sx2t0eev3a96hz6jcjqeh8a0bfn | 1 | RED (expected) | ORACLE_SELFTEST=1 across the real kill: observed_survive FAIL, seq 49 planted-lost (seed=1837281636) |
| nd720pky7jpcq440xbhgph7w0h8a1g5a | 10 | 5 green / 5 VOID | all voids on the 2s arm: kill point unreachable at ~1.5 acks/s (x4), one negative-lag sample; led to arm-scaled kill_after |
| nd7aacs0608v8vm3kvjrm40c2x8a01wm | 6 | 6/6 green | arm-scaled kill point: default x4 / 500ms / 2s, lag=1 at every kill, zero voids |

## Test-reviewer gate (REDO -> fixed)

Verdict: REDO (narrow) — attack/oracle/evidence stand; two VOID-masking
holes in the restart leg: (1) server serving but stream persistently denied
post-restart (the strongest form of the finding) exited VOID via
wait_stream_ready; (2) read_all exited VOID on any non-200 below tail,
single-shot. Plus ORACLE_SELFTEST=gap was not wired into this mode.

Fixes: wait_stream_ready REDs (observed_survive) when the process serves but
denies the stream >=25 consecutive polls while >=50 observed records are
held (connection-level failure stays VOID); read_all takes 10 bounded
retries then REDs (readback_dense) in the post-restart leg; drop_seq wired
(gap drops seq 30, below the floor).

## Post-fix runs

| run | depth | outcome | note |
|-----|-------|---------|------|
| nd7evh2sfx4yaq8wct45vfyrk58a157e | 1 | RED (expected) | ORACLE_SELFTEST=gap in-mode: follow_wellformed FAIL at seq 30 pre-kill (seed=290419235) |
| nd7539m6kq7wtsgnbwjdw1b9rd8a1b7n | 1 | GREEN (selftest miss) | first lost-stream shape polled a missing *stream* — check-tail auto-creates it on a create-stream-on-read basin (200/tail-0); reality note added |
| nd763v4tegp7nv1qqrcp35vp1n8a0qzm | 3 | 3/3 green | post-fix green revalidation across arms |
| nd794gyjm55f4wp2yvjq7cq6m18a09z5 | 1 | RED (expected) | ORACLE_SELFTEST=lost-stream via nonexistent basin: observed_survive FAIL after 89 consecutive serving denials (seed=4028217004) |
| nd72rd95yjks58bywbh9pes1s58a0fvh | 1 | GREEN | final-code sanity, lag=2 at kill (seed=3051447964) — recorded as replay |

Verdict: **done + green** (reviewer pre-committed KEEP once the three fixes
landed; all three demonstrated). `published: pending` — official fires at
wrap-up. Reviewer candidate for a future arm: manifest ⊆ readback across
this kill schedule (catches ack-before-remote-durable for lag-window
records).

## Harness lessons (also in map.md)

- Slow `SL8_FLUSH_INTERVAL` arms make the lazily-created stream 404
  (`stream_not_found`) on append until its creation record is durable,
  despite `--create-stream-on-append` — prime with a retry-bounded append.
- A serial one-request-one-ack writer can never produce lag>0 at the kill:
  ack and follow delivery gate on the same durable_seq advance, so the
  follower catches up between serial acks. Concurrent appenders are needed
  to observe acked-but-not-yet-delivered records.
