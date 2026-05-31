# Performance characteristics

Numbers below come from the profiling harness ([tests/test_profile.py](tests/test_profile.py)),
run on a single worker against a local (Dockerised) Postgres with a deterministic
workload of 100 workflows (40 parents that each spawn children, wait on a signal,
and join the children — 140 total workflow runs, ~2.7 history rows replayed per run).
The activity body is intentionally trivial so the engine's fixed per-step cost is
visible; real activities (network/LLM/subprocess work) dwarf it.

## How to profile

```bash
TEMPORAL_LIGHT_PROFILE=1 TEST_DATABASE_URL=... pytest tests/test_profile.py -s
```

`TEMPORAL_LIGHT_PROFILE` gates all instrumentation — when unset there is zero
overhead (the timing context manager is a bare yield and the connection wrapper is
not applied). `PROFILE_CONCURRENCY` varies the worker concurrency. Use concurrency
1 for honest attribution: the buckets are summed wall-time, so at higher concurrency
overlapping runs inflate them.

## Two findings, one story

**1. High fixed per-step overhead, dominated by database round-trips.** For a
trivial activity, ~97% of the time is engine, not user code:

```
USER (activity bodies)           3.0%
ENGINE                          97.0%
  db i/o (in-run)               40%     ~12 round-trips per run, ~0.8ms each
  framework / replay cpu        27%
  activity dispatch (ipc)       20%     process-pool pickling + per-call event loop
  scheduling (claim)            10%
```

Each workflow run is ~12 DB round-trips: load history, the `FOR UPDATE` waiting
mark, event writes, and the claim. At ~0.8ms each (local Postgres) that round-trip
*count* — not slow queries — is the largest single cost. It is worst when the
database is remote (every trip pays the network), and it is the main lever left for
further optimisation (batching/pipelining statements per round-trip).

**2. Near-linear parallelisation.** Because that overhead is I/O *wait*, not CPU,
concurrency overlaps it almost perfectly. Same workload, varying only worker
concurrency:

| concurrency | wall clock | speedup |
|-------------|-----------:|--------:|
| 1           |     16.7 s |    1.0× |
| 4           |      4.5 s |    3.7× |
| 8           |      2.2 s |    7.6× |

So the two observations are the same fact from opposite sides: the engine spends
most of a cheap step waiting on the database, which looks like "a lot of overhead"
serially but parallelises near-linearly. For real workloads the activity body
dominates and the per-step engine cost becomes negligible regardless.

## Tunables

- `TEMPORAL_LIGHT_LOCK_DURATION_SECONDS` (default 30) and
  `TEMPORAL_LIGHT_HEARTBEAT_INTERVAL_SECONDS` (default 10) — keep the lock
  comfortably longer than realistic compute stalls so a healthy worker's lock
  never lapses spuriously.
- `worker_concurrency` / `activity_pool_size` on `Worker` — raise concurrency to
  hide DB round-trip latency; size the activity process pool to the number of
  simultaneously-running CPU/blocking activities.
