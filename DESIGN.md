# Design: database-backed job scheduler

Postgres is the queue, the lock manager and the source of truth. There is no broker: consumers
coordinate only through row locks, leases and fencing tokens on the `jobs` table.

## 1. Choices

**PostgreSQL 16.** The design rests on four features: `FOR UPDATE SKIP LOCKED` (consumers claim
concurrently without blocking each other), data-modifying CTEs (rank, lock, claim and audit in
**one statement**), partial indexes (the claim index holds only pending jobs, so it stays small as
history grows) and `LISTEN/NOTIFY` (wake idle consumers instantly, still inside the database).
MySQL 8 has SKIP LOCKED but none of the other three; SQLite's single writer defeats horizontal consumers.

**Python 3.13 + psycopg 3 + threads.** Each consumer process runs a pool of worker threads. Most
real jobs are I/O-bound, where threads are ideal. Raw SQL, no ORM: the claim query *is* the system.

**Schema.** `organizations` (fair-share `weight`, fleet-wide `max_concurrency`); `jobs` (`status`
scheduled → running → succeeded | dead, immutable `scheduled_at`, `run_at` for retry backoff,
`lane`, attempts, `timeout_s`, lease = `lease_token` + `lease_owner` + `lease_expires_at`,
`first_started_at` for drift, `UNIQUE (org_id, idempotency_key)`); `job_attempts` (one row per
claim and how it ended); `job_effects` (the side effect, written in the same statement as success);
`consumers` (observability only). A CHECK enforces `status = 'running'` ⇔ `lease_token IS NOT NULL`.

## 2. Exactly-once

Exactly-once *delivery* is impossible over an unreliable network: a worker can finish the work
and die before reporting it. We guarantee **at-least-once execution + exactly-once commit**, and
the demo reports both numbers (how many jobs ran more than once, and that each committed once).

1. **Exclusive claim.** One statement ranks candidates, locks them `FOR UPDATE SKIP LOCKED`
   (re-checking `status` on the latest row version), flips them to `running` and mints a fresh
   `lease_token`. Tested with 16 threads racing over 3,000 jobs.
2. **Fencing.** Every exit from `running` is `UPDATE … WHERE id = $1 AND lease_token = $2`. A
   worker that stalled past its lease and was replaced matches 0 rows; its late result is rejected.
3. **Atomic effect.** Success, the `job_effects` row and the attempt's outcome are one statement.
   Results are group-committed (below). A result the database refuses would abort the whole batch,
   so the worker first checks it is valid JSON, and if a batch still fails on a data error the
   committer retries row by row: one bad result fails only its own job.
4. **External effects** (an HTTP call) cannot join our transaction. Handlers get a stable
   `idempotency_key` for downstream dedupe; producers get exactly-once enqueue via the unique key.

## 3. Failure handling

| crash point | recovery |
|---|---|
| after claim / mid-execution / before commit | lease not renewed → expires → any consumer's reaper re-queues it (attempt counted); partial work was never committed |
| during commit | the statement is atomic: committed, or re-run after lease expiry |
| zombie (alive, lease lost) | heartbeat notices and cancels the handler; any commit is fenced |
| poison job (kills every consumer) | each crash uses an attempt → `dead` after `max_attempts` |
| handler error / timeout | back to `scheduled` with exponential backoff + jitter, or `dead` |

Leases are renewed by a heartbeat thread every `lease/3`; reapers use `SKIP LOCKED`, so all
consumers can reap safely. A second path, **restart reclaim**, exists because the first demo runs
showed a crashed consumer's stranded jobs counting against their org's cap until the lease
expired. A consumer with a stable name (`--id consumer-3`, e.g. a StatefulSet pod) runs as
`consumer-3#<pid>` and on startup releases jobs held by earlier incarnations. If a name were ever
shared by two live processes, fencing still prevents double commits; the cost is wasted work.

## 4. Fairness

1. **Fair ordering.** The claim ranks each org's oldest due jobs by
   `(org's running jobs + position in its own queue) / weight`. A light org's next job outranks a
   flooding org's 11th, so a flood only delays the org that sent it. Work-conserving up to each
   org's cap: idle capacity goes to whoever has work.
2. **Per-org concurrency cap** (fleet-wide), for noisy-neighbour isolation and to protect
   downstream systems. *Soft*: two simultaneous claims can overshoot by at most
   `(concurrent claimers − 1) × headroom` (none observed).
3. **Lanes.** Long jobs may use at most `slow_slots` of each consumer's threads, so minutes-long
   reports never take the slots that millisecond jobs need.

Demo: acme floods 253 jobs (cap 10). The other orgs' p95 wait stays around 0.1–0.2s; acme runs at
~98% of its cap while its backlog waits behind itself.

## 5. Schedule adherence

Jobs are claimable when `run_at <= now()`, so never early. An idle consumer sleeps until
`min(run_at)` (indexed), capped at `poll_seconds`, and is woken early by `NOTIFY` on insert (one
per statement) or by a local slot freeing. With consumers crashing and a flood in progress,
future-scheduled jobs start with **p50 ≈ 15ms, p95 ≈ 130ms** late. Drift under *overload* is a
capacity question, not a scheduling one: acme's 253 jobs due at the same instant wait seconds
because its cap allows only 10 at a time. That is the cap doing its job; the remedy is more
consumers or a higher cap, and production would alert on due-backlog age.

## 6. Scaling

Measured on a 10-core laptop, Postgres in Docker on the same machine (`demo/bench.py`, 16 threads per consumer):

| consumers | no-op jobs/s (scheduler overhead) | 50 ms jobs/s (I/O-bound) |
|---|---|---|
| 1 | ~2,000 | 259 |
| 2 | ~2,250 | 502 |
| 4 | ~2,050 | 948 |
| 8 | ~3,000 | 1,582 |

**Profiling.** v1 plateaued at ~1,000 jobs/s at any consumer count; wait events were
`WALWrite`/`WALSync`, one commit per job. *Fix:* a committer thread per consumer completes every
result that piled up in one statement (batches grow with load, no added latency when idle): ~2×.
Next, claims took 30–220ms under contention: when a row was just claimed elsewhere, Postgres
re-checks it (EvalPlanQual), and because the locking step joined the ranking CTE it re-ran the
whole ranking per contended row. *Fix:* collapse the ranking into an id array so the lock step
touches only `jobs`: 2–10ms.

**Ceilings.**
- **Bigger machine / more threads:** fine for I/O-bound jobs until one process spends a core on
  scheduler overhead (~2k jobs/s). CPU-bound jobs need processes (GIL) or free-threaded Python.
- **More processes / nodes:** linear while job work dominates (demo: 1 → 3 consumers ≈ 3×). Limits
  are ~8 connections per consumer (PgBouncer past ~50–100 consumers, `LISTEN` connection direct),
  then the database itself: ~5 non-HOT row writes per job make WAL and autovacuum the bottleneck,
  roughly 10–20k jobs/s on a dedicated NVMe primary.
- **10×:** bigger claim batches; partition `jobs` or move finished rows to history so the hot table
  stays tiny; aggressive autovacuum; sample `job_attempts`; one lease per consumer, not per job.
- **100×:** one primary is the ceiling. Shard by `org_id` (or Citus), one consumer group per shard,
  so fairness stays local and nothing crosses shards; global caps become per-shard quotas. With
  thousands of orgs, drive the claim from an `active_orgs` set instead of scanning all orgs.

## 7. What I'd change with more time

- **Strict** per-org caps (a token row per org, decremented in the claim statement).
- Run handlers that can't honour cancellation in a subprocess: today a handler that never calls
  `ctx.checkpoint()` keeps its thread after a timeout (its result is fenced, but the slot is lost).
- Consumer-level leases; partitioned history and retention; HOT-friendly layout.
- Adaptive lanes from observed runtime; priorities; recurring jobs keyed by occurrence.
- Transactional outbox for external effects; metrics; an HTTP enqueue API; a dead-letter re-drive tool.
