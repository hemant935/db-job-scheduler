# Design: database-backed job scheduler

Postgres is the queue, the lock manager and the source of truth. There is no broker: consumers
coordinate only through row locks, leases and fencing tokens on the `jobs` table.

## 1. Choices

**PostgreSQL 16.** The whole design rests on four Postgres features:
`FOR UPDATE SKIP LOCKED` (many consumers claim concurrently without blocking each other),
data-modifying CTEs (rank, lock, claim and audit in **one statement / one round trip**),
partial indexes (the claim index holds only pending jobs, so it stays small however much history
accumulates), and `LISTEN/NOTIFY` (wake idle consumers instantly. Still the database, not a broker).
MySQL 8 has SKIP LOCKED but lacks partial indexes, data-modifying CTEs and NOTIFY. SQLite has a
single writer, which defeats horizontal consumers.

**Python 3.13 + psycopg 3 + threads.** Each consumer process runs a `ThreadPoolExecutor` of
worker threads. Most real jobs are I/O-bound (HTTP calls, DB work), where threads are ideal and
the GIL is released. CPU-bound handlers still let the heartbeat thread run (the GIL switches every
5ms). Raw SQL instead of an ORM, because the claim query *is* the system.

## 2. Schema

| table | purpose |
|---|---|
| `organizations` | tenant, fair-share `weight`, fleet-wide `max_concurrency` |
| `jobs` | `status` (scheduled → running → succeeded \| dead), `scheduled_at` (requested, immutable), `run_at` (next eligible: retries back off here), `lane` (fast/slow), `attempts/max_attempts`, `timeout_s`, lease (`lease_token`, `lease_owner`, `lease_expires_at`), `first_started_at` (to measure drift), `idempotency_key` (unique per org) |
| `job_attempts` | one row per claim, with its outcome: `succeeded / failed / timeout / lease_expired / owner_restarted / fenced` |
| `job_effects` | the job's side effect, written in the same transaction as `succeeded` |
| `consumers` | registry + heartbeat (observability only; correctness depends on leases) |

Invariant enforced by a CHECK: `status = 'running'` ⇔ `lease_token IS NOT NULL`.

## 3. Exactly-once

Exactly-once *delivery* is impossible over an unreliable network. What we guarantee is
**at-least-once execution + exactly-once commit**:

1. **Exclusive claim.** One statement ranks candidates, locks them `FOR UPDATE SKIP LOCKED`
   (re-checking `status = 'scheduled'` on the latest row version), flips them to `running`, and mints a
   fresh `lease_token = gen_random_uuid()`. 16 threads racing over 3,000 jobs: every job claimed once (test).
2. **Fencing.** Every exit from `running` (`complete`, `fail`, timeout) is
   `UPDATE … WHERE id = $1 AND lease_token = $2`. A worker that stalled past its lease (GC pause,
   partition) and was replaced gets 0 rows: its late result is rejected and logged as `fenced`.
3. **Atomic effect.** Completion is a single statement: flip to `succeeded`, insert the
   `job_effects` row, close the attempt. The effect exists if and only if the success committed.
   The demo verifies `effects == succeeded` with 0 jobs having more than one effect.
4. **External side effects** (an HTTP call) can't join our transaction, so they are at-least-once.
   Each handler receives a stable `idempotency_key` for downstream dedupe. Producers get
   exactly-once *enqueue* via `UNIQUE (org_id, idempotency_key)`.

## 4. Failure handling

| crash point | what happens | recovery |
|---|---|---|
| after claim, before work | job is `running` with a lease nobody renews | lease expires → reaper → `scheduled` (attempt counted) |
| mid-execution | same; partial work never committed | same |
| after work, before commit | same; result lost, effect not written | re-run; effect committed once by the next attempt |
| during commit | statement is atomic: either committed or not | if not, lease expiry re-runs it |
| zombie (alive but lease lost) | heartbeat notices → cancels the handler; any commit is fenced | n/a |
| poison job (crashes every consumer) | each crash consumes an attempt | `dead` after `max_attempts`, never loops |
| handler exception / timeout | `fail()` → `scheduled` with exponential backoff + jitter, or `dead` | 1s, 2s, 4s… |

Two recovery paths, both exercised in the demo. **(a) Lease expiry:** leases are renewed by each
consumer's heartbeat thread (`lease/3`); any consumer's reaper re-queues expired leases (with
`SKIP LOCKED`, so many reapers can run safely). **(b) Restart reclaim:** a consumer with a stable
name (`--id consumer-3`, e.g. a StatefulSet pod name) runs as `consumer-3#<pid>`. On startup it
immediately releases jobs still leased to earlier `consumer-3#*` incarnations instead of waiting
out the lease. *Why this exists:* the first demo runs showed that a crashed consumer's stranded
jobs keep counting against their org's concurrency cap until their leases expire, which blocked a
small org for about 5s. If a name were ever shared by two live processes, fencing still prevents
double commits; the only cost is wasted work.

## 5. Fairness (three layers)

1. **Fair ordering across orgs.** The claim ranks each org's oldest due jobs by
   `score = (org's running jobs + position in its own queue) / weight`. A light org's next job always
   outranks a flooding org's 11th, so a flood only delays the org that sent it. This is work-conserving: a
   lone org can use the whole fleet.
2. **Per-org concurrency cap** (`max_concurrency`, fleet-wide), for noisy-neighbour isolation and
   protecting downstream systems. *Soft* across consumers: two simultaneous claims read the same
   running count, so overshoot is at most `(concurrent claimers − 1) × headroom` (none observed in the demo).
3. **Lanes.** Long jobs (`lane = 'slow'`) may occupy at most `slow_slots` of each consumer's
   threads, so minutes-long reports can never take the slots that millisecond jobs need.

Demo result (acme floods 253 jobs, cap 10; three orgs send a handful): small orgs' p95 wait is
**0.1–0.2s**, while acme's backlog waits up to about 12s behind *itself*.

## 6. Schedule adherence

Jobs are claimable when `run_at <= now()` (they are never started early). An idle consumer
sleeps exactly until `min(run_at)` (via an index), capped at `poll_seconds`. It is woken early by
`NOTIFY` on insert (statement-level trigger, so a bulk insert sends one notification) or by a local
slot freeing. Measured with consumers crashing and a flood in progress: **p50 ≈ 25ms, p95 ≈ 150ms**.
Drift is `first_started_at − scheduled_at`, stored per job, so it is auditable.

## 7. Scaling

Measured on a 10-core laptop, with Postgres in Docker Desktop on the **same** machine:

| consumers × 16 threads | no-op jobs/s (pure scheduler overhead) | 50 ms jobs/s (I/O-bound work) |
|---|---|---|
| 1 | ~2,000 | 259 (ideal 320) |
| 2 | ~2,250 | 502 |
| 4 | ~2,050 | 948 |
| 8 | ~3,000 | 1,582 |

**What profiling found** (`pg_stat_activity` wait events + `EXPLAIN ANALYZE` under load):
- v1 plateaued at ~1,000 jobs/s at *any* consumer count. Wait events were dominated by
  `WALWrite`/`WALSync`: one commit per job. **Fix: group commit.** A committer thread per
  consumer completes every result that piled up in one statement, so batches grow with load and
  add no latency when idle. Result: about 2×.
- Next, the claim was CPU-bound at 30–220ms under contention (2ms when idle). Cause: when a
  candidate row was just claimed by another consumer, Postgres re-checks it via **EvalPlanQual**,
  and because the locking step joined the ranking CTE, it re-ran the entire fair-share ranking
  *per contended row*. **Fix:** collapse the ranking into an id array so the `FOR UPDATE SKIP LOCKED` step
  touches only `jobs`. Claim under load: **2–10ms**.

**Where each approach hits its ceiling:**
- **Bigger machine / more threads per process.** Good for I/O-bound jobs until one Python
  process saturates a core on scheduler overhead (~2k jobs/s). CPU-bound jobs don't scale with threads
  (GIL); use more processes, a process pool for CPU handlers, or free-threaded Python.
- **More processes / more nodes.** Linear while job work dominates (table above). Ceilings:
  (a) **connections.** About 8 per consumer (pool of 7 + the LISTEN connection); past ~50–100 consumers put PgBouncer in front (transaction
  pooling, with the `LISTEN` connection going direct, or fall back to polling).
  (b) **the database.** Every job costs ~5 row writes (claim, attempt row, completion, effect, attempt close), and none are HOT updates (`status` is in every
  partial index's predicate), so WAL volume and autovacuum on the hot table become the limit. A
  dedicated server with NVMe should reach roughly 10–20k jobs/s on one primary.
- **10×:** larger claim batches; partition `jobs` (or move terminal rows to a `job_history`
  table) so the hot table and its indexes stay tiny; aggressive autovacuum on the hot table;
  make `job_attempts` optional or sampled; one consumer-level lease instead of per-job heartbeats.
- **100×:** a single primary is the ceiling. **Shard by `org_id`** across N Postgres clusters
  (or Citus). Each shard has its own consumer group, which keeps tenant fairness local and
  avoids cross-shard transactions; global per-org caps split into per-shard quotas. With thousands of
  orgs, the claim's per-org probe (O(#orgs)) should read from an `active_orgs` set of orgs that
  have due work, instead of scanning all orgs.

## 8. What I'd change with more time

- **Strict** per-org caps (a per-org token row decremented in the claim statement).
- Consumer-level leases: heartbeat one `consumers` row instead of N job rows; reap by dead owner.
- Partitioned history and retention; HOT-friendly layout (fillfactor, fewer indexes).
- Adaptive lanes from observed runtime per job type instead of a static `lane`; priorities.
- Recurring jobs: materialize the next occurrence with `idempotency_key = (job, occurrence_ts)`.
- Transactional outbox for external effects; Prometheus metrics; an HTTP enqueue API; a DLQ re-drive tool.
