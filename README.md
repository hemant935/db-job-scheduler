# db-job-scheduler

A multi-tenant job scheduling and execution system that uses **only PostgreSQL** for queueing
and coordination: no Redis, RabbitMQ, SQS, Kafka or Celery. Many consumer processes pull due jobs
concurrently and run them on worker threads with:

- **exactly-once commit**: atomic `SKIP LOCKED` claims, per-claim fencing tokens, and the side
  effect committed in the same statement as success
- **crash safety at any point**: leases + heartbeats + a reaper, plus instant reclaim when a
  supervised consumer restarts
- **fairness**: fair-share ordering across orgs, per-org concurrency caps, and a capped slow lane
  for long jobs
- **schedule adherence**: sleep-until-next-due + `LISTEN/NOTIFY` (p95 drift ≈ 150ms in the demo)

The design rationale, failure analysis and scaling measurements are in **[DESIGN.md](DESIGN.md)**.

## Quick start

Requirements: Docker, Python ≥ 3.11, [uv](https://docs.astral.sh/uv/) (or plain pip, see below).

```bash
docker compose up -d          # Postgres 16 on localhost:5433 (+ a scheduler_test database)
uv sync                       # installs psycopg 3 + pytest into .venv
uv run python demo/run_demo.py
```

Without uv: `python -m venv .venv && .venv/bin/pip install "psycopg[binary]>=3.2" psycopg-pool pytest`,
then use `.venv/bin/python` in place of `uv run python`.

## Live dashboard (manual testing)

```bash
uv run python -m scheduler dashboard      # then open http://127.0.0.1:8765
```

A browser UI that updates every second. It shows running jobs and due backlog per org (last 3
minutes), the per-org running/cap, wait p50/p95 and early starts, a live **exactly-once check**,
consumer cards with busy slots, the jobs each consumer is holding, and stranded jobs with a lease
countdown. The attempt log shows every failure, reap, restart reclaim and fenced commit.
Buttons drive every scenario: add work (flood acme, scheduled jobs, long reports, flaky, poison),
start a normal or chaos consumer, and **Kill -9 / Stop / Restart** any consumer the dashboard
started. It binds to 127.0.0.1 only; stopping it (Ctrl-C) also stops the consumers it launched.

## The demo (`demo/run_demo.py`, ~40s)

1. Creates 4 orgs and enqueues **306 jobs**. `acme` floods 250 short jobs plus 3 long reports;
   `globex`, `initech` and `umbrella` submit a handful each, mostly scheduled 1–25s in the future.
   The mix includes slow-lane reports, CPU-bound jobs, flaky jobs that need retries, one job that
   always fails (it must end in the dead letter), and a duplicate submission (deduped by idempotency key).
2. Starts **3 consumer processes** (6 worker threads each, at most 2 for slow jobs).
3. Breaks things on purpose:
   - `consumer-3` hard-crashes (`os._exit`) **after claiming**, then **mid-execution**, then
     **right before committing**, once per incarnation. A supervisor restarts it each time.
   - `consumer-2` is **SIGKILLed** while holding jobs, and its "machine" stays down longer than the
     lease, so the surviving consumers must reap its jobs.
   - A deterministic **zombie** check: a worker stalls past its lease, the job is re-run elsewhere,
     then the zombie tries to commit.
4. Verifies from the database and prints PASS/FAIL for each requirement:

```
1. EXACTLY-ONCE
  [PASS] no job lost: 305 succeeded + 1 dead-lettered = 306 of 306 (0 stuck in scheduled/running)
  [PASS] no duplicates: 305 effect rows for 305 succeeded jobs, 0 jobs with >1 effect
2. CRASHES AT ANY POINT
  [PASS] hard crashes hit all three points: after claim, mid-execution, before commit (+ SIGKILL)
  [PASS] all 4 stranded jobs recovered and re-run: 4 committed exactly once
  [PASS] both recovery paths exercised: lease-expiry reaping (node lost) and restart reclaim
  [PASS] zombie's late commit rejected by fencing token
3. FAIRNESS
  org       jobs  cap  peak_running  p50_wait_s  p95_wait_s  max_wait_s
  acme      253   10   10            6.820       12.498      12.854
  globex    27    8    5             0.026       0.186       0.899
  initech   19    8    2             0.027       0.123       0.174
  umbrella  7     4    2             0.007       0.036       0.042
  [PASS] small orgs unaffected by the flood: worst p95 wait 0.19s (acme queues behind itself: p95 12.5s)
  [PASS] long jobs never took a consumer's fast slots: peak slow-lane jobs per consumer 2 <= 2 of 6
4. SCHEDULE ADHERENCE
  [PASS] no job started before its scheduled_at
  [PASS] drift SLO p95 < 1s over all 53 future-scheduled jobs: p95 146ms, max 0.90s
```

Each consumer's log is written to `demo/logs/<consumer>.log`. Grep it for `FAULT INJECTED`,
`reaped`, `recovered job`, or `FENCED`.

## Scaling benchmark (`demo/bench.py`)

```bash
uv run python demo/bench.py --jobs 20000 --consumers 1 2 4 8             # scheduler overhead (no-op jobs)
uv run python demo/bench.py --jobs 8000 --consumers 1 2 4 8 --job-ms 50  # I/O-bound jobs
```

The results and the analysis of where each scaling approach hits its ceiling are in
[DESIGN.md §7](DESIGN.md#7-scaling).

## Running it by hand

```bash
uv run python -m scheduler migrate
uv run python -m scheduler org acme --max-concurrency 5
uv run python -m scheduler org beta --weight 2
uv run python -m scheduler enqueue --org acme --type email.send --count 200
uv run python -m scheduler enqueue --org beta --type report.generate --count 5 --in-seconds 10 --payload '{"seconds": 20}'

# in 2–3 terminals:
uv run python -m scheduler consumer --id consumer-1
uv run python -m scheduler consumer --id consumer-2 --fault mid_exec=0.05   # crashes ~5% of jobs

uv run python -m scheduler stats --watch 1   # status by org, drift, attempts, integrity, consumers
```

Kill a consumer with `kill -9` and watch its jobs come back after the lease expires (10s by
default). Restart it with the same `--id` and it reclaims them immediately. `Ctrl-C` drains
gracefully: it stops claiming and finishes in-flight jobs. Job types: `email.send`,
`report.generate` (slow lane), `image.resize` (CPU-bound), `flaky`, `noop`.
Consumer options: `--slots`, `--slow-slots`, `--lease`, `--heartbeat`, `--poll`, and
`--fault point=prob|@n` (points: `after_claim`, `mid_exec`, `before_complete`).

## Tests

```bash
uv run pytest      # uses the scheduler_test database; never touches demo data
```

The tests cover: 16 threads racing to claim 3,000 jobs (no overlaps); zombie fencing; heartbeat
renewal; dead-lettering of poison jobs; retry backoff; permanent errors; no early starts; org
interleaving, caps and weights; lane separation; idempotent enqueue; restart reclaim; and
end-to-end consumers draining a mixed workload, including timeouts.

## Layout

```
migrations/001_init.sql   schema, partial indexes, NOTIFY trigger
scheduler/store.py        every coordination primitive: claim, complete_many, fail, heartbeat, reap
scheduler/consumer.py     consumer process: claim loop, worker threads, committer, heartbeat, listener
scheduler/handlers.py     job-type registry + demo job types
scheduler/faults.py       crash injection
scheduler/report.py       stats / integrity queries
scheduler/dashboard.py    live web UI + consumer process control (static/dashboard.html)
scheduler/cli.py          python -m scheduler {migrate,reset,org,enqueue,consumer,stats,dashboard}
demo/run_demo.py          the five-scenario demo
demo/bench.py             throughput benchmark
tests/                    pytest suite
```
