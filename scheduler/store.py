"""The job store. Every coordination primitive is one SQL statement or one short transaction.

Invariants (enforced here, verified by tests/ and the demo):
  * Only a 'scheduled' row whose run_at has passed can be claimed, and only by one consumer
    (FOR UPDATE SKIP LOCKED + status re-check under the row lock).
  * Every claim mints a fresh lease_token. Every exit from 'running' (complete / fail / timeout)
    is conditional on that token, so a consumer that lost its lease cannot commit.
  * A job's side effect (job_effects row) is written in the same transaction as its
    'succeeded' transition -> it is committed exactly once.
  * A lease that is not renewed expires; the reaper puts the job back to 'scheduled' (or 'dead'
    once max_attempts is used up, so a job that crashes every consumer cannot loop forever).
"""
from __future__ import annotations

import os
import random
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import psycopg
from psycopg.types.json import Jsonb

from scheduler import handlers


@dataclass(frozen=True)
class ClaimedJob:
    id: int
    org_id: str
    job_type: str
    lane: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    timeout_s: int
    lease_token: uuid.UUID
    scheduled_at: datetime
    idempotency_key: str | None


# --- producers --------------------------------------------------------------------------------


def upsert_org(
    conn: psycopg.Connection, org_id: str, *, name: str | None = None, weight: int = 1,
    max_concurrency: int = 10,
) -> None:
    conn.execute(
        """
        INSERT INTO organizations (id, name, weight, max_concurrency)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
           SET name = EXCLUDED.name, weight = EXCLUDED.weight,
               max_concurrency = EXCLUDED.max_concurrency
        """,
        (org_id, name or org_id, weight, max_concurrency),
    )


def ensure_org(conn: psycopg.Connection, org_id: str) -> None:
    conn.execute(
        "INSERT INTO organizations (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
        (org_id, org_id),
    )


_ENQUEUE_SQL = """
    INSERT INTO jobs (org_id, job_type, lane, payload, idempotency_key,
                      scheduled_at, run_at, max_attempts, timeout_s)
    VALUES (%(org_id)s, %(job_type)s, %(lane)s, %(payload)s, %(idempotency_key)s,
            coalesce(%(scheduled_at)s::timestamptz, now()),
            coalesce(%(scheduled_at)s::timestamptz, now()),
            %(max_attempts)s, %(timeout_s)s)
    ON CONFLICT (org_id, idempotency_key) DO NOTHING
    RETURNING id
"""


def _job_params(
    org_id: str, job_type: str, *, scheduled_at: datetime | None = None,
    payload: dict[str, Any] | None = None, lane: str | None = None,
    idempotency_key: str | None = None, max_attempts: int = 5, timeout_s: int = 300,
) -> dict[str, Any]:
    return {
        "org_id": org_id,
        "job_type": job_type,
        "lane": lane or handlers.lane_for(job_type),
        "payload": Jsonb(payload or {}),
        "idempotency_key": idempotency_key,
        "scheduled_at": scheduled_at,
        "max_attempts": max_attempts,
        "timeout_s": timeout_s,
    }


def enqueue(conn: psycopg.Connection, org_id: str, job_type: str, **kw: Any) -> int | None:
    """Insert one job. Returns its id, or None if (org_id, idempotency_key) already exists."""
    row = conn.execute(_ENQUEUE_SQL, _job_params(org_id, job_type, **kw)).fetchone()
    return row["id"] if row else None


def enqueue_many(conn: psycopg.Connection, jobs: Iterable[dict[str, Any]]) -> int:
    """Bulk insert in one transaction (pipelined). Each item: kwargs for enqueue() incl. org_id/job_type."""
    params = [_job_params(**j) for j in jobs]
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(_ENQUEUE_SQL.replace("RETURNING id", ""), params)
    return len(params)


# --- consumers: claim ---------------------------------------------------------------------------

# Fair claim, in one statement:
#   running    fleet-wide running count per org (for caps and fair share)
#   quota      orgs below their max_concurrency, with remaining headroom
#   candidates per org, its oldest eligible jobs (at most headroom) via an index-only LATERAL probe
#   ranked     interleave orgs: score = (already running + position in own queue) / weight.
#              An org with 0 running gets its 1st job at score 1, an org with 8 running gets its
#              next one at 9 -> light orgs are served first and a flood cannot starve anyone.
#              Collapsed into one ordered id array (an InitPlan, computed once).
#   locked     lock the best `limit` rows, skipping rows another consumer is claiming right now;
#              status/run_at are re-checked on the locked (latest) row version.
#              The locking query deliberately touches ONLY `jobs` (id = ANY(array)): when a
#              candidate was just claimed by another consumer, Postgres re-checks it with
#              EvalPlanQual, and with a join/CTE in this query that re-check re-ran the ranking
#              per contended row (measured: 2ms -> 50-220ms per claim at 8 consumers).
#   claimed    flip to running, bump attempts, mint a fresh fencing token, set lease expiry.
#   logged     audit row per attempt.
# Per-org caps are "soft" across consumers: two consumers claiming at the same instant both see
# the same running count, so a cap can be overshot by at most (concurrent claimers - 1) x headroom.
_CLAIM_SQL = """
WITH running AS (
    SELECT org_id, count(*) AS n FROM jobs WHERE status = 'running' GROUP BY org_id
),
quota AS (
    SELECT o.id AS org_id, o.weight, coalesce(r.n, 0) AS running,
           o.max_concurrency - coalesce(r.n, 0) AS headroom
    FROM organizations o LEFT JOIN running r ON r.org_id = o.id
    WHERE o.max_concurrency > coalesce(r.n, 0)
),
candidates AS (
    SELECT c.id, c.run_at, q.running, q.weight,
           row_number() OVER (PARTITION BY q.org_id ORDER BY c.run_at, c.id) AS rn
    FROM quota q
    CROSS JOIN LATERAL (
        SELECT j.id, j.run_at FROM jobs j
        WHERE j.org_id = q.org_id AND j.lane = %(lane)s
          AND j.status = 'scheduled' AND j.run_at <= now()
        ORDER BY j.run_at, j.id
        LIMIT least(q.headroom, %(fetch)s)
    ) c
),
ranked AS (
    SELECT array_agg(id ORDER BY score, run_at, id) AS ids
    FROM (SELECT id, (running + rn)::float8 / weight AS score, run_at
            FROM candidates ORDER BY score, run_at, id LIMIT %(fetch)s) top
),
locked AS (
    SELECT j.id FROM jobs j
    WHERE j.id = ANY ((SELECT ids FROM ranked)::bigint[])
      AND j.status = 'scheduled' AND j.run_at <= now()
    ORDER BY array_position((SELECT ids FROM ranked), j.id)
    LIMIT %(limit)s
    FOR UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE jobs j
       SET status = 'running',
           attempts = j.attempts + 1,
           lease_token = gen_random_uuid(),
           lease_owner = %(consumer)s,
           lease_expires_at = now() + %(lease)s * interval '1 second',
           started_at = now(),
           first_started_at = coalesce(j.first_started_at, now())
      FROM locked l
     WHERE j.id = l.id
    RETURNING j.id, j.org_id, j.job_type, j.lane, j.payload, j.attempts, j.max_attempts,
              j.timeout_s, j.lease_token, j.scheduled_at, j.idempotency_key
),
logged AS (
    INSERT INTO job_attempts (job_id, attempt, lease_token, consumer_id)
    SELECT id, attempts, lease_token, %(consumer)s FROM claimed
)
SELECT * FROM claimed
"""


def claim(
    conn: psycopg.Connection, consumer_id: str, lane: str, limit: int, lease_seconds: float
) -> list[ClaimedJob]:
    if limit <= 0:
        return []
    rows = conn.execute(
        _CLAIM_SQL,
        {
            "consumer": consumer_id,
            "lane": lane,
            "limit": limit,
            # Over-fetch candidates so rows skipped (locked by a concurrent claimer) still leave
            # enough to fill `limit`.
            "fetch": limit * 3,
            "lease": lease_seconds,
        },
    ).fetchall()
    return [
        ClaimedJob(
            id=r["id"], org_id=r["org_id"], job_type=r["job_type"], lane=r["lane"],
            payload=r["payload"], attempt=r["attempts"], max_attempts=r["max_attempts"],
            timeout_s=r["timeout_s"], lease_token=r["lease_token"],
            scheduled_at=r["scheduled_at"], idempotency_key=r["idempotency_key"],
        )
        for r in rows
    ]


# --- consumers: finish ------------------------------------------------------------------------


# Batched, single-statement completion (one round trip, one commit for N jobs). Per row:
#   done   flip to succeeded only where the fencing token still matches
#   eff    write the side effect for exactly those rows
#   att    close each attempt: succeeded, or fenced if its token no longer matched
# A statement is atomic, so a job's effect is committed if and only if its 'succeeded' is.
_COMPLETE_SQL = """
WITH input AS (
    SELECT * FROM unnest(%(ids)s::bigint[], %(tokens)s::uuid[], %(results)s::jsonb[])
           AS t(id, token, result)
),
done AS (
    UPDATE jobs j
       SET status = 'succeeded', finished_at = now(), last_error = NULL,
           lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
      FROM input i
     WHERE j.id = i.id AND j.lease_token = i.token AND j.status = 'running'
    RETURNING j.id, i.token, i.result
),
eff AS (
    INSERT INTO job_effects (job_id, lease_token, consumer_id, result)
    SELECT id, token, %(consumer)s, result FROM done
),
att AS (
    UPDATE job_attempts a
       SET outcome = CASE WHEN d.token IS NULL THEN 'fenced' ELSE 'succeeded' END,
           error = CASE WHEN d.token IS NULL THEN 'late completion rejected: lease lost' ELSE a.error END,
           finished_at = now()
      FROM input i LEFT JOIN done d ON d.token = i.token
     WHERE a.lease_token = i.token
       AND (a.outcome IS NULL OR a.outcome IN ('lease_expired', 'owner_restarted'))
)
SELECT token FROM done
"""


def complete_many(
    conn: psycopg.Connection, consumer_id: str,
    items: list[tuple[ClaimedJob, dict[str, Any] | None]],
) -> set[uuid.UUID]:
    """Commit success + side effect for each job whose lease we still hold.
    Returns the lease tokens that committed; any other token was fenced."""
    if not items:
        return set()
    rows = conn.execute(
        _COMPLETE_SQL,
        {
            "ids": [j.id for j, _ in items],
            "tokens": [j.lease_token for j, _ in items],
            "results": [Jsonb(r or {}) for _, r in items],
            "consumer": consumer_id,
        },
    ).fetchall()
    return {r["token"] for r in rows}


def complete(
    conn: psycopg.Connection, job: ClaimedJob, consumer_id: str, result: dict[str, Any] | None
) -> bool:
    """Commit success + side effect atomically, iff we still hold the lease. False = fenced."""
    return job.lease_token in complete_many(conn, consumer_id, [(job, result)])


def retry_backoff(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Exponential backoff with +-20% jitter: 1s, 2s, 4s, ... capped."""
    return min(base * 2 ** (attempt - 1), cap) * random.uniform(0.8, 1.2)


def fail(
    conn: psycopg.Connection, job: ClaimedJob, error: str, *, retryable: bool = True,
    outcome: str = "failed", backoff_s: float | None = None,
) -> str:
    """Record a failed attempt. Returns the job's new status ('scheduled' | 'dead') or 'fenced'."""
    backoff = retry_backoff(job.attempt) if backoff_s is None else backoff_s
    with conn.transaction():
        row = conn.execute(
            """
            UPDATE jobs
               SET status = CASE WHEN attempts >= max_attempts OR NOT %(retryable)s
                                 THEN 'dead' ELSE 'scheduled' END::job_status,
                   run_at = CASE WHEN attempts >= max_attempts OR NOT %(retryable)s
                                 THEN run_at ELSE now() + %(backoff)s * interval '1 second' END,
                   finished_at = CASE WHEN attempts >= max_attempts OR NOT %(retryable)s
                                      THEN now() END,
                   last_error = %(error)s,
                   lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
             WHERE id = %(id)s AND lease_token = %(token)s AND status = 'running'
            RETURNING status::text AS status
            """,
            {"id": job.id, "token": job.lease_token, "retryable": retryable,
             "backoff": backoff, "error": error[:4000]},
        ).fetchone()
        if row is None:
            _close_attempt(conn, job.lease_token, "fenced", f"late failure rejected: {error[:500]}")
            return "fenced"
        _close_attempt(conn, job.lease_token, outcome, error[:4000])
    return row["status"]


def _close_attempt(conn: psycopg.Connection, token: uuid.UUID, outcome: str, error: str | None) -> None:
    # An attempt's outcome is written once; the only refinement allowed is a recovered attempt
    # (lease_expired / owner_restarted) whose zombie later tried to commit (fenced).
    conn.execute(
        "UPDATE job_attempts SET outcome = %s, error = coalesce(%s, error), finished_at = now()"
        " WHERE lease_token = %s AND (outcome IS NULL OR outcome IN ('lease_expired', 'owner_restarted'))",
        (outcome, error, token),
    )


# --- leases -----------------------------------------------------------------------------------


def heartbeat(
    conn: psycopg.Connection, tokens: list[uuid.UUID], lease_seconds: float
) -> set[uuid.UUID]:
    """Extend our leases. Returns the tokens we still hold; anything missing was reaped."""
    if not tokens:
        return set()
    rows = conn.execute(
        """
        UPDATE jobs SET lease_expires_at = now() + %s * interval '1 second'
         WHERE status = 'running' AND lease_token = ANY(%s)
        RETURNING lease_token
        """,
        (lease_seconds, tokens),
    ).fetchall()
    return {r["lease_token"] for r in rows}


def reap(conn: psycopg.Connection, limit: int = 500) -> list[dict[str, Any]]:
    """Recover jobs whose lease expired (owner crashed / hung / partitioned).

    Any consumer may run this concurrently; SKIP LOCKED keeps reapers from colliding.
    """
    return _recover(
        conn, "lease_expires_at < now()", {}, limit,
        reason="lease expired; consumer presumed dead: ", outcome="lease_expired",
    )


def release_previous_incarnations(
    conn: psycopg.Connection, name: str, current_id: str
) -> list[dict[str, Any]]:
    """On startup, a consumer with a stable name ('consumer-3', a StatefulSet pod name, ...)
    immediately recovers jobs still leased to its earlier, now-dead incarnations ('consumer-3#<pid>')
    instead of waiting for their leases to expire. Even if the name were accidentally shared by
    a live process, fencing guarantees that process can no longer commit — the cost is only
    wasted work, never a duplicate."""
    return _recover(
        conn, "split_part(lease_owner, '#', 1) = %(name)s AND lease_owner <> %(me)s",
        {"name": name, "me": current_id}, 10_000,
        reason="owner restarted; released by its successor: ", outcome="owner_restarted",
    )


def _recover(
    conn: psycopg.Connection, where: str, params: dict[str, Any], limit: int, *,
    reason: str, outcome: str,
) -> list[dict[str, Any]]:
    """Put 'running' jobs matching `where` back to 'scheduled' (or 'dead' when out of attempts)."""
    return conn.execute(
        f"""
        WITH expired AS (
            SELECT id, lease_token, lease_owner FROM jobs
             WHERE status = 'running' AND ({where})
             ORDER BY lease_expires_at
             LIMIT %(limit)s
             FOR UPDATE SKIP LOCKED
        ),
        upd AS (
            UPDATE jobs j
               SET status = CASE WHEN j.attempts >= j.max_attempts THEN 'dead'
                                 ELSE 'scheduled' END::job_status,
                   finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
                   last_error = %(reason)s || e.lease_owner,
                   lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
              FROM expired e
             WHERE j.id = e.id
            RETURNING j.id, j.status::text AS status, j.attempts, e.lease_owner, e.lease_token
        ),
        att AS (
            UPDATE job_attempts a
               SET outcome = %(outcome)s, finished_at = now(), error = %(reason)s || upd.lease_owner
              FROM upd WHERE a.lease_token = upd.lease_token
        )
        SELECT id, status, attempts, lease_owner FROM upd
        """,
        {**params, "limit": limit, "reason": reason, "outcome": outcome},
    ).fetchall()


def seconds_until_next_due(conn: psycopg.Connection) -> float | None:
    row = conn.execute(
        "SELECT extract(epoch FROM min(run_at) - now())::float8 AS s FROM jobs WHERE status = 'scheduled'"
    ).fetchone()
    return row["s"] if row else None


# --- consumer registry (observability) ----------------------------------------------------------


def register_consumer(conn: psycopg.Connection, consumer_id: str, slots: int) -> None:
    conn.execute(
        """
        INSERT INTO consumers (id, hostname, pid, slots) VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET pid = EXCLUDED.pid, started_at = now(),
               last_heartbeat_at = now(), stopped_at = NULL
        """,
        (consumer_id, socket.gethostname(), os.getpid(), slots),
    )


def touch_consumer(conn: psycopg.Connection, consumer_id: str, inflight: int) -> None:
    conn.execute(
        "UPDATE consumers SET last_heartbeat_at = now(), inflight = %s WHERE id = %s",
        (inflight, consumer_id),
    )


def consumer_stopped(conn: psycopg.Connection, consumer_id: str) -> None:
    conn.execute(
        "UPDATE consumers SET stopped_at = now(), inflight = 0 WHERE id = %s", (consumer_id,)
    )
