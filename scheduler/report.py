"""Read-only queries used by `python -m scheduler stats` and the demo's verification report."""
from __future__ import annotations

from typing import Any, Sequence

import psycopg


def integrity(conn: psycopg.Connection) -> dict[str, int]:
    """Exactly-once accounting. Healthy system: pending=0 (when drained), duplicate_effects=0,
    missing_effects=0, effects_on_unsucceeded=0."""
    return conn.execute(
        """
        SELECT
          (SELECT count(*) FROM jobs)                                         AS total,
          (SELECT count(*) FROM jobs WHERE status = 'succeeded')              AS succeeded,
          (SELECT count(*) FROM jobs WHERE status = 'dead')                   AS dead,
          (SELECT count(*) FROM jobs WHERE status IN ('scheduled','running')) AS pending,
          (SELECT count(*) FROM job_effects)                                  AS effects,
          (SELECT count(*) FROM (SELECT job_id FROM job_effects
                                 GROUP BY job_id HAVING count(*) > 1) d)      AS duplicate_effects,
          (SELECT count(*) FROM jobs j WHERE j.status = 'succeeded' AND NOT EXISTS
                 (SELECT 1 FROM job_effects e WHERE e.job_id = j.id))         AS missing_effects,
          (SELECT count(*) FROM job_effects e JOIN jobs j ON j.id = e.job_id
            WHERE j.status <> 'succeeded')                                    AS effects_on_unsucceeded
        """
    ).fetchone()


def status_by_org(conn: psycopg.Connection) -> list[dict[str, Any]]:
    return conn.execute(
        """
        SELECT o.id AS org, o.max_concurrency AS cap,
               count(j.*) FILTER (WHERE j.status = 'scheduled') AS scheduled,
               count(j.*) FILTER (WHERE j.status = 'running')   AS running,
               count(j.*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
               count(j.*) FILTER (WHERE j.status = 'dead')      AS dead
        FROM organizations o LEFT JOIN jobs j ON j.org_id = o.id
        GROUP BY o.id ORDER BY o.id
        """
    ).fetchall()


def drift_by_org(conn: psycopg.Connection, where: str = "TRUE") -> list[dict[str, Any]]:
    """Schedule drift = first pickup - requested time. `where` filters jobs (trusted SQL only)."""
    return conn.execute(
        f"""
        SELECT org_id AS org, count(*) AS jobs,
               round(percentile_cont(0.50) WITHIN GROUP (ORDER BY d)::numeric, 3) AS p50_s,
               round(percentile_cont(0.95) WITHIN GROUP (ORDER BY d)::numeric, 3) AS p95_s,
               round(max(d)::numeric, 3) AS max_s,
               count(*) FILTER (WHERE d < 0) AS started_early
        FROM (SELECT org_id, extract(epoch FROM first_started_at - scheduled_at) AS d
                FROM jobs WHERE first_started_at IS NOT NULL AND ({where})) x
        GROUP BY org_id ORDER BY org_id
        """
    ).fetchall()


def attempts_by_outcome(conn: psycopg.Connection) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT coalesce(outcome, 'in_flight') AS outcome, count(*) AS attempts"
        " FROM job_attempts GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()


def consumers(conn: psycopg.Connection) -> list[dict[str, Any]]:
    return conn.execute(
        """
        SELECT id, pid, slots, inflight,
               round(extract(epoch FROM now() - last_heartbeat_at)::numeric, 1) AS heartbeat_age_s,
               CASE WHEN stopped_at IS NOT NULL THEN 'stopped'
                    WHEN now() - last_heartbeat_at > interval '15 seconds' THEN 'dead?'
                    ELSE 'alive' END AS state
        FROM consumers ORDER BY started_at
        """
    ).fetchall()


def table(rows: Sequence[dict[str, Any]], title: str | None = None) -> str:
    if not rows:
        return (f"{title}\n" if title else "") + "  (none)"
    cols = list(rows[0].keys())
    widths = {c: max(len(str(c)), *(len(_fmt(r[c])) for r in rows)) for c in cols}
    line = "  " + "  ".join(str(c).ljust(widths[c]) for c in cols)
    sep = "  " + "  ".join("-" * widths[c] for c in cols)
    body = ["  " + "  ".join(_fmt(r[c]).ljust(widths[c]) for c in cols) for r in rows]
    return "\n".join(([title] if title else []) + [line, sep, *body])


def _fmt(v: Any) -> str:
    return "-" if v is None else str(v)


def print_stats(conn: psycopg.Connection) -> None:
    print(table(status_by_org(conn), "Jobs by org"))
    print()
    print(table(drift_by_org(conn), "Schedule drift (first pickup - scheduled_at)"))
    print()
    print(table(attempts_by_outcome(conn), "Attempts by outcome"))
    print()
    print(table([integrity(conn)], "Exactly-once accounting"))
    print()
    print(table(consumers(conn), "Consumers"))
