from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from scheduler.config import dsn

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
_MIGRATION_LOCK = 72_271_001  # arbitrary advisory-lock key; serialises concurrent migrators


def connect(url: str | None = None) -> psycopg.Connection:
    return psycopg.connect(url or dsn(), autocommit=True, row_factory=dict_row)


def make_pool(max_size: int, name: str, url: str | None = None) -> ConnectionPool:
    return ConnectionPool(
        url or dsn(),
        min_size=1,
        max_size=max_size,
        kwargs={"autocommit": True, "row_factory": dict_row},
        name=name,
        open=True,
    )


def migrate(conn: psycopg.Connection) -> list[str]:
    """Apply pending migrations/*.sql in order. Safe to call from many processes at once."""
    applied_now: list[str] = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATION_LOCK,))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
        done = {r["name"] for r in conn.execute("SELECT name FROM schema_migrations")}
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if path.name in done:
                continue
            conn.execute(path.read_text())
            conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,))
            applied_now.append(path.name)
    return applied_now


def reset(conn: psycopg.Connection) -> None:
    """Wipe all scheduler data (keeps the schema)."""
    conn.execute(
        "TRUNCATE job_effects, job_attempts, jobs, consumers, organizations RESTART IDENTITY CASCADE"
    )
