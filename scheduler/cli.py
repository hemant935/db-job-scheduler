"""python -m scheduler <command> ..."""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

from scheduler import db, faults, handlers, report, store
from scheduler.config import ConsumerConfig


def _setup_logging(consumer_id: str | None = None) -> None:
    prefix = f"[{consumer_id}] " if consumer_id else ""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s.%(msecs)03d {prefix}%(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("psycopg.pool").setLevel(logging.WARNING)


def cmd_migrate(_: argparse.Namespace) -> None:
    with db.connect() as conn:
        applied = db.migrate(conn)
    print("applied:", ", ".join(applied) if applied else "nothing (up to date)")


def cmd_reset(_: argparse.Namespace) -> None:
    with db.connect() as conn:
        db.migrate(conn)
        db.reset(conn)
    print("all scheduler data wiped")


def cmd_org(a: argparse.Namespace) -> None:
    with db.connect() as conn:
        store.upsert_org(conn, a.id, name=a.name, weight=a.weight, max_concurrency=a.max_concurrency)
    print(f"org {a.id}: weight={a.weight} max_concurrency={a.max_concurrency}")


def cmd_enqueue(a: argparse.Namespace) -> None:
    payload = json.loads(a.payload)
    now = datetime.now(timezone.utc)
    jobs = [
        {
            "org_id": a.org,
            "job_type": a.type,
            "payload": payload,
            "scheduled_at": now + timedelta(seconds=a.in_seconds + random.uniform(0, a.spread)),
            "max_attempts": a.max_attempts,
            "timeout_s": a.timeout,
        }
        for _ in range(a.count)
    ]
    with db.connect() as conn:
        if a.create_org:
            store.ensure_org(conn, a.org)
        n = store.enqueue_many(conn, jobs)
    print(f"enqueued {n} x {a.type} for {a.org} due in {a.in_seconds}s (+0..{a.spread}s)")


def cmd_consumer(a: argparse.Namespace) -> None:
    from scheduler.consumer import Consumer

    cfg = ConsumerConfig(
        slots=a.slots, slow_slots=a.slow_slots, lease_seconds=a.lease,
        heartbeat_seconds=a.heartbeat, poll_seconds=a.poll, reaper_seconds=a.reaper,
        drain_seconds=a.drain, faults=faults.parse(a.fault),
    )
    _setup_logging(a.id)
    with db.connect() as conn:
        db.migrate(conn)
    c = Consumer(cfg, name=a.id)
    c.run()
    # Worker threads that ignored cancellation would block interpreter exit; leases cover them.
    sys.stdout.flush()
    os._exit(0)


def cmd_stats(a: argparse.Namespace) -> None:
    while True:
        with db.connect() as conn:
            if a.watch:
                print("\033[2J\033[H", end="")
            report.print_stats(conn)
        if not a.watch:
            return
        time.sleep(a.watch)


def cmd_dashboard(a: argparse.Namespace) -> None:
    from scheduler.dashboard import serve

    serve(a.port)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m scheduler", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("migrate", help="apply schema migrations").set_defaults(fn=cmd_migrate)
    sub.add_parser("reset", help="wipe all jobs/orgs (keeps schema)").set_defaults(fn=cmd_reset)

    o = sub.add_parser("org", help="create/update an organization")
    o.add_argument("id")
    o.add_argument("--name")
    o.add_argument("--weight", type=int, default=1)
    o.add_argument("--max-concurrency", type=int, default=10)
    o.set_defaults(fn=cmd_org)

    e = sub.add_parser("enqueue", help="enqueue jobs")
    e.add_argument("--org", required=True)
    e.add_argument("--type", required=True, choices=handlers.job_types())
    e.add_argument("--count", type=int, default=1)
    e.add_argument("--in-seconds", type=float, default=0, help="schedule this far in the future")
    e.add_argument("--spread", type=float, default=0, help="spread randomly over this many seconds")
    e.add_argument("--payload", default="{}", help="JSON payload")
    e.add_argument("--max-attempts", type=int, default=5)
    e.add_argument("--timeout", type=int, default=300, help="per-attempt timeout (s)")
    e.add_argument("--create-org", action="store_true", help="create the org if missing")
    e.set_defaults(fn=cmd_enqueue)

    c = sub.add_parser("consumer", help="run a consumer process")
    c.add_argument(
        "--id", help="stable consumer name, unique among live consumers (e.g. pod name); on restart "
        "it reclaims its predecessor's jobs immediately. Default: anonymous host-pid-rand")
    c.add_argument("--slots", type=int, default=8)
    c.add_argument("--slow-slots", type=int, default=4)
    c.add_argument("--lease", type=float, default=10.0)
    c.add_argument("--heartbeat", type=float, default=3.0)
    c.add_argument("--poll", type=float, default=0.5)
    c.add_argument("--reaper", type=float, default=2.0)
    c.add_argument("--drain", type=float, default=30.0)
    c.add_argument(
        "--fault", action="append", default=[],
        help=f"inject crashes, e.g. --fault mid_exec=0.05 (points: {', '.join(faults.POINTS)})",
    )
    c.set_defaults(fn=cmd_consumer)

    s = sub.add_parser("stats", help="print status, drift and integrity checks")
    s.add_argument("--watch", type=float, default=0, help="refresh every N seconds")
    s.set_defaults(fn=cmd_stats)

    d = sub.add_parser("dashboard", help="live web UI with buttons for every test scenario")
    d.add_argument("--port", type=int, default=8765)
    d.set_defaults(fn=cmd_dashboard)

    args = p.parse_args(argv)
    args.fn(args)
