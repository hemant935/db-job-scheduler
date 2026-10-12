"""Throughput benchmark: how fast can N consumer processes drain a backlog?

    uv run python demo/bench.py --jobs 20000 --consumers 1 2 4 8
    uv run python demo/bench.py --jobs 5000 --consumers 1 2 4 --job-ms 50   # I/O-bound jobs

With --job-ms 0 (default) jobs are no-ops, so this measures pure scheduler overhead: the claim
query + the per-job completion transaction. That is the number that tells you where the database
becomes the bottleneck.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scheduler import db, report, store  # noqa: E402


def run_once(n_consumers: int, jobs: int, slots: int, orgs: int, job_ms: int) -> dict:
    conn = db.connect()
    db.migrate(conn)
    db.reset(conn)
    for i in range(orgs):
        store.upsert_org(conn, f"org{i}", max_concurrency=100_000)
    payload = {"ms": job_ms} if job_ms else {}
    store.enqueue_many(conn, ({"org_id": f"org{i % orgs}", "job_type": "noop", "payload": payload}
                              for i in range(jobs)))
    conn.execute("VACUUM ANALYZE jobs")

    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "scheduler", "consumer", "--id", f"bench-{i}",
             "--slots", str(slots), "--slow-slots", "0", "--lease", "30", "--heartbeat", "5"],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env={**os.environ, "LOG_LEVEL": "WARNING"},
        )
        for i in range(n_consumers)
    ]
    t0 = time.monotonic()
    done_sql = "SELECT count(*) AS n FROM jobs WHERE status = 'succeeded'"
    while conn.execute(done_sql).fetchone()["n"] < jobs:
        time.sleep(0.5)
        if time.monotonic() - t0 > 600:
            raise TimeoutError("benchmark did not finish in 10 minutes")
    for p in procs:
        p.send_signal(signal.SIGTERM)
    for p in procs:
        p.wait(timeout=30)

    row = conn.execute(
        """
        SELECT extract(epoch FROM max(finished_at) - min(first_started_at))::float8 AS secs FROM jobs
        """
    ).fetchone()
    integ = report.integrity(conn)
    conn.close()
    return {
        "consumers": n_consumers,
        "threads": n_consumers * slots,
        "jobs": jobs,
        "seconds": round(row["secs"], 2),
        "jobs_per_s": round(jobs / row["secs"]),
        "per_consumer": round(jobs / row["secs"] / n_consumers),
        "duplicates": integ["duplicate_effects"],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jobs", type=int, default=20_000)
    ap.add_argument("--consumers", type=int, nargs="+", default=[1, 2, 4, 8])
    ap.add_argument("--slots", type=int, default=16, help="worker threads per consumer")
    ap.add_argument("--orgs", type=int, default=10)
    ap.add_argument("--job-ms", type=int, default=0, help="simulated work per job (0 = no-op)")
    a = ap.parse_args()

    print(f"{a.jobs} jobs across {a.orgs} orgs, {a.slots} threads/consumer, job work = {a.job_ms}ms\n")
    rows = []
    for n in a.consumers:
        rows.append(run_once(n, a.jobs, a.slots, a.orgs, a.job_ms))
        print(f"  {n} consumer(s): {rows[-1]['jobs_per_s']} jobs/s")
    print()
    print(report.table(rows))


if __name__ == "__main__":
    main()
