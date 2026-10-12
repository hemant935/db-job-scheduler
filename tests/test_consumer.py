"""Consumer integration: several consumers drain a mixed workload exactly once, honouring lanes."""
from __future__ import annotations

import threading
import time

from scheduler import db, report, store
from scheduler.config import ConsumerConfig
from scheduler.consumer import Consumer


def _run_until_drained(consumers: list[Consumer], conn, timeout: float = 30) -> None:
    threads = [threading.Thread(target=c.run, daemon=True) for c in consumers]
    for t in threads:
        t.start()
    deadline = time.monotonic() + timeout
    while report.integrity(conn)["pending"] and time.monotonic() < deadline:
        time.sleep(0.1)
    for c in consumers:
        c.stop()
    for t in threads:
        t.join(timeout=10)


def test_consumers_drain_mixed_workload_exactly_once(conn, dsn):
    for org in ("a", "b", "c"):
        store.upsert_org(conn, org, max_concurrency=6)
    jobs = [{"org_id": o, "job_type": "email.send", "payload": {"ms": 20}} for o in "abc" for _ in range(40)]
    jobs += [{"org_id": "a", "job_type": "report.generate", "payload": {"seconds": 0.5}} for _ in range(4)]
    jobs += [{"org_id": "b", "job_type": "flaky", "payload": {"fail_rate": 0.5, "ms": 5}} for _ in range(5)]
    store.enqueue_many(conn, jobs)

    cfg = ConsumerConfig(slots=4, slow_slots=1, lease_seconds=5, heartbeat_seconds=1, poll_seconds=0.1)
    _run_until_drained([Consumer(cfg, f"t{i}", dsn=dsn) for i in range(3)], conn)

    stats = report.integrity(conn)
    assert stats["pending"] == 0
    assert stats["duplicate_effects"] == 0 and stats["missing_effects"] == 0
    assert stats["succeeded"] + stats["dead"] == stats["total"] == len(jobs)
    assert stats["effects"] == stats["succeeded"]


def test_timeout_reschedules_and_fences_late_result(conn, dsn):
    store.upsert_org(conn, "a")
    # 2s job with a 1s timeout and a single attempt -> dead; its late completion is fenced.
    jid = store.enqueue(conn, "a", "report.generate", payload={"seconds": 2}, timeout_s=1, max_attempts=1)
    cfg = ConsumerConfig(slots=2, slow_slots=1, lease_seconds=5, heartbeat_seconds=1, poll_seconds=0.1)
    _run_until_drained([Consumer(cfg, "t", dsn=dsn)], conn, timeout=10)

    row = conn.execute("SELECT status, last_error FROM jobs WHERE id = %s", (jid,)).fetchone()
    assert row["status"] == "dead" and "timeout" in row["last_error"]
    assert report.integrity(conn)["effects"] == 0
