"""Store-level invariants: exclusive claims, fencing, lease expiry, fairness, scheduling."""
from __future__ import annotations

import threading
from collections import Counter
from datetime import datetime, timedelta, timezone

from scheduler import db, report, store


def _expire_all_leases(conn) -> None:
    conn.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE status = 'running'")


def test_concurrent_claims_never_overlap(conn, dsn):
    """16 threads (each its own DB session) race to claim 3000 jobs: every job claimed exactly once."""
    store.upsert_org(conn, "a", max_concurrency=100_000)
    store.upsert_org(conn, "b", max_concurrency=100_000)
    store.enqueue_many(conn, [{"org_id": "ab"[i % 2], "job_type": "noop"} for i in range(3000)])

    claimed: list[int] = []
    lock = threading.Lock()

    def worker(n: int) -> None:
        with db.connect(dsn) as c:
            while batch := store.claim(c, f"w{n}", "fast", 7, 30):
                with lock:
                    claimed.extend(j.id for j in batch)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(claimed) == 3000
    assert len(set(claimed)) == 3000, "a job was claimed by two consumers"
    assert conn.execute("SELECT count(*) AS n FROM job_attempts").fetchone()["n"] == 3000


def test_zombie_worker_is_fenced(conn):
    """A consumer that stalls past its lease cannot commit after the job was re-granted."""
    store.upsert_org(conn, "a")
    jid = store.enqueue(conn, "a", "noop")
    (zombie,) = store.claim(conn, "zombie", "fast", 1, 30)

    _expire_all_leases(conn)  # zombie stalls (GC pause / partition) past its lease
    assert [r["id"] for r in store.reap(conn)] == [jid]
    (rescuer,) = store.claim(conn, "rescuer", "fast", 1, 30)
    assert rescuer.id == jid and rescuer.attempt == 2
    assert rescuer.lease_token != zombie.lease_token

    assert store.complete(conn, rescuer, "rescuer", {"by": "rescuer"}) is True
    assert store.complete(conn, zombie, "zombie", {"by": "zombie"}) is False  # fenced
    assert store.fail(conn, zombie, "late error") == "fenced"

    effects = conn.execute("SELECT consumer_id FROM job_effects WHERE job_id = %s", (jid,)).fetchall()
    assert [e["consumer_id"] for e in effects] == ["rescuer"]
    outcomes = dict(
        (r["consumer_id"], r["outcome"])
        for r in conn.execute("SELECT consumer_id, outcome FROM job_attempts").fetchall()
    )
    assert outcomes == {"zombie": "fenced", "rescuer": "succeeded"}


def test_lease_renewal_prevents_reaping(conn):
    store.upsert_org(conn, "a")
    store.enqueue(conn, "a", "noop")
    (job,) = store.claim(conn, "c", "fast", 1, 0.5)
    assert store.heartbeat(conn, [job.lease_token], 30) == {job.lease_token}
    assert store.reap(conn) == []


def test_crashed_job_dead_letters_after_max_attempts(conn):
    """A job that kills every consumer that touches it must not loop forever."""
    store.upsert_org(conn, "a")
    jid = store.enqueue(conn, "a", "noop", max_attempts=3)
    for attempt in range(1, 4):
        (job,) = store.claim(conn, f"c{attempt}", "fast", 1, 30)
        assert job.attempt == attempt
        _expire_all_leases(conn)
        (reaped,) = store.reap(conn)
    assert reaped["status"] == "dead"
    assert store.claim(conn, "c4", "fast", 1, 30) == []
    assert conn.execute("SELECT status FROM jobs WHERE id = %s", (jid,)).fetchone()["status"] == "dead"


def test_failure_retries_with_backoff_then_dead_letters(conn):
    store.upsert_org(conn, "a")
    store.enqueue(conn, "a", "flaky", max_attempts=2)
    (job,) = store.claim(conn, "c", "fast", 1, 30)
    assert store.fail(conn, job, "boom", backoff_s=60) == "scheduled"
    assert store.claim(conn, "c", "fast", 1, 30) == []  # backing off, not eligible yet
    conn.execute("UPDATE jobs SET run_at = now()")
    (job,) = store.claim(conn, "c", "fast", 1, 30)
    assert store.fail(conn, job, "boom again") == "dead"


def test_permanent_error_skips_retries(conn):
    store.upsert_org(conn, "a")
    store.enqueue(conn, "a", "noop", max_attempts=5)
    (job,) = store.claim(conn, "c", "fast", 1, 30)
    assert store.fail(conn, job, "bad payload", retryable=False) == "dead"


def test_future_jobs_are_not_claimed_early(conn):
    store.upsert_org(conn, "a")
    store.enqueue(conn, "a", "noop", scheduled_at=datetime.now(timezone.utc) + timedelta(seconds=30))
    assert store.claim(conn, "c", "fast", 10, 30) == []
    assert 25 < store.seconds_until_next_due(conn) <= 30


def test_fair_claim_interleaves_orgs(conn):
    """A flood from one org does not push a small org to the back of the line."""
    for org in ("big", "small1", "small2"):
        store.upsert_org(conn, org, max_concurrency=100)
    store.enqueue_many(conn, [{"org_id": "big", "job_type": "noop"} for _ in range(500)])
    store.enqueue_many(conn, [{"org_id": o, "job_type": "noop"} for o in ("small1", "small2") for _ in range(3)])

    batch = store.claim(conn, "c", "fast", 9, 30)
    assert Counter(j.org_id for j in batch) == {"big": 3, "small1": 3, "small2": 3}


def test_org_concurrency_cap(conn):
    store.upsert_org(conn, "capped", max_concurrency=2)
    store.upsert_org(conn, "other", max_concurrency=50)
    store.enqueue_many(conn, [{"org_id": o, "job_type": "noop"} for o in ("capped", "other") for _ in range(20)])

    first = store.claim(conn, "c", "fast", 10, 30)
    assert Counter(j.org_id for j in first) == {"capped": 2, "other": 8}
    second = store.claim(conn, "c", "fast", 10, 30)
    assert {j.org_id for j in second} == {"other"}  # capped org is full until one finishes
    store.complete(conn, next(j for j in first if j.org_id == "capped"), "c", None)
    third = store.claim(conn, "c", "fast", 10, 30)
    assert Counter(j.org_id for j in third)["capped"] == 1


def test_weights_bias_share(conn):
    store.upsert_org(conn, "gold", weight=3, max_concurrency=100)
    store.upsert_org(conn, "free", weight=1, max_concurrency=100)
    store.enqueue_many(conn, [{"org_id": o, "job_type": "noop"} for o in ("gold", "free") for _ in range(50)])
    batch = store.claim(conn, "c", "fast", 8, 30)
    assert Counter(j.org_id for j in batch) == {"gold": 6, "free": 2}


def test_lanes_are_separate(conn):
    store.upsert_org(conn, "a")
    store.enqueue(conn, "a", "report.generate")  # registered as slow lane
    store.enqueue(conn, "a", "email.send")
    assert [j.job_type for j in store.claim(conn, "c", "slow", 5, 30)] == ["report.generate"]
    assert [j.job_type for j in store.claim(conn, "c", "fast", 5, 30)] == ["email.send"]


def test_idempotent_enqueue(conn):
    store.upsert_org(conn, "a")
    first = store.enqueue(conn, "a", "noop", idempotency_key="invoice-42")
    assert first is not None
    assert store.enqueue(conn, "a", "noop", idempotency_key="invoice-42") is None
    assert report.integrity(conn)["total"] == 1


def test_restarted_consumer_reclaims_its_predecessors_jobs(conn):
    """A supervised restart recovers stranded jobs immediately rather than after lease expiry."""
    store.upsert_org(conn, "a")
    store.enqueue_many(conn, [{"org_id": "a", "job_type": "noop"} for _ in range(3)])
    old = store.claim(conn, "consumer-3#100", "fast", 2, 300)        # crashed incarnation
    other = store.claim(conn, "consumer-30#200", "fast", 1, 300)     # different consumer, same prefix

    released = store.release_previous_incarnations(conn, "consumer-3", "consumer-3#101")
    assert sorted(r["id"] for r in released) == sorted(j.id for j in old)
    assert store.heartbeat(conn, [other[0].lease_token], 300) == {other[0].lease_token}  # untouched
    assert all(store.complete(conn, j, "consumer-3#100", None) is False for j in old)    # fenced
    assert len(store.claim(conn, "consumer-3#101", "fast", 5, 300)) == 2
