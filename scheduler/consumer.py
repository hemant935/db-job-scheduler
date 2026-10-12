"""Consumer process: pulls due jobs from Postgres and runs them on a pool of worker threads.

Threads in one consumer process:
  main       claim loop: reap expired leases -> enforce timeouts -> claim into free slots ->
             sleep until (next job due | NOTIFY | a slot frees up | poll interval)
  heartbeat  renews the leases of every in-flight job; detects leases we have lost
  listener   LISTEN jobs_due -> wakes the main loop the moment new work is inserted
  worker-N   execute handlers, then hand the result to the committer (or record a failure)
  committer  group commit: completes every result that piled up since its last commit in ONE
             statement (fencing checked per row), so commits/s stay flat as jobs/s grow

The DB work is tiny and I/O-bound, so it never competes meaningfully with handlers for the GIL;
CPU-heavy handlers still yield every 5ms (sys.getswitchinterval), so heartbeats keep flowing.
"""
from __future__ import annotations

import json
import logging
import os
import queue
import signal
import socket
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg

from scheduler import db, handlers, store
from scheduler.config import ConsumerConfig
from scheduler.faults import FaultInjector
from scheduler.store import ClaimedJob

log = logging.getLogger("scheduler.consumer")


@dataclass
class InFlight:
    job: ClaimedJob
    started: float = field(default_factory=time.monotonic)
    cancel: threading.Event = field(default_factory=threading.Event)
    # Set when we gave the job up (timeout / lease lost). The worker thread may still be running
    # (Python threads can't be killed), but whatever it tries to commit will be fenced.
    abandoned: bool = False
    finishing: bool = False


def _check_storable(result: object) -> None:
    """Fail a job whose result can't be stored as JSON here, on its own worker, instead of letting
    it break the committer's batch. Deterministic, so retrying is pointless: permanent."""
    try:
        json.dumps(result)
    except (TypeError, ValueError) as e:
        raise handlers.PermanentError(f"handler returned a non-JSON result: {e}") from None


@dataclass
class _PendingCommit:
    job: ClaimedJob
    result: dict | None
    done: Future = field(default_factory=Future)  # -> True committed / False fenced


class Consumer:
    def __init__(self, cfg: ConsumerConfig, name: str | None = None, dsn: str | None = None):
        """`name` is an optional stable identity (e.g. a StatefulSet pod name) that must be unique
        among live consumers. With it, a restarted consumer immediately recovers the jobs its
        crashed predecessor held instead of waiting for their leases to expire."""
        self.cfg = cfg
        self.name = name
        self.id = f"{name}#{os.getpid()}" if name else f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:4]}"
        self._dsn = dsn
        # Main loop, heartbeat and committer each hold one briefly; workers use the rest for failures.
        self.pool = db.make_pool(max_size=min(cfg.slots, 4) + 3, name=self.id, url=dsn)
        self._executor = ThreadPoolExecutor(max_workers=cfg.slots, thread_name_prefix="worker")
        self._faults = FaultInjector(cfg.faults)
        self._inflight: dict[uuid.UUID, InFlight] = {}
        self._lock = threading.Lock()
        self._wake = threading.Event()        # something changed: re-run the claim loop now
        self._stopping = threading.Event()    # stop claiming new work
        self._bg_stop = threading.Event()     # stop heartbeat/listener/committer (after drain)
        self._commits: queue.Queue[_PendingCommit] = queue.Queue()

    # --- lifecycle ----------------------------------------------------------------------------

    def run(self) -> None:
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, self._on_signal)
            signal.signal(signal.SIGINT, self._on_signal)
        with self.pool.connection() as conn:
            store.register_consumer(conn, self.id, self.cfg.slots)
            if self.name:
                for r in store.release_previous_incarnations(conn, self.name, self.id):
                    log.warning(
                        "recovered job=%d from my previous incarnation %s -> %s",
                        r["id"], r["lease_owner"], r["status"],
                    )
        for target in (self._heartbeat_loop, self._listen_loop, self._commit_loop):
            threading.Thread(target=target, name=target.__name__, daemon=True).start()
        log.info(
            "consumer %s started pid=%d slots=%d slow_slots=%d lease=%.1fs faults=%s",
            self.id, os.getpid(), self.cfg.slots, self.cfg.slow_slots, self.cfg.lease_seconds,
            self.cfg.faults or "none",
        )

        last_reap = 0.0
        while not self._stopping.is_set():
            self._wake.clear()
            try:
                if time.monotonic() - last_reap >= self.cfg.reaper_seconds:
                    self._reap()
                    last_reap = time.monotonic()
                self._enforce_timeouts()
                if self._claim_and_dispatch():
                    continue  # claimed and still have free slots: there may be more due work
                self._wake.wait(self._idle_wait())
            except psycopg.OperationalError as e:
                # DB unreachable: keep going. If this outlasts our leases, other consumers (or we,
                # once reconnected) reap and re-run the jobs — correctness never depends on us.
                log.warning("db error in claim loop (%s); retrying in 1s", e)
                self._stopping.wait(1.0)
        self._drain()

    def stop(self) -> None:
        self._stopping.set()
        self._wake.set()

    def _on_signal(self, signum: int, _frame: object) -> None:
        if self._stopping.is_set():
            log.warning("second signal: exiting immediately")
            os._exit(130)
        log.info("received %s: no new claims, draining in-flight jobs", signal.Signals(signum).name)
        self.stop()

    def _drain(self) -> None:
        deadline = time.monotonic() + self.cfg.drain_seconds
        while self.inflight_count() and time.monotonic() < deadline:
            self._wake.wait(0.2)
            self._wake.clear()
        left = self.inflight_count()
        if left:
            log.warning("drain timeout: exiting with %d unfinished job(s); their leases will expire", left)
        self._bg_stop.set()
        try:
            with self.pool.connection() as conn:
                store.consumer_stopped(conn, self.id)
        except psycopg.Error:
            pass
        self._executor.shutdown(wait=not left, cancel_futures=True)
        self.pool.close()
        log.info("consumer %s stopped", self.id)

    def inflight_count(self) -> int:
        with self._lock:
            return len(self._inflight)

    # --- main loop steps ----------------------------------------------------------------------

    def _claim_and_dispatch(self) -> bool:
        with self._lock:
            busy = len(self._inflight)
            busy_slow = sum(1 for f in self._inflight.values() if f.job.lane == "slow")
        free = self.cfg.slots - busy
        if free <= 0:
            return False
        claimed: list[ClaimedJob] = []
        with self.pool.connection() as conn:
            # Slow lane first but bounded by slow_slots, so `slots - slow_slots` slots are always
            # reserved for fast jobs: a burst of multi-minute jobs can never block short ones.
            slow_quota = min(self.cfg.slow_slots - busy_slow, free)
            if slow_quota > 0:
                claimed += store.claim(conn, self.id, "slow", slow_quota, self.cfg.lease_seconds)
            fast_quota = free - len(claimed)
            if fast_quota > 0:
                claimed += store.claim(conn, self.id, "fast", fast_quota, self.cfg.lease_seconds)
        for job in claimed:
            self._dispatch(job)
        return 0 < len(claimed) < free

    def _dispatch(self, job: ClaimedJob) -> None:
        self._faults.maybe_crash("after_claim", job.id)
        inf = InFlight(job)
        with self._lock:
            self._inflight[job.lease_token] = inf
        drift = (datetime.now(timezone.utc) - job.scheduled_at).total_seconds()
        log.info(
            "claimed job=%d org=%s type=%s lane=%s attempt=%d/%d drift=%.3fs",
            job.id, job.org_id, job.job_type, job.lane, job.attempt, job.max_attempts, drift,
        )
        self._executor.submit(self._execute, inf)

    def _idle_wait(self) -> float:
        """Sleep until the next job is due, bounded by poll_seconds (NOTIFY/slot-free wake us early)."""
        try:
            with self.pool.connection() as conn:
                secs = store.seconds_until_next_due(conn)
        except psycopg.Error:
            return self.cfg.poll_seconds
        if secs is None or secs <= 0:
            # Nothing pending, or due work we couldn't take (slots full / org at its cap):
            # a slot freeing up locally sets _wake; caps freeing elsewhere are caught by polling.
            return self.cfg.poll_seconds
        return min(self.cfg.poll_seconds, secs + 0.002)

    def _reap(self) -> None:
        with self.pool.connection() as conn:
            for r in store.reap(conn):
                log.warning(
                    "reaped job=%d from %s (lease expired) -> %s after %d attempt(s)",
                    r["id"], r["lease_owner"], r["status"], r["attempts"],
                )

    def _enforce_timeouts(self) -> None:
        now = time.monotonic()
        timed_out: list[InFlight] = []
        with self._lock:
            for inf in self._inflight.values():
                if not inf.abandoned and not inf.finishing and now - inf.started > inf.job.timeout_s:
                    inf.abandoned = True
                    inf.cancel.set()
                    timed_out.append(inf)
        for inf in timed_out:
            with self.pool.connection() as conn:
                status = store.fail(
                    conn, inf.job, f"timeout after {inf.job.timeout_s}s", outcome="timeout"
                )
            log.warning("timeout job=%d after %ds -> %s", inf.job.id, inf.job.timeout_s, status)

    # --- worker thread ------------------------------------------------------------------------

    def _execute(self, inf: InFlight) -> None:
        job = inf.job
        ctx = handlers.JobContext(
            job_id=job.id,
            org_id=job.org_id,
            attempt=job.attempt,
            idempotency_key=job.idempotency_key or f"job-{job.id}",
            cancel=inf.cancel,
            on_midpoint=lambda: self._faults.maybe_crash("mid_exec", job.id),
        )
        t0 = time.monotonic()
        try:
            result = handlers.get(job.job_type)(ctx, job.payload)
            _check_storable(result)
            self._faults.maybe_crash("before_complete", job.id)
            inf.finishing = True
            pending = _PendingCommit(job, result)
            self._commits.put(pending)
            committed = pending.done.result()
            if committed:
                log.info("done job=%d org=%s in %.3fs", job.id, job.org_id, time.monotonic() - t0)
            else:
                log.warning("FENCED job=%d: lease lost, late result discarded", job.id)
        except handlers.Cancelled:
            log.info("job=%d cancelled (timed out or lease lost)", job.id)
        except Exception as e:  # handler error, or DB error while committing
            if not inf.abandoned:
                self._record_failure(job, e)
        finally:
            with self._lock:
                self._inflight.pop(job.lease_token, None)
            self._wake.set()

    def _record_failure(self, job: ClaimedJob, err: Exception) -> None:
        retryable = not isinstance(err, handlers.PermanentError)
        try:
            with self.pool.connection() as conn:
                status = store.fail(conn, job, f"{type(err).__name__}: {err}", retryable=retryable)
            log.warning(
                "failed job=%d attempt=%d/%d -> %s (%s)",
                job.id, job.attempt, job.max_attempts, status, err,
            )
        except psycopg.Error as db_err:
            # Can't even record the failure: the lease will expire and the reaper retries it.
            log.error("could not record failure for job=%d (%s); leaving it to the reaper", job.id, db_err)

    # --- background threads -------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        while not self._bg_stop.wait(self.cfg.heartbeat_seconds):
            try:
                self._heartbeat_once()
            except Exception:
                log.exception("heartbeat failed")

    def _heartbeat_once(self) -> None:
        with self._lock:
            tokens = [t for t, f in self._inflight.items() if not f.abandoned]
        with self.pool.connection() as conn:
            held = store.heartbeat(conn, tokens, self.cfg.lease_seconds)
            store.touch_consumer(conn, self.id, len(tokens))
        lost: list[int] = []
        with self._lock:
            for t in set(tokens) - held:
                inf = self._inflight.get(t)
                if inf and not inf.finishing:  # finishing jobs just left 'running' themselves
                    inf.abandoned = True
                    inf.cancel.set()
                    lost.append(inf.job.id)
        if lost:
            log.warning("lost lease on job(s) %s (reaped elsewhere); cancelling, results will be fenced", lost)

    def _commit_loop(self) -> None:
        """Group commit. No artificial delay: at low load a batch is one job; under load, results
        accumulate while the previous commit is in flight and the next batch is bigger."""
        while not self._bg_stop.is_set():
            try:
                batch = [self._commits.get(timeout=0.2)]
            except queue.Empty:
                continue
            while len(batch) < 500:
                try:
                    batch.append(self._commits.get_nowait())
                except queue.Empty:
                    break
            self._commit_batch(batch)

    def _commit_batch(self, batch: list[_PendingCommit]) -> None:
        try:
            with self.pool.connection() as conn:
                committed = store.complete_many(conn, self.id, [(p.job, p.result) for p in batch])
            for p in batch:
                p.done.set_result(p.job.lease_token in committed)
        except psycopg.OperationalError as e:
            # DB unreachable: the whole batch failed (or its outcome is unknown). Workers record
            # failures; anything that did commit fences those, so nothing runs twice by mistake.
            for p in batch:
                p.done.set_exception(e)
        except Exception as e:
            # A row the DB refused (e.g. a \u0000 in a result) aborts the whole statement. Commit
            # one by one so a single bad result fails only its own job, not up to 500 healthy ones.
            if len(batch) > 1:
                log.warning("batch commit of %d failed (%s); retrying one by one", len(batch), e)
                for p in batch:
                    self._commit_batch([p])
                return
            batch[0].done.set_exception(
                handlers.PermanentError(f"result rejected by the database: {type(e).__name__}: {e}")
            )

    def _listen_loop(self) -> None:
        while not self._bg_stop.is_set():
            try:
                with db.connect(self._dsn) as conn:
                    conn.execute("LISTEN jobs_due")
                    while not self._bg_stop.is_set():
                        for _ in conn.notifies(timeout=1.0):
                            self._wake.set()
            except psycopg.Error as e:
                log.warning("listener error (%s); reconnecting", e)
                self._bg_stop.wait(1.0)
