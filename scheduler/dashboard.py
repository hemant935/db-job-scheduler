"""Local web dashboard: watch the scheduler live and drive every test scenario with buttons.

    python -m scheduler dashboard        # http://127.0.0.1:8765

Read-only views come straight from Postgres. The dashboard can also start / gracefully stop /
SIGKILL / restart consumer processes it launched itself (logs in demo/logs/ui-<name>.log).
Binds to 127.0.0.1 only: it can spawn and kill processes, so it is a local dev tool.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from scheduler import db, report, store

ROOT = Path(__file__).resolve().parent.parent
STATIC = Path(__file__).resolve().parent / "static"
LOG_DIR = ROOT / "demo" / "logs"
DEMO_ORGS = {"acme": 5, "beta": 5, "gamma": 5}  # id -> max_concurrency

log = logging.getLogger("scheduler.dashboard")


# --- consumer process management --------------------------------------------------------------


class Fleet:
    def __init__(self) -> None:
        self.procs: dict[str, subprocess.Popen] = {}
        self.faults: dict[str, list[str]] = {}
        self.killed: set[str] = set()   # names we SIGKILLed (vs. exited on their own)
        self._lock = threading.Lock()

    def start(self, name: str | None = None, faults: list[str] | None = None) -> str:
        with self._lock:
            if name is None:
                n = 1
                while f"consumer-{n}" in self.procs:
                    n += 1
                name = f"consumer-{n}"
            if name in self.procs and self.procs[name].poll() is None:
                raise ValueError(f"{name} is already running")
            if faults is not None:
                self.faults[name] = faults
            args = [sys.executable, "-u", "-m", "scheduler", "consumer", "--id", name,
                    "--slots", "6", "--slow-slots", "2"]
            for f in self.faults.get(name, []):
                args += ["--fault", f]
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            out = open(LOG_DIR / f"ui-{name}.log", "a")
            self.procs[name] = subprocess.Popen(args, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT)
            self.killed.discard(name)
            return name

    def signal(self, name: str, sig: signal.Signals) -> None:
        with self._lock:
            p = self.procs.get(name)
            if p is None or p.poll() is not None:
                raise ValueError(f"{name} is not running")
            if sig == signal.SIGKILL:
                self.killed.add(name)
            p.send_signal(sig)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            out = {}
            for name, p in self.procs.items():
                rc = p.poll()
                state = "running" if rc is None else (
                    "killed" if name in self.killed or rc == -signal.SIGKILL
                    else "crashed" if rc not in (0, None) else "stopped")
                out[name] = {"pid": p.pid, "state": state, "faults": self.faults.get(name, []),
                             "exit_code": rc}
            return out

    def shutdown(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.procs.values():
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()


# --- sampling (history for the charts) --------------------------------------------------------


class Sampler(threading.Thread):
    def __init__(self, pool) -> None:
        super().__init__(daemon=True)
        self.pool = pool
        self.samples: deque[dict[str, Any]] = deque(maxlen=180)  # 3 minutes at 1 Hz
        self._lock = threading.Lock()

    def run(self) -> None:
        while True:
            try:
                with self.pool.connection() as conn:
                    rows = conn.execute(
                        """
                        SELECT org_id,
                               count(*) FILTER (WHERE status = 'running') AS running,
                               count(*) FILTER (WHERE status = 'scheduled' AND run_at <= now()) AS waiting
                        FROM jobs WHERE status IN ('running', 'scheduled') GROUP BY org_id
                        """
                    ).fetchall()
                sample = {"t": time.time(),
                          "running": {r["org_id"]: r["running"] for r in rows},
                          "waiting": {r["org_id"]: r["waiting"] for r in rows}}
                with self._lock:
                    self.samples.append(sample)
            except Exception as e:  # keep sampling through DB hiccups / resets
                log.debug("sampler: %s", e)
            time.sleep(1.0)

    def history(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.samples)

    def clear(self) -> None:
        with self._lock:
            self.samples.clear()


# --- state ------------------------------------------------------------------------------------


def build_state(pool, fleet: Fleet, sampler: Sampler) -> dict[str, Any]:
    with pool.connection() as conn:
        orgs = report.status_by_org(conn)
        drift = {r["org"]: r for r in report.drift_by_org(conn)}
        for o in orgs:
            d = drift.get(o["org"], {})
            o["p50_wait_s"] = _num(d.get("p50_s"))
            o["p95_wait_s"] = _num(d.get("p95_s"))
            o["max_wait_s"] = _num(d.get("max_s"))
            o["started_early"] = d.get("started_early", 0)
        integrity = report.integrity(conn)
        outcomes = {r["outcome"]: r["attempts"] for r in report.attempts_by_outcome(conn)}
        # Who holds which running jobs, and when those leases run out (stranded jobs of a
        # killed consumer stay here until the reaper or a restart reclaims them).
        holders = {
            r["lease_owner"]: r for r in conn.execute(
                """
                SELECT lease_owner, array_agg(id ORDER BY id) AS job_ids,
                       min(extract(epoch FROM lease_expires_at - now()))::float8 AS lease_left_s
                FROM jobs WHERE status = 'running' GROUP BY lease_owner
                """
            ).fetchall()
        }
        db_consumers = conn.execute(
            """
            SELECT DISTINCT ON (split_part(id, '#', 1)) split_part(id, '#', 1) AS name, id, pid,
                   inflight, stopped_at IS NOT NULL AS stopped,
                   extract(epoch FROM now() - last_heartbeat_at)::float8 AS heartbeat_age_s
            FROM consumers ORDER BY split_part(id, '#', 1), started_at DESC
            """
        ).fetchall()
        events = _events(conn, interesting_only=False)
        notable = _events(conn, interesting_only=True)

    managed = fleet.snapshot()
    consumers = []
    seen = set()
    for c in db_consumers:
        seen.add(c["name"])
        m = managed.get(c["name"])
        if m is None and (c["stopped"] or c["heartbeat_age_s"] > 15):
            continue  # old external consumer, long gone
        consumers.append(_consumer_view(c["name"], c, m, holders))
    for name, m in managed.items():
        if name not in seen:
            consumers.append(_consumer_view(name, None, m, holders))
    # Jobs held by incarnations that are no longer the latest one for their name (stranded).
    current_ids = {c["id"] for c in consumers if c.get("id")}
    stranded = [
        {"owner": owner, "job_ids": h["job_ids"], "lease_left_s": round(h["lease_left_s"], 1)}
        for owner, h in holders.items() if owner not in current_ids
    ]
    for c in consumers:
        if c["state"] in ("killed", "crashed", "dead?") and c.get("id") in holders:
            h = holders[c["id"]]
            c["stranded"] = {"job_ids": h["job_ids"], "lease_left_s": round(h["lease_left_s"], 1)}

    return {
        "now": time.time(),
        "orgs": orgs,
        "integrity": integrity,
        "outcomes": outcomes,
        "consumers": sorted(consumers, key=lambda c: c["name"]),
        "stranded": stranded,
        "events": events,
        "notable": notable,
        "history": sampler.history(),
    }


def _consumer_view(name, row, managed, holders) -> dict[str, Any]:
    cid = row["id"] if row else None
    if managed:
        state = managed["state"]
    elif row["stopped"]:
        state = "stopped"
    else:
        state = "running" if row["heartbeat_age_s"] < 15 else "dead?"
    held = holders.get(cid) if cid else None
    return {
        "name": name,
        "id": cid,
        "pid": (managed or {}).get("pid") or (row or {}).get("pid"),
        "state": state,
        "managed": managed is not None,
        "faults": (managed or {}).get("faults", []),
        "inflight": len(held["job_ids"]) if held and state == "running" else 0,
        "holding": held["job_ids"] if held and state == "running" else [],
        "heartbeat_age_s": round(row["heartbeat_age_s"], 1) if row else None,
    }


def _events(conn, interesting_only: bool) -> list[dict[str, Any]]:
    where = "WHERE a.outcome IS DISTINCT FROM 'succeeded' AND a.outcome IS NOT NULL" if interesting_only else ""
    rows = conn.execute(
        f"""
        SELECT a.id, a.job_id, j.org_id, j.job_type, a.attempt, a.consumer_id,
               coalesce(a.outcome, 'running') AS outcome, a.error,
               extract(epoch FROM coalesce(a.finished_at, a.claimed_at))::float8 AS at
        FROM job_attempts a JOIN jobs j ON j.id = a.job_id
        {where}
        ORDER BY coalesce(a.finished_at, a.claimed_at) DESC, a.id DESC LIMIT 40
        """
    ).fetchall()
    return rows


def _num(v: Any) -> float | None:
    return None if v is None else float(v)


# --- scenarios --------------------------------------------------------------------------------


def run_scenario(pool, name: str) -> str:
    now = datetime.now(timezone.utc)
    jobs: list[dict[str, Any]]
    if name == "flood":
        jobs = [{"org_id": "acme", "job_type": "email.send"} for _ in range(300)]
        msg = "acme: 300 short jobs, all due now (noisy neighbour)"
    elif name == "scheduled":
        jobs = [{"org_id": "beta", "job_type": "email.send",
                 "scheduled_at": now + timedelta(seconds=5 + 20 * i / 19)} for i in range(20)]
        msg = "beta: 20 jobs scheduled 5s to 25s from now"
    elif name == "long":
        jobs = [{"org_id": "gamma", "job_type": "report.generate", "payload": {"seconds": 20}}
                for _ in range(6)]
        msg = "gamma: 6 long reports (20s each, slow lane)"
    elif name == "flaky":
        jobs = [{"org_id": "gamma", "job_type": "flaky", "payload": {"fail_rate": 0.5}}
                for _ in range(8)]
        msg = "gamma: 8 flaky jobs (fail 50% of attempts, retried with backoff)"
    elif name == "poison":
        jobs = [{"org_id": "beta", "job_type": "flaky", "payload": {"fail_rate": 1.0},
                 "max_attempts": 3}]
        msg = "beta: 1 job that always fails (dead-letters after 3 attempts)"
    else:
        raise ValueError(f"unknown scenario {name!r}")
    with pool.connection() as conn:
        for org, cap in DEMO_ORGS.items():
            store.ensure_org(conn, org)
            conn.execute("UPDATE organizations SET max_concurrency = %s WHERE id = %s", (cap, org))
        store.enqueue_many(conn, jobs)
    return msg


def reset_data(pool, sampler: Sampler) -> None:
    with pool.connection() as conn:
        db.reset(conn)
        for org, cap in DEMO_ORGS.items():
            store.upsert_org(conn, org, max_concurrency=cap)
    sampler.clear()


# --- HTTP -------------------------------------------------------------------------------------


def make_handler(pool, fleet: Fleet, sampler: Sampler):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:  # keep the console quiet
            pass

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json(self, code: int, obj: Any) -> None:
            self._send(code, json.dumps(obj, default=str).encode(), "application/json")

        def do_GET(self) -> None:
            if self.path in ("/", "/index.html"):
                self._send(200, (STATIC / "dashboard.html").read_bytes(), "text/html; charset=utf-8")
            elif self.path == "/api/state":
                try:
                    self._json(200, build_state(pool, fleet, sampler))
                except Exception as e:
                    self._json(503, {"error": str(e)})
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self) -> None:
            # Same-origin guard: refuse cross-site form posts to this process-spawning API.
            origin = self.headers.get("Origin")
            if origin and not origin.startswith(("http://127.0.0.1", "http://localhost")):
                return self._json(403, {"error": "cross-origin request refused"})
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
            parts = self.path.strip("/").split("/")
            try:
                if parts == ["api", "consumers"]:
                    faults = ["mid_exec=0.05"] if body.get("chaos") else []
                    name = fleet.start(faults=faults)
                    return self._json(200, {"message": f"started {name}"
                                                       + (" with 5% mid-execution crashes" if faults else "")})
                if len(parts) == 4 and parts[:2] == ["api", "consumers"]:
                    name, action = parts[2], parts[3]
                    if action == "kill":
                        fleet.signal(name, signal.SIGKILL)
                        return self._json(200, {"message": f"SIGKILL sent to {name}"})
                    if action == "stop":
                        fleet.signal(name, signal.SIGTERM)
                        return self._json(200, {"message": f"{name}: graceful stop (draining)"})
                    if action == "restart":
                        fleet.start(name)
                        return self._json(200, {"message": f"restarted {name} (same name, new pid)"})
                if len(parts) == 3 and parts[:2] == ["api", "scenario"]:
                    return self._json(200, {"message": run_scenario(pool, parts[2])})
                if parts == ["api", "reset"]:
                    reset_data(pool, sampler)
                    return self._json(200, {"message": "all jobs wiped; orgs acme, beta, gamma ready"})
                self._json(404, {"error": "unknown endpoint"})
            except ValueError as e:
                self._json(400, {"error": str(e)})
            except Exception as e:
                self._json(500, {"error": str(e)})

    return Handler


def serve(port: int = 8765) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    with db.connect() as conn:
        db.migrate(conn)
        for org, cap in DEMO_ORGS.items():
            store.ensure_org(conn, org)
            conn.execute("UPDATE organizations SET max_concurrency = %s WHERE id = %s", (cap, org))
    pool = db.make_pool(max_size=6, name="dashboard")
    fleet = Fleet()
    sampler = Sampler(pool)
    sampler.start()
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(pool, fleet, sampler))

    def shutdown(*_: Any) -> None:
        log.info("stopping consumers launched from the dashboard...")
        fleet.shutdown()
        os._exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    log.info("dashboard on http://127.0.0.1:%d", port)
    server.serve_forever()
