"""End-to-end demo: enqueue a multi-tenant workload, run 3 consumer processes, crash them in every
way we can think of, then prove from the database that all five requirements held.

    docker compose up -d
    uv run python demo/run_demo.py            # ~60s (--skip-scaling: ~40s)

Consumer logs: demo/logs/<consumer-id>.log
"""
from __future__ import annotations

import argparse
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from demo import bench  # noqa: E402
from scheduler import db, report, store  # noqa: E402

LOG_DIR = ROOT / "demo" / "logs"

# --- scenario parameters ------------------------------------------------------------------------
ORGS = {  # id: (max_concurrency, weight)
    "acme": (10, 1),      # the noisy neighbour: floods the queue
    "globex": (8, 1),
    "initech": (8, 1),
    "umbrella": (4, 1),   # submits a handful
}
CONSUMERS = 3
SLOTS, SLOW_SLOTS = 6, 2
LEASE, HEARTBEAT, POLL = 5.0, 1.5, 0.25
# consumer-3's fault plan, one entry per incarnation: it crashes at a different point each time it
# is restarted, so every crash point is exercised on every run. Then it behaves.
CHAOS_PLAN = [["mid_exec=@8"], ["before_complete=@6"], ["after_claim=@4"], []]
KILL_AT = 5.0          # seconds after T0: SIGKILL consumer-2
NODE_LOSS_DOWNTIME = 8.0  # consumer-2's "machine" stays down longer than the lease -> reaper path
RESTART_DELAY = 1.5    # supervisor restarts crashed consumers after this long
SMALL_ORGS = ("globex", "initech", "umbrella")
SCALE_JOBS, SCALE_JOB_MS, SCALE_SLOTS = 3000, 20, 8  # scenario 5: short scaling run


def build_workload(t0: datetime) -> list[dict]:
    def at(lo: float, hi: float | None = None) -> datetime:
        return t0 + timedelta(seconds=lo if hi is None else random.uniform(lo, hi))

    jobs: list[dict] = []
    # acme: 250 short jobs + 3 long reports, all due immediately (a burst far beyond its share).
    jobs += [{"org_id": "acme", "job_type": "email.send", "scheduled_at": at(0),
              "payload": {"ms": random.randint(150, 350)}} for _ in range(250)]
    jobs += [{"org_id": "acme", "job_type": "report.generate", "scheduled_at": at(0),
              "payload": {"seconds": random.uniform(6, 9)}} for _ in range(3)]
    # globex: steady trickle + some long + some CPU-bound jobs, scheduled into the future.
    jobs += [{"org_id": "globex", "job_type": "email.send", "scheduled_at": at(2, 20)} for _ in range(20)]
    jobs += [{"org_id": "globex", "job_type": "report.generate", "scheduled_at": at(4),
              "payload": {"seconds": 3}} for _ in range(3)]
    jobs += [{"org_id": "globex", "job_type": "image.resize", "scheduled_at": at(6, 10),
              "payload": {"rounds": 150_000}} for _ in range(4)]
    # initech: future-scheduled jobs + transiently failing ones (retried with backoff).
    jobs += [{"org_id": "initech", "job_type": "email.send", "scheduled_at": at(1, 25)} for _ in range(15)]
    jobs += [{"org_id": "initech", "job_type": "flaky", "scheduled_at": at(3),
              "payload": {"fail_rate": 0.5}} for _ in range(4)]
    # umbrella: a handful, plus one job that always fails -> must end in the dead letter, not loop.
    jobs += [{"org_id": "umbrella", "job_type": "email.send", "scheduled_at": at(5, 15)} for _ in range(6)]
    jobs += [{"org_id": "umbrella", "job_type": "flaky", "scheduled_at": at(2),
              "payload": {"fail_rate": 1.0}, "max_attempts": 3, "idempotency_key": "always-fails"}]
    # Producer retry: the same idempotency key submitted again must not create a second job.
    jobs += [{"org_id": "umbrella", "job_type": "flaky", "scheduled_at": at(2),
              "payload": {"fail_rate": 1.0}, "max_attempts": 3, "idempotency_key": "always-fails"}]
    return jobs


# --- process supervision ------------------------------------------------------------------------


class Fleet:
    """Starts consumer processes and restarts any that die (like systemd / Kubernetes would)."""

    def __init__(self) -> None:
        self.procs: dict[str, subprocess.Popen] = {}   # stable consumer name -> current process
        self.ids: dict[str, str] = {}                  # name -> current incarnation id (name#pid)
        self.restarts: dict[str, int] = defaultdict(int)
        self.restart_delay: dict[str, float] = {}
        self.plans: dict[str, list[list[str]]] = {}
        self.crash_log: list[str] = []
        self.stopping = False
        self._lock = threading.Lock()

    def start(self, slot: str, plan: list[list[str]] | None = None) -> None:
        if plan is not None:
            self.plans[slot] = plan
        n = self.restarts[slot]
        faults = self.plans[slot][min(n, len(self.plans[slot]) - 1)]
        args = [sys.executable, "-u", "-m", "scheduler", "consumer", "--id", slot,
                "--slots", str(SLOTS), "--slow-slots", str(SLOW_SLOTS), "--lease", str(LEASE),
                "--heartbeat", str(HEARTBEAT), "--poll", str(POLL), "--reaper", "1", "--drain", "15"]
        for f in faults:
            args += ["--fault", f]
        log = open(LOG_DIR / f"{slot}.log", "a")
        p = subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        with self._lock:
            self.procs[slot], self.ids[slot] = p, f"{slot}#{p.pid}"

    def supervise(self) -> None:
        restart_at: dict[str, float] = {}  # slot -> monotonic deadline (never block other slots)
        while not self.stopping:
            for slot, p in list(self.procs.items()):
                if slot in restart_at:
                    if time.monotonic() >= restart_at[slot]:
                        del restart_at[slot]
                        self.restarts[slot] += 1
                        self.start(slot)
                    continue
                rc = p.poll()
                if rc is not None:
                    cause = "SIGKILL" if rc == -signal.SIGKILL else f"exit {rc}"
                    self.crash_log.append(f"{self.ids[slot]} died ({cause})")
                    restart_at[slot] = time.monotonic() + self.restart_delay.pop(slot, RESTART_DELAY)
            time.sleep(0.05)

    def kill(self, slot: str, downtime: float) -> str:
        cid = self.ids[slot]
        self.restart_delay[slot] = downtime
        self.procs[slot].send_signal(signal.SIGKILL)
        return cid

    def alive(self) -> int:
        return sum(1 for p in self.procs.values() if p.poll() is None)

    def stop(self) -> None:
        self.stopping = True
        for p in self.procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.procs.values():
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()


# --- sampling (fairness evidence) ---------------------------------------------------------------


class Sampler(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.peak_org: dict[str, int] = defaultdict(int)
        self.peak_slow_per_consumer = 0
        self.timeline: list[tuple[float, dict[str, int], int]] = []  # (t, running by org, acme backlog)

    def run(self) -> None:
        t_start = time.monotonic()
        with db.connect() as conn:
            while not self.stop.wait(0.2):
                by_org = {r["org_id"]: r["n"] for r in conn.execute(
                    "SELECT org_id, count(*) AS n FROM jobs WHERE status = 'running' GROUP BY org_id")}
                for org, n in by_org.items():
                    self.peak_org[org] = max(self.peak_org[org], n)
                slow = conn.execute(
                    "SELECT coalesce(max(n), 0) AS m FROM (SELECT count(*) AS n FROM jobs"
                    " WHERE status = 'running' AND lane = 'slow' GROUP BY lease_owner) x").fetchone()["m"]
                self.peak_slow_per_consumer = max(self.peak_slow_per_consumer, slow)
                backlog = conn.execute(
                    "SELECT count(*) AS n FROM jobs WHERE org_id = 'acme' AND status = 'scheduled'"
                    " AND run_at <= now()").fetchone()["n"]
                self.timeline.append((time.monotonic() - t_start, by_org, backlog))


# --- report helpers ---------------------------------------------------------------------------


def banner(title: str) -> None:
    print(f"\n{'=' * 100}\n{title}\n{'=' * 100}")


def verdict(ok: bool, text: str) -> bool:
    print(f"  [{'PASS' if ok else 'FAIL'}] {text}")
    return ok


def grep_logs(pattern: str) -> list[str]:
    rx = re.compile(pattern)
    out = []
    for f in sorted(LOG_DIR.glob("*.log")):
        out += [line.rstrip() for line in f.read_text().splitlines() if rx.search(line)]
    return out


def zombie_scenario(conn) -> bool:
    """Deterministic version of the nastiest race: a worker stalls past its lease, the job is
    re-granted, the rescuer commits, then the zombie wakes up and tries to commit too."""
    store.ensure_org(conn, "zombie-check")
    jid = store.enqueue(conn, "zombie-check", "noop")
    (zombie,) = store.claim(conn, "zombie-worker", "fast", 1, 30)
    conn.execute("UPDATE jobs SET lease_expires_at = now() - interval '1s' WHERE id = %s", (jid,))
    store.reap(conn)
    (rescuer,) = store.claim(conn, "rescuer-worker", "fast", 1, 30)
    rescued = store.complete(conn, rescuer, "rescuer-worker", {"by": "rescuer"})
    zombie_ok = store.complete(conn, zombie, "zombie-worker", {"by": "zombie"})
    effects = conn.execute("SELECT consumer_id FROM job_effects WHERE job_id = %s", (jid,)).fetchall()
    print(f"  job={jid}: zombie token={str(zombie.lease_token)[:8]}..  rescuer token={str(rescuer.lease_token)[:8]}..")
    print(f"  rescuer commit -> {rescued};  zombie late commit -> {zombie_ok} (fenced);  "
          f"effects written by: {[e['consumer_id'] for e in effects]}")
    return rescued and not zombie_ok and len(effects) == 1


# --- main ---------------------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--timeout", type=float, default=150, help="give up waiting after N seconds")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--skip-scaling", action="store_true", help="skip the 1-vs-3 consumer throughput run")
    args = ap.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for f in LOG_DIR.glob("*.log"):
        f.unlink()

    try:
        conn = db.connect()
    except Exception as e:  # noqa: BLE001
        print(f"cannot reach Postgres ({e}).\nStart it with: docker compose up -d")
        return 2
    db.migrate(conn)
    db.reset(conn)
    for org, (cap, weight) in ORGS.items():
        store.upsert_org(conn, org, max_concurrency=cap, weight=weight)

    banner("SETUP")
    t0 = datetime.now(timezone.utc) + timedelta(seconds=2)
    workload = build_workload(t0)
    store.enqueue_many(conn, workload)
    totals = {r["org"]: r["scheduled"] for r in report.status_by_org(conn)}
    print(f"  enqueued {sum(totals.values())} jobs ({len(workload)} submitted; 1 duplicate idempotency key "
          f"deduped): " + ", ".join(f"{o}={n}" for o, n in totals.items()))
    print(f"  {CONSUMERS} consumers x {SLOTS} worker threads (max {SLOW_SLOTS} slow-lane), lease={LEASE}s, "
          f"heartbeat={HEARTBEAT}s")
    print("  consumer-3 hard-crashes (os._exit) at a different point in each incarnation: "
          + " -> ".join(f[0] if f else "healthy" for f in CHAOS_PLAN))
    print(f"  consumer-2 is SIGKILLed at T0+{KILL_AT:.0f}s and its machine stays down {NODE_LOSS_DOWNTIME:.0f}s "
          f"(> lease): other consumers must reap its jobs")
    print(f"  crashed consumer-3 is restarted by the supervisor after {RESTART_DELAY}s (same name, new pid) and "
          "reclaims its predecessor's jobs at startup")
    print(f"  logs: {LOG_DIR.relative_to(ROOT)}/")

    fleet = Fleet()
    for i in range(1, CONSUMERS + 1):
        fleet.start(f"consumer-{i}", CHAOS_PLAN if i == 3 else [[]])
    threading.Thread(target=fleet.supervise, daemon=True).start()
    sampler = Sampler()
    sampler.start()

    banner("RUNNING")
    held_at_kill: list[int] = []
    killed_id = None
    start = time.monotonic()
    last_print = 0.0
    while True:
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        if killed_id is None and elapsed >= KILL_AT:
            # Make sure the victim is mid-flight so the kill actually strands work.
            held_at_kill = [r["id"] for r in conn.execute(
                "SELECT id FROM jobs WHERE status = 'running' AND lease_owner = %s ORDER BY id",
                (fleet.ids["consumer-2"],))]
            killed_id = fleet.kill("consumer-2", NODE_LOSS_DOWNTIME)
            print(f"  T0{elapsed:+5.1f}s  >>> SIGKILL {killed_id} while it holds {len(held_at_kill)} "
                  f"running job(s): {held_at_kill}")
        s = report.integrity(conn)
        if time.monotonic() - last_print >= 2:
            last_print = time.monotonic()
            st = conn.execute("SELECT status::text, count(*) AS n FROM jobs GROUP BY 1").fetchall()
            st = {r["status"]: r["n"] for r in st}
            print(f"  T0{elapsed:+5.1f}s  scheduled={st.get('scheduled', 0):<4} running={st.get('running', 0):<3}"
                  f" succeeded={st.get('succeeded', 0):<4} dead={st.get('dead', 0):<2}"
                  f" | consumers alive={fleet.alive()} crashes so far={len(fleet.crash_log)}")
        if s["pending"] == 0 and killed_id is not None:
            break
        if time.monotonic() - start > args.timeout:
            print("  timed out waiting for the queue to drain")
            break
        time.sleep(0.25)

    sampler.stop.set()
    fleet.stop()
    run_seconds = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"  queue drained at T0+{run_seconds:.1f}s; consumers stopped (SIGTERM, graceful drain)")

    results: dict[str, bool] = {}

    # 1 ------------------------------------------------------------------------------------------
    banner("1. EXACTLY-ONCE  (no job lost, no job executed-and-committed twice)")
    s = report.integrity(conn)
    print(report.table([s]))
    print(report.table(report.attempts_by_outcome(conn), "\n  Attempts by outcome (one row per claim)"))
    ok = verdict(s["pending"] == 0, f"no job lost: {s['succeeded']} succeeded + {s['dead']} dead-lettered "
                 f"= {s['succeeded'] + s['dead']} of {s['total']} (0 stuck in scheduled/running)")
    ok &= verdict(s["duplicate_effects"] == 0 and s["effects"] == s["succeeded"],
                  f"no duplicates: {s['effects']} effect rows for {s['succeeded']} succeeded jobs, "
                  f"{s['duplicate_effects']} jobs with >1 effect")
    ok &= verdict(s["missing_effects"] == 0 and s["effects_on_unsucceeded"] == 0,
                  "every succeeded job has exactly one committed effect; no effect without success")
    # Be explicit about what "exactly-once" means: a handler can run more than once (a crash after
    # it started, before its commit), but only one run's result is ever committed.
    rerun = conn.execute(
        """
        SELECT count(*) AS jobs, coalesce(sum(n), 0) AS claims,
               count(*) FILTER (WHERE crashed) AS by_crash, count(*) FILTER (WHERE NOT crashed) AS by_error
        FROM (SELECT a.job_id, count(*) AS n,
                     bool_or(a.outcome IN ('lease_expired', 'owner_restarted', 'fenced')) AS crashed
                FROM job_attempts a JOIN jobs j ON j.id = a.job_id
               WHERE j.status = 'succeeded' GROUP BY a.job_id HAVING count(*) > 1) x
        """).fetchone()
    print(f"\n  Execution is at-least-once, commit is exactly-once: {rerun['jobs']} succeeded jobs were picked\n"
          f"  up more than once ({rerun['by_crash']} because their consumer crashed, {rerun['by_error']} retried\n"
          f"  after a handler error; {rerun['claims']} pickups in total). Each of them has exactly one committed\n"
          f"  effect. Side effects outside this database get the job's stable idempotency_key for dedupe.")
    results["exactly-once"] = ok

    # 2 ------------------------------------------------------------------------------------------
    banner("2. CRASHES AT ANY POINT")
    faults = grep_logs(r"FAULT INJECTED")
    by_point = defaultdict(int)
    for line in faults:
        by_point[re.search(r"crash at (\w+)", line).group(1)] += 1
    print(f"  consumer deaths: {len(fleet.crash_log)}  ->  " + "; ".join(fleet.crash_log))
    print("  injected crash points: " + (", ".join(f"{k}={v}" for k, v in sorted(by_point.items())) or "none"))
    covered = verdict(set(by_point) == {"after_claim", "mid_exec", "before_complete"},
                      "hard crashes hit all three points: after claim, mid-execution, before commit (+ SIGKILL)")
    reaped = grep_logs(r"reaped job=")
    released = grep_logs(r"recovered job=.*previous incarnation")
    fenced = grep_logs(r"FENCED|lost lease")
    print(f"  stranded jobs recovered: {len(released)} by the restarted consumer at startup, "
          f"{len(reaped)} by another consumer's reaper after lease expiry")
    print(f"  late commits fenced off: {len(fenced)}")
    if held_at_kill:
        rows = conn.execute(
            """
            SELECT j.id, j.org_id AS org, j.status::text AS status,
                   j.attempts || '/' || j.max_attempts AS attempts,
                   j.attempts >= j.max_attempts AS exhausted,
                   (SELECT string_agg(consumer_id || ':' || coalesce(outcome, '?'), ' -> ' ORDER BY a.id)
                      FROM job_attempts a WHERE a.job_id = j.id) AS attempt_history,
                   (SELECT count(*) FROM job_effects e WHERE e.job_id = j.id) AS effects
            FROM jobs j WHERE j.id = ANY(%s) ORDER BY j.id
            """, (held_at_kill,)).fetchall()
        print(report.table([{k: v for k, v in r.items() if k != "exhausted"} for r in rows],
                           f"\n  Jobs that {killed_id} held when it was SIGKILLed:"))
        good = [r for r in rows if (r["status"] == "succeeded" and r["effects"] == 1)
                or (r["status"] == "dead" and r["effects"] == 0 and r["exhausted"])]
        n_dead = sum(r["status"] == "dead" for r in rows)
        ok = verdict(len(good) == len(rows),
                     f"all {len(rows)} stranded jobs recovered and re-run: {len(rows) - n_dead} committed exactly "
                     f"once" + (f", {n_dead} dead-lettered (that crash used up its last attempt)" if n_dead else ""))
    else:
        ok = verdict(False, "consumer-2 held no jobs at kill time (re-run the demo)")
    crashed_mid = conn.execute(
        "SELECT count(DISTINCT job_id) AS n FROM job_attempts"
        " WHERE outcome IN ('lease_expired', 'owner_restarted', 'fenced')"
    ).fetchone()["n"]
    ok &= covered
    ok &= verdict(len(reaped) > 0 and len(released) > 0,
                  "both recovery paths exercised: lease-expiry reaping (node lost) and restart reclaim")
    ok &= verdict(crashed_mid > 0 and s["duplicate_effects"] == 0 and s["pending"] == 0,
                  f"{crashed_mid} jobs had an attempt die with its consumer; all recovered, none duplicated")
    print("\n  Deterministic zombie check (worker stalls past its lease, then wakes up and commits):")
    ok &= verdict(zombie_scenario(conn), "zombie's late commit rejected by fencing token")
    results["crash-safety"] = ok

    # 3 ------------------------------------------------------------------------------------------
    banner("3. FAIRNESS  (acme floods 253 jobs; the others submit a handful)")
    drift_all = {r["org"]: r for r in report.drift_by_org(conn)}
    rows = []
    for org, (cap, weight) in ORGS.items():
        d = drift_all.get(org, {})
        rows.append({"org": org, "jobs": d.get("jobs"), "weight": weight, "cap": cap,
                     "peak_running": sampler.peak_org.get(org, 0),
                     "p50_wait_s": d.get("p50_s"), "p95_wait_s": d.get("p95_s"), "max_wait_s": d.get("max_s")})
    print(report.table(rows))
    busy = [(t, by_org) for t, by_org, backlog in sampler.timeline if backlog > 0]
    if busy:
        share = defaultdict(float)
        for _, by_org in busy:
            total = sum(by_org.values()) or 1
            for org, n in by_org.items():
                share[org] += n / total / len(busy)
        print("  share of running slots while acme had a backlog: "
              + ", ".join(f"{o}={share[o]:.0%}" for o in ORGS))
    small_p95 = max(float(drift_all[o]["p95_s"]) for o in SMALL_ORGS)
    acme_p95 = float(drift_all["acme"]["p95_s"])
    ok = verdict(small_p95 < 1.5,
                 f"small orgs unaffected by the flood: worst p95 wait {small_p95:.2f}s (acme queues behind "
                 f"itself: p95 {acme_p95:.1f}s)")
    ok &= verdict(sampler.peak_org.get("acme", 0) <= ORGS["acme"][0] + CONSUMERS - 1,
                  f"acme held to its concurrency cap: peak {sampler.peak_org.get('acme', 0)} vs cap "
                  f"{ORGS['acme'][0]} (soft cap; tolerance = consumers-1)")
    ok &= verdict(sampler.peak_slow_per_consumer <= SLOW_SLOTS,
                  f"long jobs never took a consumer's fast slots: peak slow-lane jobs per consumer "
                  f"{sampler.peak_slow_per_consumer} <= {SLOW_SLOTS} of {SLOTS}")
    results["fairness"] = ok

    # 4 ------------------------------------------------------------------------------------------
    banner("4. SCHEDULE ADHERENCE  (future-scheduled jobs from non-flooding orgs)")
    future = (f"scheduled_at > '{(t0 + timedelta(seconds=0.5)).isoformat()}'"
              f" AND org_id IN ({', '.join(repr(o) for o in SMALL_ORGS)})")
    drift = report.drift_by_org(conn, future)
    print(report.table(drift))
    slo = conn.execute(
        f"""SELECT count(*) AS n,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY d) AS p95, max(d) AS max
            FROM (SELECT extract(epoch FROM first_started_at - scheduled_at)::float8 AS d
                    FROM jobs WHERE {future}) x""").fetchone()
    early = sum(r["started_early"] for r in drift)
    ok = verdict(early == 0, "no job started before its scheduled_at")
    ok &= verdict(slo["p95"] < 1.0,
                  f"drift SLO p95 < 1s over all {slo['n']} future-scheduled jobs: p95 {slo['p95'] * 1000:.0f}ms, "
                  f"max {slo['max']:.2f}s (consumers crashing + acme flooding throughout)")
    # acme's jobs were all due at once, far beyond its cap, so they wait in line. That is capacity,
    # not scheduler drift: show its wait, and show it ran at its cap the whole time it had a backlog.
    acme = conn.execute(
        """
        SELECT count(*) AS n,
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM first_started_at - scheduled_at)) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM first_started_at - scheduled_at)) AS p95
        FROM jobs WHERE org_id = 'acme' AND status = 'succeeded'
        """).fetchone()
    cap = ORGS["acme"][0]
    backlogged = [by_org.get("acme", 0) for _, by_org, backlog in sampler.timeline if backlog > 0]
    util = sum(backlogged) / len(backlogged) / cap if backlogged else 0.0
    print(f"\n  acme ({acme['n']} jobs due at the same moment, cap {cap}): wait p50 {acme['p50']:.1f}s, "
          f"p95 {acme['p95']:.1f}s. That is queueing behind\n  its own burst, not drift: while it had a backlog "
          f"it averaged {util * cap:.1f} of {cap} running jobs. Lower wait needs a\n  higher cap or more consumers.")
    ok &= verdict(util >= 0.8, f"flooding org kept at its cap while backlogged: {util:.0%} of cap on average "
                  f"(stranded jobs hold their slot until reclaimed)")
    results["schedule"] = ok

    # 5 ------------------------------------------------------------------------------------------
    banner("5. SCALING  (same backlog drained by 1, then 3 consumer processes)")
    if args.skip_scaling:
        print("  skipped (--skip-scaling)")
    else:
        # Runs last: the benchmark resets the database.
        print(f"  {SCALE_JOBS} I/O-bound jobs ({SCALE_JOB_MS}ms each) across 10 orgs, {SCALE_SLOTS} threads per consumer")
        runs = []
        for n in (1, 3):
            runs.append(bench.run_once(n, SCALE_JOBS, SCALE_SLOTS, 10, SCALE_JOB_MS))
            print(f"    {n} consumer(s): {runs[-1]['jobs_per_s']} jobs/s")
        print(report.table(runs))
        speedup = runs[1]["jobs_per_s"] / runs[0]["jobs_per_s"]
        ok = verdict(speedup >= 2.0, f"3 consumers drain {speedup:.1f}x faster than 1 (ideal 3.0x)")
        ok &= verdict(all(r["duplicates"] == 0 for r in runs), "no duplicates at either scale")
        results["scaling"] = ok
    print("  Where each approach hits its ceiling (measured with demo/bench.py, see DESIGN.md 'Scaling'):\n"
          "    more threads   ~2k jobs/s per process of scheduler overhead; CPU-bound jobs need processes (GIL)\n"
          "    more processes linear while job work dominates; ~8 connections each -> PgBouncer past ~50-100\n"
          "    one Postgres   WAL + vacuum on the hot table; ~10-20k jobs/s on dedicated NVMe -> shard by org_id")

    banner("SUMMARY")
    for name, passed in results.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {name}")
    conn.close()
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    os.chdir(ROOT)
    sys.exit(main())
