"""Job-type registry and the demo job implementations.

A handler is `fn(ctx, payload) -> result_dict`. It runs on a worker thread. Raising an exception
fails the attempt (retried with backoff); raising PermanentError dead-letters it immediately.
Handlers should call ctx.checkpoint() periodically so timeouts / lost leases can cancel them.
"""
from __future__ import annotations

import hashlib
import random
import threading
import time
from typing import Any, Callable

Handler = Callable[["JobContext", dict[str, Any]], dict[str, Any] | None]

_HANDLERS: dict[str, Handler] = {}
_LANES: dict[str, str] = {}


class Cancelled(Exception):
    """Raised inside a handler when its lease was lost or it exceeded its timeout."""


class PermanentError(Exception):
    """Non-retryable failure: the job goes straight to 'dead'."""


class JobContext:
    def __init__(
        self,
        job_id: int,
        org_id: str,
        attempt: int,
        idempotency_key: str,
        cancel: threading.Event,
        on_midpoint: Callable[[], None] = lambda: None,
    ) -> None:
        self.job_id = job_id
        self.org_id = org_id
        self.attempt = attempt
        # Stable across retries: hand this to external systems so a retried job's side effect
        # (e.g. an HTTP POST) can be deduplicated downstream.
        self.idempotency_key = idempotency_key
        self._cancel = cancel
        self._on_midpoint = on_midpoint

    def checkpoint(self) -> None:
        if self._cancel.is_set():
            raise Cancelled()

    def midpoint(self) -> None:
        """Called once, halfway through the work (fault-injection hook for 'mid_exec' crashes)."""
        self._on_midpoint()

    def sleep(self, seconds: float) -> None:
        """Simulated I/O wait that stays cancellable and reports its midpoint."""
        deadline = time.monotonic() + seconds
        half = time.monotonic() + seconds / 2
        hit_mid = False
        while (remaining := deadline - time.monotonic()) > 0:
            self.checkpoint()
            if not hit_mid and time.monotonic() >= half:
                hit_mid = True
                self.midpoint()
            time.sleep(min(0.05, remaining))
        if not hit_mid:
            self.midpoint()
        self.checkpoint()


def register(name: str, lane: str = "fast") -> Callable[[Handler], Handler]:
    def deco(fn: Handler) -> Handler:
        _HANDLERS[name] = fn
        _LANES[name] = lane
        return fn

    return deco


def get(name: str) -> Handler:
    try:
        return _HANDLERS[name]
    except KeyError:
        raise PermanentError(f"unknown job type {name!r}") from None


def lane_for(name: str) -> str:
    return _LANES.get(name, "fast")


def job_types() -> list[str]:
    return sorted(_HANDLERS)


# --- demo job types ---------------------------------------------------------------------------


@register("email.send")
def send_email(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    """Short I/O-bound job (~100-400ms)."""
    ctx.sleep(payload.get("ms", random.randint(100, 400)) / 1000)
    return {"delivered_to": payload.get("to", f"user{ctx.job_id}@{ctx.org_id}.example")}


@register("report.generate", lane="slow")
def generate_report(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    """Long-running job (seconds to minutes). Lives in the slow lane."""
    seconds = float(payload.get("seconds", 5))
    ctx.sleep(seconds)
    return {"report": f"{ctx.org_id}-{ctx.job_id}.pdf", "seconds": seconds}


@register("image.resize")
def resize_image(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    """CPU-bound job. Holds the GIL in bursts — heartbeats still run (GIL switch interval 5ms)."""
    rounds = int(payload.get("rounds", 200_000))
    digest = hashlib.sha256(str(ctx.job_id).encode()).digest()
    for i in range(rounds):
        digest = hashlib.sha256(digest).digest()
        if i % 10_000 == 0:
            ctx.checkpoint()
        if i == rounds // 2:
            ctx.midpoint()
    return {"thumbnail_sha": digest.hex()[:16]}


@register("flaky")
def flaky(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    """Fails transiently with probability fail_rate (1.0 = always -> ends in the dead letter)."""
    ctx.sleep(payload.get("ms", 100) / 1000)
    if random.random() < float(payload.get("fail_rate", 0.5)):
        raise RuntimeError("simulated transient failure (downstream returned 503)")
    return {"ok": True, "attempt": ctx.attempt}


@register("noop")
def noop(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    """Benchmark job: measures pure scheduler overhead (or a fixed sleep if ms is given)."""
    if ms := payload.get("ms"):
        time.sleep(ms / 1000)
    return {}
