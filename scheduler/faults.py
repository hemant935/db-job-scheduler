"""Fault injection: hard-crash the consumer process at chosen points (simulates kill -9 / OOM).

os._exit skips every finally/atexit handler and drops the DB connections mid-flight, which is
exactly what a real crash looks like to Postgres.

Spec per point:  'mid_exec=0.05'  crash with probability 5% each time the point is reached
                 'mid_exec=@12'   crash deterministically the 12th time the point is reached
"""
from __future__ import annotations

import logging
import os
import random
import threading

POINTS = ("after_claim", "mid_exec", "before_complete")
CRASH_EXIT_CODE = 137

log = logging.getLogger("scheduler.faults")


def parse(specs: list[str]) -> dict[str, str]:
    """['mid_exec=0.05', 'before_complete=@3'] -> {'mid_exec': '0.05', 'before_complete': '@3'}"""
    out: dict[str, str] = {}
    for spec in specs:
        point, _, value = spec.partition("=")
        if point not in POINTS:
            raise ValueError(f"unknown fault point {point!r}; choose from {POINTS}")
        value = value or "1.0"
        float(value[1:] if value.startswith("@") else value)  # validate
        out[point] = value
    return out


class FaultInjector:
    def __init__(self, specs: dict[str, str]) -> None:
        self.specs = specs
        self._hits: dict[str, int] = {}
        self._lock = threading.Lock()

    def maybe_crash(self, point: str, job_id: int) -> None:
        spec = self.specs.get(point)
        if spec is None:
            return
        with self._lock:
            self._hits[point] = n = self._hits.get(point, 0) + 1
        crash = n == int(spec[1:]) if spec.startswith("@") else random.random() < float(spec)
        if crash:
            log.error("FAULT INJECTED: hard crash at %s while holding job=%d", point, job_id)
            for h in logging.getLogger().handlers:
                h.flush()
            os._exit(CRASH_EXIT_CODE)
