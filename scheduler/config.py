from __future__ import annotations

import os
from dataclasses import dataclass, field

DEFAULT_DSN = "postgresql://scheduler:scheduler@localhost:5433/scheduler"


def dsn() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DSN)


@dataclass
class ConsumerConfig:
    slots: int = 8                  # worker threads in this consumer process
    slow_slots: int = 4             # at most this many slots may run 'slow'-lane jobs
    lease_seconds: float = 10.0     # a claim is valid this long unless renewed by heartbeat
    heartbeat_seconds: float = 3.0  # lease renewal cadence (must be well under lease_seconds)
    poll_seconds: float = 0.5       # upper bound on idle sleep (NOTIFY usually wakes us sooner)
    reaper_seconds: float = 2.0     # how often this consumer sweeps expired leases
    drain_seconds: float = 30.0     # graceful-shutdown budget for in-flight jobs
    faults: dict[str, str] = field(default_factory=dict)  # see scheduler/faults.py

    def __post_init__(self) -> None:
        if not 0 <= self.slow_slots <= self.slots:
            raise ValueError("slow_slots must be between 0 and slots")
        if self.heartbeat_seconds * 2 > self.lease_seconds:
            raise ValueError("heartbeat_seconds must be < lease_seconds / 2")
