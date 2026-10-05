"""Validated settings for concurrent executors and their durable retry policy."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerSettings:
    poll_interval: float = 1.0
    heartbeat_interval: float = 5.0
    heartbeat_max_age: float = 20.0
    job_max_retries: int = 3
    job_retry_base_s: float = 30.0
    job_retry_max_s: float = 3600.0

    def __post_init__(self) -> None:
        for name in ("poll_interval", "heartbeat_interval", "heartbeat_max_age", "job_retry_base_s", "job_retry_max_s"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and greater than zero")
        if isinstance(self.job_max_retries, bool) or not isinstance(self.job_max_retries, int) or self.job_max_retries < 0:
            raise ValueError("job_max_retries must be a nonnegative integer")
        if self.job_retry_max_s < self.job_retry_base_s:
            raise ValueError("job_retry_max_s must be at least job_retry_base_s")
        if self.heartbeat_max_age < 2 * self.heartbeat_interval:
            raise ValueError("heartbeat_max_age must be at least twice heartbeat_interval")
