"""Bound active PostgreSQL sessions per process without reusing their ownership."""
from __future__ import annotations

import math
import os
import threading
from dataclasses import dataclass

from psycopg import OperationalError

from chess_crawl.settings import dataclass_settings


@dataclass(frozen=True)
class DatabaseAdmissionSettings:
    max_connections: int = 32
    admission_timeout_s: float = 5.0

    def __post_init__(self) -> None:
        if type(self.max_connections) is not int or self.max_connections < 1:
            raise ValueError("CHESS_CRAWL_DATABASE_MAX_CONNECTIONS must be a positive integer")
        if (isinstance(self.admission_timeout_s, bool) or not isinstance(self.admission_timeout_s, (int, float))
                or not math.isfinite(self.admission_timeout_s) or not 0 < self.admission_timeout_s <= threading.TIMEOUT_MAX):
            raise ValueError("CHESS_CRAWL_DATABASE_ADMISSION_TIMEOUT_S must be finite, positive, and supported by the threading timer")

    @classmethod
    def from_env(cls) -> DatabaseAdmissionSettings:
        return dataclass_settings(cls, prefix="CHESS_CRAWL_DATABASE_")

    def require_worker_capacity(self) -> None:
        if self.max_connections < 2:
            raise ValueError("Workers require CHESS_CRAWL_DATABASE_MAX_CONNECTIONS >= 2 for execution and heartbeat sessions")


class SessionAdmission:
    """One permit remains attached to one dedicated connection until close."""

    def __init__(self, settings: DatabaseAdmissionSettings) -> None:
        self.settings = settings
        self._condition = threading.Condition()
        self._active = 0

    def acquire(self) -> SessionPermit:
        with self._condition:
            available = self._condition.wait_for(
                lambda: self._active < self.settings.max_connections,
                timeout=self.settings.admission_timeout_s,
            )
            if not available:
                raise OperationalError("The process PostgreSQL session capacity is occupied; retry later")
            self._active += 1
        return SessionPermit(self)

    def release(self) -> None:
        with self._condition:
            if self._active <= 0:
                raise RuntimeError("PostgreSQL session permit was released without ownership")
            self._active -= 1
            self._condition.notify()

    @property
    def active(self) -> int:
        with self._condition:
            return self._active


class SessionPermit:
    """Return one admission exactly once, including concurrent close calls."""

    def __init__(self, admission: SessionAdmission) -> None:
        self._admission = admission
        self._lock = threading.Lock()
        self._released = False

    def release(self) -> None:
        with self._lock:
            if not self._released:
                self._released = True
                self._admission.release()


_registry_lock = threading.Lock()
_admission: SessionAdmission | None = None


def process_session_admission() -> SessionAdmission:
    """Changing deployment settings never creates an additional concurrent pool."""
    global _admission
    with _registry_lock:
        if _admission is None:
            _admission = SessionAdmission(DatabaseAdmissionSettings.from_env())
        return _admission


def reset_session_admission() -> None:
    """Explicit idle-only reset for isolated tests; deployments change via restart."""
    global _admission
    with _registry_lock:
        if _admission is not None and _admission.active:
            raise RuntimeError("Cannot reset PostgreSQL session admission while connections are active")
        _admission = None


def _after_fork() -> None:
    # A new process has its own sessions; inherited thread locks/permits cannot
    # represent them. Worker ownership connections must never be fork-shared.
    global _registry_lock, _admission
    _registry_lock = threading.Lock()
    _admission = None


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
