"""Kernel-owned process locks for archive execution and independent publishers.

The worker uses Linux/POSIX flock on the archive inode, including hard-link
aliases. Locks are released by the kernel after process death. Heartbeat age
never permits stealing a held lock. Named publisher locks use persistent
sidecar files and must never be unlinked while the service can run.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from chess_crawl.storage.db import database_paths

try:
    import fcntl
except ImportError:  # pragma: no cover - deployment target is Linux
    fcntl = None  # type: ignore[assignment]


class ExecutorBusy(RuntimeError):
    """An executor already owns this archive."""


@dataclass
class ExecutorLease:
    identity: tuple[int, int] | int
    purpose: str = "worker"
    handle: BinaryIO | None = None
    active: bool = True

    def require(self, conn: sqlite3.Connection) -> None:
        if not self.active or self.purpose != "worker" or self.identity != _identity(conn):
            raise RuntimeError("An active executor lease for this archive is required")


def _identity(conn: sqlite3.Connection) -> tuple[int, int] | int:
    paths = database_paths(conn)
    if not paths:
        return id(conn)
    stat = paths[0].stat()
    return stat.st_dev, stat.st_ino


@contextmanager
def archive_lock(path: str | Path, *, purpose: str = "worker") -> Iterator[ExecutorLease]:
    if str(path) == ":memory:":
        raise ValueError("A daemon requires a file-backed archive")
    if fcntl is None:  # pragma: no cover
        raise RuntimeError("Archive process locks require POSIX flock; run the worker in Linux/Docker")
    if not purpose or not purpose.replace("_", "").isalnum():
        raise ValueError("Lock purpose must contain only letters, digits or underscores")
    archive = Path(path).resolve()
    archive.parent.mkdir(parents=True, exist_ok=True)
    target = archive if purpose == "worker" else Path(f"{archive}.{purpose}.lock")
    with target.open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ExecutorBusy(f"An active {purpose} process already owns archive {archive}") from exc
        stat = target.stat()
        lease = ExecutorLease((stat.st_dev, stat.st_ino), purpose=purpose, handle=handle)
        try:
            yield lease
        finally:
            lease.active = False
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def executor_lock(conn: sqlite3.Connection, *, lease: ExecutorLease | None = None) -> Iterator[ExecutorLease]:
    if lease is not None:
        lease.require(conn)
        yield lease
        return
    paths = database_paths(conn)
    if paths:
        with archive_lock(paths[0]) as acquired:
            yield acquired
    else:
        # In-memory archives cannot be shared by independent processes.
        acquired = ExecutorLease(id(conn))
        try:
            yield acquired
        finally:
            acquired.active = False
