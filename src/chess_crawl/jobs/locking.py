"""Session-owned executor and publisher locks on their business connection.

A disconnected owner cannot keep mutating through another session. PostgreSQL
releases ownership on session death; heartbeat age never permits stealing it.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from chess_crawl.storage.db import (
    Connection, DatabaseError, ExecutorLeaseLost, acquire_session_lock, connection,
    owns_session_lock, release_session_lock,
)


class ExecutorBusy(RuntimeError):
    """An executor already owns this database."""


@dataclass
class ExecutorLease:
    connection: Connection
    purpose: str = "worker"
    active: bool = True

    def require(self, conn: Connection, *, purpose: str = "worker") -> None:
        if not self.active or self.purpose != purpose or self.connection is not conn or conn.closed:
            raise ExecutorLeaseLost("An active executor lease for this database is required")
        try:
            held = owns_session_lock(conn, purpose)
        except DatabaseError:
            self.active = False
            raise ExecutorLeaseLost("Database executor ownership was lost") from None
        if not held:
            self.active = False
            raise ExecutorLeaseLost("Database executor ownership was lost")


@contextmanager
def executor_lock(
    conn: Connection, *, lease: ExecutorLease | None = None, purpose: str = "worker",
) -> Iterator[ExecutorLease]:
    if lease is not None:
        lease.require(conn, purpose=purpose)
        yield lease
        return
    if not acquire_session_lock(conn, purpose):
        raise ExecutorBusy(f"An active {purpose} process already owns this database")
    acquired = ExecutorLease(conn, purpose=purpose)
    previous_purposes = conn._ownership_purposes
    conn._ownership_purposes = (*previous_purposes, purpose)
    try:
        yield acquired
    finally:
        acquired.active = False
        conn._ownership_purposes = previous_purposes
        if not conn.closed:
            try:
                release_session_lock(conn, purpose)
            except DatabaseError:
                # The connection will be closed by its owner. Cleanup must not
                # replace the original failure or retain a usable lost lease.
                conn.close()


@contextmanager
def archive_lock(target: str, *, purpose: str = "worker") -> Iterator[ExecutorLease]:
    with connection(target, mode="rw") as conn:
        with executor_lock(conn, purpose=purpose) as lease:
            yield lease


@contextmanager
def parallel_executor_lock(conn: Connection, *, lease: ExecutorLease | None = None) -> Iterator[ExecutorLease]:
    """Shared maintenance gate; execution ownership is separately per job."""
    if lease is not None:
        lease.require(conn)
        yield lease
        return
    if not acquire_session_lock(conn, "worker", shared=True):
        raise ExecutorBusy("An exclusive archive maintenance process owns this database")
    acquired = ExecutorLease(conn)
    previous = conn._ownership_purposes
    conn._ownership_purposes = (*previous, "worker")
    try:
        yield acquired
    finally:
        acquired.active = False
        conn._ownership_purposes = previous
        if not conn.closed:
            release_session_lock(conn, "worker", shared=True)
