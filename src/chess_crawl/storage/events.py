"""Durable event delivery bookkeeping; business state stays in jobs.state."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from chess_crawl.storage.db import atomic


@dataclass(frozen=True)
class PendingEvent:
    outbox_id: int
    event_id: str
    event_type: str
    resource_id: int
    payload: dict[str, Any]
    attempts: int
    next_attempt_at: float

    @property
    def resource_path(self) -> str:
        collection = "jobs" if self.event_type == "job.updated" else "runs"
        return f"/{collection}/{self.resource_id}"


def archive_id(conn: sqlite3.Connection) -> str:
    return str(conn.execute("SELECT id FROM event_archive_identity WHERE singleton = 1").fetchone()["id"])


def next_pending_event(conn: sqlite3.Connection) -> PendingEvent | None:
    """Read the oldest pending event even if its backoff has not expired.

    Skipping a delayed event would deliver newer revisions out of order. A
    separate process holds the publisher lock while reading and acknowledging.
    """
    if conn.in_transaction:
        raise RuntimeError("Events may be published only outside a database transaction")
    row = conn.execute(
        "SELECT * FROM event_outbox WHERE delivered_at IS NULL ORDER BY id LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    identity = archive_id(conn)
    event_id = f"urn:chess-crawl:{identity}:{row['id']}"
    payload = json.loads(row["payload"])
    payload.update(
        schema_version=1,
        archive_id=identity,
        event_id=event_id,
        type=row["event_type"],
        revision=row["revision"],
        occurred_at=row["occurred_at"],
    )
    return PendingEvent(
        outbox_id=int(row["id"]),
        event_id=event_id,
        event_type=str(row["event_type"]),
        resource_id=int(row["resource_id"]),
        payload=payload,
        attempts=int(row["attempts"]),
        next_attempt_at=float(row["next_attempt_at"]),
    )


@atomic
def acknowledge_event(conn: sqlite3.Connection, outbox_id: int, *, now: float) -> None:
    conn.execute(
        """UPDATE event_outbox
              SET delivered_at = ?, attempts = attempts + 1, last_error = NULL
            WHERE id = ? AND delivered_at IS NULL""",
        (now, outbox_id),
    )


@atomic
def defer_event(
    conn: sqlite3.Connection, outbox_id: int, *, next_attempt_at: float, error: str,
) -> None:
    conn.execute(
        """UPDATE event_outbox
              SET attempts = attempts + 1, next_attempt_at = ?, last_error = ?
            WHERE id = ? AND delivered_at IS NULL""",
        (next_attempt_at, error, outbox_id),
    )


def delivery_health(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """SELECT COUNT(*) AS pending,
                  MIN(occurred_at) AS oldest_pending_at,
                  MAX(attempts) AS highest_attempts
             FROM event_outbox WHERE delivered_at IS NULL"""
    ).fetchone()
    return dict(row)
