"""Durable event delivery bookkeeping; business state stays in jobs.state."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from chess_crawl.storage.db import Connection, atomic, require_row


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


def archive_id(conn: Connection) -> str:
    row = conn.execute("SELECT id FROM event_archive_identity WHERE singleton = 1").fetchone()
    if row is None:
        raise RuntimeError("The archive event identity is missing")
    return str(row["id"])


def next_pending_event(conn: Connection) -> PendingEvent | None:
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
def acknowledge_event(conn: Connection, outbox_id: int, *, now: float) -> None:
    conn.execute(
        """UPDATE event_outbox
              SET delivered_at = %s, attempts = attempts + 1, last_error = NULL
            WHERE id = %s AND delivered_at IS NULL""",
        (now, outbox_id),
    )


@atomic
def defer_event(
    conn: Connection, outbox_id: int, *, next_attempt_at: float, error: str,
) -> None:
    conn.execute(
        """UPDATE event_outbox
              SET attempts = attempts + 1, next_attempt_at = %s, last_error = %s
            WHERE id = %s AND delivered_at IS NULL""",
        (next_attempt_at, error, outbox_id),
    )


def delivery_health(conn: Connection) -> dict[str, Any]:
    row = require_row(conn.execute(
        """SELECT COUNT(*) AS pending,
                  MIN(occurred_at) AS oldest_pending_at,
                  MAX(attempts) AS highest_attempts
             FROM event_outbox WHERE delivered_at IS NULL"""
    ))
    return dict(row)
