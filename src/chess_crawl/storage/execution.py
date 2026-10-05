"""Upgrade progress and queue outbox operations on durable database state."""
from __future__ import annotations

import time
from typing import Any

from chess_crawl.storage.db import Connection, Row, atomic, operation_lock, require_row


@atomic
def start_upgrade(
    conn: Connection, *, upgrade_id: str, provider: str, parser_version: str, job_id: int,
    owner_scope: str = "public",
) -> Row:
    operation_lock(conn, "data-upgrade", upgrade_id)
    timestamp = int(time.time())
    high_water = int(require_row(conn.execute(
        """SELECT COALESCE(MAX(id),0) FROM raw_payloads WHERE provider=%s
             AND owner_scope IN ('public',%s)""",
        (provider, owner_scope),
    ))[0])
    conn.execute(
        """INSERT INTO data_upgrades(id,provider,owner_scope,kind,parser_version,state,high_water_raw_id,
                    job_id,created_at,updated_at)
           VALUES(%s,%s,%s,'normalization',%s,'running',%s,%s,%s,%s)
           ON CONFLICT(id) DO NOTHING""",
        (upgrade_id, provider, owner_scope, parser_version, high_water, job_id, timestamp, timestamp),
    )
    row = require_row(conn.execute("SELECT * FROM data_upgrades WHERE id=%s FOR UPDATE", (upgrade_id,)))
    if (row["provider"] != provider or row["owner_scope"] != owner_scope
            or row["parser_version"] != parser_version or int(row["job_id"]) != job_id):
        raise ValueError("Upgrade identity already belongs to different inputs or job")
    return row


def upgrade_batch(conn: Connection, upgrade: Row, *, batch_size: int) -> list[int]:
    return [int(row["id"]) for row in conn.execute(
        """SELECT id FROM raw_payloads WHERE provider=%s AND id>%s AND id<=%s
             AND owner_scope IN ('public',%s)
             ORDER BY id LIMIT %s""",
        (upgrade["provider"], upgrade["last_raw_id"], upgrade["high_water_raw_id"], upgrade["owner_scope"], batch_size),
    )]


@atomic
def checkpoint_upgrade(conn: Connection, upgrade_id: str, raw_id: int, *, done: bool = False) -> None:
    conn.execute(
        """UPDATE data_upgrades SET last_raw_id=GREATEST(last_raw_id,%s),
             processed=processed+CASE WHEN last_raw_id<%s THEN 1 ELSE 0 END,
             state=%s,updated_at=%s,error=NULL WHERE id=%s""",
        (raw_id, raw_id, "done" if done else "running", int(time.time()), upgrade_id),
    )


@atomic
def fail_upgrade(
    conn: Connection, upgrade_id: str, *, job_id: int, provider: str,
    owner_scope: str, error: str,
) -> None:
    conn.execute(
        """UPDATE data_upgrades SET state='error',error=%s,updated_at=%s
             WHERE id=%s AND job_id=%s AND provider=%s AND owner_scope=%s""",
        (error, int(time.time()), upgrade_id, job_id, provider, owner_scope),
    )


DispatchCursor = tuple[float, int]


def pending_dispatch(
    conn: Connection, *, now: float, limit: int = 256, after: DispatchCursor | None = None,
) -> tuple[Row | None, DispatchCursor | None]:
    """Inspect one due window before joining jobs or skipping publisher locks.

    Return the window end even when all of its hints are locked/obsolete, so a
    caller can advance to later eligible work. Reset after a selected row to
    prefer the oldest available hints again; an empty window wraps the cursor.
    """
    window = list(conn.execute(
        """SELECT id,available_at FROM dispatch_outbox
             WHERE delivered_at IS NULL AND superseded_at IS NULL AND available_at<=%s
               AND (available_at,id)>(%s,%s)
             ORDER BY available_at,id LIMIT %s""", (now, *(after or (float("-inf"), 0)), limit),
    ))
    if not window:
        return None, None
    rows = list(conn.execute(
        """SELECT d.*,j.kind,j.revision,j.state,j.next_attempt_at
             FROM dispatch_outbox d JOIN discovery_jobs j ON j.id=d.job_id
             WHERE d.id=ANY(%s) AND d.delivered_at IS NULL AND d.superseded_at IS NULL
               AND d.available_at<=%s
             ORDER BY d.available_at,d.id FOR UPDATE OF d SKIP LOCKED""",
        ([int(row["id"]) for row in window], now),
    ))
    _retire_obsolete_dispatch(conn, rows, now=now)
    selected = next((row for row in rows if _current_dispatch(row)
                     and (row["next_attempt_at"] is None or row["next_attempt_at"] <= now)), None)
    return selected, (float(window[-1]["available_at"]), int(window[-1]["id"]))


def _current_dispatch(row: Row) -> bool:
    return (row["job_revision"] == row["revision"]
            and (row["state"] == "pending" or (row["state"] == "blocked" and row["next_attempt_at"] is not None)))


def _retire_obsolete_dispatch(conn: Connection, rows: list[Row], *, now: float) -> None:
    obsolete = [int(row["id"]) for row in rows if not _current_dispatch(row)]
    if obsolete:
        conn.execute("UPDATE dispatch_outbox SET superseded_at=%s WHERE id=ANY(%s)", (now, obsolete))


@atomic
def delivered_dispatch(conn: Connection, outbox_id: int, *, now: float) -> None:
    conn.execute(
        "UPDATE dispatch_outbox SET delivered_at=%s,attempts=attempts+1,last_error=NULL WHERE id=%s",
        (now, outbox_id),
    )


@atomic
def failed_dispatch(conn: Connection, outbox_id: int, *, now: float, error: str) -> None:
    conn.execute(
        """UPDATE dispatch_outbox SET attempts=attempts+1,available_at=%s,last_error=%s
             WHERE id=%s""", (now + 30, error, outbox_id),
    )


def job_dispatch_state(conn: Connection, job_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT state,revision FROM discovery_jobs WHERE id=%s", (job_id,)).fetchone()
    return None if row is None else dict(row)


@atomic
def maintain_dispatch(
    conn: Connection, *, now: float, retention_seconds: float, limit: int, after_id: int = 0,
) -> int:
    """Reconcile one bounded pending window and prune one bounded history batch.

    The cursor wraps when the window is empty, so long-lived pending rows and
    skipped locks cannot prevent later obsolete revisions from being visited.
    Ordinary job transitions retire their own previous hint in the same write.
    """
    rows = list(conn.execute(
        """SELECT d.id,d.job_revision,j.revision,j.state,j.next_attempt_at
             FROM (SELECT * FROM dispatch_outbox
                    WHERE delivered_at IS NULL AND superseded_at IS NULL AND id>%s
                    ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED) d
             JOIN discovery_jobs j ON j.id=d.job_id ORDER BY d.id""", (after_id, limit),
    ))
    _retire_obsolete_dispatch(conn, rows, now=now)
    conn.execute(
        """DELETE FROM dispatch_outbox WHERE id IN (
             SELECT id FROM dispatch_outbox
              WHERE COALESCE(delivered_at,superseded_at)<%s
              ORDER BY COALESCE(delivered_at,superseded_at),id LIMIT %s
              FOR UPDATE SKIP LOCKED)""", (now - retention_seconds, limit),
    )
    return int(rows[-1]["id"]) if rows else 0
