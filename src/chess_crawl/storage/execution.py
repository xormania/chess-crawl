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


def pending_dispatch(conn: Connection, *, now: float) -> Row | None:
    conn.execute(
        """UPDATE dispatch_outbox d SET superseded_at=%s FROM discovery_jobs j
             WHERE j.id=d.job_id AND d.delivered_at IS NULL AND d.superseded_at IS NULL
               AND (d.job_revision<>j.revision OR j.state NOT IN ('pending','blocked'))""", (now,),
    )
    return conn.execute(
        """SELECT d.*,j.kind FROM dispatch_outbox d JOIN discovery_jobs j ON j.id=d.job_id
             WHERE d.delivered_at IS NULL AND d.superseded_at IS NULL AND d.available_at<=%s
               AND j.state IN ('pending','blocked')
               AND d.job_revision=j.revision AND (j.next_attempt_at IS NULL OR j.next_attempt_at<=%s)
             ORDER BY d.id LIMIT 1 FOR UPDATE OF d SKIP LOCKED""", (now, now),
    ).fetchone()


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
