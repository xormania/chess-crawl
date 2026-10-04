"""Database references to immutable objects and resumable offline relocation."""
from __future__ import annotations

import gzip
import io
import time
from dataclasses import dataclass

from chess_crawl.storage.db import Connection, atomic, require_row
from chess_crawl.storage.object_store import ObjectStore, digest, object_key, store_for_reference


@atomic
def store_archive_object(conn: Connection, body: bytes, *, store: ObjectStore) -> int:
    body_hash = digest(body)
    encoded = gzip.compress(body, mtime=0)
    stored_hash = digest(encoded)
    # Address encoded bytes, so a future compressor upgrade can coexist without
    # replacing the earlier representation of identical original source bytes.
    key = object_key(stored_hash)
    # Object writes cannot participate in PostgreSQL transactions. Publish and
    # verify first; rollback may leave a reusable orphan, never a dangling FK.
    store.put(key, encoded)
    if digest(store.read(key, expected_size=len(encoded))) != stored_hash:
        raise ValueError("Published archive object checksum mismatch")
    row = conn.execute(
        """INSERT INTO archive_objects(
             backend, location, object_key, body_hash, stored_hash, compression,
             body_bytes, stored_bytes, created_at)
           VALUES (%s, %s, %s, %s, %s, 'gzip', %s, %s, %s)
           ON CONFLICT (backend, location, object_key) DO NOTHING RETURNING id""",
        (store.backend, store.location, key, body_hash, stored_hash, len(body), len(encoded), int(time.time())),
    ).fetchone()
    if row is not None:
        return int(row["id"])
    existing = require_row(conn.execute(
        "SELECT * FROM archive_objects WHERE backend=%s AND location=%s AND object_key=%s",
        (store.backend, store.location, key),
    ))
    if (existing["body_hash"], existing["stored_hash"], existing["body_bytes"], existing["stored_bytes"]) != (
        body_hash, stored_hash, len(body), len(encoded),
    ):
        raise ValueError("Archive object reference conflicts with published bytes")
    return int(existing["id"])


def read_archive_object(conn: Connection, archive_object_id: int, *, store: ObjectStore | None = None) -> bytes:
    row = conn.execute("SELECT * FROM archive_objects WHERE id=%s", (archive_object_id,)).fetchone()
    if row is None:
        raise KeyError(f"Archive object not found: {archive_object_id}")
    selected = store or store_for_reference(row["backend"], row["location"])
    if selected.backend != row["backend"] or selected.location != row["location"]:
        raise ValueError("Archive adapter does not match the stored reference")
    encoded = selected.read(row["object_key"], expected_size=int(row["stored_bytes"]))
    if digest(encoded) != row["stored_hash"]:
        raise ValueError("Archive object checksum mismatch")
    # Bound decompression by the recorded original size, including corrupt gzip.
    with gzip.GzipFile(fileobj=io.BytesIO(encoded)) as stream:
        body = stream.read(int(row["body_bytes"]) + 1)
    if len(body) != row["body_bytes"] or digest(body) != row["body_hash"]:
        raise ValueError("Archive body checksum or size mismatch")
    return body


@atomic
def store_import_backup(
    conn: Connection, body: bytes, *, workspace_id: str, source_name: str, store: ObjectStore,
    media_type: str = "application/x-chess-pgn", captured_at: int | None = None,
) -> int:
    """Retain import evidence before parsing within an explicit workspace."""
    _validate_workspace(workspace_id)
    if not source_name or not media_type:
        raise ValueError("Import source name and media type are required")
    archive_id = store_archive_object(conn, body, store=store)
    row = require_row(conn.execute(
        """INSERT INTO archive_imports(archive_object_id, workspace_id, source_name, media_type, captured_at)
           VALUES (%s, %s, %s, %s, %s) RETURNING id""",
        (archive_id, workspace_id, source_name, media_type, captured_at if captured_at is not None else int(time.time())),
    ))
    return int(row["id"])


@dataclass(frozen=True)
class StoredImportBackup:
    id: int
    workspace_id: str
    source_name: str
    media_type: str
    captured_at: int
    body: bytes


def _validate_workspace(workspace_id: str) -> None:
    if not isinstance(workspace_id, str) or not workspace_id.strip():
        raise ValueError("An explicit nonempty archive import workspace is required")


def read_import_backup(conn: Connection, import_id: int, *, workspace_id: str) -> StoredImportBackup:
    """Never resolve shared object evidence until the owning workspace matches."""
    _validate_workspace(workspace_id)
    row = conn.execute(
        "SELECT * FROM archive_imports WHERE id=%s AND workspace_id=%s", (import_id, workspace_id),
    ).fetchone()
    if row is None:
        raise KeyError("Archive import not found in this workspace")
    return StoredImportBackup(
        id=int(row["id"]), workspace_id=row["workspace_id"], source_name=row["source_name"],
        media_type=row["media_type"], captured_at=int(row["captured_at"]),
        body=read_archive_object(conn, int(row["archive_object_id"])),
    )


@dataclass(frozen=True)
class RelocationResult:
    moved: int
    remaining: int


def relocate_raw_payloads(conn: Connection, *, store: ObjectStore, batch_size: int = 100) -> RelocationResult:
    """Commit each body separately; rerunning skips completed rows without a cursor."""
    if type(batch_size) is not int or not 1 <= batch_size <= 10000:
        raise ValueError("Archive relocation batch size must be between 1 and 10000")
    if conn.in_transaction:
        raise ValueError("Archive relocation requires its own per-payload transactions")
    rows = list(conn.execute(
        "SELECT id FROM raw_payloads WHERE archive_object_id IS NULL ORDER BY id LIMIT %s", (batch_size,),
    ))
    moved = sum(_relocate_payload(conn, int(row["id"]), store=store) for row in rows)
    remaining = int(require_row(conn.execute(
        "SELECT COUNT(*) FROM raw_payloads WHERE archive_object_id IS NULL",
    ))[0])
    return RelocationResult(moved, remaining)


@atomic
def _relocate_payload(conn: Connection, raw_id: int, *, store: ObjectStore) -> int:
    from chess_crawl.storage.raw import read_raw_payload

    row = require_row(conn.execute("SELECT archive_object_id FROM raw_payloads WHERE id=%s FOR UPDATE", (raw_id,)))
    if row["archive_object_id"] is not None:
        return 0
    payload = read_raw_payload(conn, raw_id)
    archive_id = store_archive_object(conn, payload.body, store=store)
    # The last durable inline copy is only released after verified object reads.
    conn.execute(
        "UPDATE raw_payloads SET raw_body=NULL, archive_object_id=%s, body_compression='gzip' WHERE id=%s",
        (archive_id, raw_id),
    )
    return 1
