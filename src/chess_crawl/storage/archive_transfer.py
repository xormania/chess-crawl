"""Verified, resumable transfer of referenced external archive evidence."""
from __future__ import annotations

import argparse
import gzip
import io
import json
import sys
from dataclasses import asdict, dataclass

from chess_crawl.costs import measure_workload
from chess_crawl.storage.archives import PreparedArchiveObject, register_archive_object
from chess_crawl.storage.db import Connection, Row, atomic, database_url, open_database, require_row
from chess_crawl.storage.object_store import ObjectStore, configured_store, digest, object_key, store_for_reference


@dataclass(frozen=True)
class TransferResult:
    objects_moved: int
    raw_payloads_moved: int
    imports_moved: int
    has_more: bool
    next_after_object_id: int


def transfer_archive_objects(
    conn: Connection, *, store: ObjectStore, batch_size: int = 100, after_object_id: int = 0,
) -> TransferResult:
    """Transfer at most one batch; a saved cursor advances without exact counts.

    This operator helper crosses workspace boundaries and must not be exposed as
    a tenant API. Pause source writers for a definitive final sweep from zero.
    """
    if type(batch_size) is not int or not 1 <= batch_size <= 10000:
        raise ValueError("Archive transfer batch size must be between 1 and 10000")
    if type(after_object_id) is not int or not 0 <= after_object_id < 2**63:
        raise ValueError("Archive transfer cursor must be a nonnegative PostgreSQL bigint")
    if conn.in_transaction:
        raise ValueError("Archive transfer requires its own per-object transactions")
    rows = list(conn.execute(
        """SELECT a.* FROM archive_objects a
           WHERE a.id > %s AND (a.backend, a.location) <> (%s, %s)
             AND (EXISTS(SELECT 1 FROM raw_payloads r WHERE r.archive_object_id=a.id)
               OR EXISTS(SELECT 1 FROM archive_imports i WHERE i.archive_object_id=a.id))
           ORDER BY a.id LIMIT %s""",
        (after_object_id, store.backend, store.location, batch_size + 1),
    ))
    objects = raw_payloads = imports = 0
    cursor = after_object_id
    for row in rows[:batch_size]:
        source = _object_metadata(row)
        prepared = _prepare_transfer(source, store)
        raw_count, import_count = _register_transfer(conn, int(row["id"]), source, prepared)
        objects += int(raw_count + import_count > 0)
        raw_payloads += raw_count
        imports += import_count
        cursor = int(row["id"])
    return TransferResult(objects, raw_payloads, imports, len(rows) > batch_size, cursor)


def _object_metadata(row: Row) -> PreparedArchiveObject:
    return PreparedArchiveObject(
        backend=row["backend"], location=row["location"], object_key=row["object_key"],
        body_hash=row["body_hash"], stored_hash=row["stored_hash"],
        body_bytes=int(row["body_bytes"]), stored_bytes=int(row["stored_bytes"]),
    )


def _prepare_transfer(source: PreparedArchiveObject, store: ObjectStore) -> PreparedArchiveObject:
    # Resolve the saved source location, independently of the target configuration.
    # Preserve encoded bytes too, rather than recompressing historical evidence.
    selected = store_for_reference(source.backend, source.location)
    with measure_workload("archive_read") as usage:
        encoded = selected.read(source.object_key, expected_size=source.stored_bytes)
        usage.add(objects_read=1, stored_bytes=len(encoded))
        if digest(encoded) != source.stored_hash:
            raise ValueError("Source archive object checksum mismatch")
        with gzip.GzipFile(fileobj=io.BytesIO(encoded)) as stream:
            body = stream.read(source.body_bytes + 1)
        if len(body) != source.body_bytes or digest(body) != source.body_hash:
            raise ValueError("Source archive body checksum or size mismatch")
        usage.add(source_bytes=len(body))
    key = object_key(source.stored_hash)
    with measure_workload("archive_write") as usage:
        usage.add(source_bytes=source.body_bytes, stored_bytes=len(encoded))
        store.put(key, encoded)
        usage.add(objects_written=1)
        if digest(store.read(key, expected_size=len(encoded))) != source.stored_hash:
            raise ValueError("Transferred archive object checksum mismatch")
        usage.add(objects_read=1)
    return PreparedArchiveObject(
        store.backend, store.location, key, source.body_hash, source.stored_hash,
        source.body_bytes, source.stored_bytes,
    )


@atomic
def _register_transfer(
    conn: Connection, source_id: int, source: PreparedArchiveObject, prepared: PreparedArchiveObject,
) -> tuple[int, int]:
    # The immutable source row is the per-source cutover lock. References created
    # during upload are included here, instead of relying on the earlier snapshot.
    current = require_row(conn.execute("SELECT * FROM archive_objects WHERE id=%s FOR UPDATE", (source_id,)))
    if _object_metadata(current) != source:
        raise ValueError("Source archive metadata changed during transfer")
    if not require_row(conn.execute(
        """SELECT EXISTS(SELECT 1 FROM raw_payloads WHERE archive_object_id=%s)
                 OR EXISTS(SELECT 1 FROM archive_imports WHERE archive_object_id=%s)""",
        (source_id, source_id),
    ))[0]:
        return 0, 0
    target_id = register_archive_object(conn, prepared)
    raw_count = conn.execute(
        """UPDATE raw_payloads SET archive_object_id=%s
           WHERE archive_object_id=%s AND body_hash=%s AND body_bytes=%s""",
        (target_id, source_id, source.body_hash, source.body_bytes),
    ).rowcount
    # A concurrently modified raw row is never repointed based on stale evidence.
    # Roll back the entire cutover if any source reference has inconsistent bytes.
    if require_row(conn.execute(
        "SELECT EXISTS(SELECT 1 FROM raw_payloads WHERE archive_object_id=%s)", (source_id,),
    ))[0]:
        raise ValueError("Raw source metadata conflicts with transferred evidence")
    import_count = conn.execute(
        "UPDATE archive_imports SET archive_object_id=%s WHERE archive_object_id=%s", (target_id, source_id),
    ).rowcount
    return raw_count, import_count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Transfer a bounded batch of referenced external archive objects")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--after-object-id", type=int, default=0)
    args = parser.parse_args(argv)
    try:
        store = configured_store()
        if store is None:
            parser.error("Select a local or s3 CHESS_CRAWL_ARCHIVE_BACKEND destination")
        with open_database(database_url(), writable=True) as conn:
            result = transfer_archive_objects(
                conn, store=store, batch_size=args.batch_size, after_object_id=args.after_object_id,
            )
    except Exception:
        # Object paths, SDK/server errors and configuration secrets stay out of logs.
        print("Archive transfer failed; incomplete objects retain their source references", file=sys.stderr)
        return 1
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
