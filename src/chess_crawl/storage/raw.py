"""Raw-first payload persistence helpers."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Mapping

from chess_crawl.providers.base import RawRecord


COMPRESSION_THRESHOLD_BYTES = 4096


@dataclass(frozen=True)
class StoredRawPayload:
    id: int
    provider: str
    endpoint_type: str
    canonical_source_key: str
    request_url: str | None
    request_params: str | None
    response_headers: str | None
    content_type: str | None
    fetched_at: int
    body_hash: str
    body: bytes
    compression: str
    normalization_status: str
    parser_version: str | None


def compute_body_hash(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def store_raw_payload(
    conn: sqlite3.Connection,
    record: RawRecord,
    *,
    parser_version: str | None = None,
    normalization_status: str = "pending",
    commit: bool = True,
) -> int:
    if record.body is None:
        raise ValueError("raw payload storage requires body bytes")

    body_hash = record.body_hash or compute_body_hash(record.body)
    existing = conn.execute(
        """
        SELECT id FROM raw_payloads
        WHERE provider = ? AND endpoint_type = ? AND canonical_source_key = ? AND body_hash = ?
        ORDER BY id LIMIT 1
        """,
        (record.provider, record.endpoint_type, record.canonical_source_key, body_hash),
    ).fetchone()
    if existing is not None:
        if commit:
            conn.commit()
        return int(existing["id"])

    compression, stored_body = _encode_body(record.body)
    response_headers = dict(record.response_headers)
    if record.etag is not None:
        response_headers.setdefault("etag", record.etag)
    if record.last_modified is not None:
        response_headers.setdefault("last_modified", record.last_modified)

    cursor = conn.execute(
        """
        INSERT INTO raw_payloads(
          provider, endpoint_type, provider_url, canonical_source_key,
          request_params, response_status, response_headers, content_type,
          fetched_at, body_hash, body_compression, raw_body, body_bytes,
          parser_version, normalization_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record.provider,
            record.endpoint_type,
            record.request_url,
            record.canonical_source_key,
            _json(record.request_params),
            record.http_status,
            _json(response_headers),
            record.media_type,
            record.fetched_at or int(time.time()),
            body_hash,
            compression,
            stored_body,
            len(record.body),
            parser_version,
            normalization_status,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("raw payload insert did not return a row id")
    raw_payload_id = int(cursor.lastrowid)
    if commit:
        conn.commit()
    return raw_payload_id


def read_raw_payload(conn: sqlite3.Connection, raw_payload_id: int) -> StoredRawPayload:
    row = conn.execute(
        "SELECT * FROM raw_payloads WHERE id = ?",
        (raw_payload_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"raw payload not found: {raw_payload_id}")

    body = _decode_body(row["raw_body"], row["body_compression"])
    body_hash = compute_body_hash(body)
    if body_hash != row["body_hash"]:
        raise ValueError(f"raw payload hash mismatch for id {raw_payload_id}")

    return StoredRawPayload(
        id=int(row["id"]),
        provider=row["provider"],
        endpoint_type=row["endpoint_type"],
        canonical_source_key=row["canonical_source_key"],
        request_url=row["provider_url"],
        request_params=row["request_params"],
        response_headers=row["response_headers"],
        content_type=row["content_type"],
        fetched_at=int(row["fetched_at"]),
        body_hash=row["body_hash"],
        body=body,
        compression=row["body_compression"],
        normalization_status=row["normalization_status"],
        parser_version=row["parser_version"],
    )


def insert_source_record(
    conn: sqlite3.Connection,
    *,
    entity_type: str,
    entity_id: int,
    provider: str,
    endpoint_type: str,
    raw_payload_id: int,
    source_key: str | None = None,
    json_pointer: str | None = None,
    first_seen_at: int | None = None,
    commit: bool = True,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO source_records(
          entity_type, entity_id, provider, endpoint_type, source_key,
          json_pointer, raw_payload_id, first_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(entity_type, entity_id, raw_payload_id) DO NOTHING
        """,
        (
            entity_type,
            entity_id,
            provider,
            endpoint_type,
            source_key,
            json_pointer,
            raw_payload_id,
            first_seen_at or int(time.time()),
        ),
    )
    if commit:
        conn.commit()

    if cursor.lastrowid:
        return int(cursor.lastrowid)

    row = conn.execute(
        """
        SELECT id FROM source_records
        WHERE entity_type = ? AND entity_id = ? AND raw_payload_id = ?
        """,
        (entity_type, entity_id, raw_payload_id),
    ).fetchone()
    if row is None:
        raise RuntimeError("source record upsert did not return or find a row")
    return int(row["id"])


def insert_fetch_log(
    conn: sqlite3.Connection,
    *,
    provider: str,
    url: str,
    endpoint_type: str,
    attempted_at: int,
    method: str = "GET",
    status_code: int | None = None,
    from_cache: bool = False,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    retry_after: int | None = None,
    bytes_count: int | None = None,
    duration_ms: int | None = None,
    attempt: int = 1,
    raw_payload_id: int | None = None,
    error_ref: int | None = None,
    commit: bool = True,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO fetch_logs(
          provider, job_id, crawl_run_id, url, endpoint_type, method,
          status_code, from_cache, etag, last_modified, retry_after, bytes,
          duration_ms, attempt, attempted_at, raw_payload_id, error_ref
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            provider,
            job_id,
            crawl_run_id,
            url,
            endpoint_type,
            method,
            status_code,
            int(from_cache),
            etag,
            last_modified,
            retry_after,
            bytes_count,
            duration_ms,
            attempt,
            attempted_at,
            raw_payload_id,
            error_ref,
        ),
    )
    if commit:
        conn.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("fetch log insert did not return a row id")
    return int(cursor.lastrowid)


def update_raw_payload_status(
    conn: sqlite3.Connection,
    raw_payload_id: int,
    *,
    status: str,
    parser_version: str | None = None,
    normalized_at: int | None = None,
    error_ref: int | None = None,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        UPDATE raw_payloads
           SET normalization_status = ?,
               parser_version = COALESCE(?, parser_version),
               normalized_at = COALESCE(?, normalized_at),
               error_ref = COALESCE(?, error_ref)
         WHERE id = ?
        """,
        (
            status,
            parser_version,
            normalized_at or int(time.time()),
            error_ref,
            raw_payload_id,
        ),
    )
    if commit:
        conn.commit()


def _encode_body(body: bytes) -> tuple[str, bytes]:
    if len(body) < COMPRESSION_THRESHOLD_BYTES:
        return "none", body
    return "gzip", gzip.compress(body)


def _decode_body(stored_body: bytes, compression: str) -> bytes:
    if compression == "none":
        return stored_body
    if compression == "gzip":
        return gzip.decompress(stored_body)
    raise ValueError(f"unsupported raw body compression: {compression}")


def _json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"))
