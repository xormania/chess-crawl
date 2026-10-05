"""Raw-first payload persistence helpers."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Mapping

from chess_crawl.storage.db import Connection, atomic
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.archives import (
    PreparedArchiveObject, prepare_or_reuse_archive_object, read_archive_object, register_archive_object,
)
from chess_crawl.storage.object_store import ObjectStore, configured_store


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
    owner_scope: str = "public"


def compute_body_hash(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def store_raw_payload(
    conn: Connection,
    record: RawRecord,
    *,
    parser_version: str | None = None,
    normalization_status: str = "pending",
    store: ObjectStore | None = None,
    prepared_object: PreparedArchiveObject | None = None,
) -> int:
    external_expected = store is not None or os.getenv("CHESS_CRAWL_ARCHIVE_BACKEND", "database") != "database"
    if prepared_object is None:
        prepared_object = prepare_raw_payload(conn, record, store=store)
    if prepared_object is not None and store is not None and (
        prepared_object.backend, prepared_object.location,
    ) != (store.backend, store.location):
        raise ValueError("Prepared archive object does not match the selected store")
    return _store_raw_payload(
        conn, record, parser_version=parser_version, normalization_status=normalization_status,
        prepared_object=prepared_object, external_expected=external_expected,
    )


def _body_hash(record: RawRecord) -> str:
    if record.body is None:
        raise ValueError("raw payload storage requires body bytes")
    body_hash = compute_body_hash(record.body)
    if record.body_hash is not None and record.body_hash != body_hash:
        raise ValueError("Supplied raw payload body hash does not match its bytes")
    return body_hash


def _existing_payload(conn: Connection, record: RawRecord, body_hash: str) -> int | None:
    existing = conn.execute(
        """
        SELECT id FROM raw_payloads
        WHERE provider = %s AND endpoint_type = %s AND canonical_source_key = %s AND body_hash = %s AND owner_scope = %s
        ORDER BY id LIMIT 1
        """,
        (record.provider, record.endpoint_type, record.canonical_source_key, body_hash, record.owner_scope),
    ).fetchone()
    if existing is not None:
        return int(existing["id"])
    return None


def prepare_raw_payload(
    conn: Connection, record: RawRecord, *, store: ObjectStore | None = None,
) -> PreparedArchiveObject | None:
    """Avoid duplicate object I/O and publish new bodies before taking write locks."""
    body_hash = _body_hash(record)
    if _existing_payload(conn, record, body_hash) is not None:
        return None
    external_expected = store is not None or os.getenv("CHESS_CRAWL_ARCHIVE_BACKEND", "database") != "database"
    if external_expected and conn.in_transaction:
        raise ValueError("External archive writes in a transaction require a prepared object")
    selected_store = store or configured_store()
    if selected_store is None:
        return None
    if record.body is None:
        raise ValueError("raw payload storage requires body bytes")
    return prepare_or_reuse_archive_object(conn, record.body, store=selected_store)


@atomic
def _store_raw_payload(
    conn: Connection, record: RawRecord, *, parser_version: str | None,
    normalization_status: str, prepared_object: PreparedArchiveObject | None,
    external_expected: bool,
) -> int:
    body_hash = _body_hash(record)
    existing = _existing_payload(conn, record, body_hash)
    if existing is not None:
        return existing
    if record.body is None:
        raise ValueError("raw payload storage requires body bytes")

    archive_id = None
    if prepared_object is None:
        if external_expected:
            raise ValueError("External archive writes in a transaction require a prepared object")
        compression, inline_body = _encode_body(record.body)
        stored_body: bytes | None = inline_body
    else:
        if prepared_object.body_hash != body_hash or prepared_object.body_bytes != len(record.body):
            raise ValueError("Prepared archive object does not match supplied source bytes")
        archive_id = register_archive_object(conn, prepared_object)
        compression, stored_body = "gzip", None
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
          parser_version, normalization_status, archive_object_id, owner_scope
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
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
            archive_id,
            record.owner_scope,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("raw payload insert did not return a row id")
    raw_payload_id = int(row["id"])
    return raw_payload_id


def read_raw_payload(conn: Connection, raw_payload_id: int) -> StoredRawPayload:
    row = conn.execute(
        "SELECT * FROM raw_payloads WHERE id = %s",
        (raw_payload_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"raw payload not found: {raw_payload_id}")

    body = (
        read_archive_object(conn, int(row["archive_object_id"]))
        if row["archive_object_id"] is not None
        else _decode_body(row["raw_body"], row["body_compression"])
    )
    body_hash = compute_body_hash(body)
    if body_hash != row["body_hash"]:
        raise ValueError(f"raw payload hash mismatch for id {raw_payload_id}")
    if len(body) != row["body_bytes"]:
        raise ValueError(f"raw payload size mismatch for id {raw_payload_id}")

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
        owner_scope=row["owner_scope"],
    )


@atomic
def insert_source_record(
    conn: Connection,
    *,
    entity_type: str,
    entity_id: int,
    provider: str,
    endpoint_type: str,
    raw_payload_id: int,
    source_key: str | None = None,
    json_pointer: str | None = None,
    first_seen_at: int | None = None,
) -> int:
    conn.execute(
        """
        INSERT INTO source_records(
          entity_type, entity_id, provider, endpoint_type, source_key,
          json_pointer, raw_payload_id, first_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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

    row = conn.execute(
        """
        SELECT id FROM source_records
        WHERE entity_type = %s AND entity_id = %s AND raw_payload_id = %s
        """,
        (entity_type, entity_id, raw_payload_id),
    ).fetchone()
    if row is None:
        raise RuntimeError("source record upsert did not return or find a row")
    return int(row["id"])


@atomic
def insert_fetch_log(
    conn: Connection,
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
    provider_user_id: int | None = None,
) -> int:
    values = (
        provider, job_id, crawl_run_id, url, endpoint_type, method, status_code,
        int(from_cache), etag, last_modified, retry_after, bytes_count, duration_ms,
        attempt, attempted_at, raw_payload_id, error_ref,
    )
    if provider_user_id is None:
        cursor = conn.execute(
            """INSERT INTO fetch_logs(
                 provider, job_id, crawl_run_id, url, endpoint_type, method,
                 status_code, from_cache, etag, last_modified, retry_after, bytes,
                 duration_ms, attempt, attempted_at, raw_payload_id, error_ref)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING id""",
            values,
        )
    else:
        cursor = conn.execute(
            """INSERT INTO fetch_logs(
                 provider, job_id, crawl_run_id, url, endpoint_type, method,
                 status_code, from_cache, etag, last_modified, retry_after, bytes,
                 duration_ms, attempt, attempted_at, raw_payload_id, error_ref, provider_user_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING id""",
            (*values, provider_user_id),
        )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("fetch log insert did not return a row id")
    return int(row["id"])


@atomic
def update_raw_payload_status(
    conn: Connection,
    raw_payload_id: int,
    *,
    status: str,
    parser_version: str | None = None,
    normalized_at: int | None = None,
    error_ref: int | None = None,
) -> None:
    conn.execute(
        """
        UPDATE raw_payloads
           SET normalization_status = %s,
               parser_version = COALESCE(%s, parser_version),
               normalized_at = COALESCE(%s, normalized_at),
               error_ref = COALESCE(%s, error_ref)
         WHERE id = %s
        """,
        (
            status,
            parser_version,
            normalized_at or int(time.time()),
            error_ref,
            raw_payload_id,
        ),
    )


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


def latest_raw_payload_id(conn: Connection, canonical_source_key: str) -> int | None:
    # Fetch evidence determines the current representation when an older body
    # hash reappears. Raw bodies and their original capture metadata stay immutable.
    row = conn.execute(
        """
        SELECT r.id
          FROM raw_payloads r
          LEFT JOIN fetch_logs f ON f.raw_payload_id = r.id
         WHERE r.canonical_source_key = %s
         ORDER BY COALESCE(f.attempted_at, r.fetched_at) DESC, f.id DESC NULLS LAST, r.id DESC
         LIMIT 1
        """,
        (canonical_source_key,),
    ).fetchone()
    return int(row["id"]) if row is not None else None


def payload_observed_at(conn: Connection, raw_payload_id: int) -> int:
    """Latest successful observation of a body, without rewriting its first capture."""
    row = conn.execute(
        """
        SELECT GREATEST(r.fetched_at, COALESCE(MAX(f.attempted_at), r.fetched_at)) AS observed_at
          FROM raw_payloads r
          LEFT JOIN fetch_logs f ON f.raw_payload_id = r.id AND f.status_code IN (200, 304)
         WHERE r.id = %s
         GROUP BY r.id
        """,
        (raw_payload_id,),
    ).fetchone()
    if row is None or row["observed_at"] is None:
        raise KeyError(f"raw payload not found: {raw_payload_id}")
    return int(row["observed_at"])


def latest_validators(conn: Connection, canonical_source_key: str) -> tuple[str | None, str | None]:
    raw_payload_id = latest_raw_payload_id(conn, canonical_source_key)
    if raw_payload_id is None:
        return None, None
    row = conn.execute(
        """
        SELECT r.response_headers,
               (SELECT etag FROM fetch_logs
                 WHERE raw_payload_id = r.id AND etag IS NOT NULL
                 ORDER BY attempted_at DESC, id DESC LIMIT 1) AS etag,
               (SELECT last_modified FROM fetch_logs
                 WHERE raw_payload_id = r.id AND last_modified IS NOT NULL
                 ORDER BY attempted_at DESC, id DESC LIMIT 1) AS last_modified
          FROM raw_payloads r
         WHERE r.id = %s
        """,
        (raw_payload_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"raw payload not found: {raw_payload_id}")
    headers = json.loads(row["response_headers"] or "{}")
    return (
        row["etag"] or headers.get("etag"),
        row["last_modified"] or headers.get("last-modified") or headers.get("last_modified"),
    )
