"""Small bounded fetch-and-normalize service used by the Phase 2 CLI."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass

import httpx

from chess_crawl.config import Config
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.providers.registry import create_provider_client
from chess_crawl.storage.raw import insert_fetch_log, store_raw_payload, update_raw_payload_status


@dataclass(frozen=True)
class IngestResult:
    provider: str
    endpoint_type: str
    status_code: int
    raw_payload_id: int | None
    normalized_ids: tuple[int, ...]
    message: str


def fetch_user_profile(
    conn: sqlite3.Connection,
    provider: str,
    username: str,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    client = create_provider_client(provider, config or Config.from_env(), transport=transport, sleeper=sleeper)
    try:
        if provider == "chess.com":
            etag, last_modified = _latest_validators(conn, f"chess.com/player/{username.strip().lower()}/profile")
            record = client.get_user_profile(username, etag=etag, last_modified=last_modified)
        else:
            record = client.get_user_profile(username)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_user_payload,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )
    finally:
        client.close()


def fetch_chesscom_stats(
    conn: sqlite3.Connection,
    username: str,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    client = create_provider_client("chess.com", config or Config.from_env(), transport=transport, sleeper=sleeper)
    try:
        key = f"chess.com/player/{username.strip().lower()}/stats"
        etag, last_modified = _latest_validators(conn, key)
        record = client.get_user_stats(username, etag=etag, last_modified=last_modified)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_user_payload,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )
    finally:
        client.close()


def fetch_chesscom_archives(
    conn: sqlite3.Connection,
    username: str,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    client = create_provider_client("chess.com", config or Config.from_env(), transport=transport, sleeper=sleeper)
    try:
        key = f"chess.com/player/{username.strip().lower()}/games/archives"
        etag, last_modified = _latest_validators(conn, key)
        record = client.get_archives_index(username, etag=etag, last_modified=last_modified)
        return _store_and_mark_skipped(conn, record, job_id=job_id, crawl_run_id=crawl_run_id)
    finally:
        client.close()


def fetch_chesscom_month(
    conn: sqlite3.Connection,
    username: str,
    year: int,
    month: int,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    client = create_provider_client("chess.com", config or Config.from_env(), transport=transport, sleeper=sleeper)
    try:
        key = f"chess.com/player/{username.strip().lower()}/games/{year:04d}/{month:02d}"
        etag, last_modified = _latest_validators(conn, key)
        record = client.get_monthly_archive(username, year, month, etag=etag, last_modified=last_modified)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_games_payload,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )
    finally:
        client.close()


def fetch_lichess_games(
    conn: sqlite3.Connection,
    username: str,
    *,
    since: int | None,
    until: int | None,
    limit: int,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    client = create_provider_client("lichess", config or Config.from_env(), transport=transport, sleeper=sleeper)
    try:
        record = client.get_user_games(username, since=since, until=until, limit=limit)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_games_payload,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )
    finally:
        client.close()


def fetch_lichess_game(
    conn: sqlite3.Connection,
    game_id: str,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    client = create_provider_client("lichess", config or Config.from_env(), transport=transport, sleeper=sleeper)
    try:
        record = client.get_game(game_id)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_games_payload,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )
    finally:
        client.close()


def _store_and_normalize(
    conn,
    record: RawRecord,
    *,
    normalizer,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    raw_payload_id = _store_raw_if_present(conn, record)
    _log_attempts(conn, record, raw_payload_id, job_id=job_id, crawl_run_id=crawl_run_id)
    if raw_payload_id is None:
        return _non_body_result(record)
    normalized = normalizer(conn, raw_payload_id)
    normalized_ids = tuple(normalized if isinstance(normalized, list) else ([normalized] if normalized else []))
    return IngestResult(
        provider=record.provider,
        endpoint_type=record.endpoint_type,
        status_code=record.http_status,
        raw_payload_id=raw_payload_id,
        normalized_ids=normalized_ids,
        message=f"stored raw #{raw_payload_id}; normalized {len(normalized_ids)} row(s)",
    )


def _store_and_mark_skipped(
    conn,
    record: RawRecord,
    *,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    raw_payload_id = _store_raw_if_present(conn, record)
    _log_attempts(conn, record, raw_payload_id, job_id=job_id, crawl_run_id=crawl_run_id)
    if raw_payload_id is None:
        return _non_body_result(record)
    update_raw_payload_status(
        conn,
        raw_payload_id,
        status="skipped",
        parser_version="phase2-archives-v1",
        normalized_at=int(time.time()),
    )
    return IngestResult(
        provider=record.provider,
        endpoint_type=record.endpoint_type,
        status_code=record.http_status,
        raw_payload_id=raw_payload_id,
        normalized_ids=(),
        message=f"stored raw #{raw_payload_id}; archives index has no normalized table",
    )


def _store_raw_if_present(conn: sqlite3.Connection, record: RawRecord) -> int | None:
    if record.body is None or record.http_status != 200:
        return None
    return store_raw_payload(conn, record, commit=True)


def _log_attempts(
    conn: sqlite3.Connection,
    record: RawRecord,
    raw_payload_id: int | None,
    *,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> None:
    if not record.fetch_attempts:
        insert_fetch_log(
            conn,
            provider=record.provider,
            url=record.request_url,
            endpoint_type=record.endpoint_type,
            status_code=record.http_status,
            attempted_at=record.fetched_at or int(time.time()),
            job_id=job_id,
            crawl_run_id=crawl_run_id,
            raw_payload_id=raw_payload_id,
            bytes_count=len(record.body) if record.body else None,
        )
        return
    for attempt in record.fetch_attempts:
        headers = dict(attempt.response_headers)
        insert_fetch_log(
            conn,
            provider=attempt.provider,
            url=attempt.url,
            endpoint_type=attempt.endpoint_type,
            method=attempt.method,
            status_code=attempt.status_code,
            from_cache=attempt.from_cache,
            etag=headers.get("etag"),
            last_modified=headers.get("last-modified"),
            retry_after=attempt.retry_after,
            bytes_count=attempt.bytes_count,
            duration_ms=attempt.duration_ms,
            attempt=attempt.attempt,
            attempted_at=attempt.attempted_at,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
            raw_payload_id=raw_payload_id if attempt.status_code == 200 else None,
            error_ref=_insert_error_for_attempt(conn, attempt),
            commit=False,
        )
    conn.commit()


def _insert_error_for_attempt(conn: sqlite3.Connection, attempt) -> int | None:
    if attempt.status_code not in {404, 410, 429}:
        return None
    kind = {404: "http_404", 410: "http_410", 429: "http_429"}[attempt.status_code]
    cursor = conn.execute(
        """
        INSERT INTO errors(provider, url, endpoint_type, error_kind, status_code, message, occurred_at, is_dead)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            attempt.provider,
            attempt.url,
            attempt.endpoint_type,
            kind,
            attempt.status_code,
            f"HTTP {attempt.status_code}",
            attempt.attempted_at,
            1 if attempt.status_code in {404, 410} else 0,
        ),
    )
    return int(cursor.lastrowid)


def _non_body_result(record: RawRecord) -> IngestResult:
    if record.http_status == 304:
        message = "not modified; no new raw payload stored"
    elif record.http_status in {404, 410}:
        message = f"provider returned HTTP {record.http_status}; no raw payload stored"
    elif record.http_status == 429:
        message = "rate limited after retry policy; no raw payload stored"
    else:
        message = f"HTTP {record.http_status}; no raw payload stored"
    return IngestResult(
        provider=record.provider,
        endpoint_type=record.endpoint_type,
        status_code=record.http_status,
        raw_payload_id=None,
        normalized_ids=(),
        message=message,
    )


def _latest_validators(conn: sqlite3.Connection, canonical_source_key: str) -> tuple[str | None, str | None]:
    row = conn.execute(
        """
        SELECT response_headers
          FROM raw_payloads
         WHERE canonical_source_key = ?
         ORDER BY fetched_at DESC, id DESC
         LIMIT 1
        """,
        (canonical_source_key,),
    ).fetchone()
    if row is None or not row["response_headers"]:
        return None, None
    headers = json.loads(row["response_headers"])
    return headers.get("etag"), headers.get("last-modified") or headers.get("last_modified")
