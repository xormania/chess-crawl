"""Bounded fetch-and-normalize service for provider data."""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import httpx

from chess_crawl.config import Config
from chess_crawl.jobs.state import defer_provider
from chess_crawl.normalize.games import PARSER_VERSION as GAMES_PARSER_VERSION
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.normalize.users import PARSER_VERSION as USERS_PARSER_VERSION
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.normalize.resources import PARSER_VERSION as RESOURCES_PARSER_VERSION
from chess_crawl.normalize.resources import normalize_resource_payload
from chess_crawl.providers.base import FetchAttempt, RawRecord
from chess_crawl.providers.registry import ProviderSession, get_provider_info
from chess_crawl.storage.db import Connection, transaction
from chess_crawl.storage.raw import (
    insert_fetch_log, latest_raw_payload_id, latest_validators,
    store_raw_payload, update_raw_payload_status,
    prepare_raw_payload,
    latest_job_payload,
    raw_payload_metadata,
    PreparedRawPayload,
)
from chess_crawl.storage.repository import insert_error, user_identity_transaction
from chess_crawl.storage.player_profiles import record_resource_attempt, resolve_capture_account
from chess_crawl.providers.resources import resource_source_key


@dataclass(frozen=True)
class IngestResult:
    provider: str
    endpoint_type: str
    status_code: int
    raw_payload_id: int | None
    normalized_ids: tuple[int, ...]
    message: str
    retry_after: float | None = None


def fetch_user_profile(
    conn: Connection,
    provider: str,
    username: str,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    session: ProviderSession | None = None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    with _provider_client(provider, config=config, transport=transport, sleeper=sleeper, session=session) as client:
        if provider == "chess.com":
            etag, last_modified = latest_validators(conn, f"chess.com/player/{username.strip().lower()}/profile")
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


def fetch_chesscom_stats(
    conn: Connection,
    username: str,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    session: ProviderSession | None = None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    with _provider_client("chess.com", config=config, transport=transport, sleeper=sleeper, session=session) as client:
        key = f"chess.com/player/{username.strip().lower()}/stats"
        etag, last_modified = latest_validators(conn, key)
        record = client.get_user_stats(username, etag=etag, last_modified=last_modified)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_user_payload,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )


def fetch_user_resource(
    conn: Connection, provider: str, username: str, resource_key: str, *,
    parameters: dict[str, Any] | None = None, owner_scope: str = "public",
    config: Config | None = None, transport: httpx.BaseTransport | None = None, sleeper=None,
    session: ProviderSession | None = None, job_id: int | None = None, crawl_run_id: int | None = None,
) -> IngestResult:
    key = resource_source_key(provider, username, resource_key, parameters, owner_scope=owner_scope)
    with _provider_client(provider, config=config, transport=transport, sleeper=sleeper, session=session) as client:
        etag, last_modified = latest_validators(conn, key) if provider == "chess.com" else (None, None)
        record = client.get_user_resource(
            username, resource_key, parameters=parameters, owner_scope=owner_scope, etag=etag, last_modified=last_modified,
        )
        return _store_and_normalize(
            conn, record, normalizer=normalize_resource_payload, job_id=job_id, crawl_run_id=crawl_run_id,
        )


def fetch_chesscom_archives(
    conn: Connection,
    username: str,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    session: ProviderSession | None = None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    key = f"chess.com/player/{username.strip().lower()}/games/archives"
    stored = latest_job_payload(conn, job_id, endpoint_type="archives_index", source_key=key) if job_id is not None and conn._work_budget_id is not None else None
    if stored is not None:
        return IngestResult("chess.com", "archives_index", 200, stored[0], (), "reused original acquisition")
    with _provider_client("chess.com", config=config, transport=transport, sleeper=sleeper, session=session) as client:
        etag, last_modified = latest_validators(conn, key)
        record = client.get_archives_index(username, etag=etag, last_modified=last_modified)
        return _store_and_mark_skipped(conn, record, job_id=job_id, crawl_run_id=crawl_run_id)


def fetch_chesscom_month(
    conn: Connection,
    username: str,
    year: int,
    month: int,
    *,
    max_games: int | None = None,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    session: ProviderSession | None = None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    key = f"chess.com/player/{username.strip().lower()}/games/{year:04d}/{month:02d}"
    reused = _reuse_game_acquisition(conn, provider="chess.com", endpoint_type="monthly_archive",
        source_key=key, max_games=max_games, job_id=job_id, crawl_run_id=crawl_run_id)
    if reused is not None:
        return reused
    with _provider_client("chess.com", config=config, transport=transport, sleeper=sleeper, session=session) as client:
        etag, last_modified = latest_validators(conn, key)
        record = client.get_monthly_archive(username, year, month, etag=etag, last_modified=last_modified)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_games_payload,
            max_games=max_games,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )


def fetch_lichess_games(
    conn: Connection,
    username: str,
    *,
    since: int | None,
    until: int | None,
    limit: int,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    session: ProviderSession | None = None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    # A bounded job owns one source window. Its remaining new-game allowance
    # can shrink after a partial commit; retain its original acquired body.
    reused = _reuse_game_acquisition(conn, provider="lichess", endpoint_type="user_games_stream",
        source_key=None, max_games=limit, job_id=job_id, crawl_run_id=crawl_run_id)
    if reused is not None:
        return reused
    with _provider_client("lichess", config=config, transport=transport, sleeper=sleeper, session=session) as client:
        record = client.get_user_games(username, since=since, until=until, limit=limit)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_games_payload,
            max_games=limit,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )


def fetch_lichess_game(
    conn: Connection,
    game_id: str,
    *,
    config: Config | None = None,
    transport: httpx.BaseTransport | None = None,
    sleeper=None,
    session: ProviderSession | None = None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    reused = _reuse_game_acquisition(conn, provider="lichess", endpoint_type="game",
        source_key=f"lichess/game/{game_id.strip()}", max_games=1, job_id=job_id, crawl_run_id=crawl_run_id)
    if reused is not None:
        return reused
    with _provider_client("lichess", config=config, transport=transport, sleeper=sleeper, session=session) as client:
        record = client.get_game(game_id)
        return _store_and_normalize(
            conn,
            record,
            normalizer=normalize_games_payload,
            max_games=1,
            job_id=job_id,
            crawl_run_id=crawl_run_id,
        )


def fetch_lichess_games_page(
    conn: Connection, username: str, *, since_ms: int | None, until_ms: int,
    limit: int, config: Config | None = None,
    transport: httpx.BaseTransport | None = None, sleeper=None,
    session: ProviderSession | None = None, job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    """Fetch a page using the provider's native millisecond boundaries."""
    unit = f"millis-{since_ms if since_ms is not None else 'open'}..{until_ms}-limit-{limit}"
    reused = _reuse_game_acquisition(conn, provider="lichess", endpoint_type="user_games_stream",
        source_key=f"lichess/games/user/{username.strip().lower()}/{unit}", max_games=None,
        job_id=job_id, crawl_run_id=crawl_run_id)
    if reused is not None:
        return reused
    with _provider_client("lichess", config=config, transport=transport, sleeper=sleeper, session=session) as client:
        record = client.get_user_games_page(
            username, since_ms=since_ms, until_ms=until_ms, limit=limit,
        )
        return _store_and_normalize(
            conn, record, normalizer=normalize_games_payload, max_games=None,
            job_id=job_id, crawl_run_id=crawl_run_id,
        )


def _reuse_game_acquisition(
    conn: Connection, *, provider: str, endpoint_type: str, source_key: str | None,
    max_games: int | None, job_id: int | None, crawl_run_id: int | None,
) -> IngestResult | None:
    """Resume the exact preserved job occurrence without fabricating a fetch."""
    if job_id is None or conn._work_budget_id is None:
        return None
    stored = latest_job_payload(conn, job_id, endpoint_type=endpoint_type, source_key=source_key)
    if stored is None:
        return None
    raw_id, fetch_id = stored
    if conn._defer_normalization:
        from chess_crawl.jobs.state import enqueue_job
        enqueue_job(conn, provider=provider, kind="normalize_payload", target=str(raw_id),
                    params={"raw_payload_id": raw_id, "max_games": max_games, "fetch_log_id": fetch_id},
                    crawl_run_id=crawl_run_id, parent_job_id=job_id, priority=20)
        ids: tuple[int, ...] = ()
    else:
        ids = tuple(normalize_games_payload(conn, raw_id, crawl_run_id=crawl_run_id, max_games=max_games))
    return IngestResult(provider, endpoint_type, 200, raw_id, ids, "reused original acquisition")


@contextmanager
def _provider_client(provider: str, *, config, transport, sleeper, session):
    if session is not None:
        yield session.client(provider)
    else:
        with ProviderSession(config or Config.from_env(), transport=transport, sleeper=sleeper) as owned:
            yield owned.client(provider)


def replay_raw_payload(
    conn: Connection, raw_payload_id: int, *,
    crawl_run_id: int | None = None, max_games: int | None = None,
    fetch_log_id: int | None = None,
) -> IngestResult:
    """Normalize a durable raw payload without making or fabricating a fetch.

    This is safe after a parser failure or interruption: normalization and
    provenance are transactional and existing normalized entities are upserted.
    """
    if conn._work_budget_id is not None:
        from chess_crawl.storage.work_budgets import reserve_normalization
        reserve_normalization(conn, conn._work_budget_id)
        conn._work_payload_read_credits += 1
    raw = raw_payload_metadata(conn, raw_payload_id)
    normalized: list[int] | int | None
    if raw["endpoint_type"] in {"user_profile", "user_stats"}:
        normalized = normalize_user_payload(conn, raw_payload_id, fetch_log_id=fetch_log_id)
    elif raw["endpoint_type"] == "user_resource":
        normalized = normalize_resource_payload(conn, raw_payload_id, fetch_log_id=fetch_log_id)
    elif raw["endpoint_type"] in {"monthly_archive", "user_games_stream", "game"}:
        normalized = normalize_games_payload(
            conn, raw_payload_id, crawl_run_id=crawl_run_id, max_games=max_games,
        )
    elif raw["endpoint_type"] == "archives_index":
        _mark_archives_skipped(conn, raw_payload_id)
        normalized = None
    else:
        raise ValueError(f"unsupported raw endpoint: {raw['endpoint_type']}")
    ids = _normalized_ids(normalized)
    return IngestResult(
        provider=raw["provider"], endpoint_type=raw["endpoint_type"],
        status_code=200, raw_payload_id=raw_payload_id, normalized_ids=ids,
        message=f"replayed raw #{raw_payload_id}; normalized {len(ids)} row(s)",
    )


def installed_parser_target(requested: str = "current") -> str:
    """Pin a mixed-endpoint replay to the complete installed parser manifest."""
    manifest = "|".join(("chesscom-archives-index-v1", GAMES_PARSER_VERSION,
                         RESOURCES_PARSER_VERSION, USERS_PARSER_VERSION))
    if requested not in {"current", manifest}:
        raise ValueError("Requested parser target is unavailable; use current or the installed parser manifest")
    return manifest


def _requires_normalization(conn: Connection, raw_payload_id: int) -> bool:
    raw = raw_payload_metadata(conn, raw_payload_id)
    if raw["endpoint_type"] in {"user_profile", "user_stats"}:
        version = USERS_PARSER_VERSION
    elif raw["endpoint_type"] == "user_resource":
        version = RESOURCES_PARSER_VERSION
    elif raw["endpoint_type"] == "archives_index":
        version = "chesscom-archives-index-v1"
    else:
        version = GAMES_PARSER_VERSION
    return raw["normalization_status"] not in {"parsed", "skipped"} or raw["parser_version"] != version


def _normalized_ids(normalized) -> tuple[int, ...]:
    return tuple(normalized if isinstance(normalized, list) else ([normalized] if normalized else []))


def _store_and_normalize(
    conn,
    record: RawRecord,
    *,
    normalizer,
    max_games: int | None = None,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    raw_payload_id, fetch_log_id = _persist_response(conn, record, job_id=job_id, crawl_run_id=crawl_run_id)
    if raw_payload_id is None:
        return _non_body_result(record)
    game_payload = record.endpoint_type in {"monthly_archive", "user_games_stream", "game"}
    current_conditional = record.http_status == 304 and not _requires_normalization(conn, raw_payload_id)
    # A new conditional observation can make this cached game source current
    # again even when its parser is unchanged and there is no crawl run.
    if conn._defer_normalization:
        from chess_crawl.jobs.state import enqueue_job
        enqueue_job(
            conn, provider=record.provider, kind="normalize_payload", target=str(raw_payload_id),
            params={"raw_payload_id": raw_payload_id, "max_games": max_games, "fetch_log_id": fetch_log_id},
            crawl_run_id=crawl_run_id, parent_job_id=job_id, priority=20,
        )
        return IngestResult(record.provider, record.endpoint_type, record.http_status,
                            raw_payload_id, (), f"stored raw #{raw_payload_id}; normalization queued",
                            retry_after=_retry_after(record))
    if game_payload:
        normalized = normalizer(conn, raw_payload_id, crawl_run_id=crawl_run_id, max_games=max_games)
    elif record.endpoint_type in {"user_profile", "user_stats", "user_resource"}:
        # A network acquisition belongs to the account currently holding the
        # requested username. Explicit raw replay keeps the retained observed
        # identity instead (see replay_raw_payload()).
        normalized = normalizer(
            conn, raw_payload_id, prefer_observed_identity=False, fetch_log_id=fetch_log_id,
        )
    else:
        normalized = normalizer(conn, raw_payload_id)
    if current_conditional:
        return _non_body_result(record)
    normalized_ids = _normalized_ids(normalized)
    return IngestResult(
        provider=record.provider,
        endpoint_type=record.endpoint_type,
        status_code=record.http_status,
        raw_payload_id=raw_payload_id,
        normalized_ids=normalized_ids,
        message=f"{'replayed' if record.http_status == 304 else 'stored'} raw #{raw_payload_id}; normalized {len(normalized_ids)} row(s)",
        retry_after=_retry_after(record),
    )


def _store_and_mark_skipped(
    conn,
    record: RawRecord,
    *,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
) -> IngestResult:
    raw_payload_id, _ = _persist_response(conn, record, job_id=job_id, crawl_run_id=crawl_run_id)
    if raw_payload_id is None:
        return _non_body_result(record)
    _mark_archives_skipped(conn, raw_payload_id)
    return IngestResult(
        provider=record.provider,
        endpoint_type=record.endpoint_type,
        status_code=record.http_status,
        raw_payload_id=raw_payload_id,
        normalized_ids=(),
        message=f"stored raw #{raw_payload_id}; archives index has no normalized table",
        retry_after=_retry_after(record),
    )


def _mark_archives_skipped(conn: Connection, raw_payload_id: int) -> None:
    update_raw_payload_status(
        conn, raw_payload_id, status="skipped",
        parser_version="chesscom-archives-index-v1", normalized_at=int(time.time()),
    )


def _persist_response(
    conn: Connection,
    record: RawRecord,
    *,
    job_id: int | None,
    crawl_run_id: int | None,
) -> tuple[int | None, int | None]:
    prepared_payload = (
        prepare_raw_payload(conn, record) if record.body is not None and record.http_status == 200 else None
    )
    # Commit the response and its attempt/error evidence together, before
    # invoking a normalizer that can fail independently.
    identity = (
        user_identity_transaction(conn, record.provider, record.target_username, None)
        if record.endpoint_type in {"user_resource", "user_stats"} and record.target_username
        else transaction(conn)
    )
    with identity:
        raw_payload_id = _store_raw_if_present(conn, record, prepared_payload=prepared_payload)
        if record.http_status == 304:
            raw_payload_id = latest_raw_payload_id(conn, record.canonical_source_key)
        provider_user_id = None
        if record.endpoint_type == "user_resource":
            provider_user_id = record_resource_attempt(conn, record, raw_payload_id)
        elif (
            record.endpoint_type == "user_stats" and record.target_username
            and record.http_status in {200, 304} and raw_payload_id is not None
        ):
            provider_user_id = resolve_capture_account(
                conn, provider=record.provider, username=record.target_username, observed_at=record.fetched_at,
            )
        fetch_log_id = _log_attempts(
            conn, record, raw_payload_id, provider_user_id=provider_user_id,
            job_id=job_id, crawl_run_id=crawl_run_id,
        )
        retry_after = _retry_after(record)
        if record.http_status in {429, 503} and retry_after is not None:
            response_at = float(record.fetched_at)
            if record.fetch_attempts:
                last_attempt = record.fetch_attempts[-1]
                response_at = last_attempt.attempted_at + (last_attempt.duration_ms or 0) / 1000
            defer_provider(
                conn, record.provider, not_before=response_at + retry_after,
                reason=f"HTTP {record.http_status}", now=response_at,
            )
    if record.http_status == 304 and raw_payload_id is None:
        raise ValueError("provider returned HTTP 304 without a stored raw payload")
    return raw_payload_id, fetch_log_id


def _store_raw_if_present(
    conn: Connection, record: RawRecord, *, prepared_payload: PreparedRawPayload | None = None,
) -> int | None:
    if record.body is None or record.http_status != 200:
        return None
    return store_raw_payload(conn, record, prepared_payload=prepared_payload)


def _log_attempts(
    conn: Connection,
    record: RawRecord,
    raw_payload_id: int | None,
    *,
    job_id: int | None = None,
    crawl_run_id: int | None = None,
    provider_user_id: int | None = None,
) -> int | None:
    if not record.fetch_attempts:
        return insert_fetch_log(
            conn,
            provider=record.provider,
            url=record.request_url,
            endpoint_type=record.endpoint_type,
            status_code=record.http_status,
            from_cache=record.http_status == 304,
            etag=record.etag,
            last_modified=record.last_modified,
            attempted_at=record.fetched_at or int(time.time()),
            job_id=job_id,
            crawl_run_id=crawl_run_id,
            raw_payload_id=raw_payload_id,
            bytes_count=len(record.body) if record.body else None,
            provider_user_id=provider_user_id,
        )
    successful_log_id = None
    for attempt in record.fetch_attempts:
        headers = dict(attempt.response_headers)
        log_id = insert_fetch_log(
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
            raw_payload_id=raw_payload_id if attempt.status_code in {200, 304} else None,
            error_ref=_insert_error_for_attempt(conn, attempt),
            provider_user_id=provider_user_id if attempt.status_code in {200, 304} else None,
        )
        if attempt.status_code in {200, 304}:
            successful_log_id = log_id
    return successful_log_id


def _insert_error_for_attempt(conn: Connection, attempt: FetchAttempt) -> int | None:
    if attempt.status_code in {200, 304}:
        return None
    if attempt.status_code in {404, 410, 429}:
        kind = f"http_{attempt.status_code}"
    else:
        kind = "timeout" if attempt.error_kind == "timeout" else "other"
    return insert_error(
        conn,
        provider=attempt.provider,
        url=attempt.url,
        endpoint_type=attempt.endpoint_type,
        error_kind=kind,
        status_code=attempt.status_code,
        message=f"HTTP {attempt.status_code}" if attempt.status_code else "provider request failed",
        occurred_at=attempt.attempted_at,
        retry_count=attempt.attempt - 1,
        is_dead=attempt.status_code in {404, 410},
    )


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
        retry_after=_retry_after(record),
    )


def _retry_after(record: RawRecord) -> float | None:
    value = record.fetch_attempts[-1].retry_after if record.fetch_attempts else None
    if record.http_status == 429:
        return get_provider_info(record.provider).policy.next_delay(429, value)
    return float(value) if value is not None else None
