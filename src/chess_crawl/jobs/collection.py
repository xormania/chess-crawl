"""Bounded, checkpointed full-history, incremental, and explicit backfill jobs."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx

from chess_crawl.config import Config
from chess_crawl.ingest import (
    IngestResult, fetch_chesscom_archives, fetch_chesscom_month, fetch_lichess_game,
    fetch_lichess_games_page, replay_raw_payload,
)
from chess_crawl.jobs.models import CollectionResult, DiscoveryJob
from chess_crawl.jobs.state import enqueue_payload_normalization
from chess_crawl.normalize.games import PARSER_VERSION
from chess_crawl.storage.normalization import observation_id
from chess_crawl.providers.registry import ProviderSession
from chess_crawl.storage import collection as store
from chess_crawl.storage.acquisition import payload_game_ids
from chess_crawl.storage.db import Connection, transaction
from chess_crawl.storage.raw import payload_observed_at, read_raw_payload, latest_job_payload


def execute_collection(
    conn: Connection, job: DiscoveryJob, params: Mapping[str, Any], *,
    config: Config | None = None, transport: httpx.BaseTransport | None = None,
    session: ProviderSession | None = None, sleeper=None,
    clock: Callable[[], float] = time.time,
    stop_requested: Callable[[], bool] | None = None,
) -> CollectionResult:
    """Perform at most batch_size monthly units, or one Lichess stream page.

    The durable checkpoint advances only after source evidence is committed.
    Coverage 'complete' means acquisition, not successful normalization/analysis.
    """
    if job.id is None:
        raise ValueError("collection requires a persisted job")
    mode = params.get("collection_mode", "incremental")
    if mode not in {"full", "incremental", "backfill"}:
        raise ValueError("collection_mode must be full, incremental, or backfill")
    batch_size = _positive(params, "batch_size", default=4, maximum=100)
    username = job.target.strip().lower()
    if not username:
        raise ValueError("collection requires a username")
    fingerprint = hashlib.sha256(json.dumps(
        [job.provider, username, {key: value for key, value in params.items()
                                 if key not in {"batch_size", "page_size", "max_page_size", "max_games"}}],
        sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()
    cursor = store.checkpoint(conn, job.id)
    if cursor is not None and cursor.get("request_fingerprint") != fingerprint:
        raise ValueError("collection parameters changed after its checkpoint was created")
    options = dict(config=config, transport=transport, session=session, sleeper=sleeper,
                   job_id=job.id, crawl_run_id=job.crawl_run_id)
    now = int(clock())
    if cursor is not None and cursor.get("done"):
        return CollectionResult(True, 0, (), "collection already complete")
    stopped = stop_requested or (lambda: False)
    if stopped():
        return CollectionResult(False, 0, (), "collection paused before acquisition")
    if job.provider == "chess.com":
        return _chesscom(conn, job, params, cursor, username, str(mode), batch_size,
                         fingerprint, options, now, stopped)
    if job.provider == "lichess":
        return _lichess(conn, job, params, cursor, username, str(mode), batch_size,
                        fingerprint, options, int(clock() * 1000), stopped)
    raise ValueError(f"unsupported collection provider: {job.provider}")


def _chesscom(conn, job, params, cursor, username, mode, batch_size, fingerprint, options, now, stopped):
    since_ms, until_ms = _milliseconds(params, "since_ms"), _milliseconds(params, "until_ms")
    if since_ms is not None and until_ms is not None and since_ms > until_ms:
        raise ValueError("since must not exceed until")
    if cursor is None:
        result = fetch_chesscom_archives(conn, username, **options)
        if not _successful(result):
            return _failure(conn, job, username, "archives_index", result, now)
        payload = read_raw_payload(conn, _raw_id(result))
        data = json.loads(payload.body)
        if not isinstance(data, dict) or not isinstance(data.get("archives"), list):
            raise ValueError("invalid Chess.com archives index")
        units = sorted({_archive_unit(str(url), username) for url in data["archives"]})
        units = [unit for unit in units if _month_overlaps(unit, since_ms, until_ms)]
        if params.get("months") is not None:
            requested = params["months"]
            if not isinstance(requested, list) or any(not isinstance(unit, str) for unit in requested):
                raise ValueError("months must be a list of YYYY/MM strings")
            for unit in requested:
                _month_bounds(unit)
            units = [unit for unit in units if unit in requested]
            for unit in set(requested) - set(units):
                store.record_coverage(conn, provider=job.provider, username=username, unit=unit,
                                      state="missing", error="not listed by provider", now=now)
        cursor = {"request_fingerprint": fingerprint, "units": units, "unit_index": 0}
        with transaction(conn):
            for unit in units:
                existing = store.coverage(conn, job.provider, username, unit)
                if existing is None:
                    store.record_coverage(conn, provider=job.provider, username=username,
                                          unit=unit, state="pending", now=now)
            store.save_checkpoint(conn, job.id, cursor, now=now)
    processed = 0
    ids: list[int] = []
    units = cursor["units"]
    while cursor["unit_index"] < len(units) and processed < batch_size:
        if stopped():
            break
        unit = units[cursor["unit_index"]]
        year, month, end = _month_bounds(unit)
        coverage = store.coverage(conn, job.provider, username, unit)
        # Coverage records acquisition progress; another importer may already
        # have preserved a newer successful observation of this same source.
        raw_id = store.latest_successful_source(conn, f"chess.com/player/{username}/games/{unit}")
        if raw_id is None:
            raw_id = coverage["raw_payload_id"] if coverage is not None else None
        sealed = False
        if raw_id is not None:
            sealed = payload_observed_at(conn, raw_id) >= end
        if mode != "backfill" and sealed and raw_id is not None:
            raw = read_raw_payload(conn, raw_id)
            body = json.loads(raw.body)
            if not isinstance(body, dict) or not isinstance(body.get("games"), list):
                raise ValueError("stored monthly source is not a complete games collection")
            ids.extend(_use_local_payload(conn, job, raw.id, options))
            result = IngestResult(job.provider, "monthly_archive", 200, raw.id, (), "reused stored archive")
        else:
            result = fetch_chesscom_month(conn, username, year, month, max_games=None, **options)
            result = _resolve_cached_response(conn, result, f"chess.com/player/{username}/games/{unit}")
            if not _successful(result):
                if result.status_code in {404, 410}:
                    with transaction(conn):
                        _failure(conn, job, username, unit, result, now)
                        cursor["unit_index"] += 1
                        store.save_checkpoint(conn, job.id, cursor, now=now)
                    processed += 1
                    continue
                return _failure(conn, job, username, unit, result, now, processed, tuple(ids))
            ids.extend(result.normalized_ids)
            sealed = now >= end
        raw = read_raw_payload(conn, _raw_id(result))
        # Validate source shape even when normalization is queued separately.
        body = json.loads(raw.body)
        if not isinstance(body, dict) or not isinstance(body.get("games"), list):
            raise ValueError("monthly response is not a complete games collection")
        cursor["unit_index"] += 1
        processed += 1
        with transaction(conn):
            store.record_coverage(
                conn, provider=job.provider, username=username, unit=unit, state="complete",
                raw_payload_id=raw.id, parser_version=raw.parser_version, sealed=sealed, now=now,
            )
            store.save_checkpoint(conn, job.id, cursor, now=now)
    done = cursor["unit_index"] == len(units)
    if done:
        cursor["done"] = True
        store.save_checkpoint(conn, job.id, cursor, now=now)
    return CollectionResult(done, processed, tuple(ids),
                            f"monthly collection {cursor['unit_index']}/{len(units)} units")


def _lichess(conn, job, params, cursor, username, mode, batch_size, fingerprint, options, now_ms, stopped):
    default_page_size = params.get("max_games")
    page_size = _positive(params, "page_size", default=1000 if default_page_size is None else default_page_size,
                          maximum=10000)
    max_page_size = _positive(params, "max_page_size", default=max(10000, page_size), maximum=100000)
    if max_page_size < page_size:
        raise ValueError("max_page_size must be at least page_size")
    now = now_ms // 1000
    if cursor is None:
        since = _milliseconds(params, "since_ms")
        until = _milliseconds(params, "until_ms")
        until = now_ms if until is None else until
        if since is not None and since > until:
            raise ValueError("since_ms must not exceed until_ms")
        requested_since = since
        missing = (store.missing_windows(conn, username, since_ms=since, until_ms=until)
                   if mode != "backfill" else [{"since_ms": since, "until_ms": until}])
        first = missing[0] if missing else {"since_ms": until, "until_ms": until}
        local_high_water = (store.covered_source_high_water(conn, username, until, requested_since)
                            if mode == "full" else 0)
        cursor = {
            "request_fingerprint": fingerprint, "since_ms": first["since_ms"], "upper_ms": until,
            "requested_since_ms": requested_since,
            "until_ms": first["until_ms"], "limit": page_size, "followup_after": "",
            "phase": "stored" if local_high_water else "followups",
            "local_after": 0, "local_high_water": local_high_water,
        }
        store.save_checkpoint(conn, job.id, cursor, now=now)
    ids: list[int] = []
    processed = 0
    if cursor["phase"] == "stored":
        sources = store.covered_sources(conn, username, until_ms=cursor["upper_ms"],
                                        after=cursor["local_after"], high_water=cursor["local_high_water"],
                                        limit=batch_size, since_ms=cursor["requested_since_ms"])
        for raw_id in sources:
            if stopped():
                return CollectionResult(False, processed, tuple(ids), "stored history replay paused")
            ids.extend(_use_local_payload(conn, job, raw_id, options))
            cursor["local_after"] = raw_id
            store.save_checkpoint(conn, job.id, cursor, now=now)
            processed += 1
        if len(sources) < batch_size:
            cursor["phase"] = "followups"
            store.save_checkpoint(conn, job.id, cursor, now=now)
        if processed:
            return CollectionResult(False, processed, tuple(ids), "stored history reused locally")
    if cursor["phase"] == "followups":
        rows = store.followups(conn, username, after=cursor["followup_after"], limit=batch_size)
        for row in rows:
            if stopped():
                return CollectionResult(False, processed, tuple(ids), "collection paused during follow-ups")
            ref = row["game_ref"]
            result = fetch_lichess_game(conn, ref, **options)
            if not _successful(result):
                if result.status_code in {404, 410}:
                    with transaction(conn):
                        _failure(conn, job, username, f"followup/{ref}", result, now)
                        cursor["followup_after"] = ref
                        store.save_checkpoint(conn, job.id, cursor, now=now)
                    processed += 1
                    continue
                return _failure(conn, job, username, f"followup/{ref}", result, now, processed, tuple(ids))
            game = json.loads(read_raw_payload(conn, _raw_id(result)).body)
            if not isinstance(game, dict) or game.get("id") != ref:
                raise ValueError("follow-up response has an unexpected game identity")
            with transaction(conn):
                _track_game(conn, username, game, now)
                cursor["followup_after"] = ref
                store.save_checkpoint(conn, job.id, cursor, now=now)
            processed += 1
            ids.extend(result.normalized_ids)
        if len(rows) == batch_size:
            return CollectionResult(False, processed, tuple(ids), "unfinished-game follow-ups checkpointed")
        cursor["phase"] = "pages"
        store.save_checkpoint(conn, job.id, cursor, now=now)
        if processed:
            return CollectionResult(False, processed, tuple(ids), "unfinished-game follow-ups complete")
    if stopped():
        return CollectionResult(False, processed, tuple(ids), "collection paused before stream page")
    requested_since = cursor.get("requested_since_ms", cursor["since_ms"])
    if mode != "backfill":
        missing = store.missing_windows(conn, username, since_ms=requested_since, until_ms=cursor["upper_ms"])
        first = missing[0] if missing else {"since_ms": cursor["upper_ms"], "until_ms": cursor["upper_ms"]}
        if (first["since_ms"], first["until_ms"]) != (cursor["since_ms"], cursor["until_ms"]):
            cursor["limit"] = page_size
        cursor["since_ms"], cursor["until_ms"] = first["since_ms"], first["until_ms"]
    limit, until, since = cursor["limit"], cursor["until_ms"], cursor["since_ms"]
    if since == until:
        with transaction(conn):
            store.record_coverage(
                conn, provider="lichess", username=username,
                unit=f"window/{requested_since if requested_since is not None else 'open'}..{cursor['upper_ms']}",
                state="complete", since_ms=requested_since, until_ms=cursor["upper_ms"], now=now,
            )
            high_water = store.history_high_water(conn, username)
            if high_water is not None:
                store.record_coverage(conn, provider="lichess", username=username, unit="history", state="complete",
                                      since_ms=None, until_ms=high_water, now=now)
            cursor["done"] = True
            store.save_checkpoint(conn, job.id, cursor, now=now)
        return CollectionResult(True, 0, (), "requested history already covered")
    unit = f"millis/{since if since is not None else 'open'}..{until}/limit/{limit}"
    result = fetch_lichess_games_page(conn, username, since_ms=since, until_ms=until, limit=limit, **options)
    if not _successful(result):
        return _failure(conn, job, username, unit, result, now)
    raw = read_raw_payload(conn, _raw_id(result))
    games = [json.loads(line) for line in raw.body.splitlines() if line.strip()]
    if len(games) > limit:
        raise ValueError("provider returned more games than the bounded page limit")
    timestamps: list[int] = []
    for game in games:
        if not isinstance(game, dict):
            raise ValueError("NDJSON game must be an object")
        created = game.get("createdAt")
        if type(created) is not int or created >= until or (since is not None and created < since):
            raise ValueError("game timestamp is outside the requested millisecond window")
        timestamps.append(created)
    if timestamps != sorted(timestamps, reverse=True):
        raise ValueError("provider page is not ordered by descending creation time")
    range_done = len(games) < limit
    # A saturated descending page proves only the range strictly newer than
    # its oldest timestamp. Retain that oldest millisecond as a missing tie.
    covered_since = since if range_done else timestamps[-1] + 1
    with transaction(conn):
        store.record_source_range(conn, username, raw.id, since_ms=covered_since, until_ms=until)
        for game in games:
            _track_game(conn, username, game, now)
    done = (range_done if mode == "backfill" else not store.missing_windows(
        conn, username, since_ms=requested_since, until_ms=cursor["upper_ms"],
    ))
    if not range_done:
        oldest = timestamps[-1]
        boundary_count = timestamps.count(oldest)
        next_until = oldest + 1  # Provider upper bound is exclusive; overlap every boundary tie.
        next_limit = page_size + boundary_count
        if next_until >= until:
            next_limit = max(next_limit, limit * 2)
        if next_limit > max_page_size:
            store.record_coverage(conn, provider="lichess", username=username, unit=unit,
                                  state="error", raw_payload_id=raw.id,
                                  error="timestamp boundary exceeds configured page budget", now=now)
            raise ValueError("timestamp boundary exceeds max_page_size; increase the budget and resume")
        cursor["until_ms"], cursor["limit"] = next_until, next_limit
    with transaction(conn):
        store.record_coverage(
            conn, provider="lichess", username=username, unit=unit, state="complete",
            raw_payload_id=raw.id, parser_version=raw.parser_version,
            since_ms=since, until_ms=until, now=now,
        )
        if range_done:
            store.record_coverage(
                conn, provider="lichess", username=username,
                unit=f"window/{since if since is not None else 'open'}..{until}",
                state="complete", raw_payload_id=raw.id, since_ms=since,
                until_ms=until, now=now,
            )
        high_water = store.history_high_water(conn, username)
        if high_water is not None:
            store.record_coverage(conn, provider="lichess", username=username, unit="history", state="complete",
                                  raw_payload_id=raw.id, since_ms=None, until_ms=high_water, now=now)
        if done:
            cursor["done"] = True
            store.record_coverage(conn, provider="lichess", username=username,
                                  unit=f"window/{requested_since if requested_since is not None else 'open'}..{cursor['upper_ms']}",
                                  state="complete", raw_payload_id=raw.id, since_ms=requested_since,
                                  until_ms=cursor["upper_ms"], now=now)
        store.save_checkpoint(conn, job.id, cursor, now=now)
    return CollectionResult(done, 1, result.normalized_ids,
                            f"stream page captured {len(games)} game(s); {'complete' if done else 'checkpointed'}")


def _use_local_payload(conn, job, raw_id, options) -> tuple[int, ...]:
    raw = read_raw_payload(conn, raw_id)
    needs_parser = raw.parser_version != PARSER_VERSION or raw.normalization_status not in {"parsed", "skipped"}
    if getattr(conn, "_defer_normalization", False) and (needs_parser or job.crawl_run_id is not None):
        captured = latest_job_payload(conn, job.id, endpoint_type=raw.endpoint_type,
                                      source_key=raw.canonical_source_key)
        enqueue_payload_normalization(
            conn, provider=job.provider, raw_payload_id=raw.id, max_games=None,
            fetch_log_id=captured[1] if captured is not None else observation_id(conn, raw.id),
            crawl_run_id=job.crawl_run_id, parent_job_id=job.id, parser_version=PARSER_VERSION,
        )
        return ()
    if needs_parser or job.crawl_run_id is not None:
        return replay_raw_payload(conn, raw.id, crawl_run_id=job.crawl_run_id).normalized_ids
    ids = sorted(payload_game_ids(conn, raw.id))
    return tuple(ids)


def _track_game(conn, username, game, now):
    ref, created, status = game.get("id"), game.get("createdAt"), game.get("status")
    if not isinstance(ref, str) or not ref or type(created) is not int or created < 0:
        raise ValueError("game identity/creation timestamp is missing")
    if not isinstance(status, str):
        raise ValueError("game status is missing")
    terminal = {"aborted", "mate", "resign", "stalemate", "timeout", "draw", "outoftime",
                "cheat", "noStart", "unknownFinish", "variantEnd"}
    store.track_followup(conn, username=username, game_ref=ref, created_ms=created,
                         finished=status in terminal, now=now)


def _successful(result: IngestResult) -> bool:
    return result.status_code in {200, 304} and result.raw_payload_id is not None


def _resolve_cached_response(conn: Connection, result: IngestResult, key: str) -> IngestResult:
    # Existing ingestion deliberately returns no body ID for a current-parser
    # 304. Coverage still needs the referenced preserved response, not a new copy.
    if result.status_code == 304 and result.raw_payload_id is None:
        raw_id = store.latest_successful_source(conn, key)
        if raw_id is not None:
            return replace(result, raw_payload_id=raw_id)
    return result


def _raw_id(result: IngestResult) -> int:
    if result.raw_payload_id is None:
        raise ValueError("successful acquisition is missing source evidence")
    return result.raw_payload_id


def _failure(conn, job, username, unit, result, now, processed=0, ids=()) -> CollectionResult:
    store.record_coverage(
        conn, provider=job.provider, username=username, unit=unit,
        state="missing" if result.status_code in {404, 410} else "error",
        raw_payload_id=result.raw_payload_id, error=result.message, now=now,
    )
    return CollectionResult(False, processed, ids, result.message,
                            result.status_code, result.retry_after)


def _positive(params: Mapping[str, Any], key: str, *, default: int, maximum: int) -> int:
    value = params.get(key, default)
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{key} must be an integer between 1 and {maximum}")
    return value


def _milliseconds(params: Mapping[str, Any], key: str) -> int | None:
    value = params.get(key)
    if value is None:
        seconds = params.get(key.removesuffix("_ms"))
        if seconds is not None:
            if type(seconds) is not int or seconds < 0:
                raise ValueError(f"{key.removesuffix('_ms')} must be a nonnegative integer")
            value = seconds * 1000
    if value is not None and (type(value) is not int or not 0 <= value < 2**63):
        raise ValueError(f"{key} must be a nonnegative PostgreSQL bigint")
    return value


def _month_overlaps(unit: str, since_ms: int | None, until_ms: int | None) -> bool:
    year, month, end = _month_bounds(unit)
    start_ms = int(datetime(year, month, 1, tzinfo=UTC).timestamp()) * 1000
    return (since_ms is None or end * 1000 > since_ms) and (until_ms is None or start_ms < until_ms)


def _archive_unit(url: str, username: str) -> str:
    parsed = urlsplit(url)
    prefix = f"/pub/player/{username}/games/"
    path = unquote(parsed.path).lower()
    if (parsed.scheme != "https" or parsed.netloc != "api.chess.com" or parsed.query
            or parsed.fragment or not path.startswith(prefix)):
        raise ValueError("archives index contains an unexpected source URL")
    unit = path[len(prefix):]
    _month_bounds(unit)
    return unit


def _month_bounds(unit: str) -> tuple[int, int, int]:
    if re.fullmatch(r"\d{4}/\d{2}", unit) is None:
        raise ValueError("monthly units must use YYYY/MM")
    year, month = map(int, unit.split("/"))
    datetime(year, month, 1, tzinfo=UTC)  # Reject impossible provider dates before a fetch.
    end = datetime(year + (month == 12), 1 if month == 12 else month + 1, 1, tzinfo=UTC)
    return year, month, int(end.timestamp())
