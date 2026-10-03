"""Normalize stored raw game payloads."""

from __future__ import annotations

import sqlite3
import time
from typing import TypedDict

from chess_crawl.normalize.codes import map_variant
from chess_crawl.providers.base import NormalizedGame, NormalizedParticipant
from chess_crawl.providers.chesscom import parser as chesscom_parser
from chess_crawl.providers.lichess import parser as lichess_parser
from chess_crawl.storage.acquisition import associate_run_game, payload_game_ids, run_game_bounds, run_game_ids
from chess_crawl.storage.db import transaction
from chess_crawl.storage.raw import insert_source_record, read_raw_payload, update_raw_payload_status
from chess_crawl.storage.repository import (
    find_existing_game,
    get_or_create_time_control,
    get_or_create_variant,
    upsert_game,
    upsert_game_participant,
    upsert_provider_user,
    upsert_rating_at_game,
)


PARSER_VERSION = "games-normalizer-v2"


class TimeControlArgs(TypedDict):
    kind: str
    initial_seconds: int | None
    increment_seconds: int | None
    days: int | None
    time_class: str
    raw_label: str


def normalize_games_payload(
    conn: sqlite3.Connection,
    raw_payload_id: int,
    *,
    crawl_run_id: int | None = None,
    max_games: int | None = None,
) -> list[int]:
    """Normalize a bounded selection, retaining the full raw payload for later work.

    With a run, the allowance counts only new run/game associations. Existing
    members can be refreshed by a changed parser without consuming capacity.
    """
    if max_games is not None and max_games < 0:
        raise ValueError("max_games must be nonnegative")
    raw = read_raw_payload(conn, raw_payload_id)
    if raw.provider == "chess.com" and raw.endpoint_type == "monthly_archive":
        games = chesscom_parser.parse_monthly_games(raw.body)
    elif raw.provider == "lichess" and raw.endpoint_type == "user_games_stream":
        games = lichess_parser.parse_games_ndjson(raw.body)
    elif raw.provider == "lichess" and raw.endpoint_type == "game":
        games = [lichess_parser.parse_game_json(raw.body)]
    else:
        return []

    with transaction(conn):
        acquired = run_game_ids(conn, crawl_run_id) if crawl_run_id is not None else set()
        bounds = (
            run_game_bounds(conn, crawl_run_id, provider=raw.provider, requested=max_games)
            if crawl_run_id is not None else None
        )
        remaining = bounds.remaining if bounds is not None else max_games
        # Provenance proves that a row was processed, not which parser produced
        # it. Reuse IDs only when the complete payload is current. A partial
        # upgrade leaves the old version in place until every supported game
        # has been processed by this parser; otherwise stale rows could be
        # silently certified by a later bounded replay.
        current_parser = (
            raw.parser_version == PARSER_VERSION
            and raw.normalization_status in {"parsed", "skipped"}
        )
        processed = payload_game_ids(conn, raw_payload_id) if current_parser else set()
        game_ids: list[int] = []
        complete = True
        supported = False
        for index, game in enumerate(games):
            if game.variant_key == "bughouse":
                continue
            supported = True
            existing = find_existing_game(
                conn, game.provider, game.provider_game_id, game.canonical_url, game.content_hash,
            )
            existing_id = int(existing["id"]) if existing is not None else None
            if bounds is not None and not bounds.includes(game.end_time):
                complete = complete and existing_id in processed
                continue
            already_acquired = existing_id in acquired
            if already_acquired and existing_id in processed:
                continue
            if not already_acquired and remaining == 0:
                complete = complete and existing_id in processed
                continue
            if crawl_run_id is not None and existing_id is not None and existing_id in processed:
                game_id = existing_id
            else:
                game_id = _normalize_game(
                    conn,
                    game,
                    raw_payload_id=raw_payload_id,
                    endpoint_type=raw.endpoint_type,
                    source_key=raw.canonical_source_key,
                    json_pointer=f"/games/{index}" if raw.endpoint_type == "monthly_archive" else f"/{index}",
                    fetched_at=raw.fetched_at,
                )
                processed.add(game_id)
            if already_acquired:
                continue
            if crawl_run_id is not None:
                associate_run_game(conn, crawl_run_id, game_id)
                acquired.add(game_id)
            game_ids.append(game_id)
            if remaining is not None:
                remaining -= 1

        if complete:
            update_raw_payload_status(
                conn,
                raw_payload_id,
                status="parsed" if supported else "skipped",
                parser_version=PARSER_VERSION,
                normalized_at=int(time.time()),
            )
        elif raw.normalization_status != "pending":
            update_raw_payload_status(conn, raw_payload_id, status="pending")
    return game_ids


def _normalize_game(
    conn: sqlite3.Connection,
    game: NormalizedGame,
    *,
    raw_payload_id: int,
    endpoint_type: str,
    source_key: str,
    json_pointer: str,
    fetched_at: int,
) -> int:
    canonical_variant, mapped = map_variant(game.provider, game.variant_raw)
    variant_id = get_or_create_variant(
        conn,
        provider=game.provider,
        provider_native_name=game.variant_raw,
        canonical_name=canonical_variant,
        mapped=mapped,
    )
    clock = parse_time_control(game.time_control_raw, game.time_class)
    time_control_id = get_or_create_time_control(conn, **clock)
    game_id = upsert_game(
        conn,
        provider=game.provider,
        provider_game_id=game.provider_game_id,
        canonical_url=game.canonical_url,
        content_hash=game.content_hash,
        variant_id=variant_id,
        time_control_id=time_control_id,
        rated=bool(game.rated),
        outcome=game.outcome,
        is_live=game.is_live,
        status_raw=game.status_raw,
        created_at=game.start_time,
        ended_at=game.end_time,
        eco=game.eco,
        opening_name=game.opening_name,
        opening_ply=game.opening_ply,
        now=fetched_at,
    )
    insert_source_record(
        conn,
        entity_type="game",
        entity_id=game_id,
        provider=game.provider,
        endpoint_type=endpoint_type,
        source_key=source_key,
        json_pointer=json_pointer,
        raw_payload_id=raw_payload_id,
        first_seen_at=fetched_at,
    )
    _normalize_participant(conn, game_id, game.provider, game.white, game.outcome, raw_payload_id, endpoint_type, source_key, fetched_at)
    _normalize_participant(conn, game_id, game.provider, game.black, game.outcome, raw_payload_id, endpoint_type, source_key, fetched_at)
    return game_id


def _normalize_participant(
    conn: sqlite3.Connection,
    game_id: int,
    provider: str,
    participant: NormalizedParticipant,
    outcome: str | None,
    raw_payload_id: int,
    endpoint_type: str,
    source_key: str,
    fetched_at: int,
) -> None:
    user_id = None
    if participant.username_normalized:
        user_id = upsert_provider_user(
            conn,
            provider=provider,
            username=participant.display_username or participant.username_normalized,
            provider_user_id=participant.provider_user_id,
            display_username=participant.display_username or participant.username_normalized,
            now=fetched_at,
        )
    participant_id = upsert_game_participant(
        conn,
        game_id=game_id,
        color=participant.color,
        provider_user_id=user_id,
        username_normalized=participant.username_normalized,
        result_raw=participant.result_raw,
        is_winner=_is_winner(participant.color, outcome),
        is_ai=participant.is_ai,
    )
    insert_source_record(
        conn,
        entity_type="game_participant",
        entity_id=participant_id,
        provider=provider,
        endpoint_type=endpoint_type,
        source_key=source_key,
        raw_payload_id=raw_payload_id,
        first_seen_at=fetched_at,
    )
    upsert_rating_at_game(
        conn,
        game_id=game_id,
        color=participant.color,
        rating=participant.rating,
        rating_diff=participant.rating_diff,
        rd=participant.rd,
    )


def parse_time_control(raw_label: str | None, time_class: str) -> TimeControlArgs:
    label = raw_label or time_class
    if "/" in label:
        try:
            _, seconds = label.split("/", 1)
            days = max(1, int(int(seconds) / 86400))
        except (TypeError, ValueError):
            days = 1
        return {
            "kind": "correspondence",
            "initial_seconds": None,
            "increment_seconds": None,
            "days": days,
            "time_class": "correspondence",
            "raw_label": label,
        }
    if time_class == "correspondence":
        return {
            "kind": "correspondence",
            "initial_seconds": None,
            "increment_seconds": None,
            "days": None,
            "time_class": time_class,
            "raw_label": label,
        }
    initial: int | None = None
    increment: int | None = 0
    try:
        if "+" in label:
            base, inc = label.split("+", 1)
            initial = int(base)
            increment = int(inc)
        else:
            initial = int(label)
    except (TypeError, ValueError):
        initial = None
        increment = None
    return {
        "kind": "clock",
        "initial_seconds": initial,
        "increment_seconds": increment,
        "days": None,
        "time_class": time_class,
        "raw_label": label,
    }


def _is_winner(color: str, outcome: str | None) -> bool | None:
    if outcome is None or outcome == "draw":
        return None
    return outcome == f"{color}_win"
