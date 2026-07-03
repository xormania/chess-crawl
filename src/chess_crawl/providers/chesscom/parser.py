"""Chess.com JSON parsers."""

from __future__ import annotations

import json
from typing import Any

from chess_crawl.normalize.codes import canonical_hash, chesscom_outcome, map_variant, normalize_time_class, normalize_username
from chess_crawl.providers.base import NormalizedGame, NormalizedParticipant, NormalizedUser


PROVIDER = "chess.com"


def parse_user_profile(body: bytes) -> NormalizedUser:
    data = json.loads(body)
    username = str(data.get("username") or "")
    status = data.get("status")
    if data.get("closed") and data.get("closed") != "false":
        status = f"closed:{status}" if status else "closed"
    return NormalizedUser(
        provider=PROVIDER,
        provider_user_id=str(data["player_id"]) if data.get("player_id") is not None else None,
        username_normalized=normalize_username(username) or "",
        display_username=username,
        title=data.get("title"),
        account_status_raw=status,
        created_at=_int_or_none(data.get("joined")),
        last_seen_at=_int_or_none(data.get("last_online")),
        country=data.get("country"),
        is_verified=_bool_or_none(data.get("is_verified")),
    )


def parse_archives_index(body: bytes) -> list[str]:
    data = json.loads(body)
    return [str(url) for url in data.get("archives", [])]


def parse_monthly_games(body: bytes) -> list[NormalizedGame]:
    data = json.loads(body)
    return [parse_game(game) for game in data.get("games", [])]


def parse_game(game: dict[str, Any]) -> NormalizedGame:
    white_data = game.get("white") or {}
    black_data = game.get("black") or {}
    white_result = _str_or_none(white_data.get("result"))
    black_result = _str_or_none(black_data.get("result"))
    outcome, is_live = chesscom_outcome(white_result, black_result)
    variant_raw = str(game.get("rules") or "chess")
    variant_key, _ = map_variant(PROVIDER, variant_raw)
    time_class = normalize_time_class(str(game.get("time_class") or "rapid"))
    status_raw = f"white:{white_result or ''};black:{black_result or ''}"

    return NormalizedGame(
        provider=PROVIDER,
        provider_game_id=_str_or_none(game.get("uuid")),
        canonical_url=_str_or_none(game.get("url")),
        content_hash=_game_hash(game),
        rated=bool(game.get("rated", False)),
        variant_key=variant_key,
        variant_raw=variant_raw,
        time_class=time_class,
        time_control_raw=_str_or_none(game.get("time_control")) or time_class,
        outcome=outcome,
        is_live=is_live,
        status_raw=status_raw,
        end_time=_int_or_none(game.get("end_time")),
        start_time=_int_or_none(game.get("start_time")),
        white=_participant("white", white_data),
        black=_participant("black", black_data),
        eco=_str_or_none(game.get("eco")),
        opening_name=None,
        opening_ply=None,
        pgn=_str_or_none(game.get("pgn")),
    )


def _participant(color: str, data: dict[str, Any]) -> NormalizedParticipant:
    username = _str_or_none(data.get("username"))
    result = _str_or_none(data.get("result"))
    return NormalizedParticipant(
        color=color,  # type: ignore[arg-type]
        provider_user_id=None,
        username_normalized=normalize_username(username),
        display_username=username,
        rating=_int_or_none(data.get("rating")),
        rating_diff=None,
        rd=None,
        result_raw=result,
        is_ai=False,
    )


def _game_hash(game: dict[str, Any]) -> str:
    return canonical_hash(
        {
            "provider": PROVIDER,
            "uuid": game.get("uuid"),
            "url": game.get("url"),
            "end_time": game.get("end_time"),
            "rated": game.get("rated"),
            "rules": game.get("rules"),
            "time_control": game.get("time_control"),
            "white": _hash_participant(game.get("white") or {}),
            "black": _hash_participant(game.get("black") or {}),
            "pgn": game.get("pgn"),
        }
    )


def _hash_participant(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "username": data.get("username"),
        "rating": data.get("rating"),
        "result": data.get("result"),
    }


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _bool_or_none(value: object) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _str_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
