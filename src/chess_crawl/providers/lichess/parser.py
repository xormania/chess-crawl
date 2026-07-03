"""Lichess JSON and NDJSON parsers."""

from __future__ import annotations

import json
from typing import Any

from chess_crawl.normalize.codes import canonical_hash, lichess_outcome, map_variant, normalize_time_class, normalize_username
from chess_crawl.providers.base import NormalizedGame, NormalizedParticipant, NormalizedUser


PROVIDER = "lichess"


def parse_user_profile(body: bytes) -> NormalizedUser:
    data = json.loads(body)
    username = str(data.get("username") or data.get("id") or "")
    profile = data.get("profile") or {}
    status = "tosViolation" if data.get("tosViolation") else None
    return NormalizedUser(
        provider=PROVIDER,
        provider_user_id=str(data.get("id") or normalize_username(username) or ""),
        username_normalized=normalize_username(username) or "",
        display_username=username,
        title=data.get("title"),
        account_status_raw=status,
        created_at=_ms_to_s(data.get("createdAt")),
        last_seen_at=_ms_to_s(data.get("seenAt")),
        country=profile.get("country"),
        is_verified=None,
    )


def parse_games_ndjson(body: bytes) -> list[NormalizedGame]:
    games: list[NormalizedGame] = []
    for line in body.splitlines():
        if not line.strip():
            continue
        games.append(parse_game(json.loads(line)))
    return games


def parse_game_json(body: bytes) -> NormalizedGame:
    return parse_game(json.loads(body))


def parse_game(game: dict[str, Any]) -> NormalizedGame:
    winner = _str_or_none(game.get("winner"))
    status = _str_or_none(game.get("status"))
    outcome, is_live = lichess_outcome(winner, status)
    variant_raw = str(game.get("variant") or "standard")
    variant_key, _ = map_variant(PROVIDER, variant_raw)
    speed = str(game.get("speed") or game.get("perf") or "rapid")
    clock = game.get("clock") or {}
    players = game.get("players") or {}

    return NormalizedGame(
        provider=PROVIDER,
        provider_game_id=_str_or_none(game.get("id")),
        canonical_url=_canonical_url(game),
        content_hash=_game_hash(game),
        rated=bool(game.get("rated", False)),
        variant_key=variant_key,
        variant_raw=variant_raw,
        time_class=normalize_time_class(speed),
        time_control_raw=_clock_label(clock, speed),
        outcome=outcome,
        is_live=is_live,
        status_raw=status,
        end_time=_ms_to_s(game.get("lastMoveAt")),
        start_time=_ms_to_s(game.get("createdAt")),
        white=_participant("white", players.get("white") or {}, winner),
        black=_participant("black", players.get("black") or {}, winner),
        eco=(game.get("opening") or {}).get("eco"),
        opening_name=(game.get("opening") or {}).get("name"),
        opening_ply=_int_or_none((game.get("opening") or {}).get("ply")),
        pgn=_str_or_none(game.get("pgn")),
    )


def _participant(color: str, data: dict[str, Any], winner: str | None) -> NormalizedParticipant:
    user = data.get("user") or {}
    username = _str_or_none(user.get("name") or user.get("id"))
    result_raw = "win" if winner == color else ("loss" if winner in {"white", "black"} else None)
    return NormalizedParticipant(
        color=color,  # type: ignore[arg-type]
        provider_user_id=_str_or_none(user.get("id")),
        username_normalized=normalize_username(user.get("id") or username),
        display_username=username,
        rating=_int_or_none(data.get("rating")),
        rating_diff=_int_or_none(data.get("ratingDiff")),
        rd=None,
        result_raw=result_raw,
        is_ai=data.get("aiLevel") is not None,
    )


def _canonical_url(game: dict[str, Any]) -> str | None:
    if game.get("url"):
        return str(game["url"])
    if game.get("id"):
        return f"https://lichess.org/{game['id']}"
    return None


def _clock_label(clock: dict[str, Any], speed: str) -> str:
    initial = _int_or_none(clock.get("initial"))
    increment = _int_or_none(clock.get("increment"))
    if initial is not None and increment is not None:
        return f"{initial}+{increment}"
    return speed


def _game_hash(game: dict[str, Any]) -> str:
    return canonical_hash(
        {
            "provider": PROVIDER,
            "id": game.get("id"),
            "rated": game.get("rated"),
            "variant": game.get("variant"),
            "speed": game.get("speed"),
            "createdAt": game.get("createdAt"),
            "lastMoveAt": game.get("lastMoveAt"),
            "status": game.get("status"),
            "winner": game.get("winner"),
            "players": {
                "white": _hash_player((game.get("players") or {}).get("white") or {}),
                "black": _hash_player((game.get("players") or {}).get("black") or {}),
            },
            "pgn": game.get("pgn"),
        }
    )


def _hash_player(player: dict[str, Any]) -> dict[str, Any]:
    user = player.get("user") or {}
    return {
        "id": user.get("id"),
        "name": user.get("name"),
        "rating": player.get("rating"),
        "ratingDiff": player.get("ratingDiff"),
    }


def _ms_to_s(value: object) -> int | None:
    parsed = _int_or_none(value)
    return None if parsed is None else parsed // 1000


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
