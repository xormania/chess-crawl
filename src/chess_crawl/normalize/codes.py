"""Shared normalization code maps used by provider parsers."""

from __future__ import annotations

import hashlib
import json
from typing import Any


OUTCOMES = ("white_win", "black_win", "draw")
TIME_CLASSES = ("bullet", "blitz", "rapid", "classical", "correspondence")
VARIANTS = (
    "standard",
    "chess960",
    "crazyhouse",
    "antichess",
    "atomic",
    "horde",
    "kingofthehill",
    "racingkings",
    "threecheck",
    "bughouse",
    "fromposition",
)

CHESSCOM_VARIANTS = {
    "chess": "standard",
    "chess960": "chess960",
    "crazyhouse": "crazyhouse",
    "kingofthehill": "kingofthehill",
    "threecheck": "threecheck",
    "bughouse": "bughouse",
}

LICHESS_VARIANTS = {
    "standard": "standard",
    "chess960": "chess960",
    "crazyhouse": "crazyhouse",
    "antichess": "antichess",
    "atomic": "atomic",
    "horde": "horde",
    "kingOfTheHill": "kingofthehill",
    "racingKings": "racingkings",
    "threeCheck": "threecheck",
    "fromPosition": "fromposition",
}


CHESSCOM_DRAW_RESULTS = {
    "agreed",
    "repetition",
    "stalemate",
    "insufficient",
    "50move",
    "timevsinsufficient",
    "timeoutvsinsufficient",
    "threecheck",
}

LICHESS_DRAW_STATUSES = {
    "draw",
    "stalemate",
    "outoftime",
    "mate",
    "resign",
    "timeout",
    "variantEnd",
}

LICHESS_NULL_OUTCOME_STATUSES = {
    "aborted",
    "unknownfinish",
    "noStart",
    "created",
    "started",
}

LIVE_STATUSES = {"created", "started"}


def normalize_username(username: str | None) -> str | None:
    if username is None:
        return None
    stripped = username.strip()
    return stripped.lower() if stripped else None


def canonical_hash(value: Any) -> str:
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(body).hexdigest()


def normalize_time_class(value: str) -> str:
    lowered = value.strip().lower()
    if lowered in {"daily", "correspondence"}:
        return "correspondence"
    if lowered in {"ultrabullet", "bullet"}:
        return "bullet"
    if lowered in {"blitz", "rapid", "classical"}:
        return lowered
    return lowered


def map_variant(provider: str, native: str | None) -> tuple[str, bool]:
    if not native:
        return "standard", False
    if provider == "chess.com":
        mapped = CHESSCOM_VARIANTS.get(native)
    elif provider == "lichess":
        mapped = LICHESS_VARIANTS.get(native)
    else:
        mapped = None
    if mapped is None:
        return native.strip().lower(), False
    return mapped, True


def chesscom_outcome(white_result: str | None, black_result: str | None) -> tuple[str | None, bool]:
    white = (white_result or "").strip().lower()
    black = (black_result or "").strip().lower()
    if white == "win":
        return "white_win", False
    if black == "win":
        return "black_win", False
    if white in CHESSCOM_DRAW_RESULTS or black in CHESSCOM_DRAW_RESULTS:
        return "draw", False
    if white in {"", "none"} and black in {"", "none"}:
        return None, True
    return None, False


def lichess_outcome(winner: str | None, status: str | None) -> tuple[str | None, bool]:
    status_value = status or ""
    if winner == "white":
        return "white_win", False
    if winner == "black":
        return "black_win", False
    if status_value in LICHESS_DRAW_STATUSES:
        return "draw", False
    if status_value in LICHESS_NULL_OUTCOME_STATUSES:
        return None, status_value in LIVE_STATUSES
    return None, False
