"""URL builders for documented Chess.com public API endpoints."""

from __future__ import annotations

from urllib.parse import quote


BASE_URL = "https://api.chess.com/pub"


def _user(username: str) -> str:
    return quote(username.strip().lower(), safe="")


def player_profile(username: str) -> str:
    return f"{BASE_URL}/player/{_user(username)}"


def player_stats(username: str) -> str:
    return f"{player_profile(username)}/stats"


def archives_index(username: str) -> str:
    return f"{player_profile(username)}/games/archives"


def monthly_archive(username: str, year: int, month: int) -> str:
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")
    return f"{player_profile(username)}/games/{year:04d}/{month:02d}"
