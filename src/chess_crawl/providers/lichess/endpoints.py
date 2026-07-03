"""URL builders for documented Lichess public API endpoints."""

from __future__ import annotations

from urllib.parse import quote, urlencode


BASE_URL = "https://lichess.org/api"


def _user(username: str) -> str:
    return quote(username.strip().lower(), safe="")


def user_profile(username: str) -> str:
    return f"{BASE_URL}/user/{_user(username)}"


def user_games(username: str, **params: object) -> str:
    clean_params = {key: value for key, value in params.items() if value is not None}
    query = urlencode(clean_params, doseq=True)
    base = f"{BASE_URL}/games/user/{_user(username)}"
    return f"{base}?{query}" if query else base


def game(game_id: str) -> str:
    return f"{BASE_URL}/game/{quote(game_id.strip(), safe='')}"
