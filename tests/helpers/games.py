"""Normalized game data builders shared by API and reporting tests."""
from __future__ import annotations

import json
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.db import Connection
from chess_crawl.storage.raw import store_raw_payload


def normalize_game(conn: Connection, *, status: str, winner: str | None = None) -> int:
    payload = {
        "id": status,
        "status": status,
        "rated": False,
        "variant": "standard",
        "speed": "blitz",
        "createdAt": 1704067200000,
        "lastMoveAt": 1704067260000,
        "clock": {"initial": 300, "increment": 0},
        "players": {
            "white": {"user": {"id": "alice", "name": "Alice"}},
            "black": {"user": {"id": "bob", "name": "Bob"}},
        },
    }
    if winner is not None:
        payload["winner"] = winner
    raw_id = store_raw_payload(conn, RawRecord(
        provider="lichess", endpoint_type="game", request_url=f"https://lichess.org/game/export/{status}",
        canonical_source_key=f"lichess/game/{status}", body=json.dumps(payload).encode(), fetched_at=1704067300,
    ))
    return normalize_games_payload(conn, raw_id)[0]
