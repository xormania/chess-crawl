"""Conditional observations can make a retained game source current again."""
from __future__ import annotations

import httpx

from chess_crawl.config import Config
from chess_crawl.ingest import fetch_chesscom_month
from chess_crawl.providers.registry import ProviderSession
from chess_crawl.storage.db import Connection, require_row
from chess_crawl.storage.game_evidence import read_game_version

NOW = 1704153600


def test_standalone_monthly_304_refreshes_current_competing_game_source(initialized_conn: Connection) -> None:
    conn = initialized_conn
    observed = [100]
    first = {"uuid": "recurring-game", "url": "https://www.chess.com/game/live/123",
             "rules": "chess", "time_class": "blitz", "time_control": "300", "end_time": NOW,
             "white": {"username": "alice", "result": "win"},
             "black": {"username": "bob", "result": "resigned"},
             "pgn": "1. e4 {[%clk 0:04:59.125]} e5 *"}
    later = {**first, "white": {"username": "alice", "result": "resigned"},
             "black": {"username": "bob", "result": "win"},
             "pgn": "1. d4 {[%clk 0:04:58.500]} d5 *"}
    requests: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if len(requests) == 3:
            assert request.headers["If-None-Match"] == '"first-month"'
            return httpx.Response(304)
        return httpx.Response(200, json={"games": [first if len(requests) == 1 else later]},
                              headers={"ETag": '"first-month"' if len(requests) == 1 else '"other-month"'})

    with ProviderSession(Config(chesscom_delay_s=0, max_retries=0), transport=httpx.MockTransport(respond), clock=lambda: observed[0]) as session:
        acquired = fetch_chesscom_month(conn, "alice", 2024, 1, session=session)
        game_id = acquired.normalized_ids[0]
        original = read_game_version(conn, game_id)
        observed[0] = 200
        fetch_chesscom_month(conn, "bob", 2024, 1, session=session)
        competing = read_game_version(conn, game_id)
        assert original is not None and competing is not None and competing["id"] != original["id"]
        observed[0] = 300
        cached = fetch_chesscom_month(conn, "alice", 2024, 1, session=session)
    current = read_game_version(conn, game_id)
    assert cached.status_code == 304
    assert require_row(conn.execute("SELECT raw_payload_id FROM fetch_logs ORDER BY id DESC LIMIT 1"))[0] == acquired.raw_payload_id
    assert current is not None and current["id"] == original["id"]
    assert current["clocks"] == original["clocks"]
    facts = require_row(conn.execute("SELECT status_raw,outcome FROM games WHERE id=%s", (game_id,)))
    assert tuple(facts.values()) == ("white:win;black:resigned", "white_win")
    assert [row[0] for row in conn.execute("SELECT result_raw FROM game_participants WHERE game_id=%s ORDER BY color", (game_id,))] == ["resigned", "win"]
    assert require_row(conn.execute("SELECT COUNT(*) FROM game_versions"))[0] == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 3
