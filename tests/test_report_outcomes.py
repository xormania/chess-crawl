"""Reports distinguish absent normalized results from games known to be in progress."""

from __future__ import annotations

from chess_crawl.storage.db import Connection, open_database, require_row, transaction

from helpers.games import normalize_game

import json
import time
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from support import seed_game
from chess_crawl.api import create_app
from chess_crawl.application import list_opponents
from chess_crawl.config import Config
from chess_crawl.ingest import fetch_chesscom_month
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.api_views import months_page
from chess_crawl.storage.queries import user_game_summary
from chess_crawl.storage.raw import read_raw_payload, store_raw_payload, update_raw_payload_status


@pytest.mark.parametrize(("status", "winner", "no_result", "in_progress", "wins", "losses", "draws"), [
    ("aborted", None, 1, 0, 0, 0, 0),
    ("unrecognized-provider-status", None, 1, 0, 0, 0, 0),
    ("started", None, 1, 1, 0, 0, 0),
    ("mate", "white", 0, 0, 1, 0, 0),
    ("mate", "black", 0, 0, 0, 1, 0),
    ("draw", None, 0, 0, 0, 0, 1),
])
def test_report_outcome_and_activity_are_independent(
    initialized_conn: Connection,
    status: str,
    winner: str | None,
    no_result: int,
    in_progress: int,
    wins: int,
    losses: int,
    draws: int,
) -> None:
    conn = initialized_conn
    game_id = normalize_game(conn, status=status, winner=winner)
    seed_game(conn, provider="chess.com", game_key="other-provider", white="Alice", black="Bob", outcome=None)
    stored = require_row(conn.execute("SELECT status_raw, is_live FROM games WHERE id = %s", (game_id,)))
    assert tuple(stored.values()) == (status, in_progress)

    user = user_game_summary(conn, "lichess", "alice")
    assert user is not None
    page = list_opponents(conn, "lichess", "alice")
    months = months_page(conn, provider="lichess", after="", limit=100)["items"]
    assert page["total"] == len(months) == 1
    surfaces = (
        (user, "wins", "losses"),
        (page["items"][0], "my_wins", "my_losses"), (months[0], "white_wins", "black_wins"),
    )
    for row, win_key, loss_key in surfaces:
        assert row["games"] == 1
        assert row["no_result"] == no_result
        assert row["in_progress"] == in_progress
        assert row["unfinished"] == row["no_result"]  # Existing API key retains its legacy meaning.
        assert row[win_key] == wins
        assert row[loss_key] == losses
        assert row["draws"] == draws

    black_player = user_game_summary(conn, "lichess", "bob")
    assert black_player is not None
    assert (black_player["wins"], black_player["losses"]) == (losses, wins)
    assert black_player["no_result"] == no_result
    assert black_player["in_progress"] == in_progress


def test_http_opponents_expose_result_and_activity_counts(database_url: str) -> None:
    with open_database(database_url, writable=True) as conn:
        for status in ("aborted", "unrecognized-provider-status", "started"):
            normalize_game(conn, status=status)
        seed_game(conn, provider="chess.com", game_key="other-provider", white="Alice", black="Bob", outcome=None)

    with TestClient(create_app(database_url, "test-token"), headers={"Authorization": "Bearer test-token"}) as client:
        response = client.get("/v1/users/lichess/alice/opponents")

    assert response.status_code == 200
    page = response.json()
    assert page["total"] == 1
    opponent = page["items"][0]
    assert opponent["games"] == 3
    assert opponent["no_result"] == opponent["unfinished"] == 3
    assert opponent["in_progress"] == 1


def chesscom_archive_body(result: str | None) -> bytes:
    game: dict[str, Any] = {
        "uuid": "archived-no-result", "url": "https://www.chess.com/game/live/123",
        "rules": "chess", "time_class": "blitz", "time_control": "300", "rated": False,
        "end_time": 1704067260, "white": {"username": "Alice"}, "black": {"username": "Bob"},
    }
    if result != "omitted":
        game["white"]["result"] = game["black"]["result"] = result
    return json.dumps({"games": [game]}).encode()


@pytest.mark.parametrize("result", ["omitted", None, "", "none"])
def test_archived_chesscom_game_without_result_is_not_known_in_progress(
    initialized_conn: Connection, result: str | None,
) -> None:
    conn = initialized_conn
    body = chesscom_archive_body(result)
    raw_id = store_raw_payload(conn, RawRecord(
        provider="chess.com", endpoint_type="monthly_archive",
        request_url="https://api.chess.com/pub/player/alice/games/2024/01",
        canonical_source_key="chess.com/player/alice/games/2024/01", body=body, fetched_at=1704067300,
    ))
    game_id = normalize_games_payload(conn, raw_id)[0]

    stored = require_row(conn.execute("SELECT outcome, is_live, ended_at FROM games WHERE id = %s", (game_id,)))
    assert tuple(stored.values()) == (None, 0, 1704067260)
    assert read_raw_payload(conn, raw_id).body == body
    if result == "none":
        assert [row[0] for row in conn.execute("SELECT result_raw FROM game_participants")] == ["none", "none"]
    user = user_game_summary(conn, "chess.com", "alice")
    assert user is not None
    for row in (
        user, list_opponents(conn, "chess.com", "alice")["items"][0],
        months_page(conn, provider="chess.com", after="", limit=100)["items"][0],
    ):
        assert row["no_result"] == row["unfinished"] == 1
        assert row["in_progress"] == 0


def test_304_repairs_previously_inferred_chesscom_activity(initialized_conn: Connection) -> None:
    conn = initialized_conn
    config = Config(chesscom_delay_s=0, max_retries=0)
    first = fetch_chesscom_month(
        conn, "alice", 2024, 1, config=config,
        transport=httpx.MockTransport(lambda request: httpx.Response(
            200, headers={"etag": '"archive"'}, content=chesscom_archive_body("omitted"),
        )),
    )
    assert first.raw_payload_id is not None
    # Archives parsed before this correction inferred activity from missing results.
    with transaction(conn):
        conn.execute("UPDATE games SET is_live = 1 WHERE id = %s", (first.normalized_ids[0],))
        update_raw_payload_status(conn, first.raw_payload_id, status="parsed", parser_version="games-normalizer-v1")

    def unchanged(request: httpx.Request) -> httpx.Response:
        assert request.headers["If-None-Match"] == '"archive"'
        return httpx.Response(304)

    refreshed = fetch_chesscom_month(
        conn, "alice", 2024, 1, config=config, transport=httpx.MockTransport(unchanged),
    )

    assert refreshed.normalized_ids == first.normalized_ids
    from chess_crawl.normalize.games import PARSER_VERSION
    assert read_raw_payload(conn, first.raw_payload_id).parser_version == PARSER_VERSION
    row = user_game_summary(conn, "chess.com", "alice")
    assert row is not None
    assert (row["no_result"], row["in_progress"]) == (1, 0)
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1


def test_standalone_game_archive_304_refreshes_the_current_source(initialized_conn: Connection) -> None:
    conn = initialized_conn
    config = Config(chesscom_delay_s=0, max_retries=0)
    old_body = chesscom_archive_body("win")
    first = fetch_chesscom_month(
        conn, "alice", 2024, 1, config=config,
        transport=httpx.MockTransport(lambda request: httpx.Response(
            200, headers={"etag": '"january"'}, content=old_body,
        )),
    )
    assert first.raw_payload_id is not None
    baseline = int(time.time())
    with transaction(conn):
        conn.execute(
            "UPDATE raw_payloads SET fetched_at=%s WHERE id=%s",
            (baseline - 2, first.raw_payload_id),
        )
        conn.execute(
            "UPDATE fetch_logs SET attempted_at=%s WHERE raw_payload_id=%s",
            (baseline - 2, first.raw_payload_id),
        )
    newer_raw = store_raw_payload(conn, RawRecord(
        provider="chess.com", endpoint_type="monthly_archive",
        request_url="https://api.chess.com/pub/player/alice/games/2024/02",
        canonical_source_key="chess.com/player/alice/games/2024/02",
        body=chesscom_archive_body("checkmated"),
        fetched_at=baseline - 1,
        media_type="application/json",
    ))
    normalize_games_payload(conn, newer_raw)
    game_id = first.normalized_ids[0]
    assert require_row(conn.execute(
        "SELECT result_raw FROM game_participants WHERE game_id=%s AND color='white'", (game_id,),
    ))[0] == "checkmated"

    def unchanged(request: httpx.Request) -> httpx.Response:
        assert request.headers["If-None-Match"] == '"january"'
        return httpx.Response(304)

    refreshed = fetch_chesscom_month(
        conn, "alice", 2024, 1, config=config, transport=httpx.MockTransport(unchanged),
    )
    assert refreshed.status_code == 304
    # A non-body response may omit result payload IDs; stored evidence is authoritative.
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 2
    observed = require_row(conn.execute("SELECT status_code,raw_payload_id FROM fetch_logs ORDER BY id DESC LIMIT 1"))
    assert (observed["status_code"], observed["raw_payload_id"]) == (304, first.raw_payload_id)
    assert require_row(conn.execute(
        "SELECT result_raw FROM game_participants WHERE game_id=%s AND color='white'", (game_id,),
    ))[0] == "win"
