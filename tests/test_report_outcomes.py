"""Reports distinguish absent normalized results from games known to be in progress."""

from __future__ import annotations

from chess_crawl.storage.db import Connection, open_database, require_row, transaction

import json
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from support import seed_game
from chess_crawl import cli
from chess_crawl.api import create_app
from chess_crawl.application import list_opponents
from chess_crawl.config import Config
from chess_crawl.ingest import fetch_chesscom_month
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.queries import games_by_month, opponent_report, user_game_summary
from chess_crawl.storage.raw import read_raw_payload, store_raw_payload, update_raw_payload_status


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
    opponents = opponent_report(conn, "lichess", "alice")
    assert user is not None and opponents is not None
    page = list_opponents(conn, "lichess", "alice")
    months = games_by_month(conn, provider="lichess")
    assert len(opponents) == page["total"] == len(months) == 1
    surfaces = (
        (user, "wins", "losses"), (opponents[0], "my_wins", "my_losses"),
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


def test_cli_labels_missing_results_separately_from_activity(
    database_url: str, capsys: pytest.CaptureFixture[str],
) -> None:
    with open_database(database_url, writable=True) as conn:
        for status in ("aborted", "unrecognized-provider-status", "started"):
            normalize_game(conn, status=status)

    assert cli.run(["report", "user", "lichess", "alice", "--database-url", str(database_url)]) == 0
    user = capsys.readouterr().out
    assert "W/D/L/no result: 0/0/0/3" in user
    assert "In progress: 1" in user
    for arguments in (
        ["report", "opponents", "lichess", "alice"],
        ["report", "games-by-month", "--provider", "lichess"],
    ):
        assert cli.run([*arguments, "--database-url", str(database_url)]) == 0
        table = capsys.readouterr().out
        assert "NO_RESULT" in table
        assert "IN_PROGRESS" in table
        assert "UNFINISHED" not in table


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
    opponents = opponent_report(conn, "chess.com", "alice")
    assert user is not None and opponents is not None
    for row in (
        user, opponents[0], list_opponents(conn, "chess.com", "alice")["items"][0],
        games_by_month(conn, provider="chess.com")[0],
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
    assert read_raw_payload(conn, first.raw_payload_id).parser_version == "games-normalizer-v2"
    row = user_game_summary(conn, "chess.com", "alice")
    assert row is not None
    assert (row["no_result"], row["in_progress"]) == (1, 0)
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
