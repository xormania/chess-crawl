"""Reports remain available when preserved provider timestamps are unusable."""

from __future__ import annotations

import json

from support import seed_game

from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.db import Connection, require_row, transaction
from chess_crawl.storage.api_views import months_page
from chess_crawl.storage.raw import read_raw_payload, store_raw_payload


def test_month_report_guards_calendar_boundaries_and_preserves_bigint_values(
    initialized_conn: Connection,
) -> None:
    conn = initialized_conn
    # Exercise the entire four-digit Gregorian report calendar, signed BIGINT
    # extremes, and NULL without constraining what the archive may preserve.
    observations = (
        -(2**63), -62135596801, -62135596800, -1, 0,
        1704067200, 253402300799, 253402300800, 2**63 - 1, None,
    )
    expected_storage: dict[int, int | None] = {}
    for index, ended_at in enumerate(observations):
        game_id, _, _ = seed_game(
            conn, provider="chess.com", game_key=f"calendar-{index}",
            white="Alice", black="Bob", ended_at=0 if ended_at is None else ended_at,
        )
        if ended_at is None:
            with transaction(conn):
                conn.execute("UPDATE games SET ended_at = NULL WHERE id = %s", (game_id,))
        expected_storage[game_id] = ended_at

    # Session timezone must not move epoch zero into the previous month.
    conn.execute("SET TIME ZONE 'America/New_York'")
    rows = months_page(conn, provider="chess.com", after="", limit=100)["items"]

    assert [(row["month"], row["games"]) for row in rows] == [
        ("0001-01", 1), ("1969-12", 1), ("1970-01", 1),
        ("2024-01", 1), ("9999-12", 1), ("unknown", 5),
    ]
    assert rows[-1]["white_wins"] == 5
    assert rows[-1]["no_result"] == rows[-1]["in_progress"] == 0
    assert {row["id"]: row["ended_at"] for row in conn.execute("SELECT id, ended_at FROM games")} == expected_storage


def test_provider_timestamp_overflow_does_not_break_report_or_rewrite_evidence(
    initialized_conn: Connection,
) -> None:
    conn = initialized_conn
    body = json.dumps({"games": [{
        "uuid": "overflow", "url": "https://www.chess.com/game/live/overflow",
        "rules": "chess", "time_class": "blitz", "time_control": "300", "rated": False,
        "end_time": 2**63 - 1,
        "white": {"username": "Alice", "result": "win"},
        "black": {"username": "Bob", "result": "resigned"},
    }]}).encode()
    raw_id = store_raw_payload(conn, RawRecord(
        provider="chess.com", endpoint_type="monthly_archive",
        request_url="https://api.chess.com/pub/player/alice/games/2024/01",
        canonical_source_key="chess.com/player/alice/games/2024/01",
        body=body, fetched_at=1704067200,
    ))
    game_id = normalize_games_payload(conn, raw_id)[0]

    report = months_page(conn, provider="chess.com", after="", limit=100)["items"]

    assert [(row["month"], row["games"]) for row in report] == [("unknown", 1)]
    assert require_row(conn.execute("SELECT ended_at FROM games WHERE id = %s", (game_id,)))["ended_at"] == 2**63 - 1
    assert read_raw_payload(conn, raw_id).body == body
