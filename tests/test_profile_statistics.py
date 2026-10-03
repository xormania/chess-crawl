"""Statistics distinguish recorded zeroes, unavailable values, and rated totals."""

from __future__ import annotations

import json
import sqlite3

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import fetch_chesscom_stats, replay_raw_payload
from chess_crawl.normalize.users import PARSER_VERSION
from chess_crawl.storage.db import transaction
from chess_crawl.storage.raw import read_raw_payload, update_raw_payload_status


@pytest.mark.parametrize(("stats", "expected"), [
    ({"chess_blitz": {"record": {"win": 0, "loss": 2, "draw": 0}}}, (2, None, 0, 2, 0)),
    ({"chess_blitz": {"record": {"win": 0, "loss": 0, "draw": 0}}}, (0, None, 0, 0, 0)),
    ({"chess_blitz": {"record": {"win": 0}}}, (None, None, 0, None, None)),
    ({"chess_blitz": {"record": {}}}, (None, None, None, None, None)),
    ({"tactics": {"highest": {"rating": 1500}}}, (None, None, None, None, None)),
    ({
        "chess_blitz": {"record": {"win": 0, "loss": 2, "draw": 0}},
        "chess_rapid": {"record": {"loss": 3, "draw": 0}},
    }, (None, None, None, 5, 0)),
    ({
        "chess_blitz": {"record": {"win": 0, "loss": 2, "draw": 0}},
        "chess_rapid": {"last": {"rating": 1500}},
    }, (None, None, None, None, None)),
    ({
        "chess_blitz": {"record": {"win": 0, "loss": 2, "draw": 0}},
        "chess960_daily": {"record": {"win": 4, "loss": 0, "draw": 1}},
        "puzzle_rush": {"best": {"score": 50}},
    }, (7, None, 4, 2, 1)),
])
def test_chesscom_stats_preserve_known_and_unknown_counts(
    initialized_conn: sqlite3.Connection, stats: dict, expected: tuple,
) -> None:
    conn = initialized_conn
    fetched = fetch_chesscom_stats(
        conn, "Alice", config=Config(chesscom_delay_s=0, max_retries=0),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=stats)),
    )

    snapshot = conn.execute(
        "SELECT count_all, count_rated, count_win, count_loss, count_draw, perfs_or_stats FROM user_snapshots"
    ).fetchone()
    assert tuple(snapshot)[:5] == expected
    assert json.loads(snapshot["perfs_or_stats"]) == stats
    assert fetched.raw_payload_id is not None
    assert json.loads(read_raw_payload(conn, fetched.raw_payload_id).body) == stats


@pytest.mark.parametrize("replay_method", ["offline", "304"])
def test_preexisting_v2_stats_are_repaired_without_duplicate_snapshots(
    initialized_conn: sqlite3.Connection, replay_method: str,
) -> None:
    conn = initialized_conn
    config = Config(chesscom_delay_s=0, max_retries=0)
    stats = {"chess_blitz": {"record": {"win": 0, "loss": 2, "draw": 0}}}
    first = fetch_chesscom_stats(
        conn, "Alice", config=config,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, headers={"etag": '"stats"'}, json=stats)),
    )
    assert first.raw_payload_id is not None
    # The v2 hash included the same source stats, but its derived totals lost
    # known zeroes and asserted the all-games record was a rated-games record.
    with transaction(conn):
        conn.execute("UPDATE user_snapshots SET count_rated = 2, count_win = NULL, count_draw = NULL")
        update_raw_payload_status(conn, first.raw_payload_id, status="parsed", parser_version="users-normalizer-v2")
    original = conn.execute("SELECT id, content_hash, captured_at FROM user_snapshots").fetchone()

    if replay_method == "offline":
        result = replay_raw_payload(conn, first.raw_payload_id)
    else:
        def unchanged(request: httpx.Request) -> httpx.Response:
            assert request.headers["If-None-Match"] == '"stats"'
            return httpx.Response(304)

        result = fetch_chesscom_stats(conn, "Alice", config=config, transport=httpx.MockTransport(unchanged))
        assert result.status_code == 304
        evidence = conn.execute("SELECT status_code, raw_payload_id, from_cache FROM fetch_logs ORDER BY id DESC").fetchone()
        assert tuple(evidence) == (304, first.raw_payload_id, 1)

    assert result.normalized_ids == first.normalized_ids
    snapshots = conn.execute("SELECT * FROM user_snapshots").fetchall()
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert (snapshot["id"], snapshot["content_hash"]) == (original["id"], original["content_hash"])
    assert snapshot["captured_at"] >= original["captured_at"]
    assert tuple(snapshot[key] for key in ("count_all", "count_rated", "count_win", "count_loss", "count_draw")) == (
        2, None, 0, 2, 0,
    )
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 1
    assert read_raw_payload(conn, first.raw_payload_id).parser_version == PARSER_VERSION
    assert PARSER_VERSION != "users-normalizer-v2"
