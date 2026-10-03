from __future__ import annotations

import copy
import json
import sqlite3
from pathlib import Path

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import fetch_chesscom_month, replay_raw_payload
from chess_crawl.jobs import state
import chess_crawl.normalize.games as games_module
from chess_crawl.normalize.games import PARSER_VERSION, normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.acquisition import associate_run_game, run_game_ids
from chess_crawl.storage.db import transaction
from chess_crawl.storage.raw import read_raw_payload, store_raw_payload, update_raw_payload_status


@pytest.fixture
def stored_archive(initialized_conn: sqlite3.Connection, fixtures_dir: Path) -> tuple[int, str]:
    archive = json.loads((fixtures_dir / "chesscom/archive_2024_01.json").read_bytes())
    second = copy.deepcopy(archive["games"][0])
    second.update(uuid="parser-replay-second", url="https://www.chess.com/game/live/parser-replay-second")
    archive["games"].append(second)
    raw_id = store_raw_payload(
        initialized_conn,
        RawRecord(
            provider="chess.com", endpoint_type="monthly_archive",
            request_url="https://api.chess.com/pub/player/samename/games/2024/01",
            canonical_source_key="chess.com/player/samename/games/2024/01",
            fetched_at=123, body=json.dumps(archive).encode(),
            media_type="application/json", etag='"games-v1"',
        ),
    )
    return raw_id, archive["games"][0]["eco"]


def _old_parser_state(conn: sqlite3.Connection, raw_id: int) -> None:
    with transaction(conn):
        conn.execute("UPDATE games SET eco = 'old-parser-value'")
        update_raw_payload_status(conn, raw_id, status="parsed", parser_version="games-normalizer-old")


@pytest.mark.parametrize("via_http", [False, True], ids=["offline", "cached-304"])
@pytest.mark.parametrize("associated", [False, True], ids=["standalone", "existing-run"])
def test_old_parser_replays_game_upserts_even_when_run_already_acquired_every_game(
    initialized_conn: sqlite3.Connection, stored_archive: tuple[int, str], via_http: bool, associated: bool,
) -> None:
    conn = initialized_conn
    raw_id, expected_eco = stored_archive
    run_id = (
        state.create_crawl_run(conn, provider="chess.com", seed_spec="samename", params={"max_games": 2})
        if associated else None
    )
    ids = normalize_games_payload(conn, raw_id, crawl_run_id=run_id, max_games=2)
    _old_parser_state(conn, raw_id)
    allowance = 0 if associated else None

    if via_http:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["If-None-Match"] == '"games-v1"'
            return httpx.Response(304)

        result = fetch_chesscom_month(
            conn, "samename", 2024, 1,
            config=Config(chesscom_delay_s=0, max_retries=0), transport=httpx.MockTransport(handler),
            crawl_run_id=run_id, max_games=allowance,
        )
        assert result.status_code == 304
    else:
        replay_raw_payload(conn, raw_id, crawl_run_id=run_id, max_games=allowance)

    assert [tuple(row) for row in conn.execute("SELECT id, eco FROM games ORDER BY id")] == [
        (game_id, expected_eco) for game_id in ids
    ]
    if run_id is not None:
        assert run_game_ids(conn, run_id) == set(ids)
    raw = read_raw_payload(conn, raw_id)
    assert raw.normalization_status == "parsed"
    assert raw.parser_version == PARSER_VERSION


def test_partial_parser_upgrade_does_not_certify_unselected_old_records(
    initialized_conn: sqlite3.Connection, stored_archive: tuple[int, str],
) -> None:
    conn = initialized_conn
    raw_id, expected_eco = stored_archive
    first, second = normalize_games_payload(conn, raw_id)
    run_id = state.create_crawl_run(conn, provider="chess.com", seed_spec="samename", params={"max_games": 1})
    associate_run_game(conn, run_id, first)
    _old_parser_state(conn, raw_id)

    for _ in range(2):
        result = replay_raw_payload(conn, raw_id, crawl_run_id=run_id, max_games=0)
        assert result.normalized_ids == ()
        assert run_game_ids(conn, run_id) == {first}
        assert [tuple(row) for row in conn.execute("SELECT id, eco FROM games ORDER BY id")] == [
            (first, expected_eco), (second, "old-parser-value"),
        ]
        raw = read_raw_payload(conn, raw_id)
        assert raw.normalization_status == "pending"
        assert raw.parser_version == "games-normalizer-old"

    replay_raw_payload(conn, raw_id)
    assert {row[0] for row in conn.execute("SELECT eco FROM games")} == {expected_eco}
    raw = read_raw_payload(conn, raw_id)
    assert raw.normalization_status == "parsed"
    assert raw.parser_version == PARSER_VERSION


def test_current_parser_attribution_reuses_ids_without_rewriting_games(
    initialized_conn: sqlite3.Connection, stored_archive: tuple[int, str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn
    raw_id, _ = stored_archive
    ids = normalize_games_payload(conn, raw_id)
    run_id = state.create_crawl_run(conn, provider="chess.com", seed_spec="samename", params={"max_games": 1})

    def unexpected_upsert(*args, **kwargs):
        raise AssertionError("Current parser attribution must reuse normalized IDs")

    monkeypatch.setattr(games_module, "_normalize_game", unexpected_upsert)
    result = replay_raw_payload(conn, raw_id, crawl_run_id=run_id, max_games=1)
    assert result.normalized_ids == (ids[0],)
    assert run_game_ids(conn, run_id) == {ids[0]}
    assert read_raw_payload(conn, raw_id).normalization_status == "parsed"


def test_explicit_standalone_replay_refreshes_current_parser_records(
    initialized_conn: sqlite3.Connection, stored_archive: tuple[int, str],
) -> None:
    conn = initialized_conn
    raw_id, expected_eco = stored_archive
    ids = normalize_games_payload(conn, raw_id)
    with transaction(conn):
        conn.execute("UPDATE games SET eco = 'needs-manual-repair'")
    assert read_raw_payload(conn, raw_id).parser_version == PARSER_VERSION

    result = replay_raw_payload(conn, raw_id)

    assert result.normalized_ids == tuple(ids)
    assert [tuple(row) for row in conn.execute("SELECT id, eco FROM games ORDER BY id")] == [
        (game_id, expected_eco) for game_id in ids
    ]
    assert read_raw_payload(conn, raw_id).normalization_status == "parsed"
