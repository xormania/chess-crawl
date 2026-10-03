"""Consistent archive views while the worker commits new state concurrently."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chess_crawl import application
from chess_crawl.jobs import state
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage import queries
from chess_crawl.storage.db import connection
from chess_crawl.storage.raw import insert_fetch_log, store_raw_payload
from support import seed_game


def test_run_snapshot_survives_concurrent_completion(archive_path: Path, monkeypatch) -> None:
    request = application.ImportRequest("lichess", "alice", 1704067200, 1704153600, 5)
    with connection(archive_path, mode="rw") as writer:
        accepted = application.submit_import(writer, request, idempotency_key="snapshot")
        original = state.get_run

        def finish_after_run_read(conn, run_id):
            row = original(conn, run_id)
            for job_id in accepted["job_ids"]:
                state.mark_done(writer, job_id)
            state.refresh_run_status(writer, run_id)
            return row

        with connection(archive_path) as reader:
            with monkeypatch.context() as patch:
                patch.setattr(state, "get_run", finish_after_run_read)
                snapshot = application.get_run(reader, accepted["run_id"])
            assert snapshot["status"] == "running"
            assert snapshot["counters"]["jobs_pending"] == 2
            assert snapshot["counters"].get("jobs_done", 0) == 0
            assert "counters_json" not in snapshot
            assert "params_json" not in snapshot
            assert not reader.in_transaction
            # The previous view releases its snapshot so the next read sees completion.
            fresh = application.get_run(reader, accepted["run_id"])
            assert fresh["status"] == "done"
            assert fresh["counters"]["jobs_done"] == 2
            assert not reader.in_transaction


def test_page_rows_count_and_freshness_share_one_snapshot(archive_path: Path, monkeypatch) -> None:
    with connection(archive_path, mode="rw") as writer:
        seed_game(writer, provider="lichess", game_key="first", white="alice", black="bob")
        original = queries.game_page

        def add_game_after_page(*args, **kwargs):
            result = original(*args, **kwargs)
            seed_game(writer, provider="lichess", game_key="second", white="alice", black="carol")
            insert_fetch_log(
                writer, provider="lichess", endpoint_type="user_games_stream",
                url="https://lichess.org/api/games/user/alice", attempted_at=123, status_code=200,
            )
            return result

        with connection(archive_path) as reader:
            with monkeypatch.context() as patch:
                patch.setattr(queries, "game_page", add_game_after_page)
                page = application.list_games(reader, provider="lichess")
            assert page["total"] == len(page["items"]) == 1
            assert page["freshness"]["last_checked_at"] is None
            assert not reader.in_transaction
            fresh = application.list_games(reader, provider="lichess")
            assert fresh["total"] == len(fresh["items"]) == 2
            assert fresh["freshness"]["last_checked_at"] == 123
            assert not reader.in_transaction


def test_read_only_views_release_transactions_on_success_and_error(seeded_archive: Path) -> None:
    with connection(seeded_archive) as reader:
        assert reader.execute("PRAGMA query_only").fetchone()[0] == 1
        statements: list[str] = []
        reader.set_trace_callback(statements.append)
        for read in (
            lambda: application.list_games(reader),
            lambda: application.list_users(reader),
            lambda: application.list_opponents(reader, "lichess", "samename"),
            lambda: application.summary(reader),
        ):
            json.dumps(read())
            assert not reader.in_transaction
        for missing in (application.get_run, application.get_job):
            with pytest.raises(application.NotFound):
                missing(reader, 999)
            assert not reader.in_transaction
        assert any(statement.upper() == "BEGIN" for statement in statements)
        assert not any("BEGIN IMMEDIATE" in statement.upper() for statement in statements)


def test_freshness_includes_successful_revalidation_without_replacing_raw(initialized_conn) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(
        conn,
        RawRecord(
            provider="lichess", endpoint_type="user_profile", request_url="https://lichess.org/api/user/alice",
            canonical_source_key="lichess/user/alice", http_status=200, fetched_at=100,
            body=b"{}", media_type="application/json", target_username="alice",
        ),
    )
    for timestamp, status in ((100, 200), (200, 304), (300, 429), (400, 500)):
        insert_fetch_log(
            conn, provider="lichess", endpoint_type="user_profile", url="https://lichess.org/api/user/alice",
            attempted_at=timestamp, status_code=status, raw_payload_id=raw_id if status in {200, 304} else None,
        )
    insert_fetch_log(
        conn, provider="chess.com", endpoint_type="user_profile", url="https://api.chess.com/pub/player/alice",
        attempted_at=500, status_code=200,
    )
    lichess = application.list_users(conn, provider="lichess")["freshness"]
    assert lichess["last_checked_at"] == 200
    assert lichess["last_fetched_at"] == 100
    assert application.summary(conn)["freshness"]["last_checked_at"] == 500
