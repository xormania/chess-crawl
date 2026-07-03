from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from conftest import seed_game
from chess_crawl import cli
from chess_crawl.ingest import IngestResult
from chess_crawl.jobs import store
from chess_crawl.jobs.discovery import OpponentEdge, record_discovery_edges
from chess_crawl.storage.db import connect
from chess_crawl.storage.migrations import initialize_database


def test_fetch_subcommands_validate_bounds_and_call_services(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "archive.sqlite"
    calls: list[tuple[object, ...]] = []

    def fake_stats(conn: sqlite3.Connection, username: str) -> IngestResult:
        calls.append(("stats", username))
        return IngestResult("chess.com", "user_stats", 200, 1, (1,), "stats ok")

    def fake_archives(conn: sqlite3.Connection, username: str) -> IngestResult:
        calls.append(("archives", username))
        return IngestResult("chess.com", "archives_index", 200, 2, (), "archives ok")

    def fake_month(conn: sqlite3.Connection, username: str, year: int, month: int) -> IngestResult:
        calls.append(("month", username, year, month))
        return IngestResult("chess.com", "monthly_archive", 200, 3, (10,), "month ok")

    def fake_lichess_games(
        conn: sqlite3.Connection,
        username: str,
        *,
        since: int | None,
        until: int | None,
        limit: int,
    ) -> IngestResult:
        calls.append(("lichess", username, since, until, limit))
        return IngestResult("lichess", "user_games_stream", 200, 4, (11,), "lichess ok")

    monkeypatch.setattr(cli, "fetch_chesscom_stats", fake_stats)
    monkeypatch.setattr(cli, "fetch_chesscom_archives", fake_archives)
    monkeypatch.setattr(cli, "fetch_chesscom_month", fake_month)
    monkeypatch.setattr(cli, "fetch_lichess_games", fake_lichess_games)

    assert cli.run(["fetch", "stats", "chess.com", "SameName", "--db", str(db_path)]) == 0
    assert cli.run(["fetch", "archives", "chess.com", "SameName", "--db", str(db_path)]) == 0
    assert cli.run(["fetch", "games", "chess.com", "SameName", "--month", "2024-01", "--db", str(db_path)]) == 0
    assert cli.run(
        [
            "fetch",
            "games",
            "lichess",
            "SameName",
            "--since",
            "2024-01-01",
            "--until",
            "2024-01-02",
            "--limit",
            "5",
            "--db",
            str(db_path),
        ]
    ) == 0
    assert calls == [
        ("stats", "SameName"),
        ("archives", "SameName"),
        ("month", "SameName", 2024, 1),
        ("lichess", "SameName", 1704067200, 1704153600, 5),
    ]

    assert cli.run(["fetch", "games", "chess.com", "SameName", "--db", str(db_path)]) == 2
    assert cli.run(["fetch", "games", "lichess", "SameName", "--limit", "0", "--db", str(db_path)]) == 2
    out = capsys.readouterr()
    assert "Chess.com game fetch requires --month" in out.err
    assert "Lichess game fetch requires --limit" in out.err


def test_crawl_opponents_cli_requires_caps_and_passes_month_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "archive.sqlite"
    seen: dict[str, Any] = {}

    def fake_create(conn, *, provider, username, since, until, bounds):
        seen.update(
            {
                "provider": provider,
                "username": username,
                "since": since,
                "until": until,
                "bounds": bounds,
            }
        )
        return 12, 34

    class FakeRunner:
        def __init__(self, conn):
            self.conn = conn

        def run(self, *, crawl_run_id=None):
            seen["crawl_run_id"] = crawl_run_id
            return SimpleNamespace(done=2, skipped=0, blocked=0, errors=0)

    monkeypatch.setattr(cli, "create_opponent_crawl", fake_create)
    monkeypatch.setattr(cli, "JobRunner", FakeRunner)

    rc = cli.run(
        [
            "crawl",
            "opponents",
            "chess.com",
            "SameName",
            "--depth",
            "1",
            "--max-users",
            "3",
            "--max-games",
            "4",
            "--max-jobs",
            "5",
            "--since",
            "2024-01",
            "--until",
            "2024-02",
            "--db",
            str(db_path),
        ]
    )

    out = capsys.readouterr()
    assert rc == 0
    assert "crawl_run #12" in out.out
    assert seen["provider"] == "chess.com"
    assert seen["username"] == "SameName"
    assert seen["since"] == 1704067200
    assert seen["until"] == 1709251200
    assert seen["crawl_run_id"] == 12
    bounds = seen["bounds"]
    assert isinstance(bounds, cli.CrawlBounds)
    assert bounds.max_depth == 1
    assert bounds.max_users == 3

    assert cli.run(
        [
            "crawl",
            "opponents",
            "chess.com",
            "SameName",
            "--depth",
            "-1",
            "--max-users",
            "3",
            "--max-games",
            "4",
            "--max-jobs",
            "5",
            "--since",
            "2024-01",
            "--until",
            "2024-02",
            "--db",
            str(db_path),
        ]
    ) == 2


def test_jobs_list_show_and_resume_paths(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "archive.sqlite"
    initialize_database(db_path)
    with closing(connect(db_path)) as conn:
        job_id = store.enqueue_job(
            conn,
            provider="lichess",
            kind="resume",
            target="local",
            params={"scope": "all"},
        ).job_id
        claimed = store.claim_next_job(conn)
        assert claimed is not None
        assert claimed.id is not None
        store.mark_blocked(conn, claimed.id, reason="waiting")

    assert cli.run(["jobs", "list", "--db", str(db_path)]) == 0
    list_out = capsys.readouterr()
    assert "resume" in list_out.out

    assert cli.run(["jobs", "show", str(job_id), "--db", str(db_path)]) == 0
    show_out = capsys.readouterr()
    assert "State: blocked" in show_out.out
    assert '"scope"' in show_out.out

    assert cli.run(["jobs", "resume", "--max-jobs", "1", "--db", str(db_path)]) == 0
    resume_out = capsys.readouterr()
    assert "Blocked -> pending: 1" in resume_out.out
    with closing(connect(db_path)) as conn:
        job = store.get_job(conn, job_id)
    assert job is not None
    assert job.state == "done"

    assert cli.run(["jobs", "list", "--limit", "0", "--db", str(db_path)]) == 2
    assert cli.run(["jobs", "show", "999", "--db", str(db_path)]) == 1
    err_out = capsys.readouterr()
    assert "must be greater than zero" in err_out.err
    assert "Job not found" in err_out.err


def test_query_game_reports_and_filtered_exports(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "archive.sqlite"
    initialize_database(db_path)
    with closing(connect(db_path)) as conn:
        game_id, same_id, opponent_id = seed_game(
            conn,
            provider="chess.com",
            game_key="cc-1",
            white="SameName",
            black="Opponent",
            outcome="white_win",
        )
        seed_game(
            conn,
            provider="lichess",
            game_key="li-1",
            white="SameName",
            black="Opponent",
            outcome="black_win",
        )
        record_discovery_edges(
            conn,
            crawl_run_id=None,
            provider="chess.com",
            from_user_id=same_id,
            depth=1,
            edges=[OpponentEdge(opponent_id, "opponent", game_id, 1)],
        )

    assert cli.run(["query", "game", "chess.com", "cc-1", "--db", str(db_path)]) == 0
    query_out = capsys.readouterr()
    assert "Players: samename vs opponent" in query_out.out
    assert "Outcome: white_win" in query_out.out

    assert cli.run(["report", "opponents", "chess.com", "SameName", "--db", str(db_path)]) == 0
    opponents_out = capsys.readouterr()
    assert "Opponent" in opponents_out.out

    assert cli.run(["report", "games-by-month", "--provider", "chess.com", "--db", str(db_path)]) == 0
    month_out = capsys.readouterr()
    assert "2024-01" in month_out.out

    games_path = tmp_path / "chesscom-games.jsonl"
    assert cli.run(
        [
            "export",
            "games",
            "--format",
            "jsonl",
            "--provider",
            "chess.com",
            "--output",
            str(games_path),
            "--db",
            str(db_path),
        ]
    ) == 0
    rows = [json.loads(line) for line in games_path.read_text().splitlines()]
    assert [row["provider"] for row in rows] == ["chess.com"]

    assert cli.run(["query", "raw", "--provider", "chess.com", "--limit", "0", "--db", str(db_path)]) == 2
    raw_out = capsys.readouterr()
    assert "--limit must be greater than zero" in raw_out.err
