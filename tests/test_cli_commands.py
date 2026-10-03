from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chess_crawl import cli
from chess_crawl.ingest import IngestResult
from chess_crawl.jobs import state
from chess_crawl.storage.db import open_database


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

    def fake_create(conn, request, *, idempotency_key, limits):
        seen.update(
            {
                "provider": request.provider,
                "username": request.username,
                "since": request.since,
                "until": request.until,
                "bounds": request,
            }
        )
        return {"run_id": 12, "job_ids": [34], "replayed": False}

    class FakeRunner:
        def __init__(self, conn, *, lease):
            lease.require(conn)
            self.conn = conn

        def run(self, *, crawl_run_id=None):
            seen["crawl_run_id"] = crawl_run_id
            return SimpleNamespace(done=2, skipped=0, blocked=0, errors=0)

    monkeypatch.setattr(cli.application, "submit_crawl", fake_create)
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
    assert seen["username"] == "samename"
    assert seen["since"] == 1704067200
    assert seen["until"] == 1706745600
    assert seen["crawl_run_id"] == 12
    bounds = seen["bounds"]
    assert isinstance(bounds, cli.application.CrawlRequest)
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


def test_jobs_list_show_and_resume_paths(
    archive_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = archive_path
    with open_database(db_path, writable=True) as conn:
        job_id = state.enqueue_job(
            conn,
            provider="lichess",
            kind="fetch_user_profile",
            target="SameName",
            params={"scope": "all"},
        ).job_id
        claimed = state.claim_next_job(conn)
        assert claimed is not None
        assert claimed.id is not None
        state.mark_blocked(conn, claimed.id, reason="waiting")

    assert cli.run(["jobs", "list", "--db", str(db_path)]) == 0
    list_out = capsys.readouterr()
    assert "fetch_user_profile" in list_out.out

    assert cli.run(["jobs", "show", str(job_id), "--db", str(db_path)]) == 0
    show_out = capsys.readouterr()
    assert "State: blocked" in show_out.out
    assert '"scope"' in show_out.out

    class FakeRunner:
        def __init__(self, conn, *, lease):
            self.conn = conn
            self.lease = lease

        def run(self, *, crawl_run_id=None, max_jobs=None, resume_stale=False, unblock=False):
            assert crawl_run_id is None
            assert max_jobs == 1
            assert resume_stale is True
            assert unblock is True
            stale = state.resume_stale_in_progress(self.conn, crawl_run_id=crawl_run_id, lease=self.lease)
            unblocked = state.unblock_jobs(self.conn, crawl_run_id=crawl_run_id)
            return SimpleNamespace(
                stale_resumed=stale,
                unblocked=unblocked,
                done=0,
                skipped=0,
                blocked=0,
                errors=0,
            )

    monkeypatch.setattr(cli, "JobRunner", FakeRunner)

    assert cli.run(["jobs", "resume", "--max-jobs", "1", "--db", str(db_path)]) == 0
    resume_out = capsys.readouterr()
    assert "Blocked -> pending: 1" in resume_out.out
    with open_database(db_path) as conn:
        job = state.get_job(conn, job_id)
    assert job is not None
    assert job.state == "pending"

    assert cli.run(["jobs", "list", "--limit", "0", "--db", str(db_path)]) == 2
    assert cli.run(["jobs", "show", "999", "--db", str(db_path)]) == 1
    err_out = capsys.readouterr()
    assert "must be greater than zero" in err_out.err
    assert "Job not found" in err_out.err


def test_query_game_reports_and_filtered_exports(
    tmp_path: Path, seeded_archive: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = seeded_archive

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

    assert cli.run(["report", "user", "chess.com", "SameName", "--db", str(db_path)]) == 0
    assert "W/D/L/no result: 1/0/0/0" in capsys.readouterr().out

    graph_path = tmp_path / "graph.csv"
    assert cli.run(["export", "graph", "--format", "csv", "--output", str(graph_path), "--db", str(db_path)]) == 0
    assert "from_username" in graph_path.read_text()

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
