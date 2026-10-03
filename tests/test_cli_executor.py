from __future__ import annotations

from pathlib import Path

import pytest

from chess_crawl import cli
from chess_crawl.ingest import IngestResult
from chess_crawl.jobs import state
from chess_crawl.jobs.locking import archive_lock
from chess_crawl.storage.db import open_database


FETCHES = [
    pytest.param(["fetch", "user", "chess.com", "test"], "fetch_user_profile", "chess.com", id="user"),
    pytest.param(["fetch", "stats", "chess.com", "test"], "fetch_chesscom_stats", "chess.com", id="stats"),
    pytest.param(["fetch", "archives", "chess.com", "test"], "fetch_chesscom_archives", "chess.com", id="archives"),
    pytest.param(
        ["fetch", "games", "chess.com", "test", "--month", "2024-01"],
        "fetch_chesscom_month", "chess.com", id="monthly-games",
    ),
    pytest.param(
        ["fetch", "games", "lichess", "test", "--since", "2024-01-01", "--until", "2024-02-01", "--limit", "10"],
        "fetch_lichess_games", "lichess", id="stream-games",
    ),
]


@pytest.mark.parametrize(("arguments", "service", "provider"), FETCHES)
def test_direct_fetch_refuses_active_executor_before_provider_acquisition(
    archive_path: Path, monkeypatch, capsys, arguments: list[str], service: str, provider: str,
) -> None:
    def forbidden(*args, **kwargs):
        pytest.fail("A direct fetch must not run beside an active archive executor")

    monkeypatch.setattr(cli, service, forbidden)
    with archive_lock(archive_path):
        assert cli.run([*arguments, "--db", str(archive_path)]) == 1
    error = capsys.readouterr().err
    assert "active worker" in error
    assert "Traceback" not in error


@pytest.mark.parametrize(("arguments", "service", "provider"), FETCHES)
def test_direct_fetch_honors_persisted_provider_deadline_and_runs_when_due(
    archive_path: Path, monkeypatch, capsys, arguments: list[str], service: str, provider: str,
) -> None:
    calls: list[bool] = []

    def fetch(*args, **kwargs):
        calls.append(True)
        return IngestResult(provider, "fixture", 200, None, (), "ok")

    monkeypatch.setattr(cli, service, fetch)
    monkeypatch.setattr(cli, "time", lambda: 100.0)
    with open_database(archive_path, writable=True) as conn:
        state.defer_provider(conn, provider, not_before=220.0, reason="HTTP 429", now=100.0)
    assert cli.run([*arguments, "--db", str(archive_path)]) == 1
    assert calls == []
    assert "cooling down" in capsys.readouterr().err
    with archive_lock(archive_path):
        pass
    monkeypatch.setattr(cli, "time", lambda: 220.0)
    assert cli.run([*arguments, "--db", str(archive_path)]) == 0
    assert calls == [True]


def test_direct_fetch_releases_lock_after_provider_failure(archive_path: Path, monkeypatch, capsys) -> None:
    def fail(*args, **kwargs):
        raise ValueError("fixture provider failed")

    monkeypatch.setattr(cli, "fetch_user_profile", fail)
    assert cli.run(["fetch", "user", "lichess", "test", "--db", str(archive_path)]) == 1
    assert "fixture provider failed" in capsys.readouterr().err
    with archive_lock(archive_path):
        pass


def test_resume_cli_reports_busy_without_reclaiming_a_live_job(archive_path: Path, capsys) -> None:
    with open_database(archive_path, writable=True) as conn:
        job_id = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="test").job_id
        state.claim_next_job(conn)
    with archive_lock(archive_path):
        assert cli.run(["jobs", "resume", "--db", str(archive_path)]) == 1
    assert "active worker" in capsys.readouterr().err
    with open_database(archive_path) as conn:
        job = state.get_job(conn, job_id)
        assert job is not None and job.state == "in_progress" and job.attempts == 1
