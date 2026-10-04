from __future__ import annotations


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

CRAWL = [
    "crawl", "opponents", "lichess", "test", "--depth", "1", "--max-users", "3",
    "--max-games", "4", "--max-jobs", "5", "--since", "2024-01", "--until", "2024-02",
]


@pytest.mark.parametrize(("arguments", "service", "provider"), FETCHES)
def test_direct_fetch_refuses_active_executor_before_database_initialization(
    database_url: str, monkeypatch, capsys, arguments: list[str], service: str, provider: str,
) -> None:
    def forbidden(*args, **kwargs):
        pytest.fail("A direct fetch must not run beside an active archive executor")

    monkeypatch.setattr(cli, service, forbidden)
    monkeypatch.setattr(cli, "initialize", forbidden)
    with archive_lock(database_url):
        assert cli.run([*arguments, "--database-url", str(database_url)]) == 1
    error = capsys.readouterr().err
    assert "active worker" in error
    assert "Traceback" not in error


@pytest.mark.parametrize(("arguments", "service", "provider"), FETCHES)
def test_direct_fetch_uses_configured_database(
    database_url: str, monkeypatch, arguments: list[str], service: str, provider: str,
) -> None:
    calls: list[bool] = []
    def fetch(conn, *args, **kwargs):
        assert state.provider_ready_at(conn, provider) is None
        calls.append(True)
        return IngestResult(provider, "fixture", 200, None, (), "ok")
    monkeypatch.setattr(cli, service, fetch)
    assert cli.run([*arguments, "--database-url", database_url]) == 0
    assert calls == [True]


@pytest.mark.parametrize(("arguments", "service", "provider"), FETCHES)
def test_direct_fetch_honors_persisted_provider_deadline_and_runs_when_due(
    database_url: str, monkeypatch, capsys, arguments: list[str], service: str, provider: str,
) -> None:
    calls: list[bool] = []

    def fetch(*args, **kwargs):
        calls.append(True)
        return IngestResult(provider, "fixture", 200, None, (), "ok")

    monkeypatch.setattr(cli, service, fetch)
    monkeypatch.setattr(cli, "time", lambda: 100.0)
    with open_database(database_url, writable=True) as conn:
        state.defer_provider(conn, provider, not_before=220.0, reason="HTTP 429", now=100.0)
    assert cli.run([*arguments, "--database-url", str(database_url)]) == 1
    assert calls == []
    assert "cooling down" in capsys.readouterr().err
    with archive_lock(database_url):
        pass
    monkeypatch.setattr(cli, "time", lambda: 220.0)
    assert cli.run([*arguments, "--database-url", str(database_url)]) == 0
    assert calls == [True]


def test_direct_fetch_releases_lock_after_provider_failure(database_url: str, monkeypatch, capsys) -> None:
    def fail(*args, **kwargs):
        raise ValueError("fixture provider failed")

    monkeypatch.setattr(cli, "fetch_user_profile", fail)
    assert cli.run(["fetch", "user", "lichess", "test", "--database-url", str(database_url)]) == 1
    assert "fixture provider failed" in capsys.readouterr().err
    with archive_lock(database_url):
        pass


@pytest.mark.parametrize("arguments", [["jobs", "resume"], CRAWL], ids=["resume", "synchronous-crawl"])
def test_execution_cli_reports_busy_before_initializing_database(
    database_url: str, monkeypatch, capsys, arguments: list[str],
) -> None:
    with open_database(database_url, writable=True) as conn:
        job_id = state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="test").job_id
        state.claim_next_job(conn)

    def forbidden(*args, **kwargs):
        pytest.fail("A losing executor must not initialize database schema")

    monkeypatch.setattr(cli, "initialize", forbidden)
    with archive_lock(database_url):
        assert cli.run([*arguments, "--database-url", str(database_url)]) == 1
    assert "active worker" in capsys.readouterr().err
    with open_database(database_url) as conn:
        job = state.get_job(conn, job_id)
        assert job is not None and job.state == "in_progress" and job.attempts == 1


def test_enqueue_only_crawl_is_allowed_while_executor_owns_archive(database_url: str) -> None:
    with archive_lock(database_url):
        assert cli.run([*CRAWL, "--enqueue-only", "--database-url", str(database_url)]) == 0
    with open_database(database_url) as conn:
        assert len(state.crawl_runs(conn)) == 1


@pytest.mark.parametrize("arguments", [["jobs", "resume"], CRAWL], ids=["resume", "synchronous-crawl"])
def test_execution_cli_reuses_acquired_lease(
    database_url: str, monkeypatch, arguments: list[str],
) -> None:
    from chess_crawl.jobs.runner import ExecutionOutcome, JobRunner

    monkeypatch.setattr(JobRunner, "_execute", lambda self, job: ExecutionOutcome("done", "fixture"))
    assert cli.run([*arguments, "--database-url", database_url]) == 0
    with archive_lock(database_url):
        pass
