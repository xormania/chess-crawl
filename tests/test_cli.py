from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from chess_crawl import cli
from chess_crawl.storage.migrations import SCHEMA_VERSION


ROOT = Path(__file__).resolve().parents[1]


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    src = str(ROOT / "src")
    env["PYTHONPATH"] = src + os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else src
    return subprocess.run(
        [sys.executable, "-m", "chess_crawl", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_cli_init_provider_list_and_db_info(tmp_path: Path,
    database_url: str,
) -> None:
    target_database_url = database_url

    init_result = run_cli("init", "--database-url", str(target_database_url))
    assert init_result.returncode == 0, init_result.stderr
    assert f"Schema version: {SCHEMA_VERSION}" in init_result.stdout
    assert "chess.com, lichess" in init_result.stdout

    provider_result = run_cli("provider", "list")
    assert provider_result.returncode == 0, provider_result.stderr
    assert "chess.com" in provider_result.stdout
    assert "lichess" in provider_result.stdout
    assert "wait 60s" in provider_result.stdout

    info_result = run_cli("db", "info", "--database-url", str(target_database_url))
    assert info_result.returncode == 0, info_result.stderr
    assert "Tables:" in info_result.stdout
    assert "Providers: chess.com, lichess" in info_result.stdout


def test_cli_db_info_errors_for_uninitialized_database(uninitialized_database_url: str) -> None:
    result = run_cli("db", "info", "--database-url", uninitialized_database_url)
    assert result.returncode == 1
    assert "unavailable" in result.stderr.lower() or "initialized" in result.stderr.lower()
    assert uninitialized_database_url not in result.stderr


def test_top_level_help_lists_commands(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.run(["--help"])

    assert exc.value.code == 0
    out = capsys.readouterr().out
    for command in ("init", "provider", "db", "fetch", "query", "crawl", "jobs", "report", "export"):
        assert command in out


def test_database_failure_diagnostics_never_expose_connection_secrets(monkeypatch, capsys) -> None:
    from chess_crawl.storage import db

    secret = "test-database-password"

    def unavailable(*args, **kwargs):
        raise db.DatabaseError(f"connection refused for password={secret}")

    monkeypatch.setattr(db, "connect", unavailable)
    assert cli.run(["init", "--database-url", f"postgresql://test:{secret}@127.0.0.1:1/unavailable"]) == 1
    output = capsys.readouterr()
    assert secret not in output.out + output.err
