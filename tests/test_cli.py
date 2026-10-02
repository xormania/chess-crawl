from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from chess_crawl import cli


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


def test_cli_init_provider_list_and_db_info(tmp_path: Path) -> None:
    db_path = tmp_path / "archive.sqlite"

    init_result = run_cli("init", "--db", str(db_path))
    assert init_result.returncode == 0, init_result.stderr
    assert "Schema version: 1" in init_result.stdout
    assert "chess.com, lichess" in init_result.stdout

    provider_result = run_cli("provider", "list")
    assert provider_result.returncode == 0, provider_result.stderr
    assert "chess.com" in provider_result.stdout
    assert "lichess" in provider_result.stdout
    assert "wait 60s" in provider_result.stdout

    info_result = run_cli("db", "info", "--db", str(db_path))
    assert info_result.returncode == 0, info_result.stderr
    assert "Tables: 16" in info_result.stdout
    assert "Providers: chess.com, lichess" in info_result.stdout


def test_cli_db_info_errors_for_missing_db(tmp_path: Path) -> None:
    result = run_cli("db", "info", "--db", str(tmp_path / "missing.sqlite"))

    assert result.returncode == 1
    assert "Database not found" in result.stderr


def test_top_level_help_lists_commands(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.run(["--help"])

    assert exc.value.code == 0
    out = capsys.readouterr().out
    for command in ("init", "provider", "db", "fetch", "query", "crawl", "jobs", "report", "export"):
        assert command in out
