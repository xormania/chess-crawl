from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from chess_crawl.ingest import IngestResult
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.raw import store_raw_payload


ROOT = Path(__file__).resolve().parents[1]


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    src = str(ROOT / "src")
    env["PYTHONPATH"] = src + os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else src
    return subprocess.run(
        [sys.executable, "-m", "chess_crawl.cli", *args],
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


def test_cli_fetch_user_query_user_and_query_raw_smoke(
    tmp_path: Path,
    fixtures_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import chess_crawl.cli as cli

    db_path = tmp_path / "archive.sqlite"
    body = (fixtures_dir / "chesscom/player.json").read_bytes()

    def fake_fetch_user_profile(conn, provider: str, username: str) -> IngestResult:
        record = RawRecord(
            provider=provider,
            endpoint_type="user_profile",
            request_url="https://api.chess.com/pub/player/samename",
            canonical_source_key="chess.com/player/samename/profile",
            http_status=200,
            fetched_at=123,
            body=body,
            media_type="application/json",
            target_username=username.lower(),
        )
        raw_payload_id = store_raw_payload(conn, record)
        user_id = normalize_user_payload(conn, raw_payload_id)
        assert user_id is not None
        return IngestResult(provider, "user_profile", 200, raw_payload_id, (user_id,), "fixture fetch")

    monkeypatch.setattr(cli, "fetch_user_profile", fake_fetch_user_profile)

    fetch_rc = cli.run(["fetch", "user", "chess.com", "SameName", "--db", str(db_path)])
    fetch_out = capsys.readouterr()
    assert fetch_rc == 0
    assert "Raw payload: 1" in fetch_out.out

    query_rc = cli.run(["query", "user", "chess.com", "SameName", "--db", str(db_path)])
    query_out = capsys.readouterr()
    assert query_rc == 0
    assert "Username: SameName (samename)" in query_out.out

    raw_rc = cli.run(["query", "raw", "--provider", "chess.com", "--limit", "1", "--db", str(db_path)])
    raw_out = capsys.readouterr()
    assert raw_rc == 0
    assert "user_profile" in raw_out.out
