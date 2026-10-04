"""Admin operations keep maintenance local and preserve product API boundaries."""
from __future__ import annotations

import json

import pytest

from chess_crawl import operations
from chess_crawl.storage.db import connection, require_row
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version


def test_migrate_initializes_and_repeats_without_provider_calls(
    uninitialized_database_url: str, capsys: pytest.CaptureFixture[str],
) -> None:
    args = ["migrate", "--database-url", uninitialized_database_url]
    assert operations.main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["version"] == SCHEMA_VERSION and first["applied"]
    assert operations.main(args) == 0
    assert json.loads(capsys.readouterr().out)["applied"] == []
    with connection(uninitialized_database_url) as conn:
        assert current_version(conn) == SCHEMA_VERSION


def test_info_does_not_modify_an_initialized_archive(database_url: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert operations.main(["info", "--database-url", database_url]) == 0
    info = json.loads(capsys.readouterr().out)
    assert info["ready"] is True and info["schema_version"] == SCHEMA_VERSION


@pytest.mark.parametrize("command", ["fetch", "submit", "report", "export", "query", "crawl"])
def test_admin_rejects_product_commands(command: str) -> None:
    with pytest.raises(SystemExit) as error:
        operations.main([command])
    assert error.value.code == 2


def test_relocation_requires_explicit_external_storage(database_url: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert operations.main(["relocate", "--database-url", database_url]) == 2
    assert "Select a local or s3" in capsys.readouterr().err


def test_invalid_connection_error_does_not_echo_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    assert operations.main(["info", "--database-url", "not-postgres secret-token-value"]) == 2
    assert "secret-token-value" not in capsys.readouterr().err


def test_info_reads_an_empty_database_without_initializing_it(
    uninitialized_database_url: str, capsys: pytest.CaptureFixture[str],
) -> None:
    assert operations.main(["info", "--database-url", uninitialized_database_url]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == 0 and result["ready"] is False
    with connection(uninitialized_database_url) as conn:
        assert require_row(conn.execute("SELECT to_regclass('schema_migrations')"))[0] is None
