"""Upgrade an existing archive without losing data or partially applying DDL."""

from __future__ import annotations

import sqlite3

import pytest

from chess_crawl.storage import migrations
from chess_crawl.storage.db import connection, transaction


def test_original_archive_upgrades_without_losing_users() -> None:
    with connection(":memory:", mode="rwc") as conn:
        with transaction(conn):
            migrations._execute_schema(conn, migrations.read_schema_sql())
            conn.execute("INSERT INTO schema_migrations VALUES (1, '0001_init', 123)")
            conn.execute(
                "INSERT INTO provider_users(provider, username_normalized, display_username, first_seen_at, updated_at) "
                "VALUES ('lichess', 'alice', 'Alice', 123, 123)"
            )
        result = migrations.initialize(conn)
        assert result.applied == tuple(name for version, name, _ in migrations.migration_resources() if version > 1)
        assert result.version == migrations.SCHEMA_VERSION
        assert tuple(conn.execute("SELECT username_normalized, first_seen_at FROM provider_users").fetchone()) == (
            "alice", 123,
        )
        assert migrations.initialize(conn).applied == ()
        assert not conn.in_transaction


def test_failed_upgrade_rolls_back_schema_and_version(initialized_conn, monkeypatch, tmp_path) -> None:
    conn = initialized_conn
    before = migrations.current_version(conn)
    filename = f"{before + 1:04d}_broken.sql"
    (tmp_path / filename).write_text("CREATE TABLE must_rollback(id INTEGER);\nINSERT INTO missing VALUES(1);\n")
    available = (*migrations.migration_resources(), (before + 1, "broken", filename))
    monkeypatch.setattr(migrations, "migration_resources", lambda: available)
    monkeypatch.setattr(migrations, "SCHEMA_VERSION", before + 1)
    monkeypatch.setattr(migrations.resources, "files", lambda package: tmp_path)
    with pytest.raises(sqlite3.OperationalError):
        migrations.initialize(conn)
    assert migrations.current_version(conn) == before
    assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='must_rollback'").fetchone() is None
    assert not conn.in_transaction


def test_future_archive_is_rejected_without_modification(initialized_conn) -> None:
    conn = initialized_conn
    with transaction(conn):
        conn.execute("INSERT INTO schema_migrations VALUES (?, 'future', 123)", (migrations.SCHEMA_VERSION + 1,))
    with pytest.raises(ValueError, match="newer"):
        migrations.initialize(conn)
    assert migrations.current_version(conn) == migrations.SCHEMA_VERSION + 1
