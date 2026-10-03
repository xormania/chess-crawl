"""Behavioral contracts for the shared SQLite and transaction boundary."""

from __future__ import annotations

import ast
import sqlite3
from pathlib import Path

import pytest

from chess_crawl.storage import db
from chess_crawl.storage.migrations import initialize, migration_resources
from chess_crawl.storage.repository import upsert_provider_user


def test_runtime_has_one_sqlite_and_transaction_owner() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "chess_crawl"
    violations = []
    for path in root.rglob("*.py"):
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text())
        sqlite_names = {"sqlite3"}
        sqlite_connect_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                sqlite_names.update(alias.asname or alias.name for alias in node.names if alias.name == "sqlite3")
            elif isinstance(node, ast.ImportFrom) and node.module == "sqlite3":
                sqlite_connect_names.update(alias.asname or alias.name for alias in node.names if alias.name == "connect")
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            direct_connect = isinstance(func, ast.Name) and func.id in sqlite_connect_names
            if isinstance(func, ast.Attribute):
                direct_connect |= (
                    func.attr == "connect" and isinstance(func.value, ast.Name) and func.value.id in sqlite_names
                )
                if func.attr in {"commit", "rollback", "executescript"} and relative != "storage/db.py":
                    violations.append((relative, node.lineno, "transaction ownership"))
                if func.attr in {"execute", "executemany"} and not (
                    relative.startswith("storage/") or relative == "jobs/state.py"
                ):
                    violations.append((relative, node.lineno, "SQL outside storage/state"))
            if direct_connect and relative != "storage/db.py":
                violations.append((relative, node.lineno, "SQLite connection ownership"))
    assert violations == []


@pytest.mark.parametrize("mode", ["ro", "rw"])
def test_existing_database_access_never_creates_missing_state(tmp_path: Path, mode) -> None:
    path = tmp_path / "missing" / "archive.db"
    with pytest.raises(FileNotFoundError, match="Database not found"):
        with db.connection(path, mode=mode):
            pytest.fail("Missing state must not be silently created")
    assert not path.parent.exists()


def test_archive_opens_once_and_readers_cannot_write(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "archive ?# name.db"
    real_connect = db.connect
    opened = []

    def tracked_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(db, "connect", tracked_connect)
    with db.open_database(path, writable=True) as conn:
        upsert_provider_user(conn, provider="lichess", username="alice")
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert len(opened) == 1
    with db.open_database(path) as reader:
        assert reader.execute("SELECT COUNT(*) FROM provider_users").fetchone()[0] == 1
        assert reader.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            reader.execute("DELETE FROM provider_users")
    for handle in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            handle.execute("SELECT 1")


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_connection_closes_on_failure(tmp_path: Path, failure) -> None:
    with pytest.raises(failure):
        with db.open_database(tmp_path / "archive.db", writable=True) as conn:
            raise failure("interrupted")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_nested_mutations_cannot_commit_the_callers_transaction(initialized_conn, failure) -> None:
    conn = initialized_conn
    with pytest.raises(failure):
        with db.transaction(conn):
            upsert_provider_user(conn, provider="lichess", username="alice")
            raise failure("interrupted")
    assert not conn.in_transaction
    assert conn.execute("SELECT COUNT(*) FROM provider_users").fetchone()[0] == 0


def test_failed_inner_operation_rolls_back_only_its_savepoint(initialized_conn) -> None:
    conn = initialized_conn
    with db.transaction(conn):
        upsert_provider_user(conn, provider="lichess", username="alice")
        with pytest.raises(RuntimeError):
            with db.transaction(conn):
                upsert_provider_user(conn, provider="lichess", username="bob")
                raise RuntimeError("inner failure")
        upsert_provider_user(conn, provider="lichess", username="carol")
    assert [row[0] for row in conn.execute("SELECT username_normalized FROM provider_users ORDER BY id")] == [
        "alice", "carol",
    ]


def test_commit_failure_rolls_back_and_releases_transaction() -> None:
    with db.connection(":memory:", mode="rwc") as conn:
        conn.execute("CREATE TABLE parents(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE children(parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)")
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction(conn):
                conn.execute("INSERT INTO children VALUES (7)")
        assert not conn.in_transaction
        assert conn.execute("SELECT COUNT(*) FROM children").fetchone()[0] == 0
        with db.transaction(conn):
            conn.execute("INSERT INTO parents VALUES (7)")
            conn.execute("INSERT INTO children VALUES (7)")
        assert conn.execute("SELECT COUNT(*) FROM children").fetchone()[0] == 1


def test_schema_initialization_does_not_commit_an_enclosing_operation() -> None:
    with db.connection(":memory:", mode="rwc") as conn:
        with pytest.raises(RuntimeError):
            with db.transaction(conn):
                initialize(conn)
                raise RuntimeError("initialization interrupted")
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0] == 0
        initialize(conn)
        assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == len(migration_resources())
