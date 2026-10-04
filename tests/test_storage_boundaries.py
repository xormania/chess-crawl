"""Behavioral contracts for the PostgreSQL connection and transaction boundary."""

from __future__ import annotations

import ast
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from chess_crawl.storage import db
from chess_crawl.storage.migrations import initialize, migration_resources
from chess_crawl.storage.repository import upsert_provider_user


def test_runtime_has_one_connection_and_transaction_owner() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "chess_crawl"
    violations = []
    for path in root.rglob("*.py"):
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text())
        driver_names = {"psycopg"}
        direct_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "sqlite3":
                        violations.append((relative, node.lineno, "SQLite import"))
                    if alias.name == "psycopg":
                        driver_names.add(alias.asname or alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module == "sqlite3":
                    violations.append((relative, node.lineno, "SQLite import"))
                if node.module == "psycopg":
                    direct_names.update(alias.asname or alias.name for alias in node.names if alias.name == "connect")
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            direct_connect = isinstance(func, ast.Name) and func.id in direct_names
            if isinstance(func, ast.Attribute):
                direct_connect |= (
                    func.attr == "connect" and isinstance(func.value, ast.Name) and func.value.id in driver_names
                )
                if func.attr in {"commit", "rollback"} and relative != "storage/db.py":
                    violations.append((relative, node.lineno, "transaction ownership"))
                if func.attr in {"execute", "executemany"} and not (
                    relative.startswith("storage/") or relative == "jobs/state.py"
                ):
                    violations.append((relative, node.lineno, "SQL outside storage/state"))
            if direct_connect and relative != "storage/db.py":
                violations.append((relative, node.lineno, "PostgreSQL connection ownership"))
    assert violations == []


@pytest.mark.parametrize("mode", ["ro", "rw", "rwc"])
def test_database_access_never_creates_missing_database(database_url: str, mode) -> None:
    missing_url = make_conninfo(database_url, dbname="chess_crawl_missing_database")
    with pytest.raises(psycopg.OperationalError, match="does not exist"):
        with db.connection(missing_url, mode=mode):
            pytest.fail("Missing databases must be provisioned explicitly")


def test_database_opens_once_and_readers_cannot_write(database_url: str, monkeypatch) -> None:
    real_connect = db.connect
    opened = []

    def tracked_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(db, "connect", tracked_connect)
    with db.open_database(database_url, writable=True) as conn:
        upsert_provider_user(conn, provider="lichess", username="alice")
    assert len(opened) == 1
    with db.open_database(database_url) as reader:
        assert db.require_row(reader.execute("SELECT COUNT(*) FROM provider_users"))[0] == 1
        assert db.require_row(reader.execute("SHOW default_transaction_read_only"))[0] == "on"
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            reader.execute("DELETE FROM provider_users")
    assert len(opened) == 2
    for handle in opened:
        assert handle.closed
        with pytest.raises(psycopg.Error, match="closed"):
            handle.execute("SELECT 1")


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_connection_closes_on_failure(database_url: str, failure) -> None:
    with pytest.raises(failure):
        with db.open_database(database_url, writable=True) as conn:
            raise failure("interrupted")
    assert conn.closed
    with pytest.raises(psycopg.Error, match="closed"):
        conn.execute("SELECT 1")


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_nested_mutations_cannot_commit_the_callers_transaction(initialized_conn, failure) -> None:
    conn = initialized_conn
    with pytest.raises(failure):
        with db.transaction(conn):
            upsert_provider_user(conn, provider="lichess", username="alice")
            raise failure("interrupted")
    assert not conn.in_transaction
    assert db.require_row(conn.execute("SELECT COUNT(*) FROM provider_users"))[0] == 0


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


def test_commit_failure_rolls_back_and_releases_transaction(uninitialized_database_url: str) -> None:
    with db.connection(uninitialized_database_url, mode="rwc") as conn:
        conn.execute("CREATE TABLE parents(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE children(parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)")
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with db.transaction(conn):
                conn.execute("INSERT INTO children VALUES (7)")
        assert not conn.in_transaction
        assert db.require_row(conn.execute("SELECT COUNT(*) FROM children"))[0] == 0
        with db.transaction(conn):
            conn.execute("INSERT INTO parents VALUES (7)")
            conn.execute("INSERT INTO children VALUES (7)")
        assert db.require_row(conn.execute("SELECT COUNT(*) FROM children"))[0] == 1


def test_schema_initialization_does_not_commit_an_enclosing_operation(uninitialized_database_url: str) -> None:
    with db.connection(uninitialized_database_url, mode="rwc") as conn:
        with pytest.raises(RuntimeError):
            with db.transaction(conn):
                initialize(conn)
                raise RuntimeError("initialization interrupted")
        assert db.require_row(conn.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'"))[0] == 0
        initialize(conn)
        assert db.require_row(conn.execute("SELECT COUNT(*) FROM schema_migrations"))[0] == len(migration_resources())


def test_consistent_read_uses_one_snapshot_until_transaction_ends(database_url: str) -> None:
    with db.connection(database_url, mode="rw") as writer, db.connection(database_url) as reader:
        upsert_provider_user(writer, provider="lichess", username="alice")
        with db.transaction(reader, write=False):
            assert db.require_row(reader.execute("SHOW transaction_isolation"))[0] == "repeatable read"
            assert db.require_row(reader.execute("SHOW transaction_read_only"))[0] == "on"
            assert db.require_row(reader.execute("SELECT COUNT(*) FROM provider_users"))[0] == 1
            upsert_provider_user(writer, provider="lichess", username="bob")
            assert db.require_row(reader.execute("SELECT COUNT(*) FROM provider_users"))[0] == 1
        with db.transaction(reader, write=False):
            assert db.require_row(reader.execute("SELECT COUNT(*) FROM provider_users"))[0] == 2


def test_readonly_snapshot_cannot_be_escalated_to_a_writer(initialized_conn) -> None:
    conn = initialized_conn
    with db.transaction(conn, write=False):
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            upsert_provider_user(conn, provider="lichess", username="alice")
        assert db.require_row(conn.execute("SELECT COUNT(*) FROM provider_users"))[0] == 0
    assert not conn.in_transaction
