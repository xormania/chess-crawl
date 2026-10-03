"""Idempotent SQLite schema initialization and migration helpers."""

from __future__ import annotations

import sqlite3
import time
import re
from dataclasses import dataclass
from importlib import resources
from chess_crawl.storage.db import atomic


def migration_resources() -> tuple[tuple[int, str, str], ...]:
    """Ordered packaged migrations; version 1 retains the original schema asset."""
    migrations = [(1, "0001_init", "schema.sql")]
    for resource in resources.files("chess_crawl.storage").iterdir():
        match = re.fullmatch(r"(\d{4})_([a-z0-9_]+)\.sql", resource.name)
        if match:
            migrations.append((int(match[1]), resource.name[:-4], resource.name))
    migrations.sort()
    versions = [version for version, _, _ in migrations]
    if len(set(versions)) != len(versions):
        raise RuntimeError("Duplicate packaged schema migration version")
    return tuple(migrations)


SCHEMA_VERSION = max(version for version, _, _ in migration_resources())


@dataclass(frozen=True)
class MigrationResult:
    version: int
    applied: tuple[str, ...]
    providers: tuple[str, ...]


def read_schema_sql() -> str:
    return resources.files("chess_crawl.storage").joinpath("schema.sql").read_text("utf-8")


@atomic
def initialize(conn: sqlite3.Connection) -> MigrationResult:
    migrations = migration_resources()
    try:
        existing_version = current_version(conn)
    except sqlite3.OperationalError:
        existing_version = 0
    if existing_version > SCHEMA_VERSION:
        raise ValueError("Archive schema is newer than this application; upgrade chess-crawl")
    applied = []
    for version, name, resource in migrations:
        if _has_migration(conn, version):
            continue
        sql = resources.files("chess_crawl.storage").joinpath(resource).read_text("utf-8")
        _execute_schema(conn, sql)
        conn.execute(
            "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
            (version, name, int(time.time())),
        )
        applied.append(name)

    providers = tuple(row["key"] for row in conn.execute("SELECT key FROM providers ORDER BY key"))
    return MigrationResult(version=current_version(conn), applied=tuple(applied), providers=providers)


def _execute_schema(conn: sqlite3.Connection, sql: str) -> None:
    # executescript commits an enclosing transaction. Execute complete schema
    # statements through the same transaction as the migration record instead.
    statement = ""
    for line in sql.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            conn.execute(statement)
            statement = ""
    if statement.strip():
        raise ValueError("Schema contains an incomplete SQL statement")


def current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(version) AS version FROM schema_migrations").fetchone()
    return int(row["version"] or 0)


def _has_migration(conn: sqlite3.Connection, version: int) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?",
            (version,),
        ).fetchone()
    except sqlite3.OperationalError:
        return False
    return row is not None
