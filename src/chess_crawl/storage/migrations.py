"""Versioned transactional PostgreSQL schema initialization."""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from importlib import resources

from chess_crawl.storage.db import Connection, migration_atomic, require_row


def migration_resources() -> tuple[tuple[int, str, str], ...]:
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


@migration_atomic
def initialize(conn: Connection) -> MigrationResult:
    migrations = migration_resources()
    has_history = require_row(conn.execute("SELECT to_regclass('schema_migrations') IS NOT NULL"))[0]
    existing_version = current_version(conn) if has_history else 0
    if existing_version > SCHEMA_VERSION:
        raise ValueError("Database schema is newer than this application; upgrade chess-crawl")
    applied = []
    for version, name, resource in migrations:
        if has_history and _has_migration(conn, version):
            continue
        sql = resources.files("chess_crawl.storage").joinpath(resource).read_text("utf-8")
        _execute_schema(conn, sql)
        conn.execute(
            "INSERT INTO schema_migrations(version, name, applied_at) VALUES (%s, %s, %s)",
            (version, name, int(time.time())),
        )
        applied.append(name)
        has_history = True
    providers = tuple(row["key"] for row in conn.execute("SELECT key FROM providers ORDER BY key"))
    return MigrationResult(version=current_version(conn), applied=tuple(applied), providers=providers)


def _execute_schema(conn: Connection, sql: str) -> None:
    # Native simple-query execution accepts a complete packaged migration,
    # including dollar-quoted trigger bodies, without splitting or committing.
    conn.execute(sql, prepare=False)


def current_version(conn: Connection) -> int:
    row = require_row(conn.execute("SELECT MAX(version) AS version FROM schema_migrations"))
    return int(row["version"] or 0)


def _has_migration(conn: Connection, version: int) -> bool:
    return conn.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (version,)).fetchone() is not None
