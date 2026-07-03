"""Idempotent SQLite schema initialization and migration helpers."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

from chess_crawl.storage.db import DbPath, connect


SCHEMA_VERSION = 1
SCHEMA_NAME = "0001_init"


@dataclass(frozen=True)
class MigrationResult:
    version: int
    applied: tuple[str, ...]
    providers: tuple[str, ...]


def read_schema_sql() -> str:
    return resources.files("chess_crawl.storage").joinpath("schema.sql").read_text("utf-8")


def initialize(conn: sqlite3.Connection) -> MigrationResult:
    before = _has_migration(conn, SCHEMA_VERSION)
    conn.executescript(read_schema_sql())
    conn.execute(
        """
        INSERT INTO schema_migrations(version, name, applied_at)
        VALUES (?, ?, ?)
        ON CONFLICT(version) DO NOTHING
        """,
        (SCHEMA_VERSION, SCHEMA_NAME, int(time.time())),
    )
    conn.commit()

    providers = tuple(
        row["key"]
        for row in conn.execute("SELECT key FROM providers ORDER BY key").fetchall()
    )
    return MigrationResult(
        version=current_version(conn),
        applied=() if before else (SCHEMA_NAME,),
        providers=providers,
    )


def initialize_database(path: DbPath) -> MigrationResult:
    conn = connect(Path(path), create_parent=True)
    try:
        return initialize(conn)
    finally:
        conn.close()


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
