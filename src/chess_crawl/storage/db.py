"""SQLite connection helpers."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


DbPath = str | Path


def is_memory_database(path: DbPath) -> bool:
    return str(path) == ":memory:"


def database_exists(path: DbPath) -> bool:
    return is_memory_database(path) or Path(path).exists()


def connect(path: DbPath, *, create_parent: bool = False) -> sqlite3.Connection:
    if create_parent and not is_memory_database(path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    configure_connection(conn, path)
    return conn


def configure_connection(conn: sqlite3.Connection, path: DbPath = ":memory:") -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    if not is_memory_database(path):
        conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    enabled = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    if enabled != 1:
        raise RuntimeError("SQLite foreign_keys pragma could not be enabled")


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        conn.execute("BEGIN")
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
