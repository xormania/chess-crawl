"""The single SQLite connection, lifetime, and transaction boundary."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import wraps
from itertools import count
from pathlib import Path
from typing import Concatenate, Literal, ParamSpec, TypeVar


DbPath = str | Path
AccessMode = Literal["ro", "rw", "rwc"]
_savepoints = count()
_P = ParamSpec("_P")
_T = TypeVar("_T")


def is_memory_database(path: DbPath) -> bool:
    return str(path) == ":memory:"


def database_exists(path: DbPath) -> bool:
    return is_memory_database(path) or Path(path).is_file()


def connect(path: DbPath, *, mode: AccessMode = "rwc") -> sqlite3.Connection:
    """Open SQLite with explicit access; the caller owns the returned handle."""
    if mode not in {"ro", "rw", "rwc"}:
        raise ValueError(f"Unknown database access mode: {mode}")
    memory = is_memory_database(path)
    if not memory:
        db_path = Path(path).resolve()
        if mode == "rwc":
            db_path.parent.mkdir(parents=True, exist_ok=True)
        elif not db_path.is_file():
            raise FileNotFoundError(f"Database not found: {path}\nRun `chess-crawl init --db PATH` first.")
        target = db_path.as_uri() + f"?mode={mode}"
    else:
        if mode != "rwc":
            raise ValueError("An in-memory database must be explicitly created with mode='rwc'")
        target = ":memory:"
    conn = sqlite3.connect(target, uri=not memory)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        if mode == "ro":
            conn.execute("PRAGMA query_only = ON")
        else:
            if not memory:
                conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
        if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            raise RuntimeError("SQLite foreign_keys pragma could not be enabled")
        return conn
    except BaseException:
        conn.close()
        raise


@contextmanager
def connection(path: DbPath, *, mode: AccessMode = "ro") -> Iterator[sqlite3.Connection]:
    """Own one configured connection and close it on success or interruption."""
    conn = connect(path, mode=mode)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def open_database(path: DbPath, *, writable: bool = False) -> Iterator[sqlite3.Connection]:
    """Open one archive; only explicit writers may create or initialize it."""
    with connection(path, mode="rwc" if writable else "ro") as conn:
        if writable:
            from chess_crawl.storage.migrations import initialize

            initialize(conn)
        yield conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Commit one operation; nested operations use savepoints, never commit the owner."""
    nested = conn.in_transaction
    savepoint = f"chess_crawl_{next(_savepoints)}" if nested else None
    conn.execute(f"SAVEPOINT {savepoint}" if nested else "BEGIN IMMEDIATE")
    try:
        yield conn
        if nested:
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        else:
            conn.commit()
    except BaseException:
        if nested:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        else:
            conn.rollback()
        raise


def atomic(operation: Callable[Concatenate[sqlite3.Connection, _P], _T]) -> Callable[Concatenate[sqlite3.Connection, _P], _T]:
    """Make a storage mutation safe both alone and inside a larger operation."""
    @wraps(operation)
    def wrapped(conn: sqlite3.Connection, /, *args: _P.args, **kwargs: _P.kwargs) -> _T:
        with transaction(conn):
            return operation(conn, *args, **kwargs)

    return wrapped


def database_paths(conn: sqlite3.Connection) -> tuple[Path, ...]:
    """File-backed databases attached to this handle, for output collision checks."""
    return tuple(Path(row["file"]) for row in conn.execute("PRAGMA database_list") if row["file"])
