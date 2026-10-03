"""The single PostgreSQL connection, lifetime, and transaction boundary.

Mutations retain the serial archive write contract through transaction advisory
locks. Read views use repeatable-read snapshots without reserving that lock.
"""
from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from typing import Any, Concatenate, Literal, ParamSpec, TypeVar, overload

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.pq import TransactionStatus

DatabaseError = psycopg.Error
DbTarget = str
AccessMode = Literal["ro", "rw", "rwc"]
_P = ParamSpec("_P")
_T = TypeVar("_T")
_WRITE_LOCK = 0x43435241574C0001
_SESSION_LOCKS = {"worker": 0x43435241574C1001, "events": 0x43435241574C1002}


class ExecutorLeaseLost(RuntimeError):
    """An executor must stop after losing its session's ownership."""


class Row(Mapping[str, Any]):
    """Named values with positional access for aggregate and repository reads."""

    def __init__(self, names: tuple[str, ...], values: Sequence[Any]) -> None:
        self._names = names
        self._values = tuple(values)
        self._positions = {name: index for index, name in enumerate(names)}

    @overload
    def __getitem__(self, key: str) -> Any: ...

    @overload
    def __getitem__(self, key: int) -> Any: ...

    def __getitem__(self, key: str | int) -> Any:
        return self._values[key if isinstance(key, int) else self._positions[key]]

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._values)


def _row_factory(cursor: psycopg.Cursor[Any]) -> Callable[[Sequence[Any]], Row]:
    names = tuple(column.name for column in cursor.description or ())
    return lambda values: Row(names, values)


class Connection(psycopg.Connection[Row]):
    _ownership_purposes: tuple[str, ...] = ()

    @property
    def in_transaction(self) -> bool:
        return self.info.transaction_status in {
            TransactionStatus.INTRANS, TransactionStatus.INERROR, TransactionStatus.ACTIVE,
        }


def require_row(cursor: psycopg.Cursor[Row]) -> Row:
    """Fetch a required result; missing invariant rows fail explicitly."""
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("A required database result was missing")
    return row


def database_url(value: str | None = None) -> str:
    """Resolve and validate PostgreSQL settings without including credentials."""
    target = value if value is not None else os.getenv("CHESS_CRAWL_DATABASE_URL")
    if not isinstance(target, str) or not target.strip():
        raise ValueError("Set CHESS_CRAWL_DATABASE_URL or --database-url to a PostgreSQL connection string")
    if "://" in target and not target.startswith(("postgresql://", "postgres://")):
        raise ValueError("Only PostgreSQL connection strings are supported")
    try:
        settings = conninfo_to_dict(target)
    except psycopg.ProgrammingError:
        raise ValueError("Invalid PostgreSQL connection string") from None
    if not settings.get("dbname"):
        raise ValueError("The PostgreSQL connection string must specify a database name")
    return target


def database_label(target: str) -> str:
    """A human label excluding passwords and arbitrary connection options."""
    settings = conninfo_to_dict(database_url(target))
    return f"PostgreSQL {settings.get('host', 'local socket')}:{settings.get('port', '5432')}/{settings['dbname']}"


def _password() -> str | None:
    value = os.getenv("CHESS_CRAWL_DATABASE_PASSWORD")
    password_file = os.getenv("CHESS_CRAWL_DATABASE_PASSWORD_FILE")
    if value is not None and password_file:
        raise ValueError("Set either CHESS_CRAWL_DATABASE_PASSWORD or CHESS_CRAWL_DATABASE_PASSWORD_FILE")
    if password_file:
        value = Path(password_file).read_text(encoding="utf-8").rstrip("\r\n")
        if not value:
            raise ValueError("The PostgreSQL password file is empty")
    return value


def connect(target: str, *, mode: AccessMode = "rwc") -> Connection:
    """Open an existing database; provisioning belongs to the operator."""
    if mode not in {"ro", "rw", "rwc"}:
        raise ValueError(f"Unknown database access mode: {mode}")
    conninfo = database_url(target)
    password = _password()
    kwargs: dict[str, Any] = {"autocommit": True, "row_factory": _row_factory, "connect_timeout": 5}
    if password is not None:
        if "password" in conninfo_to_dict(conninfo):
            raise ValueError("Configure the PostgreSQL password in either the URL or a password setting")
        kwargs["password"] = password
    conn = Connection.connect(conninfo, **kwargs)
    try:
        conn.execute("SET TIME ZONE 'UTC'")
        conn.execute("SET lock_timeout = '5s'")
        if mode == "ro":
            conn.execute("SET default_transaction_read_only = on")
        return conn
    except BaseException:
        conn.close()
        raise


@contextmanager
def connection(target: str, *, mode: AccessMode = "ro") -> Iterator[Connection]:
    conn = connect(target, mode=mode)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def open_database(target: str, *, writable: bool = False) -> Iterator[Connection]:
    with connection(target, mode="rwc" if writable else "ro") as conn:
        if writable:
            from chess_crawl.storage.migrations import initialize
            initialize(conn)
        yield conn


@contextmanager
def transaction(conn: Connection, *, write: bool = True) -> Iterator[Connection]:
    """Own one atomic operation; nested mutations use native savepoints."""
    nested = conn.in_transaction
    with conn.transaction():
        if not nested:
            conn.execute(
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED" if write else
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
        if write:
            if nested:
                isolation = require_row(conn.execute("SHOW transaction_isolation"))[0]
                readonly = require_row(conn.execute("SHOW transaction_read_only"))[0]
                if isolation != "read committed" and readonly != "on":
                    raise ValueError(
                        "Archive mutations require a READ COMMITTED outer transaction; use storage.db.transaction"
                    )
            for purpose in set(conn._ownership_purposes):
                if not owns_session_lock(conn, purpose):
                    raise ExecutorLeaseLost("Database executor ownership was lost")
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (_WRITE_LOCK,))
        yield conn


def atomic(operation: Callable[Concatenate[Connection, _P], _T]) -> Callable[Concatenate[Connection, _P], _T]:
    return _transactional(operation, write=True)


def consistent_read(operation: Callable[Concatenate[Connection, _P], _T]) -> Callable[Concatenate[Connection, _P], _T]:
    return _transactional(operation, write=False)


def _transactional(
    operation: Callable[Concatenate[Connection, _P], _T], *, write: bool,
) -> Callable[Concatenate[Connection, _P], _T]:
    @wraps(operation)
    def wrapped(conn: Connection, /, *args: _P.args, **kwargs: _P.kwargs) -> _T:
        with transaction(conn, write=write):
            return operation(conn, *args, **kwargs)
    return wrapped


def _session_key(purpose: str) -> int:
    try:
        return _SESSION_LOCKS[purpose]
    except KeyError:
        raise ValueError("Unknown database ownership purpose") from None


def acquire_session_lock(conn: Connection, purpose: str) -> bool:
    return bool(require_row(conn.execute("SELECT pg_try_advisory_lock(%s)", (_session_key(purpose),)))[0])


def owns_session_lock(conn: Connection, purpose: str) -> bool:
    key = _session_key(purpose)
    return bool(require_row(conn.execute(
        """SELECT EXISTS (SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted
               AND classid = %s AND objid = %s AND objsubid = 1)""",
        (key >> 32, key & 0xFFFFFFFF),
    ))[0])


def release_session_lock(conn: Connection, purpose: str) -> None:
    conn.execute("SELECT pg_advisory_unlock(%s)", (_session_key(purpose),))
