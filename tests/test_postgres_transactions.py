"""PostgreSQL transaction defaults cannot change archive write semantics."""

from __future__ import annotations

import threading
import time
from typing import Any

import psycopg
import pytest

from chess_crawl.application import ImportRequest, submit_import
from chess_crawl.storage.db import Connection, connection, require_row, transaction
from chess_crawl.storage.repository import upsert_provider_user


_DEFAULTS = {
    "read committed": "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
    "repeatable read": "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    "serializable": "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
}
_REQUEST = ImportRequest("chess.com", "alice", 1704067200, 1706745600, 5)


def _set_default(conn: Connection, isolation: str) -> None:
    conn.execute(_DEFAULTS[isolation])
    assert require_row(conn.execute("SHOW default_transaction_isolation"))[0] == isolation


@pytest.mark.parametrize("isolation", tuple(_DEFAULTS))
def test_same_key_waiter_replays_after_commit_under_every_session_default(
    database_url: str, isolation: str,
) -> None:
    started = threading.Event()
    finished = threading.Event()
    pids: list[int] = []
    replies: list[dict[str, Any]] = []
    failures: list[BaseException] = []

    def compete() -> None:
        try:
            with connection(database_url, mode="rw") as contender:
                _set_default(contender, isolation)
                pids.append(contender.info.backend_pid)
                started.set()
                replies.append(submit_import(contender, _REQUEST, idempotency_key="concurrent-import"))
                assert require_row(contender.execute("SHOW default_transaction_isolation"))[0] == isolation
        except BaseException as exc:
            failures.append(exc)
        finally:
            started.set()
            finished.set()

    thread = threading.Thread(target=compete, name="same-key-postgres-waiter")
    with connection(database_url, mode="rw") as winner, connection(database_url) as observer:
        _set_default(winner, isolation)
        try:
            with transaction(winner):
                assert require_row(winner.execute("SHOW transaction_isolation"))[0] == "read committed"
                first = submit_import(winner, _REQUEST, idempotency_key="concurrent-import")
                thread.start()
                assert started.wait(5), "contender did not connect"
                assert failures == [], failures
                assert len(pids) == 1
                # Observe the real lock wait before committing. A barrier or
                # fixed delay alone could let the contender start after commit
                # and miss the stale-snapshot regression this test exercises.
                deadline = time.monotonic() + 4
                while time.monotonic() < deadline:
                    waiting = require_row(observer.execute(
                        """SELECT EXISTS (SELECT 1 FROM pg_locks
                             WHERE pid = %s AND locktype = 'advisory' AND NOT granted)""",
                        (pids[0],),
                    ))[0]
                    if waiting:
                        break
                    assert not finished.wait(0.01), failures or replies
                else:
                    pytest.fail("contender never waited on the archive advisory lock")
        finally:
            if thread.ident is not None:
                thread.join(timeout=5)
        assert not thread.is_alive()
        assert failures == [], failures
        assert first["replayed"] is False
        assert replies == [{**first, "replayed": True}]
        assert require_row(winner.execute("SHOW default_transaction_isolation"))[0] == isolation
        assert require_row(observer.execute("SELECT COUNT(*) FROM application_submissions"))[0] == 1
        assert require_row(observer.execute("SELECT COUNT(*) FROM crawl_runs"))[0] == 1
        assert require_row(observer.execute("SELECT COUNT(*) FROM discovery_jobs"))[0] == 2
        assert require_row(observer.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 3


@pytest.mark.parametrize("isolation", ("repeatable read", "serializable"))
def test_external_writer_snapshot_is_rejected_before_any_archive_mutation(
    database_url: str, isolation: str,
) -> None:
    with connection(database_url, mode="rw") as conn:
        _set_default(conn, isolation)
        with conn.transaction():
            # Establish the caller's snapshot: changing isolation now is
            # forbidden, and waiting on the write lock would not refresh it.
            assert require_row(conn.execute("SELECT COUNT(*) FROM crawl_runs"))[0] == 0
            with pytest.raises(ValueError, match="READ COMMITTED"):
                submit_import(conn, _REQUEST, idempotency_key="external-snapshot")
            assert require_row(conn.execute("SHOW transaction_isolation"))[0] == isolation
            assert require_row(conn.execute("SELECT COUNT(*) FROM application_submissions"))[0] == 0
            assert require_row(conn.execute("SELECT COUNT(*) FROM crawl_runs"))[0] == 0
            assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_jobs"))[0] == 0
            assert require_row(conn.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 0
        assert not conn.in_transaction
        # A rejected caller-owned operation must leave the connection usable.
        assert submit_import(conn, _REQUEST, idempotency_key="external-snapshot")["replayed"] is False


@pytest.mark.parametrize("isolation", ("repeatable read", "serializable"))
def test_external_readonly_snapshot_preserves_native_write_rejection(
    database_url: str, isolation: str,
) -> None:
    with connection(database_url) as conn:
        _set_default(conn, isolation)
        with conn.transaction():
            assert require_row(conn.execute("SELECT COUNT(*) FROM provider_users"))[0] == 0
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                upsert_provider_user(conn, provider="lichess", username="alice")
            assert require_row(conn.execute("SHOW transaction_isolation"))[0] == isolation
            assert require_row(conn.execute("SHOW transaction_read_only"))[0] == "on"
            assert require_row(conn.execute("SELECT COUNT(*) FROM provider_users"))[0] == 0
        assert not conn.in_transaction
