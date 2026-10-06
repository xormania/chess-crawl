"""Real session admission bounds, ownership, recovery, and HTTP saturation."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from psycopg import OperationalError

from chess_crawl.api import create_app
from chess_crawl.jobs.worker import Worker
from chess_crawl.storage.db import Connection, connect, connection, lock_key, acquire_lock_key, require_row
from chess_crawl.storage.session_admission import process_session_admission, reset_session_admission


@pytest.fixture
def bounded_sessions(database_url, monkeypatch):
    # Database fixture creation/migration finishes before changing this process's
    # admission. Restore the idle registry before database fixture cleanup.
    reset_session_admission()
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_MAX_CONNECTIONS", "2")
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_ADMISSION_TIMEOUT_S", "0.15")
    yield database_url
    reset_session_admission()


def test_real_connections_are_bounded_across_threads_and_released_on_close(bounded_sessions) -> None:
    entered = threading.Barrier(3)
    release = threading.Event()
    pids = []
    errors = []

    def hold():
        try:
            with connection(bounded_sessions) as conn:
                pids.append(require_row(conn.execute("SELECT pg_backend_pid()"))[0])
                entered.wait(5)
                assert release.wait(5)
        except BaseException as exc:
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=2) as executor:
        holders = [executor.submit(hold) for _ in range(2)]
        try:
            entered.wait(5)
            assert len(set(pids)) == 2
            with pytest.raises(OperationalError, match="session capacity"):
                connect(bounded_sessions)
        finally:
            release.set()
        for holder in holders:
            holder.result(5)
    assert not errors
    with connection(bounded_sessions) as replacement:
        assert require_row(replacement.execute("SELECT 1"))[0] == 1


def test_failed_connect_and_setup_return_admission_without_stealing_sessions(bounded_sessions, monkeypatch) -> None:
    original = Connection.connect
    for _ in range(3):
        monkeypatch.setattr(Connection, "connect", lambda *args, **kwargs: (_ for _ in ()).throw(OperationalError("offline")))
        with pytest.raises(OperationalError):
            connect(bounded_sessions)
    monkeypatch.setattr(Connection, "connect", original)
    with connection(bounded_sessions, mode="rw") as first:
        key = lock_key("admission-ownership", "first")
        assert acquire_lock_key(first, key)
        first_pid = require_row(first.execute("SELECT pg_backend_pid()"))[0]
        with connection(bounded_sessions, mode="rw") as second:
            assert require_row(second.execute("SELECT pg_backend_pid()"))[0] != first_pid
            assert not acquire_lock_key(second, key)


def test_setup_failure_and_repeated_concurrent_close_return_exactly_one_permit(bounded_sessions, monkeypatch) -> None:
    original = Connection.execute

    def fail_setup(self, query, *args, **kwargs):
        if query == "SET TIME ZONE 'UTC'":
            raise OperationalError("setup failed")
        return original(self, query, *args, **kwargs)

    monkeypatch.setattr(Connection, "execute", fail_setup)
    for _ in range(3):
        with pytest.raises(OperationalError, match="setup failed"):
            connect(bounded_sessions)
    monkeypatch.setattr(Connection, "execute", original)
    conn = connect(bounded_sessions)
    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(lambda _: conn.close(), range(3)))
    with connection(bounded_sessions), connection(bounded_sessions):
        with pytest.raises(OperationalError):
            connect(bounded_sessions)


def test_changing_environment_cannot_create_another_admission_pool(bounded_sessions, monkeypatch) -> None:
    with connection(bounded_sessions), connection(bounded_sessions):
        initial = process_session_admission()
        monkeypatch.setenv("CHESS_CRAWL_DATABASE_MAX_CONNECTIONS", "100")
        assert process_session_admission() is initial
        with pytest.raises(OperationalError):
            connect(bounded_sessions)
        with pytest.raises(RuntimeError, match="connections are active"):
            reset_session_admission()


def test_http_session_saturation_returns_existing_service_unavailable_contract(bounded_sessions) -> None:
    with TestClient(create_app(bounded_sessions, "test-token"), headers={"Authorization": "Bearer test-token"}) as client:
        with connection(bounded_sessions), connection(bounded_sessions):
            response = client.get("/v1/summary")
        assert response.status_code == 503
        assert "session capacity" not in response.text
        assert client.get("/v1/summary").status_code == 200


def test_workers_reject_a_cap_that_cannot_preserve_heartbeat_session(database_url, monkeypatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_MAX_CONNECTIONS", "1")
    with pytest.raises(ValueError, match="execution and heartbeat"):
        Worker(database_url)


@pytest.mark.parametrize("role", ["acquisition", "processing"])
def test_role_validation_accepts_its_isolated_queue_only(role, database_url, monkeypatch, capsys) -> None:
    from chess_crawl import operations
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", database_url)
    monkeypatch.setenv(f"CHESS_CRAWL_SQS_{role.upper()}_QUEUE_URL", f"https://sqs.example/{role}")
    assert operations.main(["config", "validate", "--role", role]) == 0
    assert '"valid": true' in capsys.readouterr().out


def test_worker_cannot_bypass_an_initialized_one_session_cap_by_changing_environment(database_url, monkeypatch) -> None:
    reset_session_admission()
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_MAX_CONNECTIONS", "1")
    try:
        with connection(database_url):
            pass
        monkeypatch.setenv("CHESS_CRAWL_DATABASE_MAX_CONNECTIONS", "2")
        with pytest.raises(ValueError, match="execution and heartbeat"):
            Worker(database_url)
    finally:
        reset_session_admission()
