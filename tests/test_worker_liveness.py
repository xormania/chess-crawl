"""Finite worker history and current liveness with long-lived archive state."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from fastapi.testclient import TestClient

from chess_crawl.api import create_app
from chess_crawl.jobs import state
from chess_crawl.jobs.locking import parallel_executor_lock
from chess_crawl.storage.db import Connection, connection, require_row, transaction


def _seed_history(conn: Connection, now: float = 200000) -> None:
    with transaction(conn):
        for prefix, count, age, expiry_offset, status in (
            ("ancient-terminal", 2000, 90000, -90000, "stopped"),
            ("ancient-running", 2000, 90000, -89990, "running"),
            ("live", 140, 10.5, 10, "running"),
            ("recent-terminal", 20, 1, -1, "failed"),
        ):
            conn.execute(
                """INSERT INTO executor_heartbeats(worker_id,started_at,heartbeat_at,heartbeat_expires_at,status)
                   SELECT %s || '-' || lpad(n::text,4,'0'),1,%s,%s,%s FROM generate_series(1,%s) AS n""",
                (prefix, now-age, now+expiry_offset, status, count),
            )
        conn.execute(
            """INSERT INTO executor_heartbeats(worker_id,started_at,heartbeat_at,heartbeat_expires_at,status)
               VALUES('unexpired-terminal',1,%s,%s,'stopped'),('retention-boundary',1,%s,%s,'stopped')""",
            (now-90000, now+100, now-86400, now-86400),
        )
        conn.execute("UPDATE executor_heartbeats SET status='stopping' WHERE worker_id='live-0001'")


def test_status_is_bounded_prioritizes_live_workers_and_counts_all_active(initialized_conn: Connection) -> None:
    conn = initialized_conn
    _seed_history(conn)
    before = require_row(conn.execute("SELECT COUNT(*) FROM executor_heartbeats"))[0]
    status = state.worker_status(conn, now=200000)
    assert len(status["workers"]) <= 128
    assert status["active_workers"] == 140
    assert status["alive"] and status["status"] == "stopping"
    assert status["worker_id"] == "live-0001" and status["age_seconds"] == 10.5
    assert status["workers_truncated"] is True
    assert status["worker_limit"] == 128
    assert all(worker["alive"] and worker["age_seconds"] == 10.5 for worker in status["workers"])
    assert require_row(conn.execute("SELECT COUNT(*) FROM executor_heartbeats"))[0] == before
    clamped = state.worker_status(conn, now=200000, max_age=5)
    assert clamped["active_workers"] == 0 and not clamped["alive"]
    assert clamped["status"] == "failed" and clamped["age_seconds"] == 1
    assert all(not worker["alive"] for worker in clamped["workers"])
    assert not conn.in_transaction


def test_terminal_cleanup_is_batched_skips_locked_and_preserves_live_or_uncertain_rows(database_url: str) -> None:
    with connection(database_url, mode="rw") as conn:
        _seed_history(conn)
        with connection(database_url, mode="rw") as locked, locked.transaction():
            locked.execute("SELECT worker_id FROM executor_heartbeats WHERE worker_id='ancient-terminal-0001' FOR UPDATE")
            assert state.prune_worker_heartbeats(conn, now=200000) == 256
            assert require_row(conn.execute("SELECT COUNT(*) FROM executor_heartbeats WHERE worker_id='ancient-terminal-0001'"))[0] == 1
        while True:
            removed = state.prune_worker_heartbeats(conn, now=200000)
            assert 0 <= removed <= 256
            if not removed:
                break
        assert require_row(conn.execute("SELECT COUNT(*) FROM executor_heartbeats WHERE worker_id LIKE 'ancient-terminal-%%'"))[0] == 0
        assert require_row(conn.execute("SELECT COUNT(*) FROM executor_heartbeats WHERE worker_id LIKE 'ancient-running-%%'"))[0] == 2000
        assert require_row(conn.execute("SELECT COUNT(*) FROM executor_heartbeats WHERE worker_id LIKE 'live-%%'"))[0] == 140
        assert require_row(conn.execute("SELECT COUNT(*) FROM executor_heartbeats WHERE worker_id IN ('unexpired-terminal','retention-boundary')"))[0] == 2
        assert state.worker_status(conn, now=200000)["active_workers"] == 140


def test_worker_lifecycle_cleans_terminal_history_without_waiting_for_status_reads(database_url: str) -> None:
    with connection(database_url, mode="rw") as conn, parallel_executor_lock(conn) as lease:
        _seed_history(conn)
        def count() -> int:
            return int(require_row(conn.execute("SELECT COUNT(*) FROM executor_heartbeats WHERE status='stopped' AND heartbeat_at<=110000"))[0])
        before = count()
        state.start_worker(conn, "new-incarnation", lease=lease, max_age=20, now=200000)
        assert count() == before-256
        assert state.heartbeat_worker(conn, "new-incarnation", max_age=20, now=200001)
        assert count() == before-512
        state.stop_worker(conn, "new-incarnation", now=200002)
        assert count() == before-768
        assert state.worker_status(conn, now=200003)["active_workers"] == 140


def test_http_worker_snapshot_exposes_its_limit_and_full_active_count(database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(state, "time", SimpleNamespace(time=lambda: 200000))
    with connection(database_url, mode='rw') as conn:
        _seed_history(conn, now=200000)
        indexes = {row[0] for row in conn.execute("SELECT indexname FROM pg_indexes WHERE tablename='executor_heartbeats'")}
        assert {'ix_executor_heartbeat_recent','ix_executor_heartbeat_live','ix_executor_heartbeat_terminal'} <= indexes
    with TestClient(create_app(database_url, 'worker-status-test')) as client:
        response = client.get('/v1/worker', headers={'Authorization':'Bearer worker-status-test'})
    assert response.status_code == 200
    body = response.json()
    assert len(body['workers']) == body['worker_limit'] == 128
    assert body['workers_truncated'] is True and body['active_workers'] == 140
