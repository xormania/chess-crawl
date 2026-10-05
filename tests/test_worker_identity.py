"""A container's own live heartbeat cannot be supplied by another executor."""
from __future__ import annotations

import json
import runpy
import stat
import os
from helpers.processes import child_process, expect_output
import sys
import time
from pathlib import Path

import pytest

from chess_crawl.jobs import state
from chess_crawl.jobs.worker import Worker
from chess_crawl.jobs.worker_identity import local_worker_alive, read_worker_identity, write_worker_identity
from chess_crawl.storage.db import Connection, connection, require_row


def test_exact_worker_probe_and_container_command_reject_another_live_executor(
    initialized_conn: Connection, database_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn
    now = time.time()
    own, other = '1'*32, '2'*32
    identity_path = tmp_path/'worker.json'
    write_worker_identity(identity_path, own)
    conn.execute(
        """INSERT INTO executor_heartbeats(worker_id,started_at,heartbeat_at,heartbeat_expires_at,status)
           VALUES(%s,%s,%s,%s,'running'),(%s,%s,%s,%s,'running')""",
        (own,now-100,now-100,now-1,other,now,now,now+3600),
    )
    assert state.worker_status(conn, now=now)['alive']
    assert not local_worker_alive(conn, identity_path, now=now)
    probe = runpy.run_path(str(Path(__file__).resolve().parents[1]/'docker'/'healthcheck.py'))['main']
    monkeypatch.setattr('sys.argv', ['healthcheck.py','worker'])
    monkeypatch.setenv('CHESS_CRAWL_DATABASE_URL', database_url)
    monkeypatch.setenv('CHESS_CRAWL_WORKER_IDENTITY_FILE', str(identity_path))
    assert probe() == 1
    conn.execute("UPDATE executor_heartbeats SET heartbeat_at=%s,heartbeat_expires_at=%s WHERE worker_id=%s", (now,now+3600,own))
    assert local_worker_alive(conn, identity_path, now=now) and probe() == 0
    state.stop_worker(conn, own, now=now)
    assert state.worker_status(conn, now=now)['alive']
    assert not local_worker_alive(conn, identity_path, now=now) and probe() == 1
    identity_path.unlink()
    assert probe() == 1
    monkeypatch.delenv('CHESS_CRAWL_WORKER_IDENTITY_FILE')
    assert probe() == 1


def test_binding_is_private_bounded_and_rejects_pid_reuse(tmp_path: Path) -> None:
    path = tmp_path/'private'/'worker.json'
    identity = write_worker_identity(path, 'a'*32)
    assert identity.pid == int(Path('/proc/self/stat').read_text().split(' ',1)[0])
    assert read_worker_identity(path) == identity
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    data = json.loads(path.read_text())
    data['start_ticks'] += 1
    path.write_text(json.dumps(data))
    assert read_worker_identity(path) is None
    data['pid'] = True
    path.write_text(json.dumps(data))
    assert read_worker_identity(path) is None
    path.write_text(' '*4097)
    assert read_worker_identity(path) is None


def test_worker_publishes_own_binding_after_database_start_and_stopped_binding_fails_closed(
    database_url: str, tmp_path: Path,
) -> None:
    path = tmp_path/'worker.json'
    observed = []

    def runner_factory(conn, **kwargs):
        identity = read_worker_identity(path)
        assert identity is not None and identity.worker_id == kwargs['worker_id']
        observed.append(identity)
        assert local_worker_alive(conn, path)
        from chess_crawl.jobs.runner import JobRunner
        return JobRunner(conn, **kwargs)

    worker = Worker(database_url, identity_path=path, runner_factory=runner_factory)
    assert worker.run(once=True) == 0
    assert len(observed) == 1 and read_worker_identity(path) == observed[0]
    with connection(database_url) as conn:
        assert not local_worker_alive(conn, path)
        assert require_row(conn.execute("SELECT status FROM executor_heartbeats WHERE worker_id=%s", (worker.worker_id,)))[0] == 'stopped'


def test_identity_write_failure_stops_heartbeat_and_marks_worker_failed(
    database_url: str, tmp_path: Path,
) -> None:
    parent = tmp_path/'not-a-directory'
    parent.write_text('occupied')
    worker = Worker(database_url, identity_path=parent/'worker.json')
    with pytest.raises(RuntimeError, match='process identity could not be stored'):
        worker.run(once=True)
    assert worker._heartbeat_stop.is_set()
    with connection(database_url) as conn:
        assert require_row(conn.execute("SELECT status FROM executor_heartbeats WHERE worker_id=%s", (worker.worker_id,)))[0] == 'failed'


def test_persisted_binding_rejects_a_process_after_it_exits(tmp_path: Path) -> None:
    path = tmp_path/'worker.json'
    with child_process(
        [sys.executable,'-c',
         "import sys; from chess_crawl.jobs.worker_identity import write_worker_identity; "
         "write_worker_identity(sys.argv[1], 'c'*32); print('ready', flush=True); sys.stdin.readline()", str(path)],
        env={**os.environ, 'PYTHONPATH':str(Path(__file__).resolve().parents[1]/'src')},
    ) as child:
        expect_output(child, "ready")
        assert read_worker_identity(path) is not None
        _, error = child.communicate(input='\n', timeout=5)
        assert child.returncode == 0, error
        assert path.exists() and read_worker_identity(path) is None
