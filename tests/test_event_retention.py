from __future__ import annotations

import pytest

from chess_crawl.events.maintenance import main
from chess_crawl.jobs import state
from chess_crawl.jobs.locking import ExecutorBusy, executor_lock
from chess_crawl.storage.db import Connection, connection, require_row
from chess_crawl.storage.events import prune_events


def enqueue(conn: Connection, target: str) -> int:
    return state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target=target).job_id


def test_delivery_off_keeps_revisions_dispatch_and_polling_state(initialized_conn: Connection) -> None:
    conn = initialized_conn
    conn.execute("SELECT set_config('chess_crawl.events_enabled','false',false)")
    run_id, job_id = state.create_crawl_run_with_root_job(
        conn, provider="lichess", seed_spec="Player", params={},
        root_kind="fetch_user_profile", root_target="Player",
    )
    state.mark_done(conn, job_id)
    state.refresh_run_status(conn, run_id)
    assert require_row(conn.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 0
    assert require_row(conn.execute("SELECT revision FROM discovery_jobs WHERE id=%s", (job_id,)))[0] == 2
    assert require_row(conn.execute("SELECT status FROM crawl_runs WHERE id=%s", (run_id,)))[0] == "done"
    assert require_row(conn.execute("SELECT COUNT(*) FROM dispatch_outbox"))[0] > 0
    conn.execute("SELECT set_config('chess_crawl.events_enabled','true',false)")
    enqueue(conn, "Second")
    assert require_row(conn.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 1


def test_retention_is_bounded_preserves_pending_and_domain_history(initialized_conn: Connection) -> None:
    conn = initialized_conn
    jobs = [enqueue(conn, f"Player{i}") for i in range(5)]
    conn.execute("UPDATE event_outbox SET occurred_at=10,delivered_at=20 WHERE resource_id=ANY(%s)", (jobs[:3],))
    conn.execute("UPDATE event_outbox SET occurred_at=10 WHERE delivered_at IS NULL")
    with executor_lock(conn, purpose="events"):
        assert prune_events(conn, delivered_before=21, limit=2) == {"delivered_deleted": 2, "pending_discarded": 0}
        assert prune_events(conn, delivered_before=21, limit=2) == {"delivered_deleted": 1, "pending_discarded": 0}
        assert prune_events(conn, delivered_before=21, limit=2) == {"delivered_deleted": 0, "pending_discarded": 0}
        assert prune_events(conn, delivered_before=21, discard_pending_before=11, limit=1) == {
            "delivered_deleted": 0, "pending_discarded": 1,
        }
    assert require_row(conn.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_jobs"))[0] == 5


def test_cleanup_cannot_overtake_an_active_publisher(database_url: str) -> None:
    with connection(database_url, mode="rw") as publisher, connection(database_url, mode="rw") as maintenance:
        enqueue(publisher, "Player")
        with executor_lock(publisher, purpose="events"):
            with pytest.raises(ExecutorBusy):
                with executor_lock(maintenance, purpose="events"):
                    pass
            with pytest.raises(RuntimeError, match="publisher ownership"):
                prune_events(maintenance, delivered_before=100)


def test_operator_retention_checks_bounds_and_requires_current_schema(
    database_url: str, capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--database-url", database_url, "--delivered-before", "nan"]) == 2
    assert "finite" in capsys.readouterr().err
    assert main(["--database-url", database_url, "--delivered-before", "0"]) == 0
    assert '"pending_discarded": 0' in capsys.readouterr().out


def test_connections_apply_delivery_settings_across_replicas(
    database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_EVENTS_ENABLED", "false")
    with connection(database_url, mode="rw") as first, connection(database_url, mode="rw") as second:
        enqueue(first, "First")
        enqueue(second, "Second")
        assert require_row(first.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 0
    monkeypatch.setenv("CHESS_CRAWL_EVENTS_ENABLED", "invalid")
    with pytest.raises(ValueError, match="true/false"):
        with connection(database_url, mode="rw"):
            pass
