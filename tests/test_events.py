from __future__ import annotations

from chess_crawl.storage.db import Connection, connection, require_row, transaction

import json
import psycopg
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from chess_crawl.events import publisher as publisher_module
from chess_crawl.events.mercure import MercurePublisher, MercureSettings, retry_after_seconds
from chess_crawl.events.publisher import publish_pending
from chess_crawl.jobs import state
from chess_crawl.jobs.locking import archive_lock
from chess_crawl.storage import migrations
from chess_crawl.storage.events import archive_id, delivery_health, next_pending_event


def enqueue(conn: Connection, *, target: str = "Player") -> int:
    return state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target=target).job_id


@pytest.fixture
def mercure_settings() -> MercureSettings:
    return MercureSettings(
        hub_url="https://hub.example/.well-known/mercure",
        publisher_jwt="test-publisher-secret",
        topic_prefix="https://archive.example",
    )


def test_events_and_revisions_rollback_with_domain_state(initialized_conn: Connection) -> None:
    conn = initialized_conn
    with pytest.raises(RuntimeError, match="rollback"):
        with transaction(conn):
            state.create_crawl_run_with_root_job(
                conn, provider="lichess", seed_spec="Player", params={},
                root_kind="fetch_user_profile", root_target="Player",
            )
            assert require_row(conn.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 2
            raise RuntimeError("rollback")
    assert require_row(conn.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 0
    job_id = enqueue(conn)
    with pytest.raises(RuntimeError, match="rollback"):
        with transaction(conn):
            state.mark_done(conn, job_id)
            raise RuntimeError("rollback")
    row = require_row(conn.execute("SELECT state, revision FROM discovery_jobs WHERE id = %s", (job_id,)))
    assert tuple(row.values()) == ("pending", 1)
    assert delivery_health(conn)["pending"] == 1


def test_upgrade_keeps_existing_resource_at_revision_zero(monkeypatch: pytest.MonkeyPatch, uninitialized_database_url: str) -> None:
    packaged = migrations.migration_resources()
    with connection(uninitialized_database_url, mode="rwc") as conn:
        with monkeypatch.context() as patch:
            patch.setattr(migrations, "migration_resources", lambda: tuple(item for item in packaged if item[0] < 4))
            migrations.initialize(conn)
            job_id = enqueue(conn)
        migrations.initialize(conn)
        identity = archive_id(conn)
        assert next_pending_event(conn) is None
        assert require_row(conn.execute("SELECT revision FROM discovery_jobs WHERE id = %s", (job_id,)))[0] == 0
        state.mark_done(conn, job_id)
        event = next_pending_event(conn)
        assert event is not None and event.payload["revision"] == 1
        assert migrations.initialize(conn).applied == ()
        assert archive_id(conn) == identity


def test_only_meaningful_updates_advance_resource_revisions(initialized_conn: Connection) -> None:
    conn = initialized_conn
    run_id, job_id = state.create_crawl_run_with_root_job(
        conn, provider="lichess", seed_spec="Player", params={},
        root_kind="fetch_user_profile", root_target="Player",
    )
    state.update_crawl_run(conn, run_id, status="running", counters={}, now=1)
    assert delivery_health(conn)["pending"] == 2
    state.update_job_params(conn, job_id, {"cursor_index": 1})
    state.mark_done(conn, job_id)
    state.refresh_run_status(conn, run_id)
    jobs = conn.execute(
        "SELECT revision, payload FROM event_outbox WHERE event_type = 'job.updated' ORDER BY id"
    ).fetchall()
    assert [row["revision"] for row in jobs] == [1, 2, 3]
    assert json.loads(jobs[-1]["payload"])["status"] == "done"
    run = require_row(conn.execute("SELECT revision FROM crawl_runs WHERE id = %s", (run_id,)))
    assert run["revision"] == 2
    state.update_crawl_run(conn, run_id, status="done", now=2)
    assert require_row(conn.execute("SELECT revision FROM crawl_runs WHERE id = %s", (run_id,)))[0] == 2


def test_publisher_sees_only_committed_events_and_never_holds_write_lock(
    database_url: str, mercure_settings: MercureSettings,
) -> None:
    with connection(database_url, mode="rw") as conn, connection(database_url, mode="rw") as observer:
        with transaction(conn):
            enqueue(conn)
            assert next_pending_event(observer) is None
            with pytest.raises(RuntimeError, match="outside a database transaction"):
                next_pending_event(conn)

        def accept(request: httpx.Request) -> httpx.Response:
            assert not conn.in_transaction
            assert require_row(observer.execute("SELECT COUNT(*) FROM event_outbox"))[0] == 1
            # A distinct writer can acquire its transaction while HTTP is in flight.
            enqueue(observer, target="Second")
            return httpx.Response(201, text="hub-event-id")

        with MercurePublisher(mercure_settings, transport=httpx.MockTransport(accept)) as publisher:
            assert publish_pending(conn, publisher, limit=1) == 1
        assert delivery_health(conn)["pending"] == 1


def test_mercure_form_contract_and_stable_retry_order(
    initialized_conn: Connection, mercure_settings: MercureSettings,
) -> None:
    conn = initialized_conn
    first = enqueue(conn)
    second = enqueue(conn, target="Second")
    requests: list[dict[str, list[str]]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.headers["Authorization"] == "Bearer test-publisher-secret"
        assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
        form = parse_qs(request.content.decode())
        requests.append(form)
        if len(requests) == 1:
            return httpx.Response(429, headers={"Retry-After": "30"}, text="test-publisher-secret")
        return httpx.Response(200, text="hub-may-assign-another-id")

    with MercurePublisher(mercure_settings, transport=httpx.MockTransport(respond)) as publisher:
        assert publish_pending(conn, publisher, clock=lambda: 100) == 0
        assert publish_pending(conn, publisher, clock=lambda: 129) == 0
        assert len(requests) == 1  # The second resource cannot overtake an older event.
        assert publish_pending(conn, publisher, clock=lambda: 130) == 2

    assert requests[0] == requests[1]
    assert requests[0]["topic"] == [f"https://archive.example/jobs/{first}"]
    assert requests[2]["topic"] == [f"https://archive.example/jobs/{second}"]
    assert requests[0]["private"] == ["on"]
    assert requests[0]["type"] == ["job.updated"]
    event = json.loads(requests[0]["data"][0])
    assert event["event_id"] == requests[0]["id"][0]
    assert event["archive_id"] == archive_id(conn)
    assert event["revision"] == event["schema_version"] == 1
    assert event["status"] == "pending"
    assert event["counters"] == {"attempts": 0, "retries": 0}
    assert delivery_health(conn)["pending"] == 0
    assert conn.execute("SELECT attempts FROM event_outbox ORDER BY id").fetchall()[0][0] == 2


def test_unacknowledged_success_is_retried_with_same_id(
    initialized_conn: Connection, mercure_settings: MercureSettings, monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn
    enqueue(conn)
    requests: list[bytes] = []

    def accepted(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        return httpx.Response(200)

    def failed_ack(*args: object, **kwargs: object) -> None:
        raise psycopg.OperationalError("simulated crash before acknowledgment")

    with MercurePublisher(mercure_settings, transport=httpx.MockTransport(accepted)) as publisher:
        with monkeypatch.context() as patch:
            patch.setattr(publisher_module, "acknowledge_event", failed_ack)
            with pytest.raises(psycopg.OperationalError, match="simulated crash"):
                publish_pending(conn, publisher)
        assert publish_pending(conn, publisher) == 1
    assert requests[0] == requests[1]


@pytest.mark.parametrize("failure", ["transport", "redirect", "unauthorized"])
def test_delivery_failure_records_only_safe_diagnostics(
    initialized_conn: Connection, mercure_settings: MercureSettings, failure: str,
) -> None:
    conn = initialized_conn
    enqueue(conn)

    def fail(request: httpx.Request) -> httpx.Response:
        if failure == "transport":
            raise httpx.ConnectError("test-publisher-secret", request=request)
        if failure == "redirect":
            return httpx.Response(302, headers={"Location": "https://other.example/?secret=test-publisher-secret"})
        return httpx.Response(401, text="test-publisher-secret")

    with MercurePublisher(mercure_settings, transport=httpx.MockTransport(fail)) as publisher:
        assert publish_pending(conn, publisher, clock=lambda: 100) == 0
        assert publisher.retry_delay(100) == 300
    row = require_row(conn.execute("SELECT attempts, next_attempt_at, last_error FROM event_outbox"))
    assert tuple(row.values()) == (1, 101.0, {"transport": "transport_error", "redirect": "http_302", "unauthorized": "http_401"}[failure])
    assert "test-publisher-secret" not in repr(mercure_settings)


@pytest.mark.parametrize("value, expected", [
    ("10", 10.0), ("Thu, 01 Jan 1970 00:02:00 GMT", 20.0),
    ("Thu, 01 Jan 1970 00:00:01 GMT", 0.0), ("invalid", None), ("nan", None), ("inf", None),
])
def test_retry_after_supports_server_dates_without_nonfinite_delays(value: str, expected: float | None) -> None:
    assert retry_after_seconds(value, now=100) == expected


def test_publisher_secret_file_is_exclusive_and_never_printed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = tmp_path / "publisher.jwt"
    secret.write_text("publisher-secret\n", encoding="utf-8")
    monkeypatch.setenv("CHESS_CRAWL_MERCURE_URL", "https://hub.example/.well-known/mercure")
    monkeypatch.setenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT_FILE", str(secret))
    monkeypatch.delenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT", raising=False)
    assert MercureSettings.from_env().publisher_jwt == "publisher-secret"
    monkeypatch.setenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT", "publisher-secret")
    with pytest.raises(ValueError, match="only one"):
        MercureSettings.from_env()
    monkeypatch.delenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT_FILE")
    monkeypatch.setenv("CHESS_CRAWL_MERCURE_URL", "https://publisher-secret@hub.example/")
    with pytest.raises(ValueError) as caught:
        MercureSettings.from_env()
    assert "publisher-secret" not in str(caught.value)


def test_publisher_once_uses_independent_singleton_lock(
    database_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_MERCURE_URL", "https://hub.example/.well-known/mercure")
    monkeypatch.setenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT", "publisher-secret")
    monkeypatch.delenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT_FILE", raising=False)
    def unexpected_http(request):
        pytest.fail("An empty outbox must not contact the Mercure hub")

    monkeypatch.setattr(
        publisher_module, "MercurePublisher",
        lambda settings: MercurePublisher(settings, transport=httpx.MockTransport(unexpected_http)),
    )
    with archive_lock(database_url):
        assert publisher_module.main(["--database-url", str(database_url), "--once"]) == 0
    with archive_lock(database_url, purpose="events"):
        assert publisher_module.main(["--database-url", str(database_url), "--once"]) == 1
    output = capsys.readouterr()
    assert "active events process" in output.err
    assert "publisher-secret" not in output.err


@pytest.mark.parametrize(("table", "status_column", "changed_status"), [
    ("discovery_jobs", "state", "blocked"),
    ("crawl_runs", "status", "paused"),
])
def test_conflict_inserts_emit_only_committed_meaningful_changes(
    initialized_conn: Connection, table: str, status_column: str, changed_status: str,
) -> None:
    from psycopg import sql

    conn = initialized_conn
    run_id, job_id = state.create_crawl_run_with_root_job(
        conn, provider="lichess", seed_spec="Player", params={},
        root_kind="fetch_user_profile", root_target="Player",
    )
    resource_id = job_id if table == "discovery_jobs" else run_id
    insert = sql.SQL("INSERT INTO {table} SELECT * FROM {table} WHERE id = %s ON CONFLICT(id)").format(
        table=sql.Identifier(table),
    )
    revision = sql.SQL("SELECT revision FROM {} WHERE id = %s").format(sql.Identifier(table))
    before = delivery_health(conn)["pending"]
    with transaction(conn):
        conn.execute(insert + sql.SQL(" DO NOTHING"), (resource_id,))
        conn.execute(insert + sql.SQL(" DO UPDATE SET revision = {}.revision").format(sql.Identifier(table)), (resource_id,))
    assert require_row(conn.execute(revision, (resource_id,)))[0] == 1
    assert delivery_health(conn)["pending"] == before
    with transaction(conn):
        conn.execute(insert + sql.SQL(" DO UPDATE SET {} = %s").format(sql.Identifier(status_column)), (resource_id, changed_status))
    assert require_row(conn.execute(revision, (resource_id,)))[0] == 2
    assert delivery_health(conn)["pending"] == before + 1


def test_publisher_ownership_lost_during_http_keeps_event_for_same_id_retry(initialized_conn: Connection, mercure_settings) -> None:
    from chess_crawl.jobs.locking import ExecutorLeaseLost, executor_lock

    conn = initialized_conn
    enqueue(conn)
    requests: list[bytes] = []

    def delivered_after_unlock(request):
        requests.append(request.content)
        if len(requests) == 1:
            conn.execute("SELECT pg_advisory_unlock_all()")
        return httpx.Response(200)

    with MercurePublisher(mercure_settings, transport=httpx.MockTransport(delivered_after_unlock)) as publisher:
        with executor_lock(conn, purpose="events") as lease:
            with pytest.raises(ExecutorLeaseLost):
                publish_pending(conn, publisher, lease=lease)
        pending = next_pending_event(conn)
        assert pending is not None and pending.attempts == 0
        assert delivery_health(conn)["pending"] == 1
        with executor_lock(conn, purpose="events") as successor:
            assert publish_pending(conn, publisher, lease=successor) == 1
    assert requests[0] == requests[1]
    assert delivery_health(conn)["pending"] == 0
