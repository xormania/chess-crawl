"""Shared acquisition policy is versioned, propagated, and preserves backoff."""
from __future__ import annotations

from dataclasses import replace

import httpx
import pytest
from psycopg import Error

from chess_crawl import operations
from chess_crawl.config import Config
from chess_crawl.jobs import state
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.providers.registry import ProviderSession
from chess_crawl.storage.db import connection, transaction
from chess_crawl.storage.operating_policy import (
    PolicyVersionConflict, effective_provider_settings, provider_policy, provider_policy_history, set_provider_policy,
)


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_policy_versions_history_and_stale_operator_guard(initialized_conn) -> None:
    fallback = Config(chesscom_delay_s=1, max_retries=0).provider("chess.com")
    assert effective_provider_settings(initialized_conn, "chess.com", fallback) == fallback
    first = set_provider_policy(initialized_conn, replace(fallback, min_delay_s=4), expected_version=0, now=100)
    assert first["version"] == 1
    second = set_provider_policy(initialized_conn, replace(fallback, min_delay_s=2, max_retries=3), expected_version=1, now=101)
    assert second["version"] == 2
    with pytest.raises(PolicyVersionConflict):
        set_provider_policy(initialized_conn, fallback, expected_version=1)
    stored = provider_policy(initialized_conn, "chess.com")
    assert stored is not None and stored["version"] == 2
    history = provider_policy_history(initialized_conn, "chess.com", limit=1)
    assert history[0]["min_delay_s"] == 4
    assert provider_policy_history(initialized_conn, "chess.com", after_version=history[0]["version"])[0]["version"] == 2
    with pytest.raises(Error, match="append-only"), transaction(initialized_conn):
        initialized_conn.execute("DELETE FROM provider_operating_policy_history")
    assert len(provider_policy_history(initialized_conn, "chess.com")) == 2


def test_existing_provider_sessions_refresh_policy_and_share_pacing_between_replicas(database_url) -> None:
    clock = Clock()
    config = Config(chesscom_delay_s=0, max_retries=0)
    calls = []
    attempt_counts: dict[str, int] = {}

    def response(request):
        name = request.url.path.rsplit("/", 1)[-1]
        calls.append((name, clock()))
        attempt_counts[name] = attempt_counts.get(name, 0) + 1
        if name != "first" and attempt_counts[name] == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"username": name, "player_id": len(calls)})

    with connection(database_url, mode="rw") as admin, connection(database_url, mode="rw") as replica1, connection(database_url, mode="rw") as replica2:
        set_provider_policy(admin, replace(config.provider("chess.com"), min_delay_s=1), expected_version=0, now=int(clock()))
        for name in ("first", "second", "third"):
            state.enqueue_job(admin, provider="chess.com", kind="fetch_user_profile", target=name)
        with ProviderSession(config, transport=httpx.MockTransport(response), clock=clock, sleeper=clock.sleep) as session1, ProviderSession(
            config, transport=httpx.MockTransport(response), clock=clock, sleeper=clock.sleep,
        ) as session2:
            runner1 = JobRunner(replica1, config=config, session=session1, clock=clock, sleeper=clock.sleep)
            runner2 = JobRunner(replica2, config=config, session=session2, clock=clock, sleeper=clock.sleep)
            assert runner1.run(max_jobs=1).done == 1
            assert session1.client("chess.com").policy().max_retries == 0
            set_provider_policy(admin, replace(config.provider("chess.com"), min_delay_s=3, max_retries=1), expected_version=1, now=int(clock()))
            assert runner2.run(max_jobs=1).done == 1
            assert runner1.run(max_jobs=1).done == 1
            assert session1.client("chess.com").policy().max_retries == 1
        assert attempt_counts == {"first": 1, "second": 2, "third": 2}
        assert all(later[1] - earlier[1] >= 3 for earlier, later in zip(calls, calls[1:]))
        assert state.provider_ready_at(admin, "chess.com") >= clock() + 3


def test_policy_updated_during_response_controls_shared_completion_floor(database_url) -> None:
    clock = Clock()
    config = Config(chesscom_delay_s=0, max_retries=0)
    with connection(database_url, mode="rw") as admin, connection(database_url, mode="rw") as runner_conn:
        state.enqueue_job(admin, provider="chess.com", kind="fetch_user_profile", target="alice")

        def response(request):
            set_provider_policy(admin, replace(config.provider("chess.com"), min_delay_s=20), expected_version=0, now=int(clock()))
            return httpx.Response(200, json={"username": "alice", "player_id": 1})

        runner = JobRunner(runner_conn, config=config, transport=httpx.MockTransport(response), clock=clock, sleeper=clock.sleep)
        assert runner.run(max_jobs=1).done == 1
        assert state.provider_ready_at(admin, "chess.com") >= clock() + 20


def test_policy_update_never_shortens_existing_lichess_backoff(database_url) -> None:
    clock = Clock()
    config = Config(lichess_delay_s=0, max_retries=0)
    with connection(database_url, mode="rw") as admin, connection(database_url, mode="rw") as runner_conn:
        state.enqueue_job(admin, provider="lichess", kind="fetch_user_profile", target="alice")
        runner = JobRunner(runner_conn, config=config, transport=httpx.MockTransport(lambda request: httpx.Response(429)),
                           settings=WorkerSettings(job_max_retries=0), clock=clock, sleeper=clock.sleep)
        assert runner.run(max_jobs=1).errors == 1
        previous = state.provider_ready_at(admin, "lichess")
        assert previous >= clock() + 60
        set_provider_policy(admin, config.provider("lichess"), expected_version=0, now=int(clock()))
        assert state.provider_ready_at(admin, "lichess") == previous


def test_operator_commands_install_and_inspect_version_without_credentials(database_url, monkeypatch, capsys) -> None:
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_TOKEN", "provider-secret")
    base = ["--provider", "lichess", "--database-url", database_url]
    assert operations.main(["operating-policy", "show", *base]) == 0
    assert '"version": 0' in capsys.readouterr().out
    assert operations.main(["operating-policy", "set", *base, "--expected-version", "0", "--min-delay-s", "2"]) == 0
    captured = capsys.readouterr()
    assert '"version": 1' in captured.out and "provider-secret" not in captured.out + captured.err
    assert operations.main(["operating-policy", "set", *base, "--expected-version", "0", "--max-retries", "1"]) == 2
    assert "changed" in capsys.readouterr().err
    assert operations.main(["operating-policy", "history", *base]) == 0
    assert "provider-secret" not in capsys.readouterr().out


@pytest.mark.parametrize("waiting", ["shared", "local", "reservation"])
def test_policy_extended_during_wait_is_rechecked_before_actual_http(database_url, waiting) -> None:
    clock = Clock()
    config = Config(chesscom_delay_s=2 if waiting == "local" else 0, max_retries=0)
    requested_at = []
    with connection(database_url, mode="rw") as admin, connection(database_url, mode="rw") as runner_conn:
        state.enqueue_job(admin, provider="chess.com", kind="fetch_user_profile", target="alice")
        claimed = state.claim_next_job(runner_conn, now=clock())
        assert claimed is not None
        if waiting == "shared":
            state.defer_provider(admin, "chess.com", not_before=1002, reason="provider request pacing", now=clock())
        extended = False

        def sleep(seconds):
            nonlocal extended
            if not extended:
                extended = True
                set_provider_policy(admin, replace(config.provider("chess.com"), min_delay_s=20), expected_version=0, now=int(clock()))
            clock.sleep(seconds)

        def response(request):
            requested_at.append(clock())
            return httpx.Response(200, json={"username": "alice", "player_id": 1})

        runner = JobRunner(runner_conn, config=config, clock=clock, sleeper=sleep)
        with ProviderSession(config, transport=httpx.MockTransport(response), clock=clock, sleeper=sleep) as session:
            session.policy_resolver = runner._provider_settings
            session.before_request = runner._before_provider_request
            session.persist_deadline = runner._persist_provider_deadline
            if waiting == "reservation":
                def reserve(provider):
                    nonlocal extended
                    extended = True
                    set_provider_policy(admin, replace(config.provider(provider), min_delay_s=20),
                                        expected_version=0, now=int(clock()))
                    return 1024
                session.reserve_request = reserve
                session.finish_request = lambda provider, received: None
            client = session.client("chess.com")
            if waiting == "local":
                client.http._last_request_at = clock()
            assert client.get_user_profile("alice").http_status == 200
        assert requested_at == [1020]
        state.release_job_ownership(runner_conn)


def test_runtime_role_can_read_but_cannot_edit_provider_policy(database_url) -> None:
    from uuid import uuid4
    from psycopg import sql, errors
    from chess_crawl.storage.cloud_bootstrap import bootstrap_runtime_role
    username = "policy_runtime_" + uuid4().hex
    with connection(database_url, mode="rw") as conn:
        try:
            bootstrap_runtime_role(conn, username=username, password="disposable-long-test-password-123456")
            with transaction(conn):
                conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(username)))
                assert provider_policy(conn, "lichess") is None
                for mutation in (
                    "INSERT INTO provider_operating_policies VALUES ('lichess',1,1,1,1)",
                    "UPDATE provider_operating_policies SET min_delay_s=0",
                    "DELETE FROM provider_operating_policies",
                    "INSERT INTO provider_operating_policy_history VALUES ('lichess',1,1,1,1)",
                ):
                    with pytest.raises(errors.InsufficientPrivilege), transaction(conn):
                        conn.execute(mutation)
        finally:
            conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(username)))
            conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(username)))
