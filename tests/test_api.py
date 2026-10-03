"""Offline HTTP contracts around shared collection and archive services."""

from __future__ import annotations

from chess_crawl.storage.db import require_row

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chess_crawl.api import create_app
from chess_crawl.application import Limits
from chess_crawl.jobs import state
from chess_crawl.jobs.locking import executor_lock
from chess_crawl.storage import db
from chess_crawl.storage.migrations import SCHEMA_VERSION


TOKEN = "test-api-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
IMPORT = {
    "provider": "lichess", "username": "Alice", "since": 1704067200,
    "until": 1706745600, "max_games": 10,
}


@pytest.fixture
def api(database_url: str) -> Iterator[TestClient]:
    with TestClient(create_app(database_url, TOKEN), headers=AUTH) as client:
        yield client


@pytest.mark.parametrize("token", ["", " "])
def test_startup_requires_configured_token(token: str) -> None:
    with pytest.raises(ValueError, match="API_TOKEN"):
        create_app("postgresql://test@127.0.0.1:1/unavailable", token)


def test_environment_configuration_does_not_initialize_database(uninitialized_database_url: str, monkeypatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", uninitialized_database_url)
    monkeypatch.setenv("CHESS_CRAWL_API_TOKEN", TOKEN)
    with TestClient(create_app()) as client:
        assert client.get("/health/live").json() == {"status": "ok"}
        response = client.get("/health/ready", headers=AUTH)
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "archive_unavailable"
        assert uninitialized_database_url not in response.text
    with db.connection(uninitialized_database_url) as conn:
        assert require_row(conn.execute("SELECT to_regclass('public.schema_migrations')"))[0] is None
    with pytest.raises(ValueError, match="PostgreSQL"):
        create_app("unsupported://database", TOKEN)


def test_bearer_token_can_be_loaded_from_a_secret_file(tmp_path: Path, monkeypatch) -> None:
    token_path = tmp_path / "api-token"
    token_path.write_text(TOKEN + "\n")
    monkeypatch.delenv("CHESS_CRAWL_API_TOKEN", raising=False)
    monkeypatch.setenv("CHESS_CRAWL_API_TOKEN_FILE", str(token_path))
    with TestClient(create_app("postgresql://test@127.0.0.1:1/unavailable"), headers=AUTH) as client:
        assert client.get("/v1/providers").status_code == 200
    monkeypatch.setenv("CHESS_CRAWL_API_TOKEN", "another-token")
    with pytest.raises(ValueError, match="not both"):
        create_app("postgresql://test@127.0.0.1:1/unavailable")


@pytest.mark.parametrize("authorization", [None, "Bearer wrong", "Basic dXNlcjpwYXNz"])
def test_authentication_precedes_database_access(tmp_path: Path, monkeypatch, authorization: str | None) -> None:
    def forbidden(*args, **kwargs):
        pytest.fail("Unauthenticated requests must not connect to PostgreSQL")

    monkeypatch.setattr(db, "connect", forbidden)
    headers = {} if authorization is None else {"Authorization": authorization}
    with TestClient(create_app("postgresql://test@127.0.0.1:1/unavailable", TOKEN)) as client:
        for response in (
            client.get("/v1/summary", headers=headers),
            client.get("/health/ready", headers=headers),
            client.post("/v1/imports", headers=headers, json=IMPORT),
        ):
            assert response.status_code == 401
            assert response.headers["www-authenticate"] == "Bearer"
            assert response.json()["error"]["code"] == "unauthorized"


def test_readiness_and_openapi(api: TestClient) -> None:
    assert api.get("/health/ready").json() == {"status": "ready", "schema_version": SCHEMA_VERSION}
    schema = api.get("/openapi.json").json()
    submission = schema["paths"]["/v1/imports"]["post"]
    assert submission["security"] == [{"HTTPBearer": []}]
    assert "202" in submission["responses"]
    assert any(item["name"] == "Idempotency-Key" and item["required"] for item in submission["parameters"])
    assert "database_url" not in schema["components"]["schemas"]["ImportBody"]["properties"]


@pytest.mark.parametrize("body", [
    {**IMPORT, "provider": "unknown"},
    {**IMPORT, "max_games": 100000},
    {**IMPORT, "max_games": True},
    {**IMPORT, "since": IMPORT["until"]},
    {**IMPORT, "database_url": "postgresql://untrusted.example/other"},
])
def test_invalid_submissions_never_open_database(tmp_path: Path, monkeypatch, body: dict) -> None:
    def forbidden(*args, **kwargs):
        pytest.fail("Invalid submissions must be rejected before connecting to PostgreSQL")

    monkeypatch.setattr(db, "connect", forbidden)
    with TestClient(create_app("postgresql://test@127.0.0.1:1/unavailable", TOKEN), headers=AUTH) as client:
        response = client.post("/v1/imports", json=body, headers={"Idempotency-Key": "invalid"})
        assert response.status_code == 422
        assert response.json()["error"]["code"]


@pytest.mark.parametrize("key", [None, "", " ", "a" * 129])
def test_submission_requires_valid_idempotency_key(api: TestClient, database_url: str, key: str | None) -> None:
    response = api.post("/v1/imports", json=IMPORT, headers={} if key is None else {"Idempotency-Key": key})
    assert response.status_code == 422
    with db.connection(database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_jobs"))[0] == 0


def test_import_is_durable_idempotent_and_enqueue_only(api: TestClient, database_url: str) -> None:
    first = api.post("/v1/imports", json=IMPORT, headers={"Idempotency-Key": "alice-january"})
    assert first.status_code == 202
    accepted = first.json()
    assert accepted["replayed"] is False
    assert accepted["job_ids"]
    assert first.headers["location"] == f"/v1/runs/{accepted['run_id']}"
    with db.connection(database_url) as conn:
        before = require_row(conn.execute("SELECT COUNT(*) FROM discovery_jobs"))[0]
        assert before == len(accepted["job_ids"])
        assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 0

    # A fresh app instance still recognizes the same durable submission.
    with TestClient(create_app(database_url, TOKEN), headers=AUTH) as restarted:
        replay = restarted.post("/v1/imports", json=IMPORT, headers={"Idempotency-Key": "alice-january"})
        assert replay.status_code == 202
        assert replay.json() == {**accepted, "replayed": True}
        conflict = restarted.post(
            "/v1/imports", json={**IMPORT, "max_games": 20}, headers={"Idempotency-Key": "alice-january"},
        )
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "idempotency_conflict"
    with db.connection(database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_jobs"))[0] == before
    run = api.get(first.headers["location"])
    assert run.status_code == 200
    assert run.json()["id"] == accepted["run_id"]
    assert "freshness" in run.json()
    for job_id in accepted["job_ids"]:
        job = api.get(f"/v1/jobs/{job_id}")
        assert job.status_code == 200
        assert job.json()["state"] == "pending"
        assert isinstance(job.json()["params"], dict)


def test_bounded_crawl_and_limits(api: TestClient) -> None:
    body = {**IMPORT, "max_depth": 1, "max_users": 5, "max_jobs": 10}
    accepted = api.post("/v1/crawls", json=body, headers={"Idempotency-Key": "crawl-1"})
    assert accepted.status_code == 202
    too_deep = api.post("/v1/crawls", json={**body, "max_depth": 100}, headers={"Idempotency-Key": "crawl-2"})
    assert too_deep.status_code == 422


def test_worker_status_distinguishes_absent_alive_and_stale(api: TestClient, database_url: str, monkeypatch) -> None:
    assert api.get("/v1/worker").json()["status"] == "absent"
    with db.connection(database_url, mode="rw") as conn:
        with executor_lock(conn) as lease:
            state.start_worker(conn, "test-worker", lease=lease, max_age=20, now=100)
    monkeypatch.setattr(state.time, "time", lambda: 110)
    alive = api.get("/v1/worker").json()
    assert alive["alive"] is True
    assert alive["worker_id"] == "test-worker"
    assert alive["heartbeat_at"] == 100
    monkeypatch.setattr(state.time, "time", lambda: 121)
    assert api.get("/v1/worker").json()["alive"] is False


def test_archive_queries_are_provider_scoped_and_paginated(seeded_database_url: str) -> None:
    with TestClient(create_app(seeded_database_url, TOKEN, limits=Limits(page_size=1)), headers=AUTH) as client:
        games = client.get("/v1/games").json()
        assert len(games["items"]) == 1
        assert games["next_cursor"] is not None
        second = client.get("/v1/games", params={"after": games["next_cursor"]}).json()
        assert second["items"][0]["id"] != games["items"][0]["id"]
        assert second["next_cursor"] is None
        users = client.get("/v1/users", params={"provider": "lichess"}).json()
        assert len(users["items"]) == 1
        assert all(user["provider"] == "lichess" for user in users["items"])
        assert users["freshness"]["provider"] == "lichess"
        opponents = client.get("/v1/users/chess.com/SameName/opponents")
        assert opponents.status_code == 200
        assert len(opponents.json()["items"]) == 1
        assert client.get("/v1/games", params={"limit": 2}).status_code == 422
        assert client.get("/v1/games", params={"provider": "unknown"}).status_code == 422
        assert client.get("/v1/games", params={"after": -1}).status_code == 422
        assert client.get("/v1/summary").status_code == 200
        assert {item["key"] for item in client.get("/v1/providers").json()} == {"lichess", "chess.com"}


@pytest.mark.parametrize("path", ["/v1/runs/999", "/v1/jobs/999", "/not-a-route"])
def test_not_found_error_envelope(api: TestClient, path: str) -> None:
    response = api.get(path)
    assert response.status_code == 404
    assert set(response.json()) == {"error"}
    assert response.json()["error"]["message"]


def test_http_connections_close_on_success_and_failure(api: TestClient, monkeypatch) -> None:
    opened = []
    real_connect = db.connect

    def tracked_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(db, "connect", tracked_connect)
    assert api.get("/v1/summary").status_code == 200
    assert api.get("/v1/runs/999").status_code == 404
    assert len(opened) == 2
    assert all(handle.closed for handle in opened)


def test_database_failure_diagnostics_never_expose_connection_secrets(monkeypatch) -> None:
    secret = "test-database-password"

    def unavailable(*args, **kwargs):
        raise db.DatabaseError(f"connection refused for password={secret}")

    monkeypatch.setattr(db, "connect", unavailable)
    target = f"postgresql://test:{secret}@127.0.0.1:1/unavailable"
    with TestClient(create_app(target, TOKEN), headers=AUTH) as client:
        for response in (
            client.get("/health/ready"),
            client.get("/v1/summary"),
            client.post("/v1/imports", json=IMPORT, headers={"Idempotency-Key": "unavailable"}),
        ):
            assert response.status_code == 503
            assert secret not in response.text
            assert target not in response.text
