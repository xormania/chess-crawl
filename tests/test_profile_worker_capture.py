"""The existing worker and API accept scoped capture identity and null dates."""

import json
from dataclasses import replace

import httpx
import pytest
from fastapi.testclient import TestClient

from chess_crawl import ingest
from chess_crawl.api import create_app
from chess_crawl.jobs import state
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.storage.db import connection
from chess_crawl.storage.player_profiles import player_profile, profile_history, resource_history
from helpers.players import _record
from helpers.players import _config, _profile


@pytest.mark.parametrize("status", [200, 304])
def test_existing_worker_uses_capture_binding_after_intervening_rename(initialized_conn, monkeypatch, status):
    conn = initialized_conn
    former, _ = _profile(conn, {"username": "Eve", "player_id": 8}, provider="chess.com", at=100)
    if status == 304:
        ingest._persist_response(conn, _record("Eve", "stats", 105), job_id=None, crawl_run_id=None)
    job = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_stats", target="Eve")
    normalizer = ingest.normalize_user_payload
    fetched = []

    def rename_before_parsing(conn, raw_id, **kwargs):
        fetched.append(kwargs["fetch_log_id"])
        _profile(conn, {"username": "FormerEve", "player_id": 8}, provider="chess.com", at=200)
        _profile(conn, {"username": "Eve", "player_id": 9}, provider="chess.com", at=300)
        return normalizer(conn, raw_id, **kwargs)

    monkeypatch.setattr(ingest, "normalize_user_payload", rename_before_parsing)
    result = JobRunner(
        conn, config=_config(), clock=lambda: 110,
        transport=httpx.MockTransport(lambda request: httpx.Response(
            status, json={"chess_blitz": {"last": {"rating": 1500}}} if status == 200 else None,
        )),
    ).run(max_jobs=1)
    assert result.done == 1 and state.get_job(conn, job.job_id).state == "done"
    assert len(fetched) == 1 and fetched[0] is not None
    statistics = [row for row in profile_history(conn, former) if row["endpoint_type"] == "user_stats"]
    assert [(row["observed_at"], row["fetch_log_id"]) for row in statistics] == [(110, fetched[0])]
    current = player_profile(conn, "chess.com", "Eve")
    assert current["statistics"] is None and current["aliases"][0]["first_seen_at"] == 300
    assert player_profile(conn, "chess.com", "FormerEve")["updated_at"] == 200


@pytest.mark.parametrize("kind", ["resource", "stats"])
def test_delayed_local_replay_can_target_its_exact_capture(initialized_conn, kind):
    conn = initialized_conn
    former, _ = _profile(conn, {"username": "Eve", "player_id": 8}, provider="chess.com", at=100)
    record = _record("Eve", kind, 110)
    raw_id, first_fetch = ingest._persist_response(conn, record, job_id=None, crawl_run_id=None)
    _profile(conn, {"username": "FormerEve", "player_id": 8}, provider="chess.com", at=200)
    current, _ = _profile(conn, {"username": "Eve", "player_id": 9}, provider="chess.com", at=300)
    duplicate, next_fetch = ingest._persist_response(conn, replace(record, fetched_at=400), job_id=None, crawl_run_id=None)
    assert duplicate == raw_id and next_fetch != first_fetch
    ingest.replay_raw_payload(conn, raw_id, fetch_log_id=first_fetch)
    history = resource_history if kind == "resource" else profile_history
    old = [row for row in history(conn, former) if kind == "resource" or row["endpoint_type"] == "user_stats"]
    new = [row for row in history(conn, current) if kind == "resource" or row["endpoint_type"] == "user_stats"]
    assert [row["observed_at"] for row in old] == [110]
    assert new == []
    ingest.replay_raw_payload(conn, raw_id, fetch_log_id=next_fetch)
    new = [row for row in history(conn, current) if kind == "resource" or row["endpoint_type"] == "user_stats"]
    assert [row["observed_at"] for row in new] == [400]


def test_existing_api_users_hide_private_only_identities_until_public_evidence(database_url):
    with connection(database_url, mode="rw") as conn:
        ingest.fetch_user_resource(
            conn, "lichess", "PrivateOnly", "teams", owner_scope="alpha",
            config=_config(token="test-token", owner_scope="alpha"),
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[{"id": "private-team"}])),
        )
    with TestClient(create_app(database_url, "test-api-token"), headers={"Authorization": "Bearer test-api-token"}) as client:
        response = client.get("/v1/users", params={"provider": "lichess"})
        assert response.status_code == 200
        assert response.json()["items"] == [] and response.json()["total"] == 0
        assert client.get("/v1/users/lichess/PrivateOnly/opponents").status_code == 404
        summary = client.get("/v1/summary").json()
        assert summary["raw_payloads"] == 0
        assert next(row["users"] for row in summary["providers"] if row["provider"] == "lichess") == 0
        with connection(database_url, mode="rw") as conn:
            scoped = player_profile(conn, "lichess", "PrivateOnly", owner_scope="alpha")
            assert scoped is not None and len(scoped["resources"]) == 1
            user_id = scoped["id"]
            _profile(conn, {"id": "privateonly", "username": "PrivateOnly"}, at=200)
            assert player_profile(conn, "lichess", "PrivateOnly", owner_scope="alpha")["id"] == user_id
        response = client.get("/v1/users", params={"provider": "lichess"})
        assert response.status_code == 200 and response.json()["total"] == 1
        assert [(row["id"], row["username_normalized"], row["first_seen_at"], row["updated_at"])
                for row in response.json()["items"]] == [(user_id, "privateonly", 200, 200)]
        assert "private-team" not in json.dumps(response.json())
        summary = client.get("/v1/summary").json()
        assert next(row["users"] for row in summary["providers"] if row["provider"] == "lichess") == 1
        assert client.get("/v1/users/lichess/PrivateOnly/opponents").status_code == 200
