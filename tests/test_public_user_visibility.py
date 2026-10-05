"""Shared archive users exclude private targets while scoped reads retain them."""

import json

import httpx
from fastapi.testclient import TestClient

from chess_crawl import ingest
from chess_crawl.api import create_app
from chess_crawl.storage.db import connection
from helpers.players import _config, _profile


def test_shared_api_users_hide_private_identities_until_public_evidence(database_url):
    with connection(database_url, mode="rw") as conn:
        ingest.fetch_user_resource(
            conn, "lichess", "PrivateOnly", "teams", owner_scope="alpha",
            config=_config(token="test-token", owner_scope="alpha"),
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[{"id": "private-team"}])),
        )
    app = create_app(database_url, workspace_tokens={"alpha": "alpha-secret", "beta": "beta-secret"})
    with TestClient(app, headers={"Authorization": "Bearer beta-secret"}) as public, \
         TestClient(app, headers={"Authorization": "Bearer alpha-secret"}) as owner:
        for client in (public, owner):
            response = client.get("/v1/users", params={"provider": "lichess"})
            assert response.status_code == 200
            assert response.json()["items"] == [] and response.json()["total"] == 0
            summary = client.get("/v1/summary").json()
            assert next(row["users"] for row in summary["providers"] if row["provider"] == "lichess") == 0
        assert public.get("/v1/users/lichess/PrivateOnly/opponents").status_code == 404
        assert public.get("/v1/users/lichess/PrivateOnly").status_code == 404
        assert public.get("/v1/summary").json()["raw_payloads"] == 0
        scoped = owner.get("/v1/users/lichess/PrivateOnly")
        assert scoped.status_code == 200 and "private-team" in scoped.text
        user_id = scoped.json()["id"]
        with connection(database_url, mode="rw") as conn:
            observed, _ = _profile(conn, {"id": "privateonly", "username": "PrivateOnly"}, at=200)
            assert observed == user_id
        response = public.get("/v1/users", params={"provider": "lichess"})
        assert response.status_code == 200 and response.json()["total"] == 1
        assert [(row["id"], row["username_normalized"], row["first_seen_at"], row["updated_at"])
                for row in response.json()["items"]] == [(user_id, "privateonly", 200, 200)]
        assert "private-team" not in json.dumps(response.json())
        for client in (public, owner):
            summary = client.get("/v1/summary").json()
            assert next(row["users"] for row in summary["providers"] if row["provider"] == "lichess") == 1
        assert public.get("/v1/users/lichess/PrivateOnly/opponents").status_code == 200
        shared = public.get("/v1/users/lichess/PrivateOnly")
        assert shared.status_code == 200 and "private-team" not in shared.text
        assert "private-team" in owner.get("/v1/users/lichess/PrivateOnly").text
