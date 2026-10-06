"""Public game acquisition cannot inherit account-private OAuth access."""
from __future__ import annotations

import json

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.providers.lichess.client import LichessClient
from chess_crawl.storage.db import connection, require_row
from chess_crawl.storage.raw import read_raw_payload
from helpers.api import client


TOKEN = "account-private-token"
NOW = 1704153600


def config() -> Config:
    return Config(lichess_token=TOKEN, lichess_token_owner_scope="alpha",
                  lichess_delay_s=0, chesscom_delay_s=0, max_retries=0)


def game(identity: str, *, private: bool = False) -> dict:
    return {"id": identity, "createdAt": NOW*1000-2000, "lastMoveAt": NOW*1000-1000,
            "status": "mate", "variant": "standard", "speed": "blitz", "perf": "blitz",
            "players": {"white": {"user": {"id": "alice", "name": "Alice"}},
                        "black": {"user": {"id": "bob", "name": "Bob"}}},
            "winner": "white", "clock": {"initial": 300, "increment": 0},
            "moves": "e4 e5", "clocks": [29900, 29800], "privateMarker": private}


@pytest.mark.parametrize("method", ["game", "bounded-history", "history-page"])
def test_public_game_requests_omit_configured_oauth_but_owned_resource_retains_it(method: str) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.startswith("/api/team/of/"):
            assert request.headers["Authorization"] == f"Bearer {TOKEN}"
            return httpx.Response(200, json=[{"id": "owned-private-team"}])
        assert "authorization" not in request.headers
        return httpx.Response(401, json={"error": "Private game access is unavailable anonymously"})

    provider = LichessClient(config().provider("lichess"), transport=httpx.MockTransport(respond))
    try:
        if method == "game":
            raw = provider.get_game("Priv0001")
        elif method == "bounded-history":
            raw = provider.get_user_games("alice", since=NOW-10, until=NOW, limit=2)
        else:
            raw = provider.get_user_games_page("alice", since_ms=None, until_ms=NOW*1000, limit=2)
        assert raw.http_status == 401 and raw.owner_scope == "public"
        owned = provider.get_user_resource("alice", "teams", owner_scope="alpha")
        assert owned.owner_scope == "alpha" and owned.http_status == 200
        assert len(requests) == 2
    finally:
        provider.close()


@pytest.mark.parametrize("private", [False, True], ids=["public-game", "private-game-denied"])
def test_api_game_job_never_archives_account_private_response(database_url: str, private: bool) -> None:
    identity = "Priv0001" if private else "Publ0001"
    seen: list[str | None] = []

    def respond(request: httpx.Request) -> httpx.Response:
        authorization = request.headers.get("Authorization")
        seen.append(authorization)
        # Reproduce the dangerous provider behavior: an account credential can
        # unlock private game evidence, but anonymous collection cannot.
        if private and authorization is None:
            return httpx.Response(401, json={"error": "Private game requires account access"})
        return httpx.Response(200, json=game(identity, private=private))

    with client(database_url) as api:
        accepted = api.post("/v1/games/collect", json={"provider": "lichess", "game_id": identity},
                            headers={"Idempotency-Key": identity})
        assert accepted.status_code == 202, accepted.text
        run_id, job_id = accepted.json()["run_id"], accepted.json()["job_ids"][0]
        with connection(database_url, mode="rw") as conn:
            result = JobRunner(conn, config=config(), transport=httpx.MockTransport(respond), clock=lambda: NOW).run(
                crawl_run_id=run_id, max_jobs=1,
            )
            assert seen == [None]
            if private:
                assert result.errors == 1 and result.done == 0
                assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 0
                assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 0
            else:
                assert result.done == 1
                assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
                row = require_row(conn.execute("SELECT id,owner_scope FROM raw_payloads"))
                assert row["owner_scope"] == "public"
                assert json.loads(read_raw_payload(conn, row["id"]).body)["privateMarker"] is False
        snapshot = api.get(f"/v1/jobs/{job_id}")
        assert TOKEN not in snapshot.text
        lookup = api.get("/v1/games/lookup", params={"provider": "lichess", "key": identity})
        assert lookup.status_code == (404 if private else 200)


@pytest.mark.parametrize("mode", ["bounded", "full"])
def test_api_history_imports_store_only_anonymous_public_evidence(database_url: str, mode: str) -> None:
    seen: list[str | None] = []

    def respond(request: httpx.Request) -> httpx.Response:
        authorization = request.headers.get("Authorization")
        if request.url.path == "/api/user/alice":
            assert authorization is None
            return httpx.Response(200, json={"id": "alice", "username": "Alice"})
        seen.append(authorization)
        evidence = game("Secret01" if authorization else "Publ0001", private=bool(authorization))
        return httpx.Response(200, content=json.dumps(evidence).encode())

    with client(database_url) as api:
        accepted = api.post("/v1/imports", json={"provider": "lichess", "username": "alice",
            "since": NOW-10, "until": NOW, "max_games": 2, "collection_mode": mode},
            headers={"Idempotency-Key": mode})
        assert accepted.status_code == 202, accepted.text
        run_id = accepted.json()["run_id"]
        with connection(database_url, mode="rw") as conn:
            runner = JobRunner(conn, config=config(), transport=httpx.MockTransport(respond), clock=lambda: NOW)
            result = runner.run(crawl_run_id=run_id, max_jobs=4)
            assert result.errors == 0 and result.blocked == 0
            assert seen == [None]
            assert require_row(conn.execute("SELECT provider_game_id FROM games"))[0] == "Publ0001"
            raw = require_row(conn.execute("SELECT id,owner_scope FROM raw_payloads WHERE endpoint_type='user_games_stream'"))
            assert raw["owner_scope"] == "public"
            assert json.loads(read_raw_payload(conn, raw["id"]).body)["privateMarker"] is False
        assert api.get("/v1/games/lookup", params={"provider": "lichess", "key": "Secret01"}).status_code == 404
