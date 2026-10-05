"""HTTP archive parity, offline exports, and owned queue inspection."""
from __future__ import annotations

import csv
import asyncio
import io
import json
from typing import Any

import pytest

from chess_crawl.api.compat import ArchiveExportResponse, _export_chunks
from chess_crawl.jobs import state
from chess_crawl.providers import registry
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.db import connection, transaction
from chess_crawl.storage.discovery import OpponentEdge, record_discovery_edges
from chess_crawl.storage.raw import store_raw_payload
from chess_crawl.storage.workspaces import submission_context
from support import seed_game
from helpers.games import normalize_game
from helpers.api import client


def test_lookup_reports_and_export_are_offline_and_provider_scoped(database_url: str,monkeypatch: pytest.MonkeyPatch) -> None:
    with connection(database_url,mode="rw") as conn:
        for status in ("aborted","started","unrecognized-provider-status"):
            normalize_game(conn,status=status)
        game_id,_,_ = seed_game(conn,provider="chess.com",game_key="started",white="alice",black="bob")
    def forbidden(*args: Any,**kwargs: Any) -> Any:
        pytest.fail("Archive reads and export cannot contact providers")
    monkeypatch.setattr(registry,"create_provider_client",forbidden)
    with client(database_url) as api:
        lookup = api.get("/v1/games/lookup",params={"provider":"chess.com","key":"started"})
        assert lookup.status_code==200,lookup.text
        assert lookup.json()["id"]==game_id
        for key in ("https://example.test/chess.com/started","sha256:chess.com:started"):
            assert api.get("/v1/games/lookup",params={"provider":"chess.com","key":key}).json()["id"]==game_id
        assert api.get("/v1/games/lookup",params={"provider":"lichess","key":"missing"}).status_code==404
        assert api.get("/v1/games/lookup",params={"provider":"bogus","key":"started"}).status_code==422
        summary = api.get("/v1/users/lichess/alice/summary").json()
        assert (summary["games"],summary["no_result"],summary["in_progress"])==(3,3,1)
        month = api.get("/v1/reports/games-by-month",params={"provider":"lichess"}).json()["items"][0]
        assert (month["games"],month["no_result"],month["in_progress"])==(3,3,1)
        assert api.get("/v1/users/lichess/missing/summary").status_code==404
        games = api.get("/v1/exports/games.jsonl",params={"provider":"chess.com"})
        assert games.status_code==200 and "attachment" in games.headers["content-disposition"]
        game_rows = [json.loads(line) for line in games.text.splitlines()]
        assert len(game_rows)==1 and game_rows[0]["provider"]=="chess.com"
        assert "raw_body" not in game_rows[0]
        users = api.get("/v1/exports/users.jsonl",params={"provider":"chess.com"})
        assert {row["username_normalized"] for row in map(json.loads,users.text.splitlines())}=={"alice","bob"}
        assert api.get("/v1/exports/games.jsonl",headers={"Authorization":"Bearer unknown"}).status_code==401


def test_cached_raw_catalog_is_paged_and_cannot_read_other_workspace(database_url: str) -> None:
    with connection(database_url,mode="rw") as conn:
        for scope in ("public","alpha","beta","unassigned:legacy-profile"):
            store_raw_payload(conn,RawRecord(provider="lichess",endpoint_type="user_resource",request_url="https://example.test",
                canonical_source_key=f"fixture:{scope}",body=scope.encode(),fetched_at=1,owner_scope=scope))
    with client(database_url) as api:
        page = api.get("/v1/raw",params={"limit":1,"owner_scope":"beta"}).json()
        assert page["items"][0]["owner_scope"]=="public" and page["next_cursor"] is not None
        tail = api.get("/v1/raw",params={"after":page["next_cursor"]}).json()
        assert [row["owner_scope"] for row in tail["items"]]==["alpha"]
        assert "body" not in tail["items"][0] and "request_params" not in tail["items"][0]


def test_one_job_refresh_and_owned_queue_catalog(database_url: str,monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any,**kwargs: Any) -> Any:
        pytest.fail("Submission must only enqueue")
    monkeypatch.setattr(registry,"create_provider_client",forbidden)
    body = {"provider":"chess.com","username":"Alice"}
    with client(database_url) as alpha,client(database_url,"beta") as beta:
        profile = alpha.post("/v1/profiles/refresh",json=body,headers={"Idempotency-Key":"refresh"})
        stats = beta.post("/v1/stats/refresh",json=body,headers={"Idempotency-Key":"refresh"})
        assert profile.status_code==stats.status_code==202
        a,b = profile.json(),stats.json()
        assert len(a["job_ids"])==len(b["job_ids"])==1 and a["run_id"]!=b["run_id"]
        assert alpha.get(f"/v1/jobs/{a['job_ids'][0]}").json()["kind"]=="fetch_user_profile"
        assert beta.get(f"/v1/jobs/{b['job_ids'][0]}").json()["kind"]=="fetch_user_stats"
        assert alpha.post("/v1/profiles/refresh",json=body,headers={"Idempotency-Key":"refresh"}).json()["replayed"] is True
        assert alpha.post("/v1/stats/refresh",json=body,headers={"Idempotency-Key":"refresh"}).status_code==409
        assert alpha.post("/v1/stats/refresh",json={"provider":"lichess","username":"alice"},headers={"Idempotency-Key":"li-stats"}).status_code==422
        assert alpha.post("/v1/profiles/refresh",json={**body,"kind":"crawl_opponents","workspace_id":"beta"},headers={"Idempotency-Key":"spoof"}).status_code==422
        assert [row["id"] for row in alpha.get("/v1/jobs").json()["items"]]==a["job_ids"]
        assert [row["id"] for row in alpha.get("/v1/runs").json()["items"]]==[a["run_id"]]
        assert alpha.get("/v1/jobs",params={"run_id":b["run_id"]}).status_code==404
        assert alpha.get("/v1/jobs/status",params={"run_id":b["run_id"]}).status_code==404
        assert alpha.get("/v1/jobs/status").json()["states"]==[{"state":"pending","count":1}]


def test_graph_export_uses_owned_run_membership_and_metrics(database_url: str) -> None:
    with connection(database_url,mode="rw") as conn,transaction(conn):
        game_id,alice,bob = seed_game(conn,provider="lichess",game_key="shared",white="alice",black="bob")
        submission_context(conn,"beta")
        beta = state.create_crawl_run(conn,provider="lichess",seed_spec="beta-private",params={})
        record_discovery_edges(conn,crawl_run_id=beta,provider="lichess",from_user_id=alice,depth=0,
            edges=[OpponentEdge(bob,"bob",game_id,999)])
        submission_context(conn,"alpha")
        alpha = state.create_crawl_run(conn,provider="lichess",seed_spec="alpha",params={})
        state.enqueue_job(conn,provider="lichess",kind="crawl_opponents",target="alice",crawl_run_id=alpha,depth=2)
        conn.execute("INSERT INTO run_games(crawl_run_id,game_id) VALUES(%s,%s)",(alpha,game_id))
        record_discovery_edges(conn,crawl_run_id=alpha,provider="lichess",from_user_id=alice,depth=3,
            edges=[OpponentEdge(bob,"bob",game_id,1)])
    with client(database_url) as api:
        response = api.get("/v1/exports/graph.csv",params={"provider":"lichess"})
        assert response.status_code==200,response.text
        rows = list(csv.DictReader(io.StringIO(response.text)))
        assert len(rows)==1
        assert rows[0]["crawl_run_id"]==str(alpha) and rows[0]["crawl_run_id"]!=str(beta)
        assert rows[0]["game_count"]=="1" and rows[0]["depth"]=="3"
        assert api.get("/v1/exports/graph.csv",params={"provider":"chess.com"}).text.count("\n")==1


def test_direct_game_collection_is_bounded_owned_and_rejects_secret_or_url_ids(database_url:str) -> None:
    with client(database_url) as alpha,client(database_url,"beta") as beta:
        body={"provider":"lichess","game_id":"Ab12Cd34"}
        result=alpha.post("/v1/games/collect",json=body,headers={"Idempotency-Key":"single-game"})
        assert result.status_code==202,result.text
        assert len(result.json()["job_ids"])==1
        job_id=result.json()["job_ids"][0]
        assert alpha.get(f"/v1/jobs/{job_id}").json()["kind"]=="fetch_game_by_id"
        assert beta.get(f"/v1/jobs/{job_id}").status_code==404
        assert alpha.post("/v1/games/collect",json=body,headers={"Idempotency-Key":"single-game"}).json()["replayed"] is True
        for invalid in ("Ab12Cd34SECR","https://lichess.org/Ab12Cd34","../../private"):
            assert alpha.post("/v1/games/collect",json={**body,"game_id":invalid},headers={"Idempotency-Key":"bad"}).status_code==422
        assert alpha.post("/v1/games/collect",json={**body,"provider":"chess.com"},headers={"Idempotency-Key":"bad"}).status_code==422


def test_refresh_and_single_game_validation_precedes_database_access(monkeypatch:pytest.MonkeyPatch) -> None:
    from chess_crawl.api import compat
    def forbidden(*args:Any,**kwargs:Any) -> Any:
        pytest.fail("Invalid submissions must fail before opening storage")
    monkeypatch.setattr(compat,"connection",forbidden)
    cases=(
        ("/v1/profiles/refresh",{"provider":"bogus","username":"alice"},"valid"),
        ("/v1/profiles/refresh",{"provider":"lichess","username":"../alice"},"valid"),
        ("/v1/stats/refresh",{"provider":"lichess","username":"alice"},"valid"),
        ("/v1/games/collect",{"provider":"lichess","game_id":"Ab12Cd34SECR"},"valid"),
        ("/v1/profiles/refresh",{"provider":"lichess","username":"alice"},"contains spaces"),
    )
    with client("postgresql://test@127.0.0.1:1/unavailable") as api:
        for route,body,key in cases:
            assert api.post(route,json=body,headers={"Idempotency-Key":key}).status_code==422


def test_export_disconnect_closes_cursor_connection_and_snapshot(database_url: str,monkeypatch: pytest.MonkeyPatch) -> None:
    from contextlib import contextmanager
    from chess_crawl.api import compat
    with connection(database_url,mode="rw") as conn:
        seed_game(conn,provider="lichess",game_key="one",white="alice",black="bob")
        seed_game(conn,provider="lichess",game_key="two",white="alice",black="bob")
    observed = []
    original = compat.connection
    @contextmanager
    def tracked(target: str):
        with original(target) as conn:
            observed.append(conn)
            yield conn
    monkeypatch.setattr(compat,"connection",tracked)
    stream = _export_chunks(database_url,"games",None,"alpha")
    assert '"provider":"lichess"' in next(stream)
    assert len(observed)==1 and observed[0].closed
    stream.close()
    assert len(observed)==1 and observed[0].closed


def test_http_export_send_failure_closes_snapshot_without_garbage_collection(database_url:str,monkeypatch:pytest.MonkeyPatch) -> None:
    from contextlib import contextmanager
    from starlette.requests import ClientDisconnect
    from chess_crawl.api import compat
    with connection(database_url,mode="rw") as conn:
        seed_game(conn,provider="lichess",game_key="disconnect",white="alice",black="bob")
    observed=[]
    original=compat.connection
    @contextmanager
    def tracked(target:str):
        with original(target) as conn:
            observed.append(conn)
            yield conn
    monkeypatch.setattr(compat,"connection",tracked)
    chunks=_export_chunks(database_url,"games",None,"alpha")
    response=ArchiveExportResponse(chunks,media_type="application/x-ndjson",headers={})
    async def disconnected_receive():
        return {"type":"http.disconnect"}
    async def failing_send(message):
        if message["type"]=="http.response.body" and message.get("body"):
            raise OSError("client disconnected")
    with pytest.raises(ClientDisconnect):
        asyncio.run(response({"type":"http","asgi":{"spec_version":"2.4"}},disconnected_receive,failing_send))
    assert len(observed)==1 and observed[0].closed
    # Keep both response and generator referenced: cleanup cannot depend on GC.
    assert response.chunks is chunks and list(chunks)==[]
