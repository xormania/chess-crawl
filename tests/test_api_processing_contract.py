"""API submissions execute registered work against the integrated job schema."""
from __future__ import annotations

from typing import Any

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import installed_parser_target
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.providers import registry
from chess_crawl.storage.db import connection,require_row,transaction
from helpers.players import _profile,_resource
from helpers.api import client


def test_explicit_manifest_upgrade_is_offline_and_only_replays_owned_sources(database_url:str,monkeypatch:pytest.MonkeyPatch) -> None:
    with connection(database_url,mode="rw") as conn:
        _,public=_profile(conn,{"id":"alice","username":"Alice"},at=100)
        _,alpha=_resource(conn,"lichess","teams",[{"id":"alpha-private"}],authenticated=True,owner_scope="alpha")
        _,beta=_resource(conn,"lichess","teams",[{"id":"beta-private"}],authenticated=True,owner_scope="beta")
        with transaction(conn):
            conn.execute("UPDATE raw_payloads SET normalization_status='pending' WHERE id=ANY(%s)",([public,alpha,beta],))
    def forbidden(*args:Any,**kwargs:Any) -> Any:
        pytest.fail("Offline upgrades must not instantiate provider clients")
    monkeypatch.setattr(registry,"create_provider_client",forbidden)
    manifest=installed_parser_target()
    assert len(manifest)>100
    with client(database_url) as api,client(database_url,"beta") as other:
        response=api.post("/v1/upgrades",json={"provider":"lichess","name":"manifest-replay","parser_version":manifest,"batch_size":100},headers={"Idempotency-Key":"upgrade"})
        assert response.status_code==202,response.text
        run_id,job_id=response.json()["run_id"],response.json()["job_ids"][0]
        assert api.get(f"/v1/upgrades/{job_id}").json()["progress"] is None
        assert other.get(f"/v1/upgrades/{job_id}").status_code==404
        with connection(database_url,mode="rw") as conn:
            result=JobRunner(conn,stage="processing").run(crawl_run_id=run_id,max_jobs=1)
            assert result.done==1 and result.errors==0
            assert require_row(conn.execute("SELECT normalization_status FROM raw_payloads WHERE id=%s",(beta,)))[0]=="pending"
        progress=api.get(f"/v1/upgrades/{job_id}").json()
        assert progress["state"]=="done"
        assert progress["progress"]["processed"]==2
        assert progress["progress"]["owner_scope"]=="alpha"
        assert progress["progress"]["parser_version"]==manifest


def test_private_resource_submission_uses_credential_scope_and_defers_network(database_url:str) -> None:
    with connection(database_url,mode="rw") as conn:
        _profile(conn,{"id":"alice","username":"Alice"},at=100)
    body={"provider":"lichess","username":"alice","resource_key":"teams"}
    with client(database_url) as alpha,client(database_url,"beta") as beta:
        a=alpha.post("/v1/resources",json=body,headers={"Idempotency-Key":"teams"})
        b=beta.post("/v1/resources",json=body,headers={"Idempotency-Key":"teams"})
        assert a.status_code==b.status_code==202
        assert a.json()["run_id"]!=b.json()["run_id"]
        job=alpha.get(f"/v1/jobs/{a.json()['job_ids'][0]}").json()
        assert job["kind"]=="fetch_user_resource" and job["params"]["owner_scope"]=="alpha"
        seen=[]
        def respond(request:httpx.Request) -> httpx.Response:
            seen.append(request.url.path)
            assert request.headers["Authorization"]=="Bearer fixture-token"
            return httpx.Response(200,json=[{"id":"private-alpha","name":"Only alpha"}])
        with connection(database_url,mode="rw") as conn:
            config=Config(lichess_token="fixture-token",lichess_token_owner_scope="alpha",lichess_delay_s=0,max_retries=0)
            runner=JobRunner(conn,config=config,transport=httpx.MockTransport(respond))
            assert runner.run(crawl_run_id=a.json()["run_id"],max_jobs=1).done==1
            rejected=runner.run(crawl_run_id=b.json()["run_id"],max_jobs=1)
            assert rejected.done==0
        assert seen==["/api/team/of/alice"]
        assert "Only alpha" in alpha.get("/v1/users/lichess/alice/resources").text
        assert "Only alpha" not in beta.get("/v1/users/lichess/alice/resources").text
