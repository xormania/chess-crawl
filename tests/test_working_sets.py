"""Workspace isolation and immutable analytical inputs against PostgreSQL."""
from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from chess_crawl.api import create_app
from chess_crawl.storage.db import Connection, connection, require_row, transaction
from support import seed_game

TOKENS = {"alpha": "alpha-secret", "beta": "beta-secret"}
IMPORT: dict[str,Any] = {"provider":"lichess", "username":"alice", "since":1704067200, "until":1706745600, "max_games":10}


def client(database_url: str, workspace: str = "alpha") -> TestClient:
    return TestClient(create_app(database_url, workspace_tokens=TOKENS), headers={"Authorization":f"Bearer {TOKENS[workspace]}"})


def version(conn: Connection, game_id: int, revision: str) -> int:
    with transaction(conn):
        row = require_row(conn.execute(
            """INSERT INTO game_versions(game_id,content_hash,parser_version,first_seen_at,variant,parse_status,played_ply_count,move_text_origin)
               VALUES(%s,%s,'fixture-v1',1,'standard','complete',2,'provider.moves') RETURNING id""", (game_id,revision),
        ))
        version_id = int(row["id"])
        conn.execute("UPDATE games SET current_version_id=%s WHERE id=%s", (version_id,game_id))
        return version_id


def test_workspace_authentication_cannot_be_selected_by_request(database_url: str) -> None:
    with client(database_url) as alpha:
        assert alpha.get("/v1/workspace", headers={"X-Workspace-ID":"beta"}).json() == {"workspace_id":"alpha"}
        forbidden = alpha.post("/v1/imports", json={**IMPORT,"workspace_id":"beta"}, headers={"Idempotency-Key":"spoof"})
        assert forbidden.status_code == 422
    with pytest.raises(ValueError, match="unique"):
        create_app(database_url, workspace_tokens={"alpha":"same", "beta":"same"})


def test_submission_ownership_idempotency_and_events(database_url: str) -> None:
    with client(database_url,"alpha") as alpha, client(database_url,"beta") as beta:
        a = alpha.post("/v1/imports", json=IMPORT, headers={"Idempotency-Key":"shared-key"})
        b = beta.post("/v1/imports", json=IMPORT, headers={"Idempotency-Key":"shared-key"})
        assert a.status_code == b.status_code == 202
        assert a.json()["run_id"] != b.json()["run_id"]
        assert alpha.get(f"/v1/runs/{b.json()['run_id']}").status_code == 404
        assert beta.get(f"/v1/jobs/{a.json()['job_ids'][0]}").status_code == 404
        assert alpha.post("/v1/imports", json=IMPORT, headers={"Idempotency-Key":"shared-key"}).json()["replayed"] is True
    with connection(database_url) as conn:
        events = list(conn.execute("SELECT payload FROM event_outbox"))
        assert {json.loads(event["payload"])["workspace_id"] for event in events} == {"alpha","beta"}
        for row in conn.execute("SELECT j.workspace_id AS owner,r.workspace_id FROM discovery_jobs j JOIN crawl_runs r ON r.id=j.crawl_run_id"):
            assert row["owner"] == row["workspace_id"]


def test_working_set_remains_pinned_when_game_changes_and_pages_are_bounded(database_url: str) -> None:
    with connection(database_url,mode="rw") as conn:
        first, _, _ = seed_game(conn,provider="lichess",game_key="first",white="alice",black="bob")
        second, _, _ = seed_game(conn,provider="lichess",game_key="second",white="alice",black="bob")
        v1, v2 = version(conn,first,"source1"), version(conn,second,"source2")
    body = {"name":"Alice blitz", "filters":{"provider":"lichess","username":"Alice","time_class":"blitz"}, "settings":{"model":"fixture"}}
    with client(database_url) as alpha, client(database_url,"beta") as beta:
        response = alpha.post("/v1/working-sets",json=body,headers={"Idempotency-Key":"set"})
        assert response.status_code == 201, response.text
        saved = response.json()
        set_id = saved["id"]
        assert saved["member_count"] == 2
        assert alpha.post("/v1/working-sets",json=body,headers={"Idempotency-Key":"set"}).json()["replayed"] is True
        assert beta.get(f"/v1/working-sets/{set_id}").status_code == 404
        assert beta.get(f"/v1/working-sets/{set_id}/members").status_code == 404
        page = alpha.get(f"/v1/working-sets/{set_id}/members",params={"limit":1}).json()
        assert page["items"][0]["game_version_id"] == v1
        assert page["next_cursor"] == 1
        tail = alpha.get(f"/v1/working-sets/{set_id}/members",params={"limit":1,"after":page["next_cursor"]}).json()
        assert tail["items"][0]["game_version_id"] == v2 and tail["next_cursor"] is None
        with connection(database_url,mode="rw") as conn:
            version(conn,first,"source3")
        assert alpha.get(f"/v1/working-sets/{set_id}").json()["input_signature"] == saved["input_signature"]
        assert alpha.get(f"/v1/working-sets/{set_id}/members").json()["items"][0]["game_version_id"] == v1
        newer = alpha.post("/v1/working-sets",json=body,headers={"Idempotency-Key":"set-new"}).json()
        assert newer["input_signature"] != saved["input_signature"]


def test_results_require_exact_inputs_configuration_version_and_owner(database_url: str) -> None:
    body = {"name":"empty", "filters":{}, "settings":{"seed":1}}
    with client(database_url) as alpha, client(database_url,"beta") as beta:
        set_id = alpha.post("/v1/working-sets",json=body,headers={"Idempotency-Key":"set"}).json()["id"]
        calculation: dict[str, Any] = {"implementation":"opening-counts","implementation_version":"1", "settings":{"depth":5}, "output":{"games":0}}
        first = alpha.post(f"/v1/working-sets/{set_id}/results",json=calculation)
        assert first.status_code == 201, first.text
        saved = first.json()
        assert alpha.post(f"/v1/working-sets/{set_id}/results",json=calculation).json()["replayed"] is True
        lookup = {key:value for key,value in calculation.items() if key != "output"}
        assert alpha.post(f"/v1/working-sets/{set_id}/results/lookup",json=lookup).json()["id"] == saved["id"]
        assert alpha.post(f"/v1/working-sets/{set_id}/results/lookup",json={**lookup,"implementation_version":"missing"}).status_code == 404
        assert beta.get(f"/v1/results/{saved['id']}").status_code == 404
        assert beta.post(f"/v1/working-sets/{set_id}/results",json=calculation).status_code == 404
        assert alpha.post(f"/v1/working-sets/{set_id}/results",json={**calculation,"output":{"games":99}}).status_code == 409
        changed = alpha.post(f"/v1/working-sets/{set_id}/results",json={**calculation,"implementation_version":"2"}).json()
        assert changed["result_signature"] != saved["result_signature"]
        changed_settings = alpha.post(f"/v1/working-sets/{set_id}/results",json={**calculation,"settings":{"depth":6}}).json()
        assert changed_settings["result_signature"] != saved["result_signature"]


def test_unscoped_player_filter_and_untrusted_settings_rejected(database_url: str) -> None:
    with client(database_url) as alpha:
        assert alpha.post("/v1/working-sets",json={"name":"bad","filters":{"username":"alice"}},headers={"Idempotency-Key":"bad"}).status_code == 422
        assert alpha.post("/v1/working-sets",json={"name":"bad","filters":{"where":"DROP TABLE games"}},headers={"Idempotency-Key":"bad"}).status_code == 422


def test_completed_working_sets_are_immutable_in_storage(database_url: str) -> None:
    import psycopg
    with client(database_url) as alpha:
        set_id = alpha.post("/v1/working-sets",json={"name":"empty","filters":{}},headers={"Idempotency-Key":"immutable"}).json()["id"]
    with connection(database_url,mode="rw") as conn:
        with pytest.raises(psycopg.errors.RaiseException,match="immutable"):
            with transaction(conn):
                conn.execute("UPDATE working_sets SET filters='{}'::jsonb WHERE id=%s", (set_id,))


def test_move_clock_decimal_precision_and_local_only_api(database_url: str,monkeypatch: pytest.MonkeyPatch) -> None:
    from chess_crawl.providers import registry
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Archive APIs must not instantiate provider clients")
    monkeypatch.setattr(registry,"create_provider_client",forbidden)
    with connection(database_url,mode="rw") as conn:
        game_id, _, _ = seed_game(conn,provider="lichess",game_key="clocks",white="alice",black="bob")
        with transaction(conn):
            v = version(conn,game_id,"clocks")
            conn.execute("INSERT INTO game_move_nodes(version_id,node_index,variation_index,ply,is_mainline) VALUES(%s,0,0,0,true)",(v,))
            conn.execute("INSERT INTO game_move_nodes(version_id,node_index,parent_index,variation_index,ply,is_mainline,mover,move_san) VALUES(%s,1,0,0,1,true,'white','e4')",(v,))
            conn.execute("""INSERT INTO game_clock_observations(version_id,node_index,kind,source,source_pointer,raw_text,seconds,unit,precision_seconds,status,reference)
              VALUES(%s,1,'remaining','provider','/clocks/0','299.123456789012345678901',299.123456789012345678901,'seconds',0.000000000000000000001,'parsed','after_move')""", (v,))
    with client(database_url) as alpha:
        response = alpha.get(f"/v1/games/{game_id}/moves")
        assert response.status_code == 200, response.text
        clock = response.json()["items"][0]["clock_observations"][0]
        assert clock["seconds"] == "299.123456789012345678901"
        assert clock["precision_seconds"] == "1E-21"
        assert alpha.post("/v1/working-sets",json={"name":"offline","filters":{}},headers={"Idempotency-Key":"offline"}).status_code == 201


def test_sync_selection_limit_rolls_back_without_truncating(database_url: str) -> None:
    from chess_crawl.application import Limits
    with connection(database_url,mode="rw") as conn:
        for index in range(3):
            game_id,_,_ = seed_game(conn,provider="lichess",game_key=f"bounded-{index}",white="alice",black="bob")
            version(conn,game_id,f"revision-{index}")
    with TestClient(create_app(database_url,workspace_tokens=TOKENS,limits=Limits(max_working_set_members=2)),
                    headers={"Authorization":"Bearer alpha-secret"}) as alpha:
        response = alpha.post("/v1/working-sets",json={"name":"too large","filters":{}},headers={"Idempotency-Key":"large"})
        assert response.status_code == 422,response.text
        assert response.json()["error"]["code"] == "working_set_too_large"
        with connection(database_url) as conn:
            assert require_row(conn.execute("SELECT COUNT(*) FROM working_sets"))[0] == 0
            assert require_row(conn.execute("SELECT COUNT(*) FROM working_set_members"))[0] == 0
            assert require_row(conn.execute("SELECT COUNT(*) FROM working_set_submissions"))[0] == 0
        narrowed = alpha.post("/v1/working-sets",json={"name":"small","filters":{"provider":"chess.com"}},headers={"Idempotency-Key":"large"})
        assert narrowed.status_code == 201 and narrowed.json()["member_count"] == 0


def test_summary_does_not_reveal_other_workspace_activity_or_private_sources(database_url: str) -> None:
    from chess_crawl.providers.base import RawRecord
    from chess_crawl.storage.raw import store_raw_payload
    with connection(database_url,mode="rw") as conn:
        for owner,timestamp in (("public",10),("beta",99)):
            store_raw_payload(conn,RawRecord(provider="lichess",endpoint_type="user_profile",
                request_url="https://lichess.org/api/user/alice",canonical_source_key=f"source/{owner}",
                fetched_at=timestamp,body=b'{}',media_type="application/json",request_params={"owner_scope":owner},owner_scope=owner))
    with client(database_url) as alpha,client(database_url,"beta") as beta:
        alpha.post("/v1/imports",json=IMPORT,headers={"Idempotency-Key":"a"})
        beta.post("/v1/imports",json=IMPORT,headers={"Idempotency-Key":"b"})
        a,b = alpha.get("/v1/summary").json(),beta.get("/v1/summary").json()
        assert a["runs"] == b["runs"] == [{"status":"running","count":1}]
        assert a["jobs"] == b["jobs"] == [{"state":"pending","count":2}]
        assert a["raw_payloads"] == 1 and b["raw_payloads"] == 2
        assert a["freshness"]["last_fetched_at"] == 10 and b["freshness"]["last_fetched_at"] == 99


@pytest.mark.parametrize("mode", ["full", "incremental", "backfill"])
def test_history_import_modes_preserve_optional_dates_and_incremental_watermark(database_url: str, mode: str) -> None:
    with client(database_url) as alpha:
        response = alpha.post("/v1/imports",json={"provider":"lichess","username":"alice","max_games":10,"collection_mode":mode},headers={"Idempotency-Key":f"{mode}-watermark"})
        assert response.status_code == 202,response.text
        params = alpha.get(f"/v1/jobs/{response.json()['job_ids'][1]}").json()["params"]
        assert params["since"] is None and "since_ms" not in params
        assert params["until"] is None and "until_ms" not in params
        explicit = alpha.post("/v1/imports",json={**IMPORT,"collection_mode":mode},headers={"Idempotency-Key":f"{mode}-explicit"})
        assert explicit.status_code == 202,explicit.text
        params = alpha.get(f"/v1/jobs/{explicit.json()['job_ids'][1]}").json()["params"]
        assert params["since_ms"] == IMPORT["since"] * 1000 and params["until_ms"] == IMPORT["until"] * 1000
        assert alpha.post("/v1/imports",json={"provider":"lichess","username":"alice","max_games":10},headers={"Idempotency-Key":"missing"}).status_code == 422


def test_unattributed_successful_fetch_logs_do_not_leak_freshness(initialized_conn:Connection) -> None:
    from chess_crawl.storage.raw import insert_fetch_log
    from chess_crawl.storage.queries import archive_freshness
    insert_fetch_log(initialized_conn,provider="lichess",endpoint_type="user_resource",
                     url="https://lichess.org/api/team/of/alice",attempted_at=999,status_code=200)
    assert archive_freshness(initialized_conn,owner_scope="alpha")["last_checked_at"] is None


def test_raw_catalog_only_exposes_public_and_requested_owner(initialized_conn:Connection) -> None:
    from chess_crawl.providers.base import RawRecord
    from chess_crawl.storage.raw import store_raw_payload
    from chess_crawl.storage.api_views import raw_page
    ids={}
    for owner in ("public","alpha","beta","unassigned:legacy-profile"):
        ids[owner]=store_raw_payload(initialized_conn,RawRecord(provider="lichess",endpoint_type="user_profile",
            request_url="https://lichess.org/api/user/alice",canonical_source_key=f"raw/{owner}",
            fetched_at=1,body=b'{}',owner_scope=owner))
    assert {row["id"] for row in raw_page(initialized_conn,provider="lichess",limit=100,after=0,owner_scope="public")["items"]}=={ids["public"]}
    assert {row["id"] for row in raw_page(initialized_conn,provider="lichess",limit=100,after=0,owner_scope="alpha")["items"]}=={ids["public"],ids["alpha"]}


def test_worker_pool_status_masks_each_cross_workspace_job(database_url:str,monkeypatch:pytest.MonkeyPatch) -> None:
    from chess_crawl.jobs import state
    from chess_crawl.storage.workspaces import worker_snapshot
    with client(database_url) as alpha,client(database_url,"beta") as beta:
        a=alpha.post("/v1/imports",json=IMPORT,headers={"Idempotency-Key":"worker-a"}).json()["job_ids"][0]
        b=beta.post("/v1/imports",json=IMPORT,headers={"Idempotency-Key":"worker-b"}).json()["job_ids"][0]
    snapshot={"alive":True,"current_job_id":b,"workers":[{"worker_id":"a","current_job_id":a},{"worker_id":"b","current_job_id":b}]}
    with connection(database_url) as conn:
        result=worker_snapshot(conn,snapshot,"alpha")
    assert result["alive"] is True and result["current_job_id"] is None
    assert result["workers"][0]["current_job_id"]==a and result["workers"][1]["current_job_id"] is None
    assert snapshot["workers"][1]["current_job_id"]==b
    fields={"alive":True,"status":"running","heartbeat_at":10,"age_seconds":1}
    pooled={**snapshot,**fields,"worker_id":"b","active_workers":2,
            "workers":[{**fields,**worker} for worker in snapshot["workers"]]}
    monkeypatch.setattr(state,"worker_status",lambda conn:pooled)
    with client(database_url) as alpha:
        http=alpha.get("/v1/worker").json()
        assert http["current_job_id"] is None and http["active_workers"]==2
        assert [worker["current_job_id"] for worker in http["workers"]]==[a,None]


def test_working_set_player_alias_tracks_stable_account_after_rename(database_url:str) -> None:
    from chess_crawl.storage.player_profiles import record_alias
    with connection(database_url,mode="rw") as conn,transaction(conn):
        game_id,user_id,_=seed_game(conn,provider="lichess",game_key="before-rename",white="alice",black="bob")
        version(conn,game_id,"renamed-account")
        record_alias(conn,user_id,"Alice",observed_at=1,raw_payload_id=None)
        conn.execute("UPDATE provider_users SET username_normalized='newalice',display_username='NewAlice' WHERE id=%s",(user_id,))
    with client(database_url) as alpha:
        result=alpha.post("/v1/working-sets",json={"name":"Old alias","filters":{"provider":"lichess","username":"alice"}},headers={"Idempotency-Key":"alias"})
        assert result.status_code==201,result.text
        assert result.json()["member_count"]==1
