"""Trusted admission caps bind whole runs and cannot be reset by HTTP callers."""
from __future__ import annotations

from dataclasses import asdict, replace
import threading
import json
import time
from typing import Any

from fastapi.testclient import TestClient
import httpx
import pytest

from chess_crawl.api import create_app
from chess_crawl.config import Config
from chess_crawl.jobs import state
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.providers import registry
from chess_crawl.storage.db import connection, require_row, transaction
from test_working_sets import TOKENS


FULL = {"provider":"lichess", "username":"alice", "max_games":2, "collection_mode":"full"}


def client(archive: str, policy: BudgetPolicy, owner: str = "alpha") -> TestClient:
    return TestClient(create_app(archive, workspace_tokens=TOKENS, budget_policy=policy),
                      headers={"Authorization":f"Bearer {TOKENS[owner]}"})


def test_policy_is_server_only_and_pinned_for_run_children(database_url: str) -> None:
    policy = replace(BudgetPolicy(), job_max_games=3, job_max_remote_requests=5)
    with client(database_url, policy) as api, client(database_url, policy, "beta") as beta:
        spoof = api.post("/v1/imports", json={**FULL,"job_max_games":2**62}, headers={"Idempotency-Key":"spoof"})
        assert spoof.status_code == 422
        accepted = api.post("/v1/imports", json=FULL, headers={"Idempotency-Key":"run"})
        assert accepted.status_code == 202, accepted.text
        run_id, jobs = accepted.json()["run_id"], accepted.json()["job_ids"]
        budget = api.get(f"/v1/runs/{run_id}/budget").json()
        assert budget["policy"] == asdict(policy)
        assert api.get(f"/v1/runs/{run_id}").json()["budget"]["id"] == budget["id"]
        assert {api.get(f"/v1/jobs/{job}").json()["budget"]["id"] for job in jobs} == {budget["id"]}
        assert beta.get(f"/v1/runs/{run_id}/budget").status_code == 404
        with connection(database_url, mode="rw") as conn:
            child = state.enqueue_job(conn, provider="lichess", kind="normalize_payload", target="1",
                                      parent_job_id=jobs[-1]).job_id
        assert api.get(f"/v1/jobs/{child}").json()["budget"]["id"] == budget["id"]
    with client(database_url, replace(policy, job_max_games=1)) as changed_server:
        replay = changed_server.post("/v1/imports", json=FULL, headers={"Idempotency-Key":"run"})
        assert replay.status_code == 202 and replay.json()["replayed"]
        assert changed_server.get(f"/v1/runs/{run_id}/budget").json()["policy"] == asdict(policy)


def test_concurrent_admission_rejects_excess_backlog_atomically(database_url: str, monkeypatch) -> None:
    policy = replace(BudgetPolicy(), workspace_max_queued_jobs=1, workspace_max_active_jobs=1)
    monkeypatch.setattr(registry, "create_provider_client", lambda *args, **kwargs: pytest.fail("HTTP admission must not fetch"))
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str,int]] = []
    failures: list[BaseException] = []

    def submit(key: str) -> None:
        try:
            with client(database_url, policy) as api:
                barrier.wait(10)
                response = api.post("/v1/imports", json=FULL, headers={"Idempotency-Key":key})
                outcomes.append((key,response.status_code))
                if response.status_code == 429:
                    error = response.json()["error"]
                    assert error["code"] == "workspace_quota_exceeded"
                    assert error["quota"]["dimension"] == "queued_jobs"
        except BaseException as exc:
            failures.append(exc)

    threads = [threading.Thread(target=submit,args=(key,)) for key in ("one","two")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(15)
    assert not any(thread.is_alive() for thread in threads)
    assert failures == []
    assert sorted(status for _,status in outcomes) == [202,429]
    with connection(database_url) as conn:
        for table,expected in (("crawl_runs",1),("discovery_jobs",2),("application_submissions",1),("work_budgets",1)):
            # The relation names are fixed test constants, never request input.
            assert require_row(conn.execute(f"SELECT COUNT(*) FROM {table}"))[0] == expected
    accepted_key = next(key for key,status in outcomes if status == 202)
    with client(database_url, policy) as api, client(database_url, policy, "beta") as beta:
        assert api.post("/v1/imports",json=FULL,headers={"Idempotency-Key":accepted_key}).json()["replayed"]
        assert beta.post("/v1/imports",json=FULL,headers={"Idempotency-Key":"independent"}).status_code == 202


def test_new_keys_do_not_reset_monthly_workspace_usage(database_url: str) -> None:
    policy = replace(BudgetPolicy(), workspace_max_remote_requests=1)
    with client(database_url, policy) as api, client(database_url, policy, "beta") as beta:
        first = api.post("/v1/imports",json=FULL,headers={"Idempotency-Key":"first"})
        assert first.status_code == 202
        with connection(database_url, mode="rw") as conn, transaction(conn):
            conn.execute("UPDATE workspace_budget_periods SET remote_requests=1 WHERE workspace_id='alpha'")
        replay = api.post("/v1/imports",json=FULL,headers={"Idempotency-Key":"first"})
        assert replay.status_code == 202 and replay.json()["replayed"]
        for key in ("second","third"):
            rejected = api.post("/v1/imports",json=FULL,headers={"Idempotency-Key":key})
            assert rejected.status_code == 429
            quota = rejected.json()["error"]["quota"]
            assert quota["dimension"] == "remote_requests" and quota["remaining"] == 0
            assert quota["reset_at"] > 0
        assert beta.post("/v1/imports",json=FULL,headers={"Idempotency-Key":"first"}).status_code == 202
        with connection(database_url) as conn:
            assert require_row(conn.execute("SELECT COUNT(*) FROM crawl_runs WHERE workspace_id='alpha'"))[0] == 1
            assert require_row(conn.execute("SELECT remote_requests FROM workspace_budget_periods WHERE workspace_id='alpha'"))[0] == 1


def test_operator_lowering_survives_denied_admission_and_cannot_be_raised_by_new_requests(database_url: str) -> None:
    original = replace(BudgetPolicy(), workspace_max_remote_requests=5)
    with client(database_url, original) as api:
        first = api.post("/v1/imports", json=FULL, headers={"Idempotency-Key": "original"})
        assert first.status_code == 202
        run_id = first.json()["run_id"]
    with connection(database_url, mode="rw") as conn, transaction(conn):
        conn.execute("UPDATE workspace_budget_periods SET remote_requests=1 WHERE workspace_id='alpha'")
    lowered = replace(original, workspace_max_remote_requests=1)
    with client(database_url, lowered) as api:
        assert api.post("/v1/imports", json=FULL, headers={"Idempotency-Key": "denied"}).status_code == 429
        assert api.post("/v1/imports", json=FULL, headers={"Idempotency-Key": "original"}).json()["replayed"]
        # The accepted run's lifetime policy is immutable across a replay.
        assert api.get(f"/v1/runs/{run_id}/budget").json()["policy"] == asdict(original)
    with client(database_url, original) as stale_server:
        assert stale_server.post("/v1/imports", json=FULL, headers={"Idempotency-Key": "new-key"}).status_code == 429
    with connection(database_url) as conn:
        assert require_row(conn.execute("SELECT policy->>'workspace_max_remote_requests' FROM workspace_budget_policies WHERE workspace_id='alpha'"))[0] == "1"
        assert require_row(conn.execute("SELECT remote_requests FROM workspace_budget_periods WHERE workspace_id='alpha'"))[0] == 1
        assert require_row(conn.execute("SELECT COUNT(*) FROM crawl_runs WHERE workspace_id='alpha'"))[0] == 1


def test_api_import_worker_pause_and_operator_resume_preserve_complete_history(database_url: str, monkeypatch, capsys) -> None:
    from chess_crawl.jobs.budget import main as administer
    from chess_crawl.storage.collection import checkpoint

    now = int(time.time())
    games: list[dict[str, Any]] = [{"id": identity, "createdAt": now*1000-index, "lastMoveAt": now*1000,
              "status": "mate", "variant": "standard", "speed": "blitz", "perf": "blitz",
              "players": {"white": {"user": {"id": "alice", "name": "Alice"}},
                          "black": {"user": {"id": "bob", "name": "Bob"}}},
              "winner": "white", "clock": {"initial": 300, "increment": 0}, "moves": "e4 e5"}
             for index, identity in enumerate(("newest01", "middle01", "oldest01"), 1)]
    requests: list[tuple[int, int]] = []

    def transport(request):
        if request.url.path == "/api/user/alice":
            return httpx.Response(200, json={"id": "alice", "username": "Alice"})
        assert request.url.path == "/api/games/user/alice"
        until, size = int(request.url.params["until"]), int(request.url.params["max"])
        requests.append((until, size))
        selected = [game for game in games if game["createdAt"] < until][:size]
        return httpx.Response(200, content=b"\n".join(json.dumps(game).encode() for game in selected))

    policy = replace(BudgetPolicy(), job_max_remote_requests=2, job_max_games=2)
    with client(database_url, policy) as api:
        accepted = api.post("/v1/imports", json={**FULL, "until": now}, headers={"Idempotency-Key": "history"})
        assert accepted.status_code == 202, accepted.text
        run_id, jobs = accepted.json()["run_id"], accepted.json()["job_ids"]
        budget_id = api.get(f"/v1/runs/{run_id}/budget").json()["id"]
        with connection(database_url, mode="rw") as conn:
            acquisition = JobRunner(conn, stage="acquisition", clock=lambda: now,
                                    config=Config(lichess_delay_s=0, max_retries=0), transport=httpx.MockTransport(transport))
            assert acquisition.run(crawl_run_id=run_id, max_jobs=3).blocked == 1
            assert len(requests) == 1
            assert JobRunner(conn, stage="processing", clock=lambda: now).run(crawl_run_id=run_id, max_jobs=3).done == 1
            cursor = checkpoint(conn, jobs[-1])
            assert cursor is not None and not cursor.get("done")
            retained_cursor = dict(cursor)
        paused = api.get(f"/v1/runs/{run_id}").json()
        assert paused["status"] == "paused"
        assert paused["budget"]["games"] == 2 and paused["budget"]["remote_requests"] == 2
        stored_games = api.get("/v1/users/lichess/alice/games").json()
        assert len(stored_games["items"]) == 2 and stored_games["next_cursor"] is None
        assert api.get(f"/v1/jobs/{jobs[-1]}").json()["reason"].startswith("budget_exhausted:")
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_REMOTE_REQUESTS", "3")
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_GAMES", "3")
    assert administer(["resume", "--run-id", str(run_id), "--database-url", database_url]) == 0
    assert json.loads(capsys.readouterr().out)["budget"]["remote_requests"] == 2
    with connection(database_url, mode="rw") as conn:
        assert checkpoint(conn, jobs[-1]) == retained_cursor
        acquisition = JobRunner(conn, stage="acquisition", clock=lambda: now,
                                config=Config(lichess_delay_s=0, max_retries=0), transport=httpx.MockTransport(transport))
        assert acquisition.run(crawl_run_id=run_id, max_jobs=1).done == 1
        assert JobRunner(conn, stage="processing", clock=lambda: now).run(crawl_run_id=run_id, max_jobs=1).done == 1
        completed_cursor = checkpoint(conn, jobs[-1])
        assert completed_cursor is not None and completed_cursor["done"]
    assert len(requests) == 2 and requests[1][0] == games[1]["createdAt"]+1
    with client(database_url, policy) as api:
        completed = api.get(f"/v1/runs/{run_id}").json()
        assert completed["status"] == "done"
        assert completed["budget"]["id"] == budget_id and completed["budget"]["games"] == 3
        assert completed["budget"]["remote_requests"] == 3
        stored_games = api.get("/v1/users/lichess/alice/games").json()
        assert len(stored_games["items"]) == 3 and stored_games["next_cursor"] is None


def test_whole_archive_upgrade_is_budgeted_before_decode_and_resumes_without_reprocessing(database_url: str, monkeypatch, capsys) -> None:
    from chess_crawl.jobs.budget import main as administer
    from chess_crawl.providers.base import RawRecord
    from chess_crawl.storage import raw

    with connection(database_url, mode="rw") as conn:
        for name, scope in (("first", "public"), ("second", "public"), ("private", "beta")):
            raw.store_raw_payload(conn, RawRecord(
                provider="lichess", endpoint_type="user_profile", canonical_source_key=f"lichess/user/{name}/profile",
                request_url=f"https://lichess.org/api/user/{name}", media_type="application/json",
                body=json.dumps({"id": name, "username": name.title()}).encode(), owner_scope=scope,
            ))
    original_decode = raw._decode_body
    decoded: list[bytes] = []

    def tracked_decode(body, compression):
        decoded.append(bytes(body))
        return original_decode(body, compression)

    monkeypatch.setattr(raw, "_decode_body", tracked_decode)
    monkeypatch.setattr(registry, "create_provider_client", lambda *args, **kwargs: pytest.fail("offline upgrade must not fetch"))
    policy = replace(BudgetPolicy(), job_max_normalization_units=1, workspace_max_normalization_units=1)
    request = {"provider": "lichess", "name": "all-retained-sources", "batch_size": 100}
    with client(database_url, policy) as api, client(database_url, policy, "beta") as beta:
        accepted = api.post("/v1/upgrades", json=request, headers={"Idempotency-Key": "upgrade"})
        assert accepted.status_code == 202, accepted.text
        run_id, job_id = accepted.json()["run_id"], accepted.json()["job_ids"][0]
        with connection(database_url, mode="rw") as conn:
            assert JobRunner(conn, stage="processing").run(crawl_run_id=run_id, max_jobs=1).blocked == 1
        assert len(decoded) == 1  # No object read/decompression occurs beyond the CPU quota.
        progress = api.get(f"/v1/upgrades/{job_id}").json()
        assert progress["state"] == "running" and progress["progress"]["processed"] == 1
        assert progress["progress"]["last_raw_id"] == 1 and progress["progress"]["high_water_raw_id"] == 2
        budget = api.get(f"/v1/runs/{run_id}/budget").json()
        assert budget["normalization_units"] == 1 and budget["remote_requests"] == 0 and budget["incomplete"]
        assert beta.get(f"/v1/upgrades/{job_id}").status_code == 404
        assert api.post("/v1/upgrades", json=request, headers={"Idempotency-Key": "upgrade"}).json()["replayed"]
        denied = api.post("/v1/upgrades", json=request, headers={"Idempotency-Key": "another-key"})
        assert denied.status_code == 429 and denied.json()["error"]["quota"]["dimension"] == "normalization_units"
        with connection(database_url) as conn:
            assert require_row(conn.execute("SELECT COUNT(*) FROM data_upgrades"))[0] == 1
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_NORMALIZATION_UNITS", "2")
    monkeypatch.setenv("CHESS_CRAWL_WORKSPACE_MAX_NORMALIZATION_UNITS", "2")
    assert administer(["resume", "--run-id", str(run_id), "--database-url", database_url]) == 0
    assert json.loads(capsys.readouterr().out)["budget"]["normalization_units"] == 1
    with connection(database_url, mode="rw") as conn:
        assert JobRunner(conn, stage="processing").run(crawl_run_id=run_id, max_jobs=1).done == 1
    assert len(decoded) == 2  # Resume interprets only the remaining public source.
    with client(database_url, policy) as api:
        progress = api.get(f"/v1/upgrades/{job_id}").json()
        assert progress["state"] == "done" and progress["progress"]["processed"] == 2
        budget = api.get(f"/v1/runs/{run_id}/budget").json()
        assert budget["normalization_units"] == 2 and budget["remote_requests"] == 0 and not budget["incomplete"]


@pytest.mark.parametrize(("path","body"),[
    ("imports",FULL),
    ("crawls",{"provider":"lichess","username":"alice","since":1,"until":2,"max_games":2,"max_depth":1,"max_users":2,"max_jobs":3}),
    ("resources",{"provider":"lichess","username":"alice","resource_key":"activity"}),
    ("upgrades",{"provider":"lichess","name":"bounded-replay"}),
    ("profiles/refresh",{"provider":"chess.com","username":"alice"}),
    ("stats/refresh",{"provider":"chess.com","username":"alice"}),
    ("games/collect",{"provider":"lichess","game_id":"abcdefgh"}),
])
def test_every_queued_api_operation_uses_the_trusted_policy(database_url: str, path: str, body: dict) -> None:
    policy = replace(BudgetPolicy(), job_max_games=7)
    with client(database_url, policy) as api:
        response = api.post(f"/v1/{path}",json=body,headers={"Idempotency-Key":"operation"})
        assert response.status_code == 202,response.text
        budget = api.get(f"/v1/runs/{response.json()['run_id']}/budget").json()
        assert budget["policy"] == asdict(policy)
        for job in response.json()["job_ids"]:
            assert api.get(f"/v1/jobs/{job}").json()["budget"]["id"] == budget["id"]


def test_invalid_operator_policy_fails_before_storage(monkeypatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_GAMES", "0")
    with pytest.raises(ValueError,match="positive PostgreSQL bigint"):
        create_app("postgresql://postgres@127.0.0.1/unused",workspace_tokens=TOKENS)
