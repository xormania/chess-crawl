from __future__ import annotations

import json
import threading
from dataclasses import replace

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.jobs import state
from chess_crawl.jobs.budget import BudgetExceeded, BudgetPolicy, QuotaExceeded
from chess_crawl.jobs.dispatch import SqsConsumer
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs.worker import Worker
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.db import connection, operation_lock_held, require_row, transaction
from chess_crawl.storage.raw import store_raw_payload
from chess_crawl.storage.workspaces import submission_context
from chess_crawl.storage.work_budgets import (
    admit_run_budget, get_run_budget, install_workspace_policy, monthly_period, reserve_normalization,
    reserve_request, resume_run_budget, settle_request,
)


NOW = 1704153600


def _budget(conn, run_id: int, workspace: str):
    result = get_run_budget(conn, run_id, workspace, now=NOW)
    assert result is not None
    return result


def _job(conn, job_id: int):
    result = state.get_job(conn, job_id)
    assert result is not None
    return result


def _run(conn, workspace: str, policy: BudgetPolicy, *, kind="normalize_payload", target="1", params=None, priority=100):
    with transaction(conn):
        submission_context(conn, workspace)
        run_id = state.create_crawl_run(conn, provider="lichess", seed_spec=target, params=params or {})
        job_id = state.enqueue_job(conn, provider="lichess", kind=kind, target=target,
                                   params=params, crawl_run_id=run_id, priority=priority).job_id
        budget = admit_run_budget(conn, run_id, workspace, policy, now=NOW)
    return run_id, job_id, int(budget["id"])


def test_budget_policy_is_finite_and_operator_owned(monkeypatch) -> None:
    with pytest.raises(ValueError):
        BudgetPolicy(job_max_remote_bytes=0)
    with pytest.raises(ValueError):
        BudgetPolicy(workspace_max_active_jobs=True)
    monkeypatch.setenv("CHESS_CRAWL_JOB_MAX_GAMES", "25")
    assert BudgetPolicy.from_env().job_max_games == 25


def test_request_reservations_near_cap_settle_once_and_crash_keeps_charge(initialized_conn) -> None:
    policy = BudgetPolicy(job_max_remote_bytes=10, workspace_max_remote_bytes=10, max_response_bytes=10)
    run_id, _, budget_id = _run(initialized_conn, "alpha", policy)
    first, limit = reserve_request(initialized_conn, budget_id, now=NOW)
    assert limit == 5
    settle_request(initialized_conn, first, 6)  # Final bounded read crossed the body limit.
    settle_request(initialized_conn, first, 0)  # Duplicate delivery must not refund twice.
    assert _budget(initialized_conn, run_id, "alpha")["remote_bytes"] == 6
    _, limit = reserve_request(initialized_conn, budget_id, now=NOW)
    assert limit == 2
    with pytest.raises(BudgetExceeded):
        reserve_request(initialized_conn, budget_id, now=NOW)
    budget = _budget(initialized_conn, run_id, "alpha")
    assert budget["remote_bytes"] == 10 and budget["remote_requests"] == 2


def test_workspace_request_race_cannot_overbook_two_independent_runs(database_url: str) -> None:
    policy = BudgetPolicy(workspace_max_remote_requests=1)
    with connection(database_url, mode="rw") as conn:
        budgets = [_run(conn, "alpha", policy)[2] for _ in range(2)]
    barrier = threading.Barrier(2)
    outcomes: list[str] = []
    failures: list[BaseException] = []

    def reserve(identity: int) -> None:
        try:
            with connection(database_url, mode="rw") as conn:
                barrier.wait(5)
                try:
                    reserve_request(conn, identity, now=NOW)
                    outcomes.append("reserved")
                except QuotaExceeded:
                    outcomes.append("quota")
        except BaseException as exc:
            failures.append(exc)

    threads = [threading.Thread(target=reserve, args=(identity,)) for identity in budgets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert failures == [] and sorted(outcomes) == ["quota", "reserved"]
    with connection(database_url, mode="ro") as conn:
        assert require_row(conn.execute("SELECT remote_requests FROM workspace_budget_periods"))[0] == 1


def test_monthly_rollover_does_not_reset_job_lifetime_or_new_key_usage(initialized_conn) -> None:
    policy = BudgetPolicy(job_max_remote_requests=1, workspace_max_remote_requests=1)
    run_id, _, budget_id = _run(initialized_conn, "alpha", policy)
    ticket, _ = reserve_request(initialized_conn, budget_id, now=NOW)
    settle_request(initialized_conn, ticket, 0)
    with pytest.raises(QuotaExceeded):
        _run(initialized_conn, "alpha", policy)
    february = monthly_period(NOW)[1]
    with pytest.raises(BudgetExceeded):
        reserve_request(initialized_conn, budget_id, now=february)
    assert _budget(initialized_conn, run_id, "alpha")["remote_requests"] == 1


def test_game_budget_deduplicates_logical_games_but_bills_repeated_processing(initialized_conn) -> None:
    policy = BudgetPolicy(job_max_games=1, job_max_normalization_units=2)
    run_id, _, budget_id = _run(initialized_conn, "alpha", policy)
    reserve_normalization(initialized_conn, budget_id, game_key="lichess/one", now=NOW)
    reserve_normalization(initialized_conn, budget_id, game_key="lichess/one", now=NOW)
    with pytest.raises(BudgetExceeded):
        reserve_normalization(initialized_conn, budget_id, game_key="lichess/two", now=NOW)
    budget = _budget(initialized_conn, run_id, "alpha")
    assert budget["games"] == 1 and budget["normalization_units"] == 2


def test_admitted_game_quota_rolls_back_and_duplicates_work_at_exhausted_ceilings(initialized_conn) -> None:
    from chess_crawl.storage.work_budgets import reserve_game

    policy = BudgetPolicy(job_max_games=1, workspace_max_games=1, job_max_normalization_units=1)
    run_id, _, budget_id = _run(initialized_conn, "alpha", policy)
    reserve_normalization(initialized_conn, budget_id, now=NOW)
    with pytest.raises(RuntimeError, match="selection write failed"):
        with transaction(initialized_conn):
            assert reserve_game(initialized_conn, budget_id, game_key="lichess/one", now=NOW)
            raise RuntimeError("selection write failed")
    assert (_budget(initialized_conn, run_id, "alpha")["games"],
            _budget(initialized_conn, run_id, "alpha")["normalization_units"]) == (0, 1)
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM budget_game_items"))[0] == 0
    assert require_row(initialized_conn.execute("SELECT games FROM workspace_budget_periods"))[0] == 0
    assert reserve_game(initialized_conn, budget_id, game_key="lichess/one", now=NOW)
    assert not reserve_game(initialized_conn, budget_id, game_key="lichess/one", now=NOW)
    with pytest.raises(BudgetExceeded) as exceeded:
        reserve_game(initialized_conn, budget_id, game_key="lichess/two", now=NOW)
    assert exceeded.value.dimension == "games"
    assert (_budget(initialized_conn, run_id, "alpha")["games"],
            _budget(initialized_conn, run_id, "alpha")["normalization_units"]) == (1, 1)
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM budget_game_items"))[0] == 1
    usage = require_row(initialized_conn.execute("SELECT games,normalization_units FROM workspace_budget_periods"))
    assert (usage["games"], usage["normalization_units"]) == (1, 1)


def test_claim_rotation_and_active_caps_ignore_workspace_flood_priorities(database_url: str) -> None:
    policy = BudgetPolicy(workspace_max_active_jobs=1)
    with connection(database_url, mode="rw") as conn:
        alpha, first, _ = _run(conn, "alpha", policy, priority=1)
        for index in range(8):
            state.enqueue_job(conn, provider="lichess", kind="normalize_payload", target=str(index+2), crawl_run_id=alpha, priority=1)
        _, beta, _ = _run(conn, "beta", policy, priority=100)
    with connection(database_url, mode="rw") as owner, connection(database_url, mode="rw") as other:
        claimed = state.claim_next_job(owner, worker_id="alpha", stage="processing")
        assert claimed is not None and claimed.id == first
        selected = state.claim_next_job(other, worker_id="beta", stage="processing")
        assert selected is not None and selected.id == beta
        state.finish_attempt(owner, first, "done", reason="finished")
        state.release_job_ownership(owner)
        state.finish_attempt(other, beta, "done", reason="finished")
        state.release_job_ownership(other)


def _game(identity: str, created: int) -> dict:
    return {"id": identity, "createdAt": created, "lastMoveAt": created+1,
            "status": "mate", "variant": "standard", "speed": "blitz", "perf": "blitz",
            "players": {"white": {"user": {"id": "target", "name": "Target"}},
                        "black": {"user": {"id": "opponent", "name": "Opponent"}}},
            "winner": "white", "clock": {"initial": 300, "increment": 0}, "moves": "e4 e5"}


@pytest.mark.parametrize("game_ceiling", [10, 1], ids=["sufficient-game-quota", "last-game-quota"])
def test_competing_payloads_charge_only_the_game_admitted_by_run_cap(
    database_url, monkeypatch, game_ceiling,
) -> None:
    from chess_crawl.normalize import games
    from chess_crawl.storage import work_budgets

    monkeypatch.setattr(work_budgets.time, "time", lambda: NOW)
    policy = BudgetPolicy(job_max_games=game_ceiling, workspace_max_games=game_ceiling)
    with connection(database_url, mode="rw") as setup:
        run_id, parent, budget_id = _run(
            setup, "alpha", policy, kind="fetch_user_games", target="target",
            params={"since": NOW-60, "until": NOW+60, "max_games": 1, "limit": 1},
        )
        children = {}
        raw_ids = {}
        for role in ("winner", "loser"):
            raw_ids[role] = store_raw_payload(setup, RawRecord(
                provider="lichess", endpoint_type="user_games_stream",
                request_url=f"https://lichess.org/api/games/user/{role}",
                canonical_source_key=f"lichess/games/user/{role}", fetched_at=NOW,
                body=json.dumps(_game(role, NOW*1000)).encode(), media_type="application/x-ndjson",
            ))
            children[role] = state.enqueue_payload_normalization(
                setup, provider="lichess", raw_payload_id=raw_ids[role], fetch_log_id=None,
                max_games=1, parent_job_id=parent, crawl_run_id=run_id, parser_version=games.PARSER_VERSION,
            ).job_id

    preflight = threading.Barrier(2)
    winner_finished = threading.Event()
    original_bounds = games.run_game_bounds
    outcomes = {}
    failures = []

    def competing_bounds(conn, crawl_run_id, **kwargs):
        bounds = original_bounds(conn, crawl_run_id, **kwargs)
        if not operation_lock_held(conn, "run-game-budget", crawl_run_id, exclusive=True):
            # Both workers observe the last slot before either writes. Keep the
            # loser's stale read until the winner has committed its selection.
            assert bounds.remaining == 1
            preflight.wait(30)
            if threading.current_thread().name == "loser":
                assert winner_finished.wait(45)
        return bounds

    monkeypatch.setattr(games, "run_game_bounds", competing_bounds)

    def process(role):
        try:
            with connection(database_url, mode="rw") as conn:
                outcomes[role] = JobRunner(conn, stage="processing", budget_policy=policy, clock=lambda: NOW).run(
                    crawl_run_id=run_id, job_id=children[role], max_jobs=1,
                )
        except BaseException as error:
            failures.append(error)
        finally:
            if role == "winner":
                winner_finished.set()

    workers = [threading.Thread(target=process, args=(role,), name=role) for role in children]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(60)
    assert all(not worker.is_alive() for worker in workers)
    assert failures == []
    with connection(database_url, mode="rw") as conn:
        assert outcomes.keys() == children.keys()
        assert all(result.done == 1 and result.blocked == result.errors == 0 for result in outcomes.values()), {
            role: (_job(conn, child).state, _job(conn, child).reason) for role, child in children.items()
        }
        assert require_row(conn.execute("SELECT COUNT(*) FROM run_games WHERE crawl_run_id=%s", (run_id,)))[0] == 1
        assert require_row(conn.execute("SELECT provider_game_id FROM games"))[0] == "winner"
        assert require_row(conn.execute("SELECT COUNT(*) FROM budget_game_items WHERE budget_id=%s", (budget_id,)))[0] == 1
        assert _budget(conn, run_id, "alpha")["games"] == 1
        assert _budget(conn, run_id, "alpha")["normalization_units"] == 4  # Two source reads and two interpretations.
        usage = require_row(conn.execute("SELECT games,normalization_units FROM workspace_budget_periods WHERE workspace_id='alpha'"))
        assert (usage["games"], usage["normalization_units"]) == (1, 4)
        assert [_job(conn, child).state for child in children.values()] == ["done", "done"]
        assert require_row(conn.execute("SELECT normalization_status FROM raw_payloads WHERE id=%s", (raw_ids["loser"],)))[0] == "pending"
        no_http = httpx.MockTransport(lambda request: pytest.fail("Satisfied run cap must not fetch another source"))
        completed = JobRunner(conn, stage="acquisition", transport=no_http, budget_policy=policy, clock=lambda: NOW).run(
            crawl_run_id=run_id, job_id=parent, max_jobs=1,
        )
        assert completed.done == 1 and completed.blocked == completed.errors == 0
        assert _job(conn, parent).state == "done"
        run = state.get_run(conn, run_id)
        assert run is not None and run["status"] == "done"


def test_full_history_budget_pause_resume_reuses_proven_pages_without_false_completion(initialized_conn) -> None:
    games = [_game("newest", NOW*1000-1), _game("middle", NOW*1000-2), _game("oldest", NOW*1000-3)]
    calls: list[tuple[int,int]] = []

    def transport(request):
        until, limit = int(request.url.params["until"]), int(request.url.params["max"])
        calls.append((until, limit))
        selected = [game for game in games if game["createdAt"] < until][:limit]
        return httpx.Response(200, content=b"\n".join(json.dumps(game).encode() for game in selected))

    policy = BudgetPolicy(job_max_remote_requests=1, job_max_games=2)
    params = {"collection_mode": "full", "page_size": 2, "until_ms": NOW*1000,
              "max_games": 2, "strategy": "import"}
    run_id, job_id, budget_id = _run(initialized_conn, "alpha", policy, kind="fetch_user_games", target="target", params=params)
    config = Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0)
    acquisition = JobRunner(initialized_conn, stage="acquisition", config=config,
                            transport=httpx.MockTransport(transport), clock=lambda: NOW)
    assert acquisition.run(max_jobs=1).claimed == 1
    assert acquisition.run(max_jobs=1).blocked == 1
    job = state.get_job(initialized_conn, job_id)
    assert job is not None and job.state == "blocked" and (job.reason or "").startswith("budget_exhausted:")
    assert len(calls) == 1
    assert JobRunner(initialized_conn, stage="processing").run(max_jobs=1).done == 1
    assert _budget(initialized_conn, run_id, "alpha")["games"] == 2
    assert acquisition.run(max_jobs=1).claimed == 0
    resume_run_budget(initialized_conn, run_id, "alpha", replace(policy, job_max_remote_requests=3, job_max_games=3))
    assert acquisition.run(max_jobs=1).done == 1
    assert calls[1][0] == games[1]["createdAt"]+1
    assert JobRunner(initialized_conn, stage="processing").run(max_jobs=1).done == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM run_games WHERE crawl_run_id=%s", (run_id,)))[0] == 3
    assert _budget(initialized_conn, run_id, "alpha")["id"] == budget_id


@pytest.mark.parametrize("body", [b"x"*100, b"{malformed"])
def test_http_work_exhaustion_retains_finite_charge_and_never_archives_oversized_body(initialized_conn, body) -> None:
    policy = BudgetPolicy(max_response_bytes=8, job_max_remote_requests=1)
    run_id, job_id, _ = _run(initialized_conn, "alpha", policy, kind="fetch_user_profile", target="target")
    calls: list[int] = []
    def transport(request):
        calls.append(1)
        return httpx.Response(200, content=body)
    runner = JobRunner(initialized_conn, transport=httpx.MockTransport(transport),
                       config=Config(lichess_delay_s=0,max_retries=0), clock=lambda: NOW)
    assert runner.run(max_jobs=1).blocked == 1
    assert (_job(initialized_conn, job_id).reason or "").startswith("budget_exhausted:")
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 0
    state.unblock_jobs(initialized_conn)
    assert runner.run(max_jobs=1).blocked == 1
    assert len(calls) == 1
    assert _budget(initialized_conn, run_id, "alpha")["remote_requests"] == 1


def test_sqs_hot_workspace_hint_cannot_bypass_rotation(database_url: str) -> None:
    class Queue:
        deleted: list[str] = []
        def receive_message(self, **kwargs):
            return {"Messages": [{"Body": json.dumps({"job_id": alpha_second}), "ReceiptHandle": "hot"}]}
        def delete_message(self, **kwargs):
            self.deleted.append(kwargs["ReceiptHandle"])
        def send_message(self, **kwargs):
            return {}

    policy = BudgetPolicy(workspace_max_active_jobs=1)
    with connection(database_url, mode="rw") as conn:
        raw_id = store_raw_payload(conn, RawRecord(
            provider="lichess", endpoint_type="user_profile", request_url="https://lichess.org/api/user/local",
            canonical_source_key="lichess/user/local/profile", body=b'{"id":"local","username":"Local"}', media_type="application/json",
        ))
        alpha, first, _ = _run(conn, "alpha", policy, target=str(raw_id), priority=1)
        alpha_second = state.enqueue_job(conn, provider="lichess", kind="normalize_payload", target=str(raw_id),
                                         params={"distinct": True}, crawl_run_id=alpha, priority=1).job_id
        _, beta, _ = _run(conn, "beta", policy, target=str(raw_id), priority=100)
        assert state.claim_next_job(conn, worker_id="first", job_id=first) is not None
        state.finish_attempt(conn, first, "done", reason="prior turn")
        state.release_job_ownership(conn)
    queue = Queue()
    assert Worker(database_url, queue_consumer=SqsConsumer(queue, "queue", wait_seconds=0)).run(once=True) == 1
    with connection(database_url, mode="ro") as conn:
        assert _job(conn, beta).state == "done"
        assert _job(conn, alpha_second).state == "pending"
    assert queue.deleted == []


def test_workspace_policy_tightening_survives_rejected_admission_and_old_job_policy(initialized_conn) -> None:
    loose = BudgetPolicy(workspace_max_remote_requests=3)
    run_id, _, budget_id = _run(initialized_conn, "alpha", loose)
    ticket, _ = reserve_request(initialized_conn, budget_id, now=NOW)
    settle_request(initialized_conn, ticket, 0)
    strict = replace(loose, workspace_max_remote_requests=1)
    install_workspace_policy(initialized_conn, "alpha", strict, now=NOW)
    with pytest.raises(QuotaExceeded):
        _run(initialized_conn, "alpha", loose)
    with pytest.raises(QuotaExceeded):
        reserve_request(initialized_conn, budget_id, now=NOW)
    snapshot = _budget(initialized_conn, run_id, "alpha")
    assert snapshot["policy"]["workspace_max_remote_requests"] == 3
    assert snapshot["workspace_policy"]["workspace_max_remote_requests"] == 1
    assert snapshot["workspace_period"]["remaining"]["remote_requests"] == 0
    assert snapshot["remote_requests"] == 1
    assert get_run_budget(initialized_conn, run_id, "beta") is None


def test_child_backlog_race_uses_authoritative_run_budget(database_url: str) -> None:
    policy = BudgetPolicy(workspace_max_queued_jobs=1, workspace_max_active_jobs=1)
    with connection(database_url, mode="rw") as conn:
        run_id, parent, _ = _run(conn, "alpha", policy)
    barrier = threading.Barrier(2)
    outcomes: list[str] = []
    failures: list[BaseException] = []
    def enqueue(index: int) -> None:
        try:
            with connection(database_url, mode="rw") as conn:
                barrier.wait(5)
                try:
                    state.enqueue_job(conn, provider="lichess", kind="normalize_payload", target=str(index),
                                      crawl_run_id=run_id, parent_job_id=parent)
                    outcomes.append("inserted")
                except QuotaExceeded:
                    outcomes.append("quota")
        except BaseException as exc:
            failures.append(exc)
    threads = [threading.Thread(target=enqueue, args=(index,)) for index in (2, 3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert failures == [] and sorted(outcomes) == ["inserted", "quota"]
    with connection(database_url, mode="ro") as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_jobs WHERE crawl_run_id=%s", (run_id,)))[0] == 2


def test_bounded_partial_game_budget_resumes_original_response_without_http(initialized_conn) -> None:
    games = [_game("first", NOW*1000-2000), _game("second", NOW*1000-1000)]
    calls: list[int] = []
    def transport(request):
        calls.append(int(request.url.params["max"]))
        return httpx.Response(200, content=b"\n".join(json.dumps(game).encode() for game in games))
    policy = BudgetPolicy(job_max_games=1)
    params = {"collection_mode": "bounded", "max_games": 3, "limit": 3,
              "since": NOW-10, "until": NOW, "strategy": "import"}
    run_id, job_id, _ = _run(initialized_conn, "alpha", policy, kind="fetch_user_games", target="target", params=params)
    runner = JobRunner(initialized_conn, config=Config(lichess_delay_s=0,max_retries=0),
                       transport=httpx.MockTransport(transport), clock=lambda: NOW)
    assert runner.run(max_jobs=1).claimed == 1  # Capture only.
    assert runner.run(max_jobs=1).blocked == 1  # Processing preserves its per-game checkpoint.
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM run_games WHERE crawl_run_id=%s", (run_id,)))[0] == 1
    resume_run_budget(initialized_conn, run_id, "alpha", replace(policy, job_max_games=3))
    assert runner.run(max_jobs=1).done == 1
    assert runner.run(max_jobs=1).done == 1  # Acquisition completes without another request.
    assert calls == [3]  # Remaining run allowance is now 2, but source remains the original max=3 response.
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs WHERE job_id=%s", (job_id,)))[0] == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM run_games WHERE crawl_run_id=%s", (run_id,)))[0] == 2


def test_full_acquisition_backlog_pause_keeps_month_checkpoint_and_no_refetch(initialized_conn) -> None:
    calls: list[str] = []
    def transport(request):
        path = request.url.path
        calls.append(path)
        if path.endswith("/archives"):
            return httpx.Response(200, json={"archives": [f"https://api.chess.com/pub/player/target/games/2020/{month:02d}" for month in range(1,5)]})
        return httpx.Response(200, json={"games": []})
    policy = BudgetPolicy(workspace_max_queued_jobs=1, workspace_max_active_jobs=1)
    params = {"collection_mode": "full", "batch_size": 4, "max_games": 10, "strategy": "import"}
    with transaction(initialized_conn):
        submission_context(initialized_conn, "alpha")
        run_id = state.create_crawl_run(initialized_conn, provider="chess.com", seed_spec="target", params=params)
        job_id = state.enqueue_job(initialized_conn, provider="chess.com", kind="fetch_user_games", target="target", params=params, crawl_run_id=run_id).job_id
        admit_run_budget(initialized_conn, run_id, "alpha", policy, now=NOW)
    acquisition = JobRunner(initialized_conn, stage="acquisition", config=Config(chesscom_delay_s=0,max_retries=0),
                            transport=httpx.MockTransport(transport), clock=lambda: NOW)
    assert acquisition.run(max_jobs=1).blocked == 1
    assert len(calls) == 2  # Inventory and first month; second month's HTTP is never sent.
    assert require_row(initialized_conn.execute("SELECT cursor FROM collection_checkpoints WHERE job_id=%s", (job_id,)))[0]["unit_index"] == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM discovery_jobs WHERE state IN ('pending','in_progress','blocked')"))[0] == 2
    assert JobRunner(initialized_conn, stage="processing").run(max_jobs=1).done == 1
    resume_run_budget(initialized_conn, run_id, "alpha", replace(policy, workspace_max_queued_jobs=4))
    assert acquisition.run(max_jobs=1).done == 1
    assert len(calls) == 5 and len(set(calls)) == 5
    assert JobRunner(initialized_conn, stage="processing").run(max_jobs=3).done == 3


def test_precharged_source_read_preserves_acquired_interval_when_processing_units_exhaust(initialized_conn) -> None:
    game = _game("one", NOW*1000-1)
    calls: list[int] = []
    def transport(request):
        calls.append(1)
        return httpx.Response(200, content=json.dumps(game).encode())
    policy = BudgetPolicy(job_max_normalization_units=1)
    params = {"collection_mode": "full", "page_size": 2, "max_games": 2, "until_ms": NOW*1000, "strategy": "import"}
    run_id, job_id, _ = _run(initialized_conn, "alpha", policy, kind="fetch_user_games", target="target", params=params)
    acquisition = JobRunner(initialized_conn, stage="acquisition", config=Config(lichess_delay_s=0,max_retries=0),
                            transport=httpx.MockTransport(transport), clock=lambda: NOW)
    assert acquisition.run(max_jobs=1).done == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM collection_source_ranges"))[0] == 1
    assert JobRunner(initialized_conn, stage="processing").run(max_jobs=1).blocked == 1
    assert _budget(initialized_conn, run_id, "alpha")["incomplete"] is True
    resume_run_budget(initialized_conn, run_id, "alpha", replace(policy, job_max_normalization_units=5))
    assert JobRunner(initialized_conn, stage="processing").run(max_jobs=1).done == 1
    assert calls == [1] and _job(initialized_conn, job_id).state == "done"


def test_oversized_retained_payload_blocks_before_decode(initialized_conn, monkeypatch) -> None:
    from chess_crawl.storage import raw
    raw_id = store_raw_payload(initialized_conn, RawRecord(
        provider="lichess", endpoint_type="user_profile", request_url="https://lichess.org/api/user/legacy",
        canonical_source_key="lichess/user/legacy/profile", body=b"x"*100, media_type="application/json",
    ))
    policy = BudgetPolicy(max_response_bytes=8)
    run_id, job_id, _ = _run(initialized_conn, "alpha", policy, target=str(raw_id))
    def forbidden_decode(*args):
        raise AssertionError("Oversized preserved source was decoded")
    monkeypatch.setattr(raw, "_decode_body", forbidden_decode)
    assert JobRunner(initialized_conn, stage="processing").run(max_jobs=1).blocked == 1
    assert "processing_payload_bytes" in (_job(initialized_conn, job_id).reason or "")
    assert _budget(initialized_conn, run_id, "alpha")["normalization_units"] == 1


@pytest.mark.parametrize("unit", ["2024/01", "2023/12"])
def test_crash_between_enqueue_and_coverage_keeps_original_occurrence_job(initialized_conn, monkeypatch, unit) -> None:
    from chess_crawl.providers.base import ProviderRequestStopped
    from chess_crawl.storage import collection
    policy = BudgetPolicy(workspace_max_queued_jobs=1, workspace_max_active_jobs=1)
    params = {"collection_mode": "full", "batch_size": 1, "strategy": "import"}
    calls: list[str] = []
    def provider(request):
        calls.append(str(request.url))
        body = {"archives": [f"https://api.chess.com/pub/player/target/games/{unit}"]} if str(request.url).endswith("/archives") else {"games": []}
        return httpx.Response(200, json=body)
    with transaction(initialized_conn):
        submission_context(initialized_conn, "alpha")
        run_id = state.create_crawl_run(initialized_conn, provider="chess.com", seed_spec="target", params=params)
        job_id = state.enqueue_job(initialized_conn, provider="chess.com", kind="fetch_user_games", target="target", params=params, crawl_run_id=run_id).job_id
        admit_run_budget(initialized_conn, run_id, "alpha", policy, now=NOW)
    runner = JobRunner(initialized_conn, stage="acquisition", config=Config(chesscom_delay_s=0,max_retries=0),
                       transport=httpx.MockTransport(provider), clock=lambda: NOW)
    original = collection.record_coverage
    stopped = False
    def interrupt(*args, **kwargs):
        nonlocal stopped
        if kwargs.get("state") == "complete" and kwargs.get("unit") == unit and not stopped:
            stopped = True
            raise ProviderRequestStopped("Interrupted after child enqueue before checkpoint")
        return original(*args, **kwargs)
    monkeypatch.setattr(collection, "record_coverage", interrupt)
    assert runner.run(max_jobs=1).claimed == 1
    before = require_row(initialized_conn.execute("SELECT id,params_json FROM discovery_jobs WHERE kind='normalize_payload'"))
    assert runner.run(max_jobs=1).done == 1
    after = initialized_conn.execute("SELECT id,params_json FROM discovery_jobs WHERE kind='normalize_payload'").fetchall()
    assert len(after) == 1 and after[0]["id"] == before["id"]
    assert len(calls) == 2
    occurrence = json.loads(before["params_json"])["fetch_log_id"]
    assert occurrence == require_row(initialized_conn.execute("SELECT id FROM fetch_logs WHERE job_id=%s AND endpoint_type='monthly_archive'", (job_id,)))[0]


def test_offline_upgrade_lifetime_and_workspace_quotas_stop_before_next_decode(initialized_conn, monkeypatch) -> None:
    from chess_crawl.storage import raw
    raws = [store_raw_payload(initialized_conn, RawRecord(
        provider="lichess", endpoint_type="user_profile", request_url=f"https://lichess.org/api/user/{name}",
        canonical_source_key=f"lichess/user/{name}/profile", body=json.dumps({"id":name,"username":name.title()}).encode(),
        media_type="application/json",
    )) for name in ("first", "second")]
    policy = BudgetPolicy(job_max_normalization_units=1, workspace_max_normalization_units=1)
    params = {"upgrade_id":"owned-upgrade", "owner_scope":"alpha", "batch_size":1}
    run_id, job_id, _ = _run(initialized_conn, "alpha", policy, kind="reprocess_archive", target="owned-upgrade", params=params)
    decodes: list[int] = []
    original = raw._decode_body
    def decode(body, compression):
        decodes.append(1)
        return original(body, compression)
    monkeypatch.setattr(raw, "_decode_body", decode)
    runner = JobRunner(initialized_conn, stage="processing", clock=lambda: NOW)
    assert runner.run(max_jobs=2).blocked == 1
    upgrade = require_row(initialized_conn.execute("SELECT * FROM data_upgrades WHERE id='owned-upgrade'"))
    assert upgrade["last_raw_id"] == raws[0] and upgrade["state"] != "done" and upgrade["processed"] == 1
    assert decodes == [1]  # Metadata dispatch does not read/decode the source a second time.
    assert "normalization_units" in (_job(initialized_conn, job_id).reason or "")
    with pytest.raises(QuotaExceeded):
        with transaction(initialized_conn):
            submission_context(initialized_conn, "alpha")
            new_run = state.create_crawl_run(initialized_conn, provider="lichess", seed_spec="new-key", params=params)
            admit_run_budget(initialized_conn, new_run, "alpha", policy)
    resume_run_budget(initialized_conn, run_id, "alpha", replace(policy, job_max_normalization_units=2, workspace_max_normalization_units=2))
    assert runner.run(max_jobs=2).done == 1
    assert len(decodes) == 2
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 0
    assert require_row(initialized_conn.execute("SELECT state FROM data_upgrades WHERE id='owned-upgrade'"))[0] == "done"


def test_atomic_graph_fanout_rejection_keeps_games_and_resumes_without_fetch(initialized_conn) -> None:
    games = [_game("one", NOW*1000-2000), _game("two", NOW*1000-1000)]
    for index, game in enumerate(games):
        game["players"]["black"]["user"] = {"id":f"opponent{index}", "name":f"Opponent{index}"}
    calls: list[int] = []
    def provider(request):
        calls.append(1)
        return httpx.Response(200, content=b"\n".join(json.dumps(game).encode() for game in games))
    policy = BudgetPolicy(workspace_max_queued_jobs=1, workspace_max_active_jobs=1)
    params = {"strategy":"opponents", "since":NOW-10, "until":NOW, "max_depth":1,
              "max_users":10, "max_games":3, "max_jobs":100}
    run_id, job_id, _ = _run(initialized_conn, "alpha", policy, kind="crawl_opponents", target="target", params=params)
    runner = JobRunner(initialized_conn, config=Config(lichess_delay_s=0,max_retries=0),
                       transport=httpx.MockTransport(provider), clock=lambda: NOW)
    assert runner.run(max_jobs=1).claimed == 1  # Capture.
    assert runner.run(max_jobs=1).done == 1  # Normalize.
    assert runner.run(max_jobs=1).done == 1  # Complete acquisition and enqueue expansion.
    assert runner.run(max_jobs=1).blocked == 1  # Fanout is atomic processing.
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM run_games WHERE crawl_run_id=%s", (run_id,)))[0] == 2
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM discovery_edges WHERE crawl_run_id=%s", (run_id,)))[0] == 0
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM discovery_jobs WHERE crawl_run_id=%s", (run_id,)))[0] == 3
    resume_run_budget(initialized_conn, run_id, "alpha", replace(policy, workspace_max_queued_jobs=3))
    assert runner.run(max_jobs=1).done == 1
    assert calls == [1]
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM discovery_edges WHERE crawl_run_id=%s", (run_id,)))[0] == 2
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM discovery_jobs WHERE parent_job_id=%s AND kind='crawl_opponents'", (job_id,)))[0] == 2
