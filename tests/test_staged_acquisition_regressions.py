from __future__ import annotations

import json
import threading
from copy import deepcopy
from datetime import UTC, datetime

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import replay_raw_payload
from chess_crawl.jobs import state
from chess_crawl.jobs.discovery import CrawlBounds, create_opponent_crawl, enqueue_opponent_children
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.normalize.games import PARSER_VERSION, normalize_games_payload
from chess_crawl.storage.acquisition import associate_run_game
from chess_crawl.storage.db import connection, require_row, transaction
from chess_crawl.storage.discovery import OpponentEdge
from chess_crawl.storage.raw import raw_payload_metadata
from support import seed_game


SINCE = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp())
UNTIL = int(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
CONFIG = Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0)


def _job(conn, job_id):
    job = state.get_job(conn, job_id)
    assert job is not None
    return job


def _frontier(conn, *, full_caps=False):
    capacity = 2 if full_caps else 10
    run, root = create_opponent_crawl(
        conn, provider="lichess", username="a", since=SINCE, until=UNTIL,
        bounds=CrawlBounds(max_depth=3, max_users=capacity, max_games=100, max_jobs=capacity),
    )
    params = state.load_params(_job(conn, root).params_json)
    deep = state.enqueue_job(conn, provider="lichess", kind="crawl_opponents", target="e",
                             params=params, crawl_run_id=run, parent_job_id=root, depth=3).job_id
    game, _, opponent = seed_game(conn, provider="lichess", game_key="d-e", white="d", black="e")
    return run, root, deep, params, OpponentEdge(opponent, "e", game, 1)


@pytest.mark.parametrize("full_caps", [False, True])
@pytest.mark.parametrize("terminal", [False, True], ids=["pending", "done"])
def test_shallower_existing_frontier_is_promoted_without_new_acquisition(initialized_conn, full_caps, terminal) -> None:
    conn = initialized_conn
    run, root, deep, params, edge = _frontier(conn, full_caps=full_caps)
    if terminal:
        state.mark_done(conn, deep)
        old = state.enqueue_opponent_expansion(conn, _job(conn, deep)).job_id
        state.mark_done(conn, old)
    assert enqueue_opponent_children(conn, crawl_run_id=run, parent_job_id=root, provider="lichess",
                                     params=params, next_depth=2, edges=[edge]) == 0
    assert _job(conn, deep).depth == 2
    assert state.known_crawl_depth(conn, crawl_run_id=run, provider="lichess", username="e") == 2
    assert state.crawl_user_count(conn, run) == state.acquisition_job_count(conn, run) == 2
    promotions = conn.execute("SELECT id,depth FROM discovery_jobs WHERE kind='expand_opponents' AND depth=2").fetchall()
    assert len(promotions) == 1
    assert enqueue_opponent_children(conn, crawl_run_id=run, parent_job_id=root, provider="lichess",
                                     params=params, next_depth=2, edges=[edge]) == 0
    assert len(conn.execute("SELECT id FROM discovery_jobs WHERE kind='expand_opponents' AND depth=2").fetchall()) == 1


def test_full_frontier_caps_do_not_skip_later_known_promotions(initialized_conn) -> None:
    conn = initialized_conn
    run, root, deep, params, edge = _frontier(conn, full_caps=True)
    game, _, unknown = seed_game(conn, provider="lichess", game_key="d-unknown", white="d", black="unknown")
    assert enqueue_opponent_children(conn, crawl_run_id=run, parent_job_id=root, provider="lichess",
                                     params=params, next_depth=2, edges=[OpponentEdge(unknown, "unknown", game, 1), edge]) == 0
    assert _job(conn, deep).depth == 2
    assert state.crawl_user_count(conn, run) == 2


@pytest.mark.parametrize("terminal", ["error", "skipped"])
def test_failed_acquisition_promotion_does_not_leave_unrunnable_expansion(initialized_conn, terminal) -> None:
    conn = initialized_conn
    run, root, deep, params, edge = _frontier(conn)
    state.mark_job(conn, deep, terminal, reason="fixture acquisition failed")
    enqueue_opponent_children(conn, crawl_run_id=run, parent_job_id=root, provider="lichess",
                              params=params, next_depth=2, edges=[edge])
    assert _job(conn, deep).depth == 2 and _job(conn, deep).state == terminal
    assert require_row(conn.execute("SELECT COUNT(*) FROM discovery_jobs WHERE kind='expand_opponents'"))[0] == 0


@pytest.mark.parametrize("outcome", ["done", "error", "skipped"])
def test_active_frontier_promotion_does_not_wait_for_owners_shared_row_lock(database_url, outcome) -> None:
    with connection(database_url, mode="rw") as owner, connection(database_url, mode="rw") as promoter:
        run, root, deep, params, edge = _frontier(owner, full_caps=True)
        claimed = state.claim_next_job(owner, job_id=deep, stage="acquisition", worker_id="active-owner")
        assert claimed is not None
        try:
            promoter.execute("SET statement_timeout=5000")
            # The active executor's fenced transaction holds FOR SHARE on its
            # own row. A competing depth UPDATE would wait here or deadlock.
            with transaction(owner):
                assert enqueue_opponent_children(promoter, crawl_run_id=run, parent_job_id=root, provider="lichess",
                                                 params=params, next_depth=2, edges=[edge]) == 0
                assert _job(promoter, deep).depth == 3
                assert state.known_crawl_depth(promoter, crawl_run_id=run, provider="lichess", username="e") == 2
            state.finish_attempt(owner, deep, outcome, reason="fixture capture completed")
            assert _job(owner, deep).depth == 2
        finally:
            state.release_job_ownership(owner)
        assert state.acquisition_job_count(owner, run) == 2
        if outcome != "done":
            assert JobRunner(promoter, stage="processing").run(max_jobs=1).skipped == 1
            assert require_row(promoter.execute("SELECT COUNT(*) FROM discovery_jobs WHERE kind='expand_opponents' AND state='pending'"))[0] == 0


def test_terminal_frontier_is_reexpanded_at_shallower_depth_from_retained_evidence(initialized_conn) -> None:
    conn = initialized_conn
    run, root, deep, params, edge = _frontier(conn)
    game, _, _ = seed_game(conn, provider="lichess", game_key="e-f", white="e", black="f")
    associate_run_game(conn, run, game)
    state.mark_done(conn, deep)
    old = state.enqueue_opponent_expansion(conn, _job(conn, deep)).job_id
    assert JobRunner(conn, stage="processing").run(max_jobs=1).done == 1
    assert _job(conn, old).state == "done"
    assert state.known_crawl_depth(conn, crawl_run_id=run, provider="lichess", username="f") is None
    enqueue_opponent_children(conn, crawl_run_id=run, parent_job_id=root, provider="lichess",
                              params=params, next_depth=2, edges=[edge])
    no_http = httpx.MockTransport(lambda request: pytest.fail("frontier promotion must reuse retained evidence"))
    assert JobRunner(conn, stage="processing", transport=no_http).run(max_jobs=1).done == 1
    assert state.known_crawl_depth(conn, crawl_run_id=run, provider="lichess", username="f") == 3
    assert state.acquisition_job_count(conn, run) == 3


def test_promotion_during_active_expansion_keeps_a_new_shallower_pass(database_url, monkeypatch) -> None:
    from chess_crawl.jobs import discovery

    started, resume = threading.Event(), threading.Event()
    failures = []
    original = discovery.expand_opponent_frontier

    def paused(conn, job):
        started.set()
        if not resume.wait(30):
            raise RuntimeError("Promotion did not release the expansion fixture")
        return original(conn, job)

    with connection(database_url, mode="rw") as promoter:
        run, root, deep, params, edge = _frontier(promoter)
        game, _, _ = seed_game(promoter, provider="lichess", game_key="e-f", white="e", black="f")
        associate_run_game(promoter, run, game)
        state.mark_done(promoter, deep)
        old = state.enqueue_opponent_expansion(promoter, _job(promoter, deep)).job_id
        monkeypatch.setattr(discovery, "expand_opponent_frontier", paused)

        def expand():
            try:
                with connection(database_url, mode="rw") as owner:
                    assert JobRunner(owner, stage="processing").run(max_jobs=1).done == 1
            except BaseException as error:
                failures.append(error)

        worker = threading.Thread(target=expand)
        worker.start()
        try:
            assert started.wait(30)
            enqueue_opponent_children(promoter, crawl_run_id=run, parent_job_id=root, provider="lichess",
                                      params=params, next_depth=2, edges=[edge])
        finally:
            resume.set()
            worker.join(45)
        assert not worker.is_alive() and not failures
        assert _job(promoter, old).state == "done"
        monkeypatch.setattr(discovery, "expand_opponent_frontier", original)
        assert JobRunner(promoter, stage="processing").run(max_jobs=1).done == 1
        assert state.known_crawl_depth(promoter, crawl_run_id=run, provider="lichess", username="f") == 3
        assert state.acquisition_job_count(promoter, run) == 3


def test_standalone_duplicate_months_count_unique_games_and_keep_duplicate_refresh(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    first = json.loads((fixtures_dir / "chesscom/archive_2024_01.json").read_bytes())["games"][0]
    second = deepcopy(first)
    second["uuid"], second["url"] = "second", "https://www.chess.com/game/live/second"
    paths = []

    def provider(request):
        paths.append(request.url.path)
        # The last duplicate is still interpreted after the local allowance
        # reaches zero; both months must complete their observation checkpoints.
        return httpx.Response(200, json={"games": [first] if request.url.path.endswith("/01") else [first, second, first]})

    parent = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_games", target="samename",
                               params={"since": SINCE, "until": UNTIL, "max_games": 2}).job_id
    outcome = JobRunner(conn, config=CONFIG, transport=httpx.MockTransport(provider)).run()
    assert outcome.errors == 0 and _job(conn, parent).state == "done"
    assert len(paths) == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
    assert require_row(conn.execute("SELECT games FROM work_budgets WHERE standalone_job_id=%s", (parent,)))[0] == 2
    raw = require_row(conn.execute("SELECT params_json FROM discovery_jobs WHERE kind='normalize_payload' ORDER BY id DESC LIMIT 1"))
    raw_id = state.load_params(raw["params_json"])["raw_payload_id"]
    assert raw_payload_metadata(conn, raw_id)["normalization_status"] == "parsed"
    # A direct operator replay without a work budget retains its historical
    # encounter-based limit and returns the selected existing entity.
    assert len(normalize_games_payload(conn, raw_id, max_games=1)) == 1


@pytest.mark.parametrize("requested", ["unavailable-old-parser", None, PARSER_VERSION])
def test_processing_honors_endpoint_parser_pin_before_work_charge(initialized_conn, fixtures_dir, requested) -> None:
    conn = initialized_conn
    body = json.loads((fixtures_dir / "chesscom/archive_2024_01.json").read_bytes())
    parent = state.enqueue_job(conn, provider="chess.com", kind="fetch_user_games", target="samename",
                               params={"since": SINCE, "until": UNTIL, "max_games": 1}).job_id
    acquire = JobRunner(conn, config=CONFIG, stage="acquisition",
                        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body)))
    acquire.run(max_jobs=1)
    child = require_row(conn.execute("SELECT id,params_json,work_budget_id FROM discovery_jobs WHERE kind='normalize_payload'"))
    params = state.load_params(child["params_json"])
    if requested is None:
        params.pop("parser_version")
    else:
        params["parser_version"] = requested
    state.update_job_params(conn, child["id"], params)
    before = dict(require_row(conn.execute("SELECT normalization_units,games FROM work_budgets WHERE id=%s", (child["work_budget_id"],))))
    outcome = JobRunner(conn, stage="processing").run(max_jobs=1)
    if requested == "unavailable-old-parser":
        assert outcome.errors == 1 and _job(conn, child["id"]).state == "error"
        assert "parser target is unavailable" in (_job(conn, child["id"]).reason or "")
        after = dict(require_row(conn.execute("SELECT normalization_units,games FROM work_budgets WHERE id=%s", (child["work_budget_id"],))))
        assert after == before
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 0
        assert raw_payload_metadata(conn, params["raw_payload_id"])["parser_version"] is None
        assert acquire.run(max_jobs=1).errors == 1
        assert _job(conn, parent).state == "error"
    else:
        assert outcome.done == 1
        assert raw_payload_metadata(conn, params["raw_payload_id"])["parser_version"] == PARSER_VERSION
        assert replay_raw_payload(conn, params["raw_payload_id"], parser_version=PARSER_VERSION).status_code == 200
