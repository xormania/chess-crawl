"""Offline acquisition contracts against real PostgreSQL and fake provider APIs."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.jobs.collection import CollectionResult, execute_collection, _milliseconds
from chess_crawl.jobs.models import DiscoveryJob
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs.state import enqueue_job, get_job
from chess_crawl.jobs.state import create_crawl_run_with_root_job
from chess_crawl.providers.base import RawRecord
from chess_crawl.providers.registry import ProviderSession
from chess_crawl.storage import collection as store
from chess_crawl.storage.db import Connection, require_row
from chess_crawl.storage.raw import read_raw_payload, store_raw_payload, update_raw_payload_status
from support import Clock


def _job(conn: Connection, provider: str, params: dict) -> DiscoveryJob:
    inserted = enqueue_job(conn, provider=provider, kind="fetch_user_games", target="SameName", params=params)
    job = get_job(conn, inserted.job_id)
    assert job is not None
    return job


def _checkpoint(conn: Connection, job: DiscoveryJob) -> dict:
    assert job.id is not None
    result = store.checkpoint(conn, job.id)
    assert result is not None
    return result


def _coverage(conn: Connection, provider: str, unit: str) -> dict:
    result = store.coverage(conn, provider, "samename", unit)
    assert result is not None
    return result


def _step(conn: Connection, job: DiscoveryJob, params: dict, handler, clock: Clock) -> CollectionResult:
    config = Config(max_retries=0, chesscom_delay_s=0, lichess_delay_s=0)
    with ProviderSession(config, transport=httpx.MockTransport(handler), sleeper=clock.sleep, clock=clock) as session:
        return execute_collection(conn, job, params, config=config, session=session,
                                  clock=clock, sleeper=clock.sleep)


@pytest.mark.parametrize("params", [{"until_ms": 2**63}, {"since": 2**63}, {"until_ms": True}])
def test_native_timestamp_bounds_reject_bigint_overflow_before_database_or_network(params) -> None:
    key = "since_ms" if "since" in params else "until_ms"
    with pytest.raises(ValueError):
        _milliseconds(params, key)


def test_older_backfill_keeps_history_watermark_and_records_its_window(initialized_conn: Connection) -> None:
    store.record_coverage(initialized_conn, provider="lichess", username="samename", unit="history",
                          state="complete", since_ms=None, until_ms=200000, now=900)
    clock = Clock(1000)
    params = {"collection_mode": "backfill", "until_ms": 100000}
    requests: list[httpx.Request] = []
    assert _step(initialized_conn, _job(initialized_conn, "lichess", params), params,
                 _lichess_provider([], requests), clock).done
    assert _coverage(initialized_conn, "lichess", "history")["window_until_ms"] == 200000
    assert _coverage(initialized_conn, "lichess", "window/open..100000")["state"] == "complete"


def test_equal_second_completion_cannot_regress_history(initialized_conn: Connection) -> None:
    for upper in (200999, 200111):
        store.record_coverage(initialized_conn, provider="lichess", username="samename", unit="history",
                              state="complete", since_ms=None, until_ms=upper, now=200)
    assert _coverage(initialized_conn, "lichess", "history")["window_until_ms"] == 200999
    # A delayed wider completion extends coverage even if its worker clock is older.
    store.record_coverage(initialized_conn, provider="lichess", username="samename", unit="history",
                          state="complete", since_ms=None, until_ms=300000, now=199)
    assert _coverage(initialized_conn, "lichess", "history")["window_until_ms"] == 300000


def _chesscom_source(conn: Connection, body: dict, *, month: str, observed: int) -> int:
    return store_raw_payload(conn, RawRecord(
        provider="chess.com", endpoint_type="monthly_archive",
        request_url=f"https://api.chess.com/pub/player/samename/games/{month}",
        canonical_source_key=f"chess.com/player/samename/games/{month}",
        body=json.dumps(body).encode(), fetched_at=observed,
    ))


def _monthly_game(fixtures_dir: Path, key: str) -> dict:
    body = json.loads((fixtures_dir / "chesscom/archive_2024_01.json").read_bytes())
    body["games"][0]["uuid"] = key
    body["games"][0]["url"] = f"https://www.chess.com/game/live/{key}"
    return body


def test_monthly_reuse_prefers_newer_archived_source_over_coverage(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 4, 10, tzinfo=UTC).timestamp())
    first = _chesscom_source(conn, _monthly_game(fixtures_dir, "older-source"), month="2024/01", observed=int(clock()) - 100)
    store.record_coverage(conn, provider="chess.com", username="samename", unit="2024/01",
                          state="complete", raw_payload_id=first, sealed=True, now=int(clock()) - 100)
    newer = _monthly_game(fixtures_dir, "newer-source")
    newer["games"].extend(_monthly_game(fixtures_dir, "additional-game")["games"])
    latest = _chesscom_source(conn, newer, month="2024/01", observed=int(clock()) - 1)
    requested: list[str] = []

    def provider(request):
        requested.append(request.url.path)
        assert request.url.path.endswith("archives"), "Sealed history should be reused locally"
        return httpx.Response(200, json={"archives": ["https://api.chess.com/pub/player/samename/games/2024/01"]})

    params = {"collection_mode": "full"}
    assert _step(conn, _job(conn, "chess.com", params), params, provider, clock).done
    assert len(requested) == 1
    assert _coverage(conn, "chess.com", "2024/01")["raw_payload_id"] == latest
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2


def test_chesscom_batches_resume_and_reuse_existing_closed_sources(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 4, 10, tzinfo=UTC).timestamp())
    params = {"collection_mode": "full", "batch_size": 2}
    job = _job(conn, "chess.com", params)
    _chesscom_source(conn, _monthly_game(fixtures_dir, "January"), month="2024/01", observed=int(clock()))
    requested: list[str] = []

    def provider(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        requested.append(path)
        if path.endswith("archives"):
            return httpx.Response(200, json={"archives": [
                f"https://api.chess.com/pub/player/samename/games/2024/{month}"
                for month in ("01", "02", "03")
            ]})
        return httpx.Response(200, json=_monthly_game(fixtures_dir, path[-2:]))

    first = _step(conn, job, params, provider, clock)
    assert not first.done and first.processed_units == 2
    assert _checkpoint(conn, job)["unit_index"] == 2
    assert not any(path.endswith("2024/01") for path in requested)
    assert len(requested) == 2  # Index plus February; January is replayed locally.
    second = _step(conn, job, params, provider, clock)
    assert second.done and second.processed_units == 1
    assert len(requested) == 3
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM games"))["n"] == 3
    assert all(row["state"] == "complete" and row["sealed"] for row in store.coverage_rows(conn, "chess.com", "samename"))
    assert _step(conn, job, params, provider, clock).processed_units == 0
    assert len(requested) == 3


def test_archive_captured_in_open_month_is_refreshed_once_after_close(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
    body = _monthly_game(fixtures_dir, "January")
    _chesscom_source(conn, body, month="2024/01", observed=int(datetime(2024, 1, 15, tzinfo=UTC).timestamp()))
    params = {"collection_mode": "incremental"}
    requested: list[str] = []

    def provider(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        if request.url.path.endswith("archives"):
            return httpx.Response(200, json={"archives": ["https://api.chess.com/pub/player/samename/games/2024/01"]})
        return httpx.Response(200, json=body)

    job = _job(conn, "chess.com", params)
    assert _step(conn, job, params, provider, clock).done
    assert _coverage(conn, "chess.com", "2024/01")["sealed"]
    # A distinct job repeats its inventory but reuses the sealed source.
    second_job = _job(conn, "chess.com", {**params, "request": "second"})
    assert _step(conn, second_job, {**params, "request": "second"}, provider, clock).done
    assert len([path for path in requested if path.endswith("2024/01")]) == 1


def test_parser_upgrade_replays_local_source_without_redownloading(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
    raw_id = _chesscom_source(conn, _monthly_game(fixtures_dir, "January"), month="2024/01", observed=int(clock()))
    update_raw_payload_status(conn, raw_id, status="parsed", parser_version="old-parser")
    params = {"collection_mode": "incremental"}

    def provider(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("archives")
        return httpx.Response(200, json={"archives": ["https://api.chess.com/pub/player/samename/games/2024/01"]})

    assert _step(conn, _job(conn, "chess.com", params), params, provider, clock).done
    assert read_raw_payload(conn, raw_id).parser_version != "old-parser"
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM games"))["n"] == 1


def test_explicit_backfill_refreshes_sealed_month_and_missing_months_are_recorded(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
    body = _monthly_game(fixtures_dir, "January")
    _chesscom_source(conn, body, month="2024/01", observed=int(clock()))
    params = {"collection_mode": "backfill", "upgrade_id": "extra-clock-fields-v1", "months": ["2024/01", "2024/02"]}
    requested: list[str] = []

    def provider(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        if request.url.path.endswith("archives"):
            return httpx.Response(200, json={"archives": ["https://api.chess.com/pub/player/samename/games/2024/01"]})
        return httpx.Response(200, json=body)

    assert _step(conn, _job(conn, "chess.com", params), params, provider, clock).done
    assert any(path.endswith("2024/01") for path in requested)
    assert _coverage(conn, "chess.com", "2024/02")["state"] == "missing"


def test_http_failure_does_not_advance_checkpoint_and_retry_captures_unit(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
    params = {"collection_mode": "full"}
    job = _job(conn, "chess.com", params)
    fail = True

    def provider(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("archives"):
            return httpx.Response(200, json={"archives": ["https://api.chess.com/pub/player/samename/games/2024/01"]})
        if fail:
            return httpx.Response(503, json={"error": "temporary"})
        return httpx.Response(200, json=_monthly_game(fixtures_dir, "January"))

    first = _step(conn, job, params, provider, clock)
    assert not first.done and first.status_code == 503
    assert _checkpoint(conn, job)["unit_index"] == 0
    assert _coverage(conn, "chess.com", "2024/01")["state"] == "error"
    fail = False
    assert _step(conn, job, params, provider, clock).done


def test_unchanged_monthly_304_references_preserved_body(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
    body = _monthly_game(fixtures_dir, "January")
    params = {"collection_mode": "backfill"}
    unchanged = False

    def provider(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("archives"):
            return httpx.Response(200, json={"archives": ["https://api.chess.com/pub/player/samename/games/2024/01"]})
        if unchanged:
            assert request.headers["If-None-Match"] == '"monthly-v1"'
            return httpx.Response(304)
        return httpx.Response(200, json=body, headers={"ETag": '"monthly-v1"'})

    assert _step(conn, _job(conn, "chess.com", params), params, provider, clock).done
    raw_id = _coverage(conn, "chess.com", "2024/01")["raw_payload_id"]
    unchanged = True
    new_params = {**params, "request": "second"}
    assert _step(conn, _job(conn, "chess.com", new_params), new_params, provider, clock).done
    assert _coverage(conn, "chess.com", "2024/01")["raw_payload_id"] == raw_id
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM games"))["n"] == 1


def _lichess_game(fixtures_dir: Path, key: str, created: int, *, status: str = "draw") -> dict:
    game = json.loads((fixtures_dir / "lichess/game.json").read_bytes())
    game.update(id=key, createdAt=created, lastMoveAt=created + 2000, status=status)
    game["pgn"] = game["pgn"].replace("single01", key)
    return game


def _lichess_provider(games: list[dict], requests: list[httpx.Request]):
    def provider(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.startswith("/game/export/"):
            return httpx.Response(200, json=next(game for game in games if game["id"] == request.url.path.split("/")[-1]))
        params = request.url.params
        assert params["sort"] == "dateDesc" and params["clocks"] == "true"
        assert params["ongoing"] == "true" and params["pgnInJson"] == "true"
        since, until, limit = int(params.get("since", 0)), int(params["until"]), int(params["max"])
        selected = sorted((game for game in games if since <= game["createdAt"] < until),
                          key=lambda game: game["createdAt"], reverse=True)[:limit]
        return httpx.Response(200, content=b"\n".join(json.dumps(game).encode() for game in selected),
                              headers={"Content-Type": "application/x-ndjson"})
    return provider


def test_lichess_native_millisecond_pages_preserve_all_boundary_ties(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    base = 1704153600000
    games = [_lichess_game(fixtures_dir, f"game000{i}", base + offset)
             for i, offset in enumerate((7, 5, 5, 5, 4, 0))]
    requests: list[httpx.Request] = []
    params = {"collection_mode": "full", "page_size": 2, "max_page_size": 16}
    job = _job(conn, "lichess", params)
    result = None
    for _ in range(8):
        result = _step(conn, job, params, _lichess_provider(games, requests), clock)
        assert result.processed_units == 1
        if result.done:
            break
    assert result is not None and result.done
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM games"))["n"] == len(games)
    assert int(requests[1].url.params["until"]) == base + 6
    assert [int(request.url.params["max"]) for request in requests] == [2, 3, 6]
    assert _coverage(conn, "lichess", "history")["window_until_ms"] == int(clock() * 1000)


def test_incremental_refreshes_earlier_unfinished_game_and_fetches_only_new_window(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    previous_upper = int(clock() * 1000)
    games = [_lichess_game(fixtures_dir, "oldgame1", 1704153600101),
             _lichess_game(fixtures_dir, "ongame01", 1704153600102, status="started")]
    requests: list[httpx.Request] = []
    params = {"collection_mode": "incremental", "page_size": 10}
    first = _job(conn, "lichess", params)
    provider = _lichess_provider(games, requests)
    assert _step(conn, first, params, provider, clock).done
    assert len(store.followups(conn, "samename", after="", limit=100)) == 1
    games[1]["status"] = "draw"
    games.append(_lichess_game(fixtures_dir, "newgame1", previous_upper + 1001))
    clock.now += 3600
    new_params = {**params, "request": "second"}
    second = _job(conn, "lichess", new_params)
    requests.clear()
    assert not _step(conn, second, new_params, provider, clock).done
    assert requests[0].url.path == "/game/export/ongame01"
    assert _step(conn, second, new_params, provider, clock).done
    assert int(requests[1].url.params["since"]) == previous_upper
    assert store.followups(conn, "samename", after="", limit=100) == []
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM games"))["n"] == 3


def test_repeated_full_reuses_stored_history_and_queues_new_run_associations(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    upper = int(clock() * 1000)
    games = [_lichess_game(fixtures_dir, f"stored0{index}", upper - 10000 + index) for index in range(3)]
    requests: list[httpx.Request] = []
    params = {"collection_mode": "full", "page_size": 10}
    provider = _lichess_provider(games, requests)
    assert _step(conn, _job(conn, "lichess", params), params, provider, clock).done
    games.append(_lichess_game(fixtures_dir, "newfull1", upper + 1000))
    clock.now += 3600
    new_params = {**params, "request": "new-run", "max_games": 1}
    run_id, job_id = create_crawl_run_with_root_job(
        conn, provider="lichess", seed_spec="samename", params=new_params,
        root_kind="fetch_user_games", root_target="SameName",
    )
    job = get_job(conn, job_id)
    assert job is not None
    requests.clear()
    conn._defer_normalization = True
    try:
        assert not _step(conn, job, new_params, provider, clock).done
        assert requests == []  # Reused sources are routed to processing, not the provider.
        assert _step(conn, job, new_params, provider, clock).done
    finally:
        conn._defer_normalization = False
    assert len(requests) == 1 and int(requests[0].url.params["since"]) == upper
    assert require_row(conn.execute("SELECT COUNT(*) FROM run_games WHERE crawl_run_id=%s", (run_id,)))[0] == 0
    assert JobRunner(conn, stage="processing").run(max_jobs=10).done == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM run_games WHERE crawl_run_id=%s", (run_id,)))[0] == 4


def test_larger_full_reuses_saturated_page_and_fetches_only_missing_tail(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    base = 1704153600000
    games = [_lichess_game(fixtures_dir, f"partial{index}", base + offset)
             for index, offset in enumerate((3, 2, 1))]
    requests: list[httpx.Request] = []
    provider = _lichess_provider(games, requests)
    first = {"collection_mode": "full", "max_games": 2}
    assert not _step(conn, _job(conn, "lichess", first), first, provider, clock).done
    assert store.coverage(conn, "lichess", "samename", "history") is None
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
    second = {"collection_mode": "full", "max_games": 4}
    job = _job(conn, "lichess", second)
    requests.clear()
    assert not _step(conn, job, second, provider, clock).done
    assert requests == []
    assert _step(conn, job, second, provider, clock).done
    assert len(requests) == 1 and int(requests[0].url.params["until"]) == base + 3
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 3
    assert _coverage(conn, "lichess", "history")["window_until_ms"] == int(clock() * 1000)


def test_full_subtracts_finite_complete_interval_and_fetches_disjoint_gaps(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    base = 1704153600000
    clock = Clock((base + 10000) / 1000)
    games = [_lichess_game(fixtures_dir, f"finite0{index}", base + offset)
             for index, offset in enumerate((6000, 4000, 3000, 1000))]
    requests: list[httpx.Request] = []
    provider = _lichess_provider(games, requests)
    finite = {"collection_mode": "backfill", "since_ms": base + 2000, "until_ms": base + 5000, "page_size": 10}
    assert _step(conn, _job(conn, "lichess", finite), finite, provider, clock).done
    assert store.coverage(conn, "lichess", "samename", "history") is None
    full = {"collection_mode": "full", "page_size": 10}
    job = _job(conn, "lichess", full)
    requests.clear()
    assert not _step(conn, job, full, provider, clock).done
    assert requests == []
    assert not _step(conn, job, full, provider, clock).done
    assert store.coverage(conn, "lichess", "samename", "history") is None
    assert _step(conn, job, full, provider, clock).done
    assert [(request.url.params.get("since"), int(request.url.params["until"])) for request in requests] == [
        (str(base + 5000), base + 10000), (None, base + 2000),
    ]
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 4
    assert _coverage(conn, "lichess", "history")["window_until_ms"] == base + 10000


@pytest.mark.parametrize("deferred", [False, True], ids=["local", "processing-worker"])
def test_reused_broad_page_attributes_only_exact_requested_creation_window(initialized_conn, fixtures_dir, deferred) -> None:
    conn = initialized_conn
    base = 1704153600000
    clock = Clock((base + 10000) / 1000)
    games = [_lichess_game(fixtures_dir, f"narrow0{index}", base + offset)
             for index, offset in enumerate((1001, 1002, 1003, 1004))]
    requests: list[httpx.Request] = []
    provider = _lichess_provider(games, requests)
    full = {"collection_mode": "full", "page_size": 10}
    assert _step(conn, _job(conn, "lichess", full), full, provider, clock).done
    narrow = {**full, "since_ms": base + 1002, "until_ms": base + 1004, "max_games": 1}
    run_id, job_id = create_crawl_run_with_root_job(conn, provider="lichess", seed_spec="samename", params=narrow,
                                                  root_kind="fetch_user_games", root_target="SameName")
    job = get_job(conn, job_id)
    assert job is not None
    requests.clear()
    conn._defer_normalization = deferred
    try:
        assert not _step(conn, job, narrow, provider, clock).done
        assert _step(conn, job, narrow, provider, clock).done
    finally:
        conn._defer_normalization = False
    if deferred:
        assert JobRunner(conn, stage="processing").run(max_jobs=10).done == 1
    assert requests == []
    assert {row[0] for row in conn.execute(
        "SELECT provider_game_id FROM games JOIN run_games ON game_id=games.id WHERE crawl_run_id=%s", (run_id,),
    )} == {"narrow01", "narrow02"}


def test_full_past_window_reuses_complete_history_without_provider_request(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    requests: list[httpx.Request] = []
    params = {"collection_mode": "full", "page_size": 10}
    provider = _lichess_provider([_lichess_game(fixtures_dir, "pastfull", 1704153600001)], requests)
    assert _step(conn, _job(conn, "lichess", params), params, provider, clock).done
    upper = _coverage(conn, "lichess", "history")["window_until_ms"]
    requests.clear()
    older = {**params, "until_ms": upper - 1000}
    job = _job(conn, "lichess", older)
    assert not _step(conn, job, older, provider, clock).done
    assert _step(conn, job, older, provider, clock).done
    assert requests == []
    assert _coverage(conn, "lichess", "history")["window_until_ms"] == upper
    assert _coverage(conn, "lichess", f"window/open..{upper - 1000}")["state"] == "complete"


@pytest.mark.parametrize("state,since", [("pending", None), ("complete", 100)], ids=["incomplete", "finite-lower-bound"])
def test_full_does_not_treat_incomplete_or_finite_history_as_baseline(initialized_conn, state, since) -> None:
    conn = initialized_conn
    store.record_coverage(conn, provider="lichess", username="samename", unit="history", state=state,
                          since_ms=since, until_ms=2000000, now=100)
    params = {"collection_mode": "full"}
    requests: list[httpx.Request] = []
    assert _step(conn, _job(conn, "lichess", params), params, _lichess_provider([], requests), Clock(1000)).done
    assert len(requests) == 1 and "since" not in requests[0].url.params
    assert _coverage(conn, "lichess", "history")["window_until_ms"] == 1000000


def test_lichess_timestamp_saturation_stops_without_claiming_complete(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    games = [_lichess_game(fixtures_dir, f"game000{i}", 1704153600101) for i in range(5)]
    requests: list[httpx.Request] = []
    params = {"collection_mode": "full", "page_size": 2, "max_page_size": 2}
    job = _job(conn, "lichess", params)
    with pytest.raises(ValueError, match="max_page_size"):
        _step(conn, job, params, _lichess_provider(games, requests), clock)
    assert store.coverage(conn, "lichess", "samename", "history") is None
    assert not _checkpoint(conn, job).get("done")
    assert len(store.coverage_rows(conn, "lichess", "samename")) == 1
    # Raising a capacity setting keeps the same collection identity and resumes safely.
    params["max_page_size"] = 16
    for _ in range(6):
        if _step(conn, job, params, _lichess_provider(games, requests), clock).done:
            break
    assert _coverage(conn, "lichess", "history")["state"] == "complete"
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM games"))["n"] == 5


def test_collection_checks_identity_and_bounds_before_any_network(initialized_conn: Connection) -> None:
    conn = initialized_conn
    params = {"collection_mode": "full", "page_size": True}
    job = _job(conn, "lichess", params)
    with pytest.raises(ValueError, match="page_size"):
        execute_collection(conn, job, params)
    params = {"collection_mode": "full", "batch_size": 0}
    with pytest.raises(ValueError, match="batch_size"):
        execute_collection(conn, job, params)


def test_missing_month_does_not_hide_later_available_data(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
    params = {"collection_mode": "full"}

    def provider(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("archives"):
            return httpx.Response(200, json={"archives": [
                "https://api.chess.com/pub/player/samename/games/2024/01",
                "https://api.chess.com/pub/player/samename/games/2024/02",
            ]})
        if request.url.path.endswith("2024/01"):
            return httpx.Response(404)
        return httpx.Response(200, json=_monthly_game(fixtures_dir, "February"))

    result = _step(conn, _job(conn, "chess.com", params), params, provider, clock)
    assert result.done and result.processed_units == 2
    assert _coverage(conn, "chess.com", "2024/01")["state"] == "missing"
    assert _coverage(conn, "chess.com", "2024/02")["state"] == "complete"


def test_missing_unfinished_game_is_retained_without_blocking_new_scan(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    game = _lichess_game(fixtures_dir, "newgame1", 1704153800000)
    store.track_followup(conn, username="samename", game_ref="gonegame", created_ms=1704153600000,
                         finished=False, now=int(clock()))
    requests: list[httpx.Request] = []
    games_provider = _lichess_provider([game], requests)

    def provider(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/game/export/gonegame":
            return httpx.Response(404)
        return games_provider(request)

    params = {"collection_mode": "incremental", "page_size": 10}
    job = _job(conn, "lichess", params)
    assert not _step(conn, job, params, provider, clock).done
    assert _step(conn, job, params, provider, clock).done
    assert _coverage(conn, "lichess", "followup/gonegame")["state"] == "missing"
    assert store.followups(conn, "samename", after="", limit=10)[0]["game_ref"] == "gonegame"
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM games"))["n"] == 1


def test_full_history_max_games_is_page_budget_not_total_cap(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    games = [_lichess_game(fixtures_dir, f"game000{i}", 1704153600000 + i * 3) for i in range(5)]
    requests: list[httpx.Request] = []
    params = {"collection_mode": "full", "max_games": 2}
    run_id, job_id = create_crawl_run_with_root_job(
        conn, provider="lichess", seed_spec="samename", params=params,
        root_kind="fetch_user_games", root_target="samename",
    )
    job = get_job(conn, job_id)
    assert job is not None
    for _ in range(8):
        if _step(conn, job, params, _lichess_provider(games, requests), clock).done:
            break
    assert requests[0].url.params["max"] == "2"
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM run_games WHERE crawl_run_id = %s", (run_id,)))["n"] == 5


def test_seconds_windows_are_respected_by_lichess_native_page_request(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(1704153900)
    games = [_lichess_game(fixtures_dir, "withingm", 1704153600500),
             _lichess_game(fixtures_dir, "outside1", 1704153599999)]
    requests: list[httpx.Request] = []
    params = {"collection_mode": "full", "since": 1704153600, "until": 1704153601}
    result = _step(conn, _job(conn, "lichess", params), params, _lichess_provider(games, requests), clock)
    assert result.done
    assert requests[0].url.params["since"] == "1704153600000"
    assert requests[0].url.params["until"] == "1704153601000"
    assert require_row(conn.execute("SELECT COUNT(*) AS n FROM games"))["n"] == 1
    assert store.coverage(conn, "lichess", "samename", "history") is None


def test_chesscom_seconds_windows_select_overlapping_months(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    clock = Clock(datetime(2024, 3, 1, tzinfo=UTC).timestamp())
    params = {"collection_mode": "full", "since": int(datetime(2024, 2, 15, tzinfo=UTC).timestamp()),
              "until": int(datetime(2024, 3, 1, tzinfo=UTC).timestamp())}
    paths: list[str] = []

    def provider(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path.endswith("archives"):
            return httpx.Response(200, json={"archives": [
                f"https://api.chess.com/pub/player/samename/games/2024/{month}" for month in ("01", "02", "03")
            ]})
        return httpx.Response(200, json=_monthly_game(fixtures_dir, "February"))

    assert _step(conn, _job(conn, "chess.com", params), params, provider, clock).done
    assert paths == ["/pub/player/samename/games/archives", "/pub/player/samename/games/2024/02"]
