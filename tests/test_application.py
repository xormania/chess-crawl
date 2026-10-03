"""Application contracts shared by transport adapters, with no provider network."""

from __future__ import annotations

from chess_crawl.storage.db import Connection, open_database, require_row

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from typing import cast


import pytest

from support import seed_game
from chess_crawl.application import (
    Conflict,
    CrawlRequest,
    ImportRequest,
    Limits,
    NotFound,
    ValidationError,
    get_job,
    get_run,
    list_games,
    list_opponents,
    list_providers,
    list_users,
    submit_crawl,
    submit_import,
    summary,
    validate_crawl,
    validate_idempotency_key,
    validate_import,
)
from chess_crawl.jobs import state
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage import application as submission_storage
from chess_crawl.storage.raw import store_raw_payload, update_raw_payload_status


SINCE = 1704067200
UNTIL = 1704153600
IMPORT = ImportRequest("lichess", "SameName", SINCE, UNTIL, 10)
CRAWL = CrawlRequest("lichess", "SameName", SINCE, UNTIL, 10, 1, 10, 20)


def test_import_queues_profile_then_bounded_games_and_replays_after_completion(initialized_conn) -> None:
    conn = initialized_conn
    first = submit_import(conn, IMPORT, idempotency_key="import-1")
    jobs = [get_job(conn, job_id) for job_id in first["job_ids"]]
    assert [job["kind"] for job in jobs] == ["fetch_user_profile", "fetch_user_games"]
    assert [job["target"] for job in jobs] == ["samename", "samename"]
    assert jobs[0]["priority"] < jobs[1]["priority"]
    assert jobs[1]["params"]["since"] == SINCE
    assert jobs[1]["params"]["until"] == UNTIL
    assert jobs[1]["params"]["max_games"] == 10
    assert all(job["state"] == "pending" for job in jobs)
    for job in jobs:
        state.mark_done(conn, job["id"])
    state.refresh_run_status(conn, first["run_id"])
    replay = submit_import(
        conn, replace(IMPORT, provider=" LICHESS ", username=" samename "), idempotency_key="import-1",
    )
    assert replay == {**first, "replayed": True}
    run = get_run(conn, first["run_id"])
    assert run["status"] == "done"
    assert run["counters"]["jobs_total"] == 2
    assert run["job_ids"] == first["job_ids"]
    assert require_row(conn.execute("SELECT COUNT(*) FROM crawl_runs"))[0] == 1
    json.dumps({"run": run, "jobs": jobs})


def test_crawl_idempotency_and_conflicting_requests(initialized_conn) -> None:
    conn = initialized_conn
    first = submit_crawl(conn, CRAWL, idempotency_key="crawl-1")
    assert len(first["job_ids"]) == 1
    root = get_job(conn, first["job_ids"][0])
    assert root["kind"] == "crawl_opponents"
    assert root["params"]["max_depth"] == 1
    assert submit_crawl(conn, CRAWL, idempotency_key="crawl-1") == {**first, "replayed": True}
    with pytest.raises(Conflict) as changed:
        submit_crawl(conn, replace(CRAWL, max_games=9), idempotency_key="crawl-1")
    assert changed.value.code == "idempotency_conflict"
    with pytest.raises(Conflict):
        submit_import(conn, IMPORT, idempotency_key="crawl-1")
    assert len(state.crawl_runs(conn)) == 1
    assert len(state.list_jobs(conn)) == 1


def test_submission_identity_and_created_work_roll_back_together(initialized_conn, monkeypatch) -> None:
    original = submission_storage.record_submission

    def fail_after_record(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("submission interrupted")

    monkeypatch.setattr(submission_storage, "record_submission", fail_after_record)
    with pytest.raises(RuntimeError, match="submission interrupted"):
        submit_import(initialized_conn, IMPORT, idempotency_key="retry-me")
    assert len(state.crawl_runs(initialized_conn)) == 0
    assert len(state.list_jobs(initialized_conn)) == 0
    assert submission_storage.get_submission(initialized_conn, "retry-me") is None
    assert not initialized_conn.in_transaction
    monkeypatch.setattr(submission_storage, "record_submission", original)
    assert submit_import(initialized_conn, IMPORT, idempotency_key="retry-me")["replayed"] is False


def test_concurrent_submissions_return_one_durable_identity(database_url: str) -> None:
    ready = Barrier(2)

    def submit() -> dict:
        with open_database(database_url, writable=True) as conn:
            ready.wait(timeout=5)
            return submit_import(conn, IMPORT, idempotency_key="shared-request")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(submit) for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0]["run_id"] == results[1]["run_id"]
    assert results[0]["job_ids"] == results[1]["job_ids"]
    assert sorted(result["replayed"] for result in results) == [False, True]
    with open_database(database_url) as conn:
        assert len(state.crawl_runs(conn)) == 1
        assert len(state.list_jobs(conn)) == 2


def test_conflicting_concurrent_submissions_create_only_the_winning_work(database_url: str) -> None:
    ready = Barrier(2)

    def submit(maximum: int):
        with open_database(database_url, writable=True) as conn:
            ready.wait(timeout=5)
            try:
                return submit_import(conn, replace(IMPORT, max_games=maximum), idempotency_key="conflicting-race")
            except Conflict:
                return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(submit, maximum) for maximum in (5, 10)]
        results = [future.result(timeout=10) for future in futures]
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert winners[0]["replayed"] is False
    with open_database(database_url) as conn:
        assert len(state.crawl_runs(conn)) == 1
        assert len(state.list_jobs(conn)) == 2
        assert require_row(conn.execute("SELECT COUNT(*) FROM application_submissions"))[0] == 1


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"provider": "unknown"}, "invalid_provider"),
        ({"username": "../alice"}, "invalid_username"),
        ({"username": " "}, "invalid_username"),
        ({"since": True}, "invalid_since"),
        ({"since": -1}, "invalid_since"),
        ({"until": SINCE}, "invalid_date_window"),
        ({"until": SINCE + 367 * 86400}, "invalid_date_window"),
        ({"max_games": 0}, "invalid_max_games"),
        ({"max_games": 1001}, "invalid_max_games"),
    ],
)
def test_invalid_submissions_fail_before_using_database(changes, code) -> None:
    class UnavailableConnection:
        def __getattr__(self, name):
            raise AssertionError("Invalid request must fail before database access")
    closed_conn = cast(Connection, UnavailableConnection())
    with pytest.raises(ValidationError) as error:
        submit_import(closed_conn, replace(IMPORT, **changes), idempotency_key="invalid")
    assert error.value.code == code


@pytest.mark.parametrize("key", ["", "a b", "key\n", "a" * 129, "clé"])
def test_invalid_idempotency_keys_are_rejected(key: str) -> None:
    with pytest.raises(ValidationError) as error:
        validate_idempotency_key(key)
    assert error.value.code == "invalid_idempotency_key"


def test_configurable_bounds_and_environment(monkeypatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_MAX_GAMES", "5")
    monkeypatch.setenv("CHESS_CRAWL_MAX_DEPTH", "0")
    monkeypatch.setenv("CHESS_CRAWL_PAGE_SIZE", "2")
    limits = Limits.from_env()
    assert (limits.max_games, limits.max_depth, limits.page_size) == (5, 0, 2)
    with pytest.raises(ValidationError, match="max_games"):
        validate_import(IMPORT, limits=limits)
    with pytest.raises(ValidationError, match="max_depth"):
        validate_crawl(replace(CRAWL, max_games=5), limits=limits)
    for changed in (replace(CRAWL, max_users=101), replace(CRAWL, max_jobs=201)):
        with pytest.raises(ValidationError):
            validate_crawl(changed)
    monkeypatch.setenv("CHESS_CRAWL_PAGE_SIZE", "invalid")
    with pytest.raises(ValueError, match="CHESS_CRAWL_PAGE_SIZE"):
        Limits.from_env()


def test_provider_scoped_keyset_pages_are_bounded_and_json_serializable(initialized_conn) -> None:
    conn = initialized_conn
    seed_game(conn, provider="lichess", game_key="l-1", white="SameName", black="A")
    seed_game(conn, provider="chess.com", game_key="c-1", white="SameName", black="Elsewhere")
    seed_game(conn, provider="lichess", game_key="l-2", white="SameName", black="B", outcome=None)
    first = list_games(conn, provider="lichess", limit=1)
    assert first["total"] == 2
    assert first["next_cursor"] == first["items"][0]["id"]
    assert first["items"][0]["rated"] is True
    seed_game(conn, provider="lichess", game_key="l-3", white="SameName", black="C")
    second = list_games(conn, provider="lichess", after=first["next_cursor"], limit=2)
    assert [row["provider_game_id"] for row in second["items"]] == ["l-2", "l-3"]
    assert second["next_cursor"] is None
    assert second["total"] == 3
    assert list_games(conn, after=2**63 - 1)["items"] == []
    users = list_users(conn, provider="lichess", limits=Limits(page_size=2))
    assert len(users["items"]) == 2
    assert users["total"] == 4
    assert all(row["provider"] == "lichess" for row in users["items"])
    assert list_users(conn, provider="chess.com")["total"] == 2
    opponents = list_opponents(conn, "lichess", "SameName", limit=1)
    assert opponents["total"] == 3
    assert opponents["items"][0]["opponent_username"] == "a"
    remainder = list_opponents(conn, "lichess", "samename", after=opponents["next_cursor"])
    assert [row["opponent_username"] for row in remainder["items"]] == ["b", "c"]
    assert remainder["items"][0]["unfinished"] == 1
    assert list_opponents(conn, "chess.com", "samename")["total"] == 1
    json.dumps([first, second, users, opponents, remainder, summary(conn), list_providers()])


@pytest.mark.parametrize("kwargs", [{"limit": 0}, {"limit": 101}, {"after": -1}, {"after": 2**63}])
def test_query_pages_reject_invalid_bounds(initialized_conn, kwargs) -> None:
    with pytest.raises(ValidationError):
        list_games(initialized_conn, **kwargs)


def test_missing_entities_have_stable_codes(initialized_conn) -> None:
    for read, args, code in (
        (get_run, (99,), "run_not_found"),
        (get_job, (99,), "job_not_found"),
        (list_opponents, ("lichess", "missing"), "user_not_found"),
    ):
        with pytest.raises(NotFound) as error:
            read(initialized_conn, *args)
        assert error.value.code == code


def test_freshness_distinguishes_preserved_data_from_normalized_data(initialized_conn) -> None:
    conn = initialized_conn
    record = RawRecord(
        provider="lichess", endpoint_type="user_profile", request_url="https://lichess.org/api/user/a",
        canonical_source_key="lichess/user/a", http_status=200, fetched_at=123,
        body=b'{}', media_type="application/json", target_username="a",
    )
    raw_id = store_raw_payload(conn, record)
    pending = list_users(conn, provider="lichess")["freshness"]
    assert pending == {
        "provider": "lichess", "last_fetched_at": 123, "last_normalized_at": None,
        "pending_payloads": 1, "failed_payloads": 0, "last_checked_at": None,
    }
    update_raw_payload_status(conn, raw_id, status="parsed", parser_version="test", normalized_at=456)
    normalized = summary(conn)["freshness"]
    assert normalized["last_normalized_at"] == 456
    assert normalized["pending_payloads"] == 0
    assert list_users(conn, provider="chess.com")["freshness"]["last_fetched_at"] is None
