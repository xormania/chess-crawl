from __future__ import annotations

from chess_crawl.storage.db import Connection, open_database, require_row

import copy
import json
from pathlib import Path

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import fetch_chesscom_month
from chess_crawl.jobs import state
from chess_crawl.jobs.runner import JobRunner
import chess_crawl.normalize.games as games_module
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.acquisition import associate_run_game, run_game_ids
from chess_crawl.storage.discovery import game_count_for_run
from chess_crawl.storage.raw import read_raw_payload, store_raw_payload
from support import Clock


def _archive_body(
    fixtures_dir: Path, game_keys: list[str], *, ended_at: dict[str, int | None] | None = None,
) -> bytes:
    sample = json.loads((fixtures_dir / "chesscom/archive_2024_01.json").read_bytes())["games"][0]
    games = []
    for key in game_keys:
        game = copy.deepcopy(sample)
        game.update(uuid=key, url=f"https://www.chess.com/game/live/{key}")
        if ended_at is not None and key in ended_at:
            game["end_time"] = ended_at[key]
        games.append(game)
    return json.dumps({"games": games}).encode()


def _store_archive(conn: Connection, body: bytes, month: int = 1) -> int:
    return store_raw_payload(
        conn,
        RawRecord(
            provider="chess.com",
            endpoint_type="monthly_archive",
            request_url=f"https://api.chess.com/pub/player/samename/games/2024/{month:02d}",
            canonical_source_key=f"chess.com/player/samename/games/2024/{month:02d}",
            fetched_at=123,
            body=body,
            media_type="application/json",
        ),
    )


def _run(conn: Connection, maximum: int) -> int:
    return state.create_crawl_run(
        conn, provider="chess.com", seed_spec="samename", params={"max_games": maximum},
    )


def _count(conn: Connection, run_id: int) -> int:
    return game_count_for_run(conn, crawl_run_id=run_id, provider="chess.com", since=None, until=None)


def test_monthly_cap_survives_restart_and_retains_full_pending_payload(
    database_url: str, fixtures_dir: Path,
) -> None:
    body = _archive_body(fixtures_dir, ["one", "two", "three", "four"])
    with open_database(database_url, writable=True) as conn:
        raw_id = _store_archive(conn, body)
        run_id = _run(conn, 2)
        selected = normalize_games_payload(conn, raw_id, crawl_run_id=run_id, max_games=100)
        assert len(selected) == 2
        assert run_game_ids(conn, run_id) == set(selected)
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
        raw = read_raw_payload(conn, raw_id)
        assert raw.body == body
        assert raw.normalization_status == "pending"

    with open_database(database_url, writable=True) as conn:
        assert normalize_games_payload(conn, raw_id, crawl_run_id=run_id, max_games=100) == []
        assert _count(conn, run_id) == 2
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
        assert read_raw_payload(conn, raw_id).normalization_status == "pending"


def test_interrupted_selection_retains_committed_games_and_resumes_remaining_games(
    initialized_conn: Connection, fixtures_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = initialized_conn
    raw_id = _store_archive(conn, _archive_body(fixtures_dir, ["one", "two", "three"]))
    run_id = _run(conn, 3)
    first = normalize_games_payload(conn, raw_id, crawl_run_id=run_id, max_games=1)
    original = games_module._normalize_game

    def interrupt_last_game(inner, game, **kwargs):
        if game.provider_game_id == "three":
            raise RuntimeError("interrupted selection")
        return original(inner, game, **kwargs)

    monkeypatch.setattr(games_module, "_normalize_game", interrupt_last_game)
    with pytest.raises(RuntimeError, match="interrupted selection"):
        normalize_games_payload(conn, raw_id, crawl_run_id=run_id, max_games=2)
    committed = run_game_ids(conn, run_id)
    assert set(first) < committed and len(committed) == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
    assert read_raw_payload(conn, raw_id).normalization_status == "pending"

    monkeypatch.setattr(games_module, "_normalize_game", original)
    remaining = normalize_games_payload(conn, raw_id, crawl_run_id=run_id, max_games=2)
    assert len(remaining) == 1
    assert committed.isdisjoint(remaining)
    assert _count(conn, run_id) == 3
    assert read_raw_payload(conn, raw_id).normalization_status == "parsed"


def test_parsed_payload_is_bounded_independently_for_each_new_run(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    raw_id = _store_archive(conn, _archive_body(fixtures_dir, ["one", "two", "three", "four"]))
    globally_normalized = normalize_games_payload(conn, raw_id)
    first_run, second_run = _run(conn, 1), _run(conn, 3)

    assert normalize_games_payload(conn, raw_id, crawl_run_id=first_run, max_games=100) == globally_normalized[:1]
    assert normalize_games_payload(conn, raw_id, crawl_run_id=second_run, max_games=100) == globally_normalized[:3]
    assert (_count(conn, first_run), _count(conn, second_run)) == (1, 3)
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 4
    assert read_raw_payload(conn, raw_id).normalization_status == "parsed"


def test_cached_month_replay_attributes_only_each_runs_bounded_selection(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    body = _archive_body(fixtures_dir, ["one", "two", "three"])
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("if-none-match"))
        if len(seen) == 1:
            return httpx.Response(200, headers={"etag": '"month-v1"'}, content=body)
        return httpx.Response(304, headers={"etag": '"month-v1"'})

    transport = httpx.MockTransport(handler)
    config = Config(chesscom_delay_s=0, max_retries=0)
    first_run, second_run = _run(conn, 1), _run(conn, 2)
    first = fetch_chesscom_month(
        conn, "SameName", 2024, 1, config=config, transport=transport, crawl_run_id=first_run, max_games=1,
    )
    second = fetch_chesscom_month(
        conn, "SameName", 2024, 1, config=config, transport=transport, crawl_run_id=second_run, max_games=2,
    )

    assert first.status_code == 200
    assert second.status_code == 304
    assert seen == [None, '"month-v1"']
    assert (_count(conn, first_run), _count(conn, second_run)) == (1, 2)
    assert len(second.normalized_ids) == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert first.raw_payload_id is not None
    assert read_raw_payload(conn, first.raw_payload_id).body == body
    assert read_raw_payload(conn, first.raw_payload_id).normalization_status == "pending"


def test_runner_enforces_one_budget_across_monthly_responses(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    bodies = {
        "01": _archive_body(fixtures_dir, ["january-one", "january-two"]),
        "02": _archive_body(fixtures_dir, ["february-one", "february-two", "february-three"]),
    }
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        month = request.url.path.rsplit("/", 1)[1]
        requests.append(month)
        return httpx.Response(200, content=bodies[month])

    run_id, _ = state.create_crawl_run_with_root_job(
        conn, provider="chess.com", seed_spec="samename",
        params={"since": 1704067200, "until": 1709251200, "max_games": 3},
        root_kind="fetch_user_games", root_target="SameName",
    )
    clock = Clock(100.0)
    result = JobRunner(
        conn, config=Config(chesscom_delay_s=0, max_retries=0), transport=httpx.MockTransport(handler),
        clock=clock, sleeper=clock.sleep,
    ).run(crawl_run_id=run_id)

    # One acquisition parent and one normalization child per captured month.
    assert result.done == 3 and result.errors == result.blocked == 0
    assert len(conn.execute("SELECT id FROM discovery_jobs WHERE kind='normalize_payload' AND state='done'").fetchall()) == 2
    assert requests == ["01", "02"]
    assert _count(conn, run_id) == 3
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 3
    assert [row[0] for row in conn.execute("SELECT normalization_status FROM raw_payloads ORDER BY id")] == [
        "parsed", "pending",
    ]
    second_raw = require_row(conn.execute("SELECT id FROM raw_payloads ORDER BY id DESC LIMIT 1"))[0]
    assert read_raw_payload(conn, second_raw).body == bodies["02"]


def test_run_attribution_boundary_enforces_capacity_and_provider(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    raw_id = _store_archive(conn, _archive_body(fixtures_dir, ["one", "two"]))
    first, second = normalize_games_payload(conn, raw_id)
    run_id = _run(conn, 1)
    assert associate_run_game(conn, run_id, first) is True
    assert associate_run_game(conn, run_id, first) is False
    with pytest.raises(ValueError, match="limit has been reached"):
        associate_run_game(conn, run_id, second)
    foreign_run = state.create_crawl_run(conn, provider="lichess", seed_spec="same name", params={"max_games": 2})
    with pytest.raises(ValueError, match="different provider"):
        associate_run_game(conn, foreign_run, first)
    assert run_game_ids(conn, run_id) == {first}
    assert run_game_ids(conn, foreign_run) == set()


@pytest.mark.parametrize("already_parsed", [False, True], ids=["new-raw", "parsed-raw"])
def test_monthly_import_applies_half_open_date_window_before_game_budget(
    initialized_conn: Connection, fixtures_dir: Path, already_parsed: bool,
) -> None:
    conn = initialized_conn
    since, until = 1705276800, 1705363200  # January 15 through January 16, exclusive.
    endings = {
        "before": 1704067200,
        "unknown": None,
        "at-until": until,
        "at-since": since,
        "within": until - 1,
    }
    body = _archive_body(fixtures_dir, list(endings), ended_at=endings)
    raw_id = _store_archive(conn, body)
    if already_parsed:
        assert len(normalize_games_payload(conn, raw_id)) == 5
    run_id, _ = state.create_crawl_run_with_root_job(
        conn, provider="chess.com", seed_spec="samename",
        params={"since": since, "until": until, "max_games": 1},
        root_kind="fetch_user_games", root_target="SameName",
    )
    clock = Clock(100.0)
    result = JobRunner(
        conn, config=Config(chesscom_delay_s=0, max_retries=0),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body)),
        clock=clock, sleeper=clock.sleep,
    ).run(crawl_run_id=run_id)

    assert result.done == 2 and result.errors == result.blocked == 0
    assert len(conn.execute("SELECT id FROM discovery_jobs WHERE kind='normalize_payload' AND state='done'").fetchall()) == 1
    selected = conn.execute(
        "SELECT provider_game_id FROM games JOIN run_games ON game_id = games.id WHERE crawl_run_id = %s",
        (run_id,),
    ).fetchall()
    assert [row[0] for row in selected] == ["at-since"]
    assert _count(conn, run_id) == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == (5 if already_parsed else 1)
    raw = read_raw_payload(conn, raw_id)
    assert raw.body == body
    # The new fetch observation was only partially refreshed under this cap.
    assert raw.normalization_status == "pending"


def test_attribution_rejects_out_of_window_game_without_consuming_capacity(
    initialized_conn: Connection, fixtures_dir: Path,
) -> None:
    conn = initialized_conn
    since, until = 1705276800, 1705363200
    body = _archive_body(fixtures_dir, ["excluded", "included"], ended_at={"excluded": until, "included": since})
    raw_id = _store_archive(conn, body)
    excluded, included = normalize_games_payload(conn, raw_id)
    run_id = state.create_crawl_run(
        conn, provider="chess.com", seed_spec="samename", params={"since": since, "until": until, "max_games": 1},
    )
    with pytest.raises(ValueError, match="date window"):
        associate_run_game(conn, run_id, excluded)
    assert associate_run_game(conn, run_id, included) is True
    assert run_game_ids(conn, run_id) == {included}
