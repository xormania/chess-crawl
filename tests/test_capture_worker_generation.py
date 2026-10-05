"""Deferred workers retain each captured owner and schedule newer observations."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import replace

import pytest

from chess_crawl import ingest
from chess_crawl.jobs import state
from chess_crawl.jobs.collection import _use_local_payload
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.normalize import games as game_normalization
from chess_crawl.normalize.resources import normalize_resource_payload
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.providers.resources import resource_source_key
from chess_crawl.storage.db import connection, require_row
from chess_crawl.storage.player_profiles import (
    player_profile, profile_history, resource_history, resolve_capture_account,
)
from chess_crawl.storage.repository import user_identity_transaction
from test_player_resources import _config, _profile


def _queue_capture(conn, record: RawRecord, normalizer):
    previous = conn._defer_normalization
    conn._defer_normalization = True
    try:
        return ingest._store_and_normalize(conn, record, normalizer=normalizer)
    finally:
        conn._defer_normalization = previous


def _processing_jobs(conn):
    return conn.execute(
        "SELECT id,state,params_json FROM discovery_jobs WHERE kind='normalize_payload' ORDER BY id"
    ).fetchall()


def _captured_record(kind: str, at: int) -> RawRecord:
    resource = kind == "resource"
    return RawRecord(
        provider="chess.com",
        endpoint_type="user_resource" if resource else "user_stats",
        canonical_source_key=(
            resource_source_key("chess.com", "Eve", "clubs")
            if resource else "chess.com/player/eve/stats"
        ),
        request_url="https://api.chess.com/pub/player/eve/" + ("clubs" if resource else "stats"),
        target_username="Eve",
        request_params=(
            {"resource_key": "clubs", "parameters": {}, "authenticated": False, "owner_scope": "public"}
            if resource else {}
        ),
        body=b'{"clubs":[]}' if resource else b'{"chess_blitz":{"last":{"rating":1500}}}',
        fetched_at=at,
    )


def _history(conn, user_id: int, kind: str):
    if kind == "resource":
        return resource_history(conn, user_id)
    return [row for row in profile_history(conn, user_id) if row["endpoint_type"] == "user_stats"]


@pytest.mark.parametrize("enqueue_path", ["network", "stored-collection"])
def test_new_capture_queues_successor_while_predecessor_worker_is_running(
    initialized_conn, database_url, fixtures_dir, monkeypatch, enqueue_path,
):
    """A deduplicated body still needs work for its newer fetch generation."""
    conn = initialized_conn
    record = RawRecord(
        provider="chess.com", endpoint_type="monthly_archive",
        canonical_source_key="chess.com/player/samename/games/2024/01",
        request_url="https://api.chess.com/pub/player/samename/games/2024/01",
        body=(fixtures_dir / "chesscom/archive_2024_01.json").read_bytes(), fetched_at=100,
    )
    selection_job = None
    if enqueue_path == "stored-collection":
        selection_id = state.enqueue_job(
            conn, provider="chess.com", kind="fetch_user_games", target="samename",
        ).job_id
        selection_job = state.get_job(conn, selection_id)

    def queue_generation(captured_record):
        if selection_job is None:
            return _queue_capture(conn, captured_record, game_normalization.normalize_games_payload).raw_payload_id
        raw_id, _ = ingest._persist_response(conn, captured_record, job_id=None, crawl_run_id=None)
        previous = conn._defer_normalization
        conn._defer_normalization = True
        try:
            _use_local_payload(conn, selection_job, raw_id, {})
        finally:
            conn._defer_normalization = previous
        return raw_id

    first_raw_id = queue_generation(record)
    first_job = _processing_jobs(conn)[0]
    entered, release = threading.Event(), threading.Event()
    failures: list[BaseException] = []
    outcomes = []
    original = game_normalization.parse_game_evidence

    def paused_parse(game):
        if threading.current_thread().name == "predecessor-normalizer":
            entered.set()
            assert release.wait(10), "Test did not release the predecessor worker"
        return original(game)

    def normalize_first():
        try:
            with connection(database_url, mode="rw") as worker_conn:
                outcomes.append(JobRunner(worker_conn, config=_config(), stage="processing").run(
                    max_jobs=1, job_id=first_job["id"],
                ))
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(game_normalization, "parse_game_evidence", paused_parse)
    thread = threading.Thread(target=normalize_first, name="predecessor-normalizer")
    thread.start()
    try:
        assert entered.wait(10), "Predecessor worker did not reach game parsing"
        assert state.get_job(conn, first_job["id"]).state == "in_progress"
        newer_raw_id = queue_generation(replace(record, fetched_at=200))
        assert newer_raw_id == first_raw_id
        jobs = _processing_jobs(conn)
        assert len(jobs) == 2, "New acquisition was absorbed by a worker already processing an older generation"
        assert [job["state"] for job in jobs] == ["in_progress", "pending"]
        assert jobs[0]["params_json"] != jobs[1]["params_json"]
    finally:
        release.set()
        thread.join(12)
    assert not thread.is_alive() and failures == []
    assert len(outcomes) == 1 and outcomes[0].done == 1
    assert require_row(conn.execute(
        "SELECT normalization_status FROM raw_payloads WHERE id=%s", (first_raw_id,),
    ))[0] == "pending"

    outcome = JobRunner(conn, config=_config(), stage="processing").run(max_jobs=1)
    assert outcome.done == 1
    assert [job["state"] for job in _processing_jobs(conn)] == ["done", "done"]
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 1
    progress = require_row(conn.execute(
        "SELECT state,observation_id FROM payload_normalization_runs WHERE raw_payload_id=%s",
        (first_raw_id,),
    ))
    assert progress["state"] == "complete"
    assert progress["observation_id"] == require_row(conn.execute(
        "SELECT MAX(id) FROM fetch_logs WHERE raw_payload_id=%s AND status_code IN (200,304)",
        (first_raw_id,),
    ))[0]
    assert require_row(conn.execute(
        "SELECT normalization_status FROM raw_payloads WHERE id=%s", (first_raw_id,),
    ))[0] == "parsed"


@pytest.mark.parametrize("kind", ["resource", "stats"])
@pytest.mark.parametrize("status", [200, 304], ids=["reused-200", "cached-304"])
def test_delayed_worker_normalizes_only_its_captured_account(initialized_conn, kind, status):
    conn = initialized_conn
    former, _ = _profile(conn, {"username": "Eve", "player_id": 8}, provider="chess.com", at=100)
    normalizer = normalize_resource_payload if kind == "resource" else normalize_user_payload
    record = _captured_record(kind, 110)
    first = _queue_capture(conn, record, normalizer)
    first_job = _processing_jobs(conn)[0]
    captured_fetch = require_row(conn.execute(
        "SELECT id FROM fetch_logs WHERE raw_payload_id=%s AND attempted_at=110", (first.raw_payload_id,),
    ))[0]
    assert json.loads(first_job["params_json"])["fetch_log_id"] == captured_fetch

    _profile(conn, {"username": "FormerEve", "player_id": 8}, provider="chess.com", at=200)
    current, _ = _profile(conn, {"username": "Eve", "player_id": 9}, provider="chess.com", at=300)
    newer_record = replace(record, fetched_at=400, http_status=status, body=record.body if status == 200 else None)
    newer = _queue_capture(conn, newer_record, normalizer)
    assert newer.raw_payload_id == first.raw_payload_id
    jobs = _processing_jobs(conn)
    assert len(jobs) == 2

    first_outcome = JobRunner(conn, config=_config(), stage="processing").run(
        max_jobs=1, job_id=first_job["id"],
    )
    assert first_outcome.done == 1
    assert [row["observed_at"] for row in _history(conn, former, kind)] == [110]
    assert _history(conn, current, kind) == [], "The first worker interpreted another job's captured occurrence"
    assert player_profile(conn, "chess.com", "FormerEve")["updated_at"] == 200
    assert player_profile(conn, "chess.com", "Eve")["aliases"][0]["first_seen_at"] == 300

    newer_outcome = JobRunner(conn, config=_config(), stage="processing").run(
        max_jobs=1, job_id=jobs[1]["id"],
    )
    assert newer_outcome.done == 1
    assert [row["observed_at"] for row in _history(conn, former, kind)] == [110]
    assert [row["observed_at"] for row in _history(conn, current, kind)] == [400]
    assert [job["state"] for job in _processing_jobs(conn)] == ["done", "done"]


def test_private_placeholder_resolution_waits_for_same_account_creation(initialized_conn, database_url):
    """A request cannot create a second placeholder before the identity owner commits."""
    conn = initialized_conn
    ready, finished = threading.Event(), threading.Event()
    pids: list[int] = []
    resolved: list[int] = []
    failures: list[BaseException] = []

    def resolve_other():
        try:
            with connection(database_url, mode="rw") as other:
                pids.append(require_row(other.execute("SELECT pg_backend_pid()"))[0])
                ready.set()
                resolved.append(resolve_capture_account(
                    other, provider="lichess", username="PrivateOnly", observed_at=200, owner_scope="alpha",
                ))
        except BaseException as exc:
            failures.append(exc)
        finally:
            finished.set()

    thread = threading.Thread(target=resolve_other, name="private-placeholder-contender")
    try:
        with user_identity_transaction(conn, "lichess", "PrivateOnly", None):
            thread.start()
            assert ready.wait(10), "Contender did not connect to the disposable database"
            deadline = time.monotonic() + 5
            blocked = False
            while time.monotonic() < deadline:
                blocked = require_row(conn.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=%s AND locktype='advisory' AND NOT granted)",
                    (pids[0],),
                ))[0]
                if blocked or finished.wait(0.01):
                    break
            assert blocked, "Contender resolved the absent account while its identity lock was owned"
            assert not finished.is_set()
            owned_id = resolve_capture_account(
                conn, provider="lichess", username="PrivateOnly", observed_at=100, owner_scope="alpha",
            )
            assert tuple(require_row(conn.execute(
                "SELECT first_seen_at,updated_at FROM provider_users WHERE id=%s", (owned_id,),
            )).values()) == (None, None)
    finally:
        if thread.ident is not None:
            thread.join(12)
    assert not thread.is_alive() and failures == []
    assert resolved == [owned_id]
    rows = conn.execute(
        "SELECT id,first_seen_at,updated_at FROM provider_users WHERE provider='lichess' AND username_normalized='privateonly'"
    ).fetchall()
    assert [tuple(row.values()) for row in rows] == [(owned_id, None, None)]
