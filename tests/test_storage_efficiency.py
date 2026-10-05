"""Bound repeated source work without weakening observation or snapshot contracts."""
from __future__ import annotations

from dataclasses import replace
import json

import pytest

from chess_crawl.ingest import _persist_response
from chess_crawl.normalize import games
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage import raw
from chess_crawl.storage.db import connection, require_row, transaction
from chess_crawl.storage.game_evidence import export_game_version_pgn, read_game_version


def game_record(*, fetched_at: int = 100, body: bytes | None = None) -> RawRecord:
    return RawRecord(
        provider="lichess", endpoint_type="game", request_url="https://lichess.org/api/game/efficient",
        canonical_source_key="lichess/game/efficient", fetched_at=fetched_at,
        body=body or json.dumps({
            "id": "efficient", "variant": "standard", "status": "started", "pgn": "1. e4 {[%clk 0:00:00.001]} *",
            "players": {"white": {"user": {"id": "alice"}}, "black": {"user": {"id": "bob"}}},
        }).encode(),
    )


@pytest.mark.parametrize("backend", ["database", "local"])
def test_ingest_prepares_new_and_duplicate_source_once(initialized_conn, tmp_path, monkeypatch, backend):
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", backend)
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(tmp_path))
    hashes, lookups, samples = [], [], []
    original_hash, original_lookup = raw._body_hash, raw._existing_payload

    def hash_body(record):
        hashes.append(record)
        return original_hash(record)

    def lookup(*args):
        lookups.append(args)
        return original_lookup(*args)

    monkeypatch.setattr(raw, "_body_hash", hash_body)
    monkeypatch.setattr(raw, "_existing_payload", lookup)
    monkeypatch.setattr(raw, "emit_sample", samples.append)
    expected_lookups = 1 if backend == "database" else 2
    first_id, first_fetch = _persist_response(initialized_conn, game_record(), job_id=None, crawl_run_id=None)
    assert (len(hashes), len(lookups), len(samples)) == (1, expected_lookups, 0)
    hashes.clear()
    lookups.clear()
    second_id, second_fetch = _persist_response(initialized_conn, game_record(fetched_at=200), job_id=None, crawl_run_id=None)
    assert first_id == second_id and first_fetch != second_fetch
    assert (len(hashes), len(lookups)) == (1, expected_lookups)
    assert [sample.deduplicated for sample in samples] == [1]
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 2
    assert len(list(tmp_path.rglob("*.gz"))) == (1 if backend == "local" else 0)


def test_preparation_does_not_count_abandoned_deduplication(initialized_conn, monkeypatch):
    record = game_record()
    raw.store_raw_payload(initialized_conn, record)
    samples = []
    monkeypatch.setattr(raw, "emit_sample", samples.append)
    prepared = raw.prepare_raw_payload(initialized_conn, record)
    assert samples == []
    with pytest.raises(ValueError, match="does not match"):
        raw.store_raw_payload(initialized_conn, replace(record, body=b"other"), prepared_payload=prepared)
    assert samples == []


@pytest.mark.parametrize("http_status", [200, 304])
def test_new_observation_reuses_evidence_and_replay_repairs_facts(initialized_conn, monkeypatch, http_status):
    conn = initialized_conn
    calls = []
    parse = games.parse_game_evidence

    def parse_evidence(game):
        assert not conn.in_transaction
        calls.append(game.provider_game_id)
        return parse(game)

    monkeypatch.setattr(games, "parse_game_evidence", parse_evidence)
    record = game_record()
    raw_id, _ = _persist_response(conn, record, job_id=None, crawl_run_id=None)
    game_id = games.normalize_games_payload(conn, raw_id)[0]
    before = read_game_version(conn, game_id)
    assert calls == ["efficient"]
    repeated = replace(record, fetched_at=200, http_status=http_status,
                       body=None if http_status == 304 else record.body)
    assert _persist_response(conn, repeated, job_id=None, crawl_run_id=None)[0] == raw_id
    with transaction(conn):
        conn.execute("UPDATE games SET status_raw='damaged' WHERE id=%s", (game_id,))
    assert games.normalize_games_payload(conn, raw_id) == [game_id]
    assert calls == ["efficient"]
    assert require_row(conn.execute("SELECT status_raw FROM games WHERE id=%s", (game_id,)))[0] == "started"
    assert read_game_version(conn, game_id) == before
    assert require_row(conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 2
    with transaction(conn):
        conn.execute("UPDATE games SET status_raw='damaged-again' WHERE id=%s", (game_id,))
    assert games.normalize_games_payload(conn, raw_id) == [game_id]
    assert require_row(conn.execute("SELECT status_raw FROM games WHERE id=%s", (game_id,)))[0] == "started"
    assert calls == ["efficient"]


@pytest.mark.parametrize("operation", ["detail", "export"])
def test_game_read_plan_excludes_unused_evidence_tables(initialized_conn, monkeypatch, operation):
    conn = initialized_conn
    raw_id = raw.store_raw_payload(conn, game_record())
    game_id = games.normalize_games_payload(conn, raw_id)[0]
    execute = conn.execute
    queries = []

    def capture(query, *args, **kwargs):
        if "FROM game_versions v JOIN games g" in str(query):
            queries.append((query, args))
        return execute(query, *args, **kwargs)

    monkeypatch.setattr(conn, "execute", capture)
    if operation == "detail":
        result = read_game_version(conn, game_id)
        assert result["clocks"][0]["seconds"] == "0.001"
    else:
        assert "[%clk 0:00:00.001]" in export_game_version_pgn(conn, game_id)
    assert len(queries) == 1
    query, args = queries[0]
    plan = require_row(execute("EXPLAIN (FORMAT JSON) " + query, *args))[0][0]["Plan"]

    def relations(node):
        return {node.get("Relation Name")} | set().union(*(relations(child) for child in node.get("Plans", [])))

    visited = relations(plan)
    if operation == "detail":
        assert "game_pgn_tokens" not in visited
        assert {"game_move_nodes", "game_clock_observations", "game_derived_timings", "game_version_sources"} <= visited
    else:
        assert "game_pgn_tokens" in visited
        assert not visited & {"game_move_nodes", "game_clock_observations", "game_derived_timings", "game_version_sources"}


def test_removed_preflight_revision_is_reparsed_after_rollback_outside_write(
    initialized_conn, database_url, monkeypatch,
):
    conn = initialized_conn
    raw_id = raw.store_raw_payload(conn, game_record())
    game_id = games.normalize_games_payload(conn, raw_id)[0]
    prior = read_game_version(conn, game_id)
    reusable_sources = games.reusable_game_sources
    parse = games.parse_game_evidence
    calls = []

    def delete_after_preflight(*args, **kwargs):
        reusable = reusable_sources(*args, **kwargs)
        assert reusable == {""}
        with connection(database_url, mode="rw") as other, transaction(other):
            other.execute("UPDATE games SET current_version_id=NULL,status_raw='needs-repair' WHERE id=%s", (game_id,))
            other.execute("DELETE FROM game_versions WHERE id=%s", (prior["id"],))
        return reusable

    def parse_after_rollback(game):
        assert not conn.in_transaction
        # The failed first attempt must not leave its mutable-fact updates behind.
        assert require_row(conn.execute("SELECT status_raw FROM games WHERE id=%s", (game_id,)))[0] == "needs-repair"
        calls.append(game.provider_game_id)
        return parse(game)

    monkeypatch.setattr(games, "reusable_game_sources", delete_after_preflight)
    monkeypatch.setattr(games, "parse_game_evidence", parse_after_rollback)
    assert games.normalize_games_payload(conn, raw_id) == [game_id]
    assert calls == ["efficient"]
    result = read_game_version(conn, game_id)
    assert result["id"] != prior["id"] and result["clocks"][0]["seconds"] == "0.001"
    assert require_row(conn.execute("SELECT status_raw FROM games WHERE id=%s", (game_id,)))[0] == "started"


def test_multi_game_payload_checks_retained_evidence_once(initialized_conn, monkeypatch):
    conn = initialized_conn
    first = json.loads(game_record().body)
    second = {**first, "id": "efficient-second"}
    record = replace(game_record(), endpoint_type="user_games_stream",
                     body=b"\n".join(json.dumps(game).encode() for game in (first, second)))
    raw_id, _ = _persist_response(conn, record, job_id=None, crawl_run_id=None)
    ids = games.normalize_games_payload(conn, raw_id)
    assert len(ids) == 2
    _persist_response(conn, replace(record, fetched_at=200), job_id=None, crawl_run_id=None)
    execute = conn.execute
    lookups = []

    def capture(query, *args, **kwargs):
        if "FROM game_version_sources s JOIN game_versions v" in str(query):
            lookups.append(query)
        return execute(query, *args, **kwargs)

    def unexpected_parse(game):
        pytest.fail("Unchanged retained games must reuse evidence")

    monkeypatch.setattr(conn, "execute", capture)
    monkeypatch.setattr(games, "parse_game_evidence", unexpected_parse)
    assert games.normalize_games_payload(conn, raw_id) == ids
    assert len(lookups) == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM game_versions"))[0] == 2
