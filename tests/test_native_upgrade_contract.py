"""Queued upgrades repair retained native evidence without reacquisition."""
from __future__ import annotations

import json

import pytest

from chess_crawl.ingest import installed_parser_target, _requires_normalization
from chess_crawl.jobs import state
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.normalize import games
from chess_crawl.normalize.game_evidence import parse_game_evidence
from chess_crawl.providers import registry
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage import game_evidence
from chess_crawl.storage.db import Connection, require_row
from chess_crawl.storage.raw import store_raw_payload


@pytest.mark.parametrize("provider", ["lichess", "chess.com"])
def test_queued_offline_upgrade_repairs_previous_native_evidence(
    initialized_conn: Connection, monkeypatch: pytest.MonkeyPatch, provider: str,
) -> None:
    if provider == "lichess":
        source = {"id": "native-upgrade", "variant": "standard", "status": "started",
                  "players": {"white": {"user": {"id": "alice"}},
                              "black": {"user": {"id": "bob"}}}}
    else:
        source = {"uuid": "native-upgrade", "url": "https://www.chess.com/game/live/native-upgrade",
                  "rules": "chess", "white": {"username": "alice"}, "black": {"username": "bob"}}
    source.update({"pgn": "1. e4 e5 *", "moves": "  d4 d5  "})
    raw_id = store_raw_payload(initialized_conn, RawRecord(
        provider=provider, endpoint_type="game" if provider == "lichess" else "monthly_archive",
        request_url="https://example.invalid/retained", canonical_source_key=provider + "/native-upgrade",
        fetched_at=123, body=json.dumps(source if provider == "lichess" else {"games": [source]}).encode(),
        media_type="application/json",
    ))

    def legacy_evidence(game):
        evidence = parse_game_evidence(game)
        evidence.source_metadata.pop("moves", None)
        return evidence

    with monkeypatch.context() as legacy:
        legacy.setattr(game_evidence, "EVIDENCE_VERSION", "game-evidence-v1")
        legacy.setattr(games, "PARSER_VERSION", "games-normalizer-v5/game-evidence-v1")
        legacy.setattr(games, "parse_game_evidence", legacy_evidence)
        game_id = games.normalize_games_payload(initialized_conn, raw_id)[0]
    previous = game_evidence.read_game_version(initialized_conn, game_id)
    assert previous is not None and "moves" not in previous["source_metadata"]
    assert _requires_normalization(initialized_conn, raw_id)
    manifest = installed_parser_target()
    assert "games-normalizer-v5/game-evidence-v2" in manifest
    assert "game-evidence-v1" not in manifest

    def forbidden(*args, **kwargs):
        pytest.fail("Retained native evidence upgrade must not create a provider client")

    monkeypatch.setattr(registry, "create_provider_client", forbidden)
    state.enqueue_job(
        initialized_conn, provider=provider, kind="reprocess_archive", target="native-evidence-upgrade",
        params={"parser_version": manifest, "batch_size": 100},
    )
    result = JobRunner(initialized_conn, stage="processing").run(max_jobs=1)
    assert result.done == 1 and result.errors == result.blocked == 0
    repaired = game_evidence.read_game_version(initialized_conn, game_id)
    assert repaired is not None and repaired["id"] != previous["id"]
    assert repaired["source_metadata"]["moves"] == "  d4 d5  "
    assert [node["move_uci"] for node in repaired["nodes"] if node["node_index"]] == ["e2e4", "e7e5"]
    assert game_evidence.read_game_version(initialized_conn, game_id, previous["id"]) == previous
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 0
    assert require_row(initialized_conn.execute("SELECT parser_version FROM raw_payloads WHERE id=%s", (raw_id,)))[0] == games.PARSER_VERSION
    progress = require_row(initialized_conn.execute("SELECT * FROM data_upgrades WHERE id='native-evidence-upgrade'"))
    assert progress["state"] == "done" and progress["processed"] == 1
    assert progress["parser_version"] == manifest and progress["last_raw_id"] == raw_id
