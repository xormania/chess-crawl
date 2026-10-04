"""Evidence retention, immutable revisions, precise clocks and local replay."""
from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path

import pytest
import psycopg

from chess_crawl.normalize.game_evidence import parse_game_evidence
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.providers.lichess.parser import parse_game
from chess_crawl.storage.db import Connection, require_row, transaction
from chess_crawl.storage.game_evidence import export_game_version_pgn, read_game_version
from chess_crawl.storage.migrations import _execute_schema, initialize, migration_resources
from chess_crawl.storage.raw import insert_fetch_log, read_raw_payload, store_raw_payload


def _store(conn: Connection, data: dict, fetched_at: int = 123) -> tuple[int, int]:
    raw_id = store_raw_payload(conn, RawRecord(
        provider="lichess", endpoint_type="game", request_url="https://lichess.org/api/game/evidence",
        canonical_source_key="lichess/game/evidence", fetched_at=fetched_at,
        body=json.dumps(data).encode(), media_type="application/json",
    ))
    game_id = normalize_games_payload(conn, raw_id)[0]
    return raw_id, game_id


def _data(pgn: str, **fields: object) -> dict:
    return {"id": "evidence", "variant": "standard", "status": "started", "pgn": pgn,
            "players": {"white": {"user": {"id": "alice"}}, "black": {"user": {"id": "bob"}}},
            **fields}


def test_tree_headers_comments_unknown_annotations_and_exact_clocks(fixtures_dir: Path) -> None:
    pgn = (fixtures_dir / "pgn/evidence.pgn").read_text()
    evidence = parse_game_evidence(parse_game(_data(pgn)))
    assert evidence.parse_status == "complete"
    assert evidence.headers["UnfamiliarTag"] == "preserved value"
    assert evidence.played_ply_count == 6
    played = [node for node in evidence.nodes if node.is_mainline and node.node_index]
    assert [node.move_uci for node in played] == ["e2e4", "e7e5", "g1f3", "b8c6", "f1b5", "a7a6"]
    variations = [node for node in evidence.nodes if not node.is_mainline]
    assert [node.move_san for node in variations] == ["d4", "d5", "Nf6"]
    assert variations[2].parent_index == variations[0].node_index
    assert "Keep this whole comment" in "".join(played[-3].comments)
    assert evidence.nodes[0].comments == ["Before play"]
    assert played[0].nags == [1]
    assert {item["command"] for item in played[0].annotations} == {"clk", "emt", "eval", "custom"}
    clocks = evidence.clocks
    assert [(clock.kind, clock.seconds, clock.status) for clock in clocks] == [
        ("remaining", Decimal("299.990"), "parsed"), ("elapsed", Decimal("0.010"), "parsed"),
        ("remaining", Decimal(0), "parsed"), ("remaining", None, "invalid"),
    ]
    assert clocks[0].precision_seconds == Decimal("0.001")
    assert evidence.time_control_rules["periods"] == [
        {"moves": 40, "seconds": "7200", "increment_seconds": None, "raw_text": "40/7200"},
        {"moves": None, "seconds": "3600", "increment_seconds": "30", "raw_text": "3600+30"},
    ]


def test_database_roundtrip_replay_and_native_clock_conflicts(initialized_conn: Connection, fixtures_dir: Path) -> None:
    conn = initialized_conn
    pgn = (fixtures_dir / "pgn/evidence.pgn").read_text()
    raw_id, game_id = _store(conn, _data(pgn, clocks=[29998, 0, None], analysis=[{"eval": 25, "depth": 20}],
                                      unfamiliar={"retained": True}))
    version = read_game_version(conn, game_id)
    assert version is not None
    assert version["source_metadata"]["unfamiliar"] == {"retained": True}
    assert "pgn" not in version["source_metadata"]
    assert version["played_ply_count"] == 6
    assert version["clocks"][0]["seconds"] == "299.990"
    provider_clocks = [clock for clock in version["clocks"] if clock["source"] == "provider"]
    assert [clock["seconds"] for clock in provider_clocks] == ["299.98", "0", None]
    assert provider_clocks[-1]["status"] == "invalid"
    assert version["derived_timings"] == []
    assert version["sources"][0]["json_pointer"] == ""
    exported = export_game_version_pgn(conn, game_id)
    roundtrip = parse_game_evidence(parse_game(_data(exported)))
    assert roundtrip.parse_status == "complete"
    assert roundtrip.headers == version["headers"]
    assert [(n.move_uci, n.parent_index, n.nags, n.comments) for n in roundtrip.nodes] == [
        (n["move_uci"], n["parent_index"], n["nags"], n["comments"]) for n in version["nodes"]
    ]
    normalize_games_payload(conn, raw_id)
    assert require_row(conn.execute("SELECT COUNT(*) FROM game_versions"))[0] == 1
    assert read_raw_payload(conn, raw_id).body == json.dumps(_data(
        pgn, clocks=[29998, 0, None], analysis=[{"eval": 25, "depth": 20}], unfamiliar={"retained": True})).encode()


def test_revisions_preserve_old_inputs_and_old_replay_cannot_regress_current(initialized_conn: Connection) -> None:
    conn = initialized_conn
    old_raw, game_id = _store(conn, _data("1. e4 e5 *", status="started"), fetched_at=100)
    old = read_game_version(conn, game_id)
    newer_raw, same_game = _store(conn, _data("1. e4 e5 2. Nf3 *", status="resign", winner="white"), fetched_at=200)
    newer = read_game_version(conn, game_id)
    assert same_game == game_id and old is not None and newer is not None
    assert old["id"] != newer["id"]
    normalize_games_payload(conn, old_raw)
    assert read_game_version(conn, game_id)["id"] == newer["id"]  # type: ignore[index]
    assert require_row(conn.execute("SELECT status_raw FROM games WHERE id = %s", (game_id,)))[0] == "resign"
    assert read_game_version(conn, game_id, old["id"])["played_ply_count"] == 2  # type: ignore[index]
    assert export_game_version_pgn(conn, game_id, old["id"]).strip() == "1 . e4 e5 *"
    assert newer_raw != old_raw


def test_repeated_old_body_can_be_current_through_new_fetch_evidence(initialized_conn: Connection) -> None:
    conn = initialized_conn
    old_raw, game_id = _store(conn, _data("1. e4 *"), 100)
    first = read_game_version(conn, game_id)
    _store(conn, _data("1. d4 *"), 200)
    insert_fetch_log(conn, provider="lichess", job_id=None, crawl_run_id=None,
                     url="https://lichess.org/api/game/evidence", endpoint_type="game",
                     status_code=304, raw_payload_id=old_raw, attempted_at=300)
    normalize_games_payload(conn, old_raw)
    assert first is not None and read_game_version(conn, game_id)["id"] == first["id"]  # type: ignore[index]
    assert require_row(conn.execute("SELECT COUNT(*) FROM game_versions"))[0] == 2


@pytest.mark.parametrize("variant", ["chess960", "atomic", "crazyhouse", "bughouse"])
def test_unsupported_variant_retains_notation_without_inventing_board_fields(
    initialized_conn: Connection, variant: str,
) -> None:
    pgn = f'[Variant "{variant}"]\n\n1. e4 {{[%clk 0:00:00]}} (1. d4) e5 *'
    _, game_id = _store(initialized_conn, _data(pgn, variant=variant))
    version = read_game_version(initialized_conn, game_id)
    assert version is not None and version["parse_status"] == "unsupported"
    assert [n["move_san"] for n in version["nodes"][1:]] == ["e4", "d4", "e5"]
    assert all(n["move_uci"] is None and n["fen_after"] is None for n in version["nodes"])
    assert version["clocks"][0]["seconds"] == "0"
    with pytest.raises(ValueError, match="partial export"):
        export_game_version_pgn(initialized_conn, game_id)
    assert "[%clk 0:00:00]" in export_game_version_pgn(initialized_conn, game_id, allow_partial=True)


def test_custom_position_black_start_and_promotion() -> None:
    data = _data('[SetUp "1"]\n[FEN "4k3/8/8/8/8/8/p7/4K3 b - - 0 20"]\n\n20... a1=Q+ *')
    evidence = parse_game_evidence(parse_game(data))
    assert evidence.parse_status == "complete"
    assert evidence.nodes[1].mover == "black"
    assert evidence.nodes[1].move_uci == "a2a1q"
    assert evidence.nodes[1].move_san == "a1=Q+"
    assert evidence.nodes[1].fen_before == data["pgn"].split('"')[3]


def test_provider_moves_without_pgn_and_unmapped_extra_clocks() -> None:
    data = _data("", moves="e4 e5", clocks=[30000, 29999, 100], initialFen=INITIAL_FEN)
    evidence = parse_game_evidence(parse_game(data))
    assert evidence.parse_status == "complete"
    assert evidence.move_text_origin == "provider.moves"
    assert evidence.played_ply_count == 2
    assert evidence.clocks[-1].node_index is None and evidence.clocks[-1].status == "unmapped"


def test_malformed_and_unknown_tokens_are_queryable_and_never_certified_complete(initialized_conn: Connection) -> None:
    pgn = '[Custom "x"]\n\n1. e4 nonsense e5 {[%custom unchanged]} (2. Nf3 *'
    _, game_id = _store(initialized_conn, _data(pgn))
    version = read_game_version(initialized_conn, game_id)
    assert version is not None and version["parse_status"] == "partial"
    tokens = [row[0] for row in initialized_conn.execute("SELECT token_text FROM game_pgn_tokens")]
    assert "nonsense" in "".join(tokens)
    assert "[%custom unchanged]" in export_game_version_pgn(initialized_conn, game_id, allow_partial=True)
    assert version["parse_issues"]


def test_decimal_clock_precision_is_not_limited_by_python_decimal_context(initialized_conn: Connection) -> None:
    value = "0.12345678901234567890123456789012345678901234567890"
    _, game_id = _store(initialized_conn, _data(f"1. e4 {{[%clk 0:00:00.{value.split('.')[1]}]}} *"))
    revision = read_game_version(initialized_conn, game_id)
    assert revision is not None
    assert revision["clocks"][0]["seconds"] == value
    assert Decimal(revision["clocks"][0]["precision_seconds"]) == Decimal("1E-50")


def test_utf8_bom_and_native_origin_key_do_not_lose_source_evidence(initialized_conn: Connection) -> None:
    pgn = '\ufeff[Event "BOM"]\n\n1. e4 *'
    _, game_id = _store(initialized_conn, _data(pgn, _move_text_origin="provider-supplied-value"))
    revision = read_game_version(initialized_conn, game_id)
    assert revision is not None and revision["parse_status"] == "complete"
    assert revision["source_metadata"]["_move_text_origin"] == "provider-supplied-value"
    assert export_game_version_pgn(initialized_conn, game_id).startswith('\ufeff[Event "BOM"]')


def test_clocks_without_any_move_text_are_preserved_as_unmapped(initialized_conn: Connection) -> None:
    _, game_id = _store(initialized_conn, _data("", clocks=[30000, 0, None], unfamiliar="retained"))
    revision = read_game_version(initialized_conn, game_id)
    assert revision is not None and revision["parse_status"] == "unavailable"
    assert [item["seconds"] for item in revision["clocks"]] == ["300", "0", None]
    assert [item["status"] for item in revision["clocks"]] == ["unmapped", "unmapped", "invalid"]
    assert all(item["node_index"] is None for item in revision["clocks"])
    assert revision["source_metadata"]["clocks"] == [30000, 0, None]


def test_grammar_gaps_and_trailing_fragments_survive_database_export(
    initialized_conn: Connection, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import chess_crawl.normalize.game_evidence as module
    grammar = module.import_format

    class FutureGrammar:
        # Exercise preservation if a dependency revision stops recognizing
        # a source fragment. Legality is still checked by the real parser.
        @staticmethod
        def finditer(text: str, start: int = 0):
            return (match for match in grammar.finditer(text, start)
                    if match.group().strip() not in {"UNHANDLED", "TRAILING"})

    monkeypatch.setattr(module, "import_format", FutureGrammar())
    _, game_id = _store(initialized_conn, _data("1. e4 UNHANDLED e5 * TRAILING"))
    revision = read_game_version(initialized_conn, game_id)
    assert revision is not None and revision["parse_status"] == "partial"
    exported = export_game_version_pgn(initialized_conn, game_id, allow_partial=True)
    assert "UNHANDLED" in exported and "TRAILING" in exported
    gaps = list(initialized_conn.execute(
        "SELECT kind,token_text FROM game_pgn_tokens WHERE kind IN ('unknown_gap','unknown_tail') ORDER BY token_index"))
    assert [row["kind"] for row in gaps] == ["unknown_gap", "unknown_tail"]
    assert "UNHANDLED" in gaps[0]["token_text"] and "TRAILING" in gaps[1]["token_text"]


@pytest.mark.parametrize("statement", [
    "UPDATE game_versions SET headers = '{}'",
    "UPDATE game_move_nodes SET move_san = 'faked'",
    "UPDATE game_clock_observations SET seconds = 123",
    "UPDATE game_pgn_tokens SET token_text = 'changed'",
    "DELETE FROM game_clock_observations",
    "DELETE FROM game_pgn_tokens",
])
def test_evidence_is_physically_immutable(initialized_conn: Connection, statement: str) -> None:
    _store(initialized_conn, _data("1. e4 {[%clk 0:05:00]} *"))
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        with transaction(initialized_conn):
            initialized_conn.execute(statement)
    assert require_row(initialized_conn.execute("SELECT seconds FROM game_clock_observations"))[0] == 300


def test_unreferenced_version_removal_can_cascade(initialized_conn: Connection) -> None:
    _, game_id = _store(initialized_conn, _data("1. e4 {[%clk 0:05:00]} *"))
    with transaction(initialized_conn):
        initialized_conn.execute("UPDATE games SET current_version_id = NULL WHERE id = %s", (game_id,))
        initialized_conn.execute("DELETE FROM game_versions WHERE game_id = %s", (game_id,))
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM game_versions"))[0] == 0


def test_committed_evidence_cannot_gain_extra_tokens(initialized_conn: Connection) -> None:
    _, game_id = _store(initialized_conn, _data("1. e4 *"))
    version = read_game_version(initialized_conn, game_id)
    assert version is not None
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        with transaction(initialized_conn):
            initialized_conn.execute(
                "INSERT INTO game_pgn_tokens(version_id,token_index,kind,token_text,start_offset,end_offset,interpretation_status) "
                "VALUES (%s,999,'unknown','new evidence',0,0,'preserved')", (version["id"],))


def test_upgrade_populated_v5_archive_then_replay_without_network(uninitialized_database_url: str) -> None:
    from chess_crawl.storage.db import connection
    from support import seed_game

    with connection(uninitialized_database_url, mode="rwc") as conn:
        with transaction(conn):
            for version, name, resource in migration_resources():
                if version > 5:
                    break
                from importlib import resources
                _execute_schema(conn, resources.files("chess_crawl.storage").joinpath(resource).read_text())
                conn.execute("INSERT INTO schema_migrations VALUES (%s,%s,123)", (version, name))
        seed_game(conn, provider="lichess", game_key="legacy", white="Alice", black="Bob")
        assert initialize(conn).version >= 6
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 1
        raw, game = _store(conn, _data("1. e4 e5 *"))
        assert read_game_version(conn, game)["played_ply_count"] == 2  # type: ignore[index]
        assert read_raw_payload(conn, raw).body


INITIAL_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"
