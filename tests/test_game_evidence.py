"""Evidence retention, immutable revisions, precise clocks and local replay."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from decimal import Decimal
import json
from pathlib import Path
from threading import Event

import pytest
import psycopg

from chess_crawl.normalize.game_evidence import parse_game_evidence
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.providers.lichess.parser import parse_game
from chess_crawl.storage.db import Connection, connection, require_row, transaction
from chess_crawl.jobs import state
from chess_crawl.storage.acquisition import run_game_ids
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


@pytest.mark.parametrize("status_code", [200, 304], ids=["deduplicated-200", "cached-304"])
def test_same_run_recurring_body_refreshes_current_inputs_without_consuming_capacity(
    initialized_conn: Connection, status_code: int,
) -> None:
    conn = initialized_conn
    run_id = state.create_crawl_run(conn, provider="lichess", seed_spec="alice", params={"max_games": 1})
    source = "https://lichess.org/api/game/evidence"
    original = _data("1. e4 {[%clk 0:04:59]} *", status="started", clocks=[29900],
                     createdAt=None, lastMoveAt=None, opening={"eco": None, "name": None, "ply": None})

    def observe(data: dict, observed_at: int, *, http_status: int = 200) -> int:
        raw_id = store_raw_payload(conn, RawRecord(
            provider="lichess", endpoint_type="game", request_url=source,
            canonical_source_key="lichess/game/evidence", fetched_at=observed_at,
            body=json.dumps(data).encode(), media_type="application/json",
        ))
        insert_fetch_log(conn, provider="lichess", url=source, endpoint_type="game",
                         attempted_at=observed_at, status_code=http_status, raw_payload_id=raw_id,
                         crawl_run_id=run_id)
        return raw_id

    first_raw = observe(original, 100)
    game_id = normalize_games_payload(conn, first_raw, crawl_run_id=run_id, max_games=1)[0]
    first = read_game_version(conn, game_id)
    assert first is not None
    second_raw = observe(_data("1. e4 e5 *", status="resign", winner="white", clocks=[25000, 20000],
                               createdAt=1000, lastMoveAt=2000,
                               opening={"eco": "C20", "name": "King's Pawn Game", "ply": 2}), 200)
    assert normalize_games_payload(conn, second_raw, crawl_run_id=run_id, max_games=0) == []
    second = read_game_version(conn, game_id)
    assert second is not None and second["id"] != first["id"]
    # Offline replay supplies no new observation and must preserve the later B.
    assert normalize_games_payload(conn, first_raw, crawl_run_id=run_id, max_games=0) == []
    assert read_game_version(conn, game_id)["id"] == second["id"]  # type: ignore[index]
    assert require_row(conn.execute("SELECT status_raw FROM games WHERE id=%s", (game_id,)))[0] == "resign"
    assert require_row(conn.execute("SELECT ended_at FROM games WHERE id=%s", (game_id,)))[0] == 2
    if status_code == 200:
        assert observe(original, 300) == first_raw
    else:
        insert_fetch_log(conn, provider="lichess", url=source, endpoint_type="game",
                         attempted_at=300, status_code=304, raw_payload_id=first_raw,
                         crawl_run_id=run_id)
    assert normalize_games_payload(conn, first_raw, crawl_run_id=run_id, max_games=0) == []
    current = read_game_version(conn, game_id)
    assert current is not None and current["id"] == first["id"]
    assert current["clocks"] == first["clocks"]
    row = require_row(conn.execute(
        "SELECT status_raw,outcome,is_live,ply_count,created_at,ended_at,eco,opening_name,opening_ply "
        "FROM games WHERE id=%s", (game_id,),
    ))
    assert tuple(row.values()) == ("started", None, 1, 1, None, None, None, None, None)
    assert all(row["result_raw"] is None and row["is_winner"] is None for row in conn.execute(
        "SELECT result_raw,is_winner FROM game_participants WHERE game_id=%s", (game_id,),
    ))
    assert run_game_ids(conn, run_id) == {game_id}
    assert require_row(conn.execute("SELECT COUNT(*) FROM game_versions"))[0] == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 2


def test_sparse_current_observation_preserves_omitted_nullable_facts(initialized_conn: Connection) -> None:
    conn = initialized_conn
    _, game_id = _store(conn, _data("1. e4 e5 *", status="resign", winner="white",
                                  createdAt=1000, lastMoveAt=2000,
                                  opening={"eco": "C20", "name": "King's Pawn Game", "ply": 2}), 100)
    _store(conn, _data("1. e4 e5 2. Nf3 *", status="resign", winner="white"), 200)
    columns = "created_at,ended_at,eco,opening_name,opening_ply"
    assert tuple(require_row(conn.execute(f"SELECT {columns} FROM games WHERE id=%s", (game_id,))).values()) == (
        1, 2, "C20", "King's Pawn Game", 2,
    )
    _store(conn, _data("1. e4 e5 2. Nf3 Nc6 *", status="resign", winner="white",
                      createdAt="unavailable", lastMoveAt="unavailable", opening={"ply": "unknown"}), 250)
    assert tuple(require_row(conn.execute(f"SELECT {columns} FROM games WHERE id=%s", (game_id,))).values()) == (
        1, 2, "C20", "King's Pawn Game", 2,
    )
    assert read_game_version(conn, game_id)["source_metadata"]["createdAt"] == "unavailable"  # type: ignore[index]
    # Explicit nulls clear known fields; omitted opening members remain sparse.
    _store(conn, _data("1. e4 e5 2. Nf3 Nc6 *", status="resign", winner="white",
                      createdAt=None, lastMoveAt=None, opening={"eco": None}), 300)
    assert tuple(require_row(conn.execute(f"SELECT {columns} FROM games WHERE id=%s", (game_id,))).values()) == (
        None, None, None, "King's Pawn Game", 2,
    )
    _store(conn, _data("1. e4 e5 2. Nf3 Nc6 3. Bb5 *", status="resign", winner="white", opening=None), 400)
    assert tuple(require_row(conn.execute(f"SELECT {columns} FROM games WHERE id=%s", (game_id,))).values()) == (
        None, None, None, None, None,
    )


def test_live_observation_clears_stale_end_even_when_native_end_is_omitted(initialized_conn: Connection) -> None:
    conn = initialized_conn
    _, game_id = _store(conn, _data("1. e4 e5 *", status="resign", winner="white", lastMoveAt=2000), 100)
    _store(conn, _data("1. e4 *", status="started"), 200)
    row = require_row(conn.execute("SELECT is_live,ended_at FROM games WHERE id=%s", (game_id,)))
    assert tuple(row.values()) == (1, None)


def test_chesscom_recurring_null_fields_replace_completed_facts(initialized_conn: Connection) -> None:
    conn = initialized_conn
    original = {"uuid": "nullable", "url": "https://www.chess.com/game/live/nullable",
                "rules": "chess", "pgn": "1. e4 *", "start_time": None, "end_time": None, "eco": None,
                "white": {"username": "alice", "result": None}, "black": {"username": "bob", "result": None}}
    completed = {**original, "pgn": "1. e4 e5 *", "start_time": 1, "end_time": 2, "eco": "C20",
                 "white": {"username": "alice", "result": "win"}, "black": {"username": "bob", "result": "resigned"}}

    def observe(data: dict, now: int) -> int:
        raw = store_raw_payload(conn, RawRecord(
            provider="chess.com", endpoint_type="monthly_archive",
            request_url="https://api.chess.com/pub/player/alice/games/2024/01",
            canonical_source_key="chess.com/player/alice/games/2024/01", fetched_at=now,
            body=json.dumps({"games": [data]}).encode(), media_type="application/json",
        ))
        insert_fetch_log(conn, provider="chess.com", endpoint_type="monthly_archive", status_code=200,
                         url="https://api.chess.com/pub/player/alice/games/2024/01",
                         attempted_at=now, raw_payload_id=raw)
        return normalize_games_payload(conn, raw)[0]

    game_id = observe(original, 100)
    assert observe(completed, 200) == game_id
    assert observe(original, 300) == game_id
    row = require_row(conn.execute("SELECT created_at,ended_at,eco FROM games WHERE id=%s", (game_id,)))
    assert tuple(row.values()) == (None, None, None)
    assert all(row["result_raw"] is None for row in conn.execute(
        "SELECT result_raw FROM game_participants WHERE game_id=%s", (game_id,),
    ))


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


@pytest.mark.parametrize("operation", ["read", "export"])
@pytest.mark.parametrize("inherited", [False, True], ids=["autocommit", "inherited-read-committed"])
def test_evidence_snapshot_survives_concurrent_current_switch_and_version_removal(
    database_url: str, monkeypatch: pytest.MonkeyPatch, operation: str, inherited: bool,
) -> None:
    selected, committed = Event(), Event()
    with connection(database_url, mode="rw") as setup:
        _, game_id = _store(setup, _data("1. e4 {[%clk 0:00:00.000000001]} *"), 100)
        before = read_game_version(setup, game_id)
        assert before is not None
        version_id = before["id"]
        with transaction(setup):
            setup.execute(
                "INSERT INTO game_derived_timings(version_id,node_index,method_version,seconds,"
                "input_observation_ids,assumptions) VALUES(%s,1,'snapshot-test',0.000000001,'[]','{}')",
                (version_id,),
            )
        before = read_game_version(setup, game_id)
        exported = export_game_version_pgn(setup, game_id)

    def replace_and_remove() -> None:
        try:
            assert selected.wait(10), "reader did not reach its selected-version barrier"
            with connection(database_url, mode="rw") as writer:
                _store(writer, _data("1. d4 d5 *", status="resign", winner="white"), 200)
                # Derived calculations are independently removable; then the
                # unreferenced immutable version can cascade to its evidence.
                with transaction(writer):
                    writer.execute("DELETE FROM game_derived_timings WHERE version_id=%s", (version_id,))
                    writer.execute("DELETE FROM game_versions WHERE id=%s", (version_id,))
                assert require_row(writer.execute("SELECT COUNT(*) FROM game_versions WHERE id=%s", (version_id,)))[0] == 0
        finally:
            committed.set()

    with ThreadPoolExecutor(max_workers=1) as pool, connection(database_url, mode="rw") as reader:
        future = pool.submit(replace_and_remove)
        original_execute = reader.execute

        def pause_after_selection(query, *args, **kwargs):
            cursor = original_execute(query, *args, **kwargs)
            if "FROM game_versions v JOIN games g" in str(query) and not selected.is_set():
                # PostgreSQL has returned the selected-version query. Allow a
                # different connection to commit deletion before fetching the
                # remaining evidence or reconstructing the export.
                selected.set()
                assert committed.wait(10), "concurrent writer did not finish"
                future.result(timeout=1)
            return cursor

        monkeypatch.setattr(reader, "execute", pause_after_selection)
        # Native outer READ COMMITTED intentionally supplies no archive write
        # lock. The read must remain coherent without upgrading its caller.
        with reader.transaction() if inherited else nullcontext():
            if inherited:
                assert require_row(reader.execute("SHOW transaction_isolation"))[0] == "read committed"
            if operation == "read":
                assert read_game_version(reader, game_id) == before
            else:
                assert export_game_version_pgn(reader, game_id) == exported
        assert selected.is_set() and not reader.in_transaction
        future.result(timeout=1)
        fresh = read_game_version(reader, game_id)
        assert fresh is not None and fresh["id"] != version_id
        assert read_game_version(reader, game_id, version_id) is None


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


def _parallel_notation_data(provider: str) -> dict:
    if provider == "lichess":
        return _data("1. e4 e5 *", moves="  d4 d5  ")
    return {"uuid": "parallel-notation", "url": "https://www.chess.com/game/live/parallel-notation",
            "rules": "chess", "pgn": "1. e4 e5 *", "moves": "  d4 d5  ",
            "white": {"username": "alice"}, "black": {"username": "bob"}}


@pytest.mark.parametrize("provider", ["lichess", "chess.com"])
def test_format_parser_preserves_native_moves_with_pgn(provider: str) -> None:
    from chess_crawl.providers.chesscom.parser import parse_game as parse_chesscom_game

    data = _parallel_notation_data(provider)
    parser = parse_game if provider == "lichess" else parse_chesscom_game
    evidence = parse_game_evidence(parser(data))
    assert evidence.move_text_origin == "pgn"
    assert evidence.source_metadata["moves"] == "  d4 d5  "
    assert [node.move_uci for node in evidence.nodes if node.node_index] == ["e2e4", "e7e5"]
    assert [token["token_text"] for token in evidence.tokens if token["kind"] == "move"] == ["e4", "e5"]


@pytest.mark.parametrize("provider", ["lichess", "chess.com"])
def test_database_preserves_native_moves_with_pgn(initialized_conn: Connection, provider: str) -> None:
    data = _parallel_notation_data(provider)
    payload = data if provider == "lichess" else {"games": [data]}
    raw_id = store_raw_payload(initialized_conn, RawRecord(
        provider=provider, endpoint_type="game" if provider == "lichess" else "monthly_archive",
        request_url="https://lichess.org/api/game/evidence" if provider == "lichess" else data["url"],
        canonical_source_key=provider + "/parallel-notation", fetched_at=123,
        body=json.dumps(payload).encode(), media_type="application/json",
    ))
    game_id = normalize_games_payload(initialized_conn, raw_id)[0]
    version = read_game_version(initialized_conn, game_id)
    assert version is not None and version["move_text_origin"] == "pgn"
    assert version["source_metadata"]["moves"] == "  d4 d5  "
    assert [node["move_uci"] for node in version["nodes"] if node["node_index"]] == ["e2e4", "e7e5"]
    assert [row[0] for row in initialized_conn.execute(
        "SELECT token_text FROM game_pgn_tokens WHERE version_id=%s AND kind='move' ORDER BY token_index",
        (version["id"],),
    )] == ["e4", "e5"]
    assert "d4" not in export_game_version_pgn(initialized_conn, game_id)


def test_unused_nontext_native_moves_are_retained() -> None:
    evidence = parse_game_evidence(parse_game(_data("", moves=["e4", "e5"])))
    assert evidence.move_text_origin == "unavailable"
    assert evidence.source_metadata["moves"] == ["e4", "e5"]


def test_offline_replay_repairs_native_move_metadata_without_mutating_old_revision(
    initialized_conn: Connection, monkeypatch,
) -> None:
    from chess_crawl.normalize import games as games_normalizer
    from chess_crawl.storage import game_evidence as stored_evidence

    def old_evidence(game):
        evidence = parse_game_evidence(game)
        evidence.source_metadata.pop("moves", None)
        return evidence

    with monkeypatch.context() as old:
        old.setattr(stored_evidence, "EVIDENCE_VERSION", "game-evidence-v1")
        old.setattr(games_normalizer, "parse_game_evidence", old_evidence, raising=False)
        old.setattr(games_normalizer, "PARSER_VERSION", "games-normalizer-v5/game-evidence-v1")
        raw_id, game_id = _store(initialized_conn, _parallel_notation_data("lichess"))
    previous = read_game_version(initialized_conn, game_id)
    assert previous is not None and "moves" not in previous["source_metadata"]
    assert normalize_games_payload(initialized_conn, raw_id) == [game_id]
    repaired = read_game_version(initialized_conn, game_id)
    assert repaired is not None and repaired["id"] != previous["id"]
    assert repaired["source_metadata"]["moves"] == "  d4 d5  "
    assert read_game_version(initialized_conn, game_id, previous["id"]) == previous
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 0
