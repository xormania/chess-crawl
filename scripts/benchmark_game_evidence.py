"""Measure evidence parsing and optional disposable PostgreSQL workloads.

This is a contributor benchmark, not a product interface. Database mode creates
and drops only its own random database; use a dedicated test administrator.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import time
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from chess_crawl.normalize.game_evidence import EVIDENCE_VERSION, parse_game_evidence
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.providers.lichess.parser import parse_game
from chess_crawl.storage.db import open_database, require_row
from chess_crawl.storage.game_evidence import read_game_version
from chess_crawl.storage.raw import store_raw_payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pgn", type=Path, default=Path(__file__).resolve().parents[1] / "tests/fixtures/pgn/evidence.pgn")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--games", type=int, default=100)
    parser.add_argument("--database", action="store_true", help="Use CHESS_CRAWL_TEST_DATABASE_URL for a disposable database")
    args = parser.parse_args()
    if args.iterations < 1 or args.games < 1:
        parser.error("iterations and games must be positive")
    pgn = args.pgn.read_text(encoding="utf-8")
    game = parse_game({"id": "benchmark", "pgn": pgn, "variant": "standard"})
    samples = []
    evidence = parse_game_evidence(game)
    for _ in range(args.iterations):
        start = time.perf_counter()
        parse_game_evidence(game)
        samples.append(time.perf_counter() - start)
    report: dict[str, object] = {
        "evidence_version": EVIDENCE_VERSION, "pgn_bytes": len(pgn.encode()),
        "played_plies": evidence.played_ply_count, "move_nodes": len(evidence.nodes),
        "clock_observations": len(evidence.clocks), "interpretation_status": evidence.parse_status,
        "iterations": args.iterations, "parse_median_seconds": statistics.median(samples),
        "parse_total_seconds": sum(samples),
    }
    if args.database:
        report.update(_database_benchmark(pgn, args.games))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _database_benchmark(pgn: str, count: int) -> dict[str, object]:
    target = os.getenv("CHESS_CRAWL_TEST_DATABASE_URL")
    if not target:
        raise ValueError("Set CHESS_CRAWL_TEST_DATABASE_URL to a disposable test administrator")
    options = conninfo_to_dict(target)
    if options.get("service"):
        raise ValueError("Benchmark administrator must use explicit settings, not a libpq service")
    password = os.getenv("CHESS_CRAWL_TEST_DATABASE_PASSWORD")
    if password is not None:
        options["password"] = password
    name = "chess_crawl_benchmark_" + uuid4().hex
    with psycopg.connect(make_conninfo("", **options), autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
        try:
            options["dbname"] = name
            # Password remains in a connection setting, never in benchmark output.
            with open_database(make_conninfo("", **options), writable=True) as conn:
                bodies = [{"id": f"benchmark-{index}", "pgn": pgn, "variant": "standard"} for index in range(count)]
                raw_id = store_raw_payload(conn, RawRecord(
                    provider="lichess", endpoint_type="user_games_stream",
                    request_url="https://example.test/benchmark", canonical_source_key="benchmark",
                    fetched_at=123, body=b"\n".join(json.dumps(body).encode() for body in bodies),
                    media_type="application/x-ndjson",
                ))
                start = time.perf_counter()
                ids = normalize_games_payload(conn, raw_id)
                imported = time.perf_counter() - start
                start = time.perf_counter()
                normalize_games_payload(conn, raw_id)
                replayed = time.perf_counter() - start
                start = time.perf_counter()
                revision = read_game_version(conn, ids[0])
                read_time = time.perf_counter() - start
                row = require_row(conn.execute(
                    "SELECT position_key_before FROM game_move_nodes WHERE version_id = "
                    "(SELECT current_version_id FROM games WHERE id = %s) AND node_index = 1", (ids[0],)))
                start = time.perf_counter()
                occurrences = require_row(conn.execute(
                    "SELECT COUNT(*) FROM game_move_nodes WHERE position_key_before = %s AND is_mainline AND node_index > 0",
                    (row[0],)))[0]
                position_time = time.perf_counter() - start
                size = require_row(conn.execute(
                    "SELECT SUM(pg_total_relation_size(table_name::regclass)) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"))[0]
                return {"imported_games": len(ids), "import_seconds": imported, "replay_seconds": replayed,
                        "game_read_seconds": read_time, "position_query_seconds": position_time,
                        "position_occurrences": occurrences, "archive_relation_bytes": int(size),
                        "read_version_exists": revision is not None}
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


if __name__ == "__main__":
    raise SystemExit(main())
