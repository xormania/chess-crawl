from __future__ import annotations

import csv
import json
from pathlib import Path

from chess_crawl import cli
from chess_crawl.export.writers import export_games_jsonl, export_graph_csv, export_users_jsonl
from chess_crawl.jobs.discovery import OpponentEdge, record_discovery_edges
from chess_crawl.reports.queries import games_by_month, opponent_report, summary_report, user_game_summary
from chess_crawl.storage.db import connect
from chess_crawl.storage.migrations import initialize, initialize_database
from chess_crawl.storage.repository import (
    get_or_create_time_control,
    get_or_create_variant,
    upsert_game,
    upsert_game_participant,
    upsert_provider_user,
)


def _seed_game(
    conn,
    *,
    provider: str,
    game_key: str,
    white: str,
    black: str,
    outcome: str | None,
    ended_at: int = 1704067200,
) -> tuple[int, int, int]:
    white_id = upsert_provider_user(conn, provider=provider, username=white)
    black_id = upsert_provider_user(conn, provider=provider, username=black)
    variant_id = get_or_create_variant(
        conn,
        provider=provider,
        provider_native_name="standard",
        canonical_name="standard",
    )
    time_control_id = get_or_create_time_control(
        conn,
        kind="clock",
        initial_seconds=300,
        increment_seconds=0,
        days=None,
        time_class="blitz",
        raw_label="300",
    )
    game_id = upsert_game(
        conn,
        provider=provider,
        provider_game_id=game_key,
        canonical_url=f"https://example.test/{provider}/{game_key}",
        content_hash=f"sha256:{provider}:{game_key}",
        variant_id=variant_id,
        time_control_id=time_control_id,
        rated=True,
        outcome=outcome,
        ended_at=ended_at,
    )
    upsert_game_participant(conn, game_id=game_id, color="white", provider_user_id=white_id, username_normalized=white.lower())
    upsert_game_participant(conn, game_id=game_id, color="black", provider_user_id=black_id, username_normalized=black.lower())
    conn.commit()
    return game_id, white_id, black_id


def test_reports_are_null_outcome_aware_and_provider_scoped() -> None:
    conn = connect(":memory:")
    initialize(conn)
    _seed_game(conn, provider="chess.com", game_key="cc-1", white="SameName", black="Opponent", outcome=None)
    _seed_game(conn, provider="lichess", game_key="li-1", white="SameName", black="Opponent", outcome="white_win")

    chess_user = user_game_summary(conn, "chess.com", "SameName")
    lichess_user = user_game_summary(conn, "lichess", "SameName")

    assert chess_user is not None
    assert lichess_user is not None
    assert chess_user["provider"] == "chess.com"
    assert chess_user["games"] == 1
    assert chess_user["wins"] == 0
    assert chess_user["unfinished"] == 1
    assert lichess_user["provider"] == "lichess"
    assert lichess_user["wins"] == 1

    opponents = opponent_report(conn, "chess.com", "SameName")
    assert opponents is not None
    assert [(row["provider"], row["opponent_username"], row["unfinished"]) for row in opponents] == [
        ("chess.com", "opponent", 1)
    ]

    months = games_by_month(conn, provider="chess.com")
    assert [(row["month"], row["games"], row["unfinished"]) for row in months] == [("2024-01", 1, 1)]


def test_exports_preserve_provider_and_omit_raw_payloads(tmp_path: Path) -> None:
    conn = connect(":memory:")
    initialize(conn)
    game_id, same_id, opponent_id = _seed_game(
        conn,
        provider="chess.com",
        game_key="cc-1",
        white="SameName",
        black="Opponent",
        outcome="white_win",
    )
    _seed_game(conn, provider="lichess", game_key="li-1", white="SameName", black="Opponent", outcome="black_win")
    record_discovery_edges(
        conn,
        crawl_run_id=None,
        provider="chess.com",
        from_user_id=same_id,
        depth=1,
        edges=[OpponentEdge(opponent_id, "opponent", game_id, 1)],
    )

    games_path = tmp_path / "games.jsonl"
    users_path = tmp_path / "users.jsonl"
    graph_path = tmp_path / "graph.csv"

    assert export_games_jsonl(conn, output=games_path) == 2
    assert export_users_jsonl(conn, output=users_path) == 4
    assert export_graph_csv(conn, output=graph_path) == 1

    game_rows = [json.loads(line) for line in games_path.read_text().splitlines()]
    assert {row["provider"] for row in game_rows} == {"chess.com", "lichess"}
    assert all("raw_body" not in row for row in game_rows)

    user_rows = [json.loads(line) for line in users_path.read_text().splitlines()]
    assert sorted((row["provider"], row["username_normalized"]) for row in user_rows) == [
        ("chess.com", "opponent"),
        ("chess.com", "samename"),
        ("lichess", "opponent"),
        ("lichess", "samename"),
    ]

    with graph_path.open(newline="", encoding="utf-8") as handle:
        graph_rows = list(csv.DictReader(handle))
    assert graph_rows[0]["provider"] == "chess.com"
    assert graph_rows[0]["from_username"] == "samename"
    assert graph_rows[0]["to_username"] == "opponent"


def test_cli_smoke_for_jobs_reports_and_exports(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "archive.sqlite"
    initialize_database(db_path)
    with connect(db_path) as conn:
        game_id, same_id, opponent_id = _seed_game(
            conn,
            provider="chess.com",
            game_key="cc-1",
            white="SameName",
            black="Opponent",
            outcome="white_win",
        )
        record_discovery_edges(
            conn,
            crawl_run_id=None,
            provider="chess.com",
            from_user_id=same_id,
            depth=1,
            edges=[OpponentEdge(opponent_id, "opponent", game_id, 1)],
        )

    assert cli.run(["jobs", "status", "--db", str(db_path)]) == 0
    jobs_out = capsys.readouterr()
    assert "Job states" in jobs_out.out

    assert cli.run(["report", "summary", "--db", str(db_path)]) == 0
    summary_out = capsys.readouterr()
    assert "chess.com" in summary_out.out

    assert cli.run(["report", "user", "chess.com", "SameName", "--db", str(db_path)]) == 0
    user_out = capsys.readouterr()
    assert "W/D/L/unfinished: 1/0/0/0" in user_out.out

    users_path = tmp_path / "users.jsonl"
    graph_path = tmp_path / "graph.csv"
    assert cli.run(["export", "users", "--format", "jsonl", "--output", str(users_path), "--db", str(db_path)]) == 0
    assert cli.run(["export", "graph", "--format", "csv", "--output", str(graph_path), "--db", str(db_path)]) == 0
    assert "samename" in users_path.read_text()
    assert "from_username" in graph_path.read_text()

    summary = summary_report(connect(db_path))
    assert summary["raw_payloads"] == 0
