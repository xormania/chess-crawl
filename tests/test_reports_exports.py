from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from support import seed_game
from chess_crawl.export.writers import export_games_jsonl, export_graph_csv, export_users_jsonl
from chess_crawl.storage.queries import games_by_month, opponent_report, summary_report, user_game_summary
from chess_crawl.storage.db import open_database


def test_reports_are_null_outcome_aware_and_provider_scoped(initialized_conn) -> None:
    conn = initialized_conn
    seed_game(conn, provider="chess.com", game_key="cc-1", white="SameName", black="Opponent", outcome=None)
    seed_game(conn, provider="lichess", game_key="li-1", white="SameName", black="Opponent", outcome="white_win")

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
    assert summary_report(conn)["raw_payloads"] == 0


def test_exports_preserve_provider_and_omit_raw_payloads(tmp_path: Path, seeded_archive: Path) -> None:
    games_path = tmp_path / "games.jsonl"
    users_path = tmp_path / "users.jsonl"
    graph_path = tmp_path / "graph.csv"

    with open_database(seeded_archive) as conn:
        assert export_games_jsonl(conn, output=games_path) == 2
        assert export_users_jsonl(conn, output=users_path) == 4
        assert export_graph_csv(conn, output=graph_path) == 1

    game_rows = [json.loads(line) for line in games_path.read_text().splitlines()]
    assert {row["provider"] for row in game_rows} == {"chess.com", "lichess"}
    assert all("raw_body" not in row for row in game_rows)
    assert all("ply_count" not in row for row in game_rows)

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



@pytest.mark.parametrize(
    ("exporter", "alias"),
    [
        pytest.param(export_games_jsonl, "direct", id="games"),
        pytest.param(export_users_jsonl, "direct", id="users"),
        pytest.param(export_graph_csv, "direct", id="graph"),
        pytest.param(export_games_jsonl, "symlink", id="symlink"),
        pytest.param(export_games_jsonl, "hardlink", id="hardlink"),
        pytest.param(export_games_jsonl, "wal", id="wal-sidecar"),
        pytest.param(export_games_jsonl, "shm", id="shm-sidecar"),
        pytest.param(export_games_jsonl, "journal", id="journal-sidecar"),
    ],
)
def test_exports_reject_database_destination_without_damaging_archive(
    seeded_archive: Path, tmp_path: Path, exporter, alias: str,
) -> None:
    output = seeded_archive
    if alias in {"wal", "shm", "journal"}:
        output = seeded_archive.with_name(f"{seeded_archive.name}-{alias}")
    elif alias != "direct":
        output = tmp_path / "archive-alias"
        if alias == "symlink":
            output.symlink_to(seeded_archive)
        else:
            output.hardlink_to(seeded_archive)

    with open_database(seeded_archive) as conn:
        before = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
        with pytest.raises(ValueError, match="database|archive"):
            exporter(conn, output=output)
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT COUNT(*) FROM games").fetchone()[0] == before

    with open_database(seeded_archive) as conn:
        assert conn.execute("SELECT COUNT(*) FROM games").fetchone()[0] == before
