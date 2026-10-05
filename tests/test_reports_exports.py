from __future__ import annotations

from chess_crawl.storage.db import open_database, require_row

import csv
import json
from pathlib import Path

import pytest

from support import seed_game
from chess_crawl.providers.base import RawRecord
from chess_crawl.export.writers import export_games_jsonl, export_graph_csv, export_users_jsonl
from chess_crawl.storage.queries import (
    archive_freshness,
    games_by_month,
    opponent_report,
    summary_report,
    user_game_summary,
)
from chess_crawl.storage.raw import insert_fetch_log, store_raw_payload


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


def test_public_archive_metrics_exclude_workspace_scoped_payloads(initialized_conn) -> None:
    conn = initialized_conn
    public_raw_id = store_raw_payload(conn, RawRecord(
        provider="lichess", endpoint_type="user_profile", request_url="https://example/public",
        canonical_source_key="user:public", fetched_at=100, body=b'{"id":"public"}',
    ))
    private_raw_id = store_raw_payload(conn, RawRecord(
        provider="lichess", endpoint_type="user_resource", request_url="https://example/private",
        canonical_source_key="user:private:teams", fetched_at=200, body=b'[{"id":"secret"}]',
        owner_scope="workspace:test",
    ))
    insert_fetch_log(
        conn, provider="lichess", url="https://example/public", endpoint_type="user_profile",
        attempted_at=110, status_code=200, raw_payload_id=public_raw_id,
    )
    insert_fetch_log(
        conn, provider="lichess", url="https://example/private", endpoint_type="user_resource",
        attempted_at=210, status_code=200, raw_payload_id=private_raw_id,
    )
    insert_fetch_log(
        conn, provider="lichess", url="https://example/unassigned", endpoint_type="user_profile",
        attempted_at=310, status_code=200, raw_payload_id=None,
    )

    assert summary_report(conn)["raw_payloads"] == 1
    assert archive_freshness(conn) == {
        "provider": None,
        "last_checked_at": 110,
        "last_fetched_at": 100,
        "last_normalized_at": None,
        "pending_payloads": 1,
        "failed_payloads": 0,
    }


def test_exports_preserve_provider_and_omit_raw_payloads(tmp_path: Path, seeded_database_url: str) -> None:
    games_path = tmp_path / "games.jsonl"
    users_path = tmp_path / "users.jsonl"
    graph_path = tmp_path / "graph.csv"

    with open_database(seeded_database_url) as conn:
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


@pytest.mark.parametrize("exporter", [export_games_jsonl, export_users_jsonl, export_graph_csv])
def test_export_output_failure_preserves_database(seeded_database_url: str, tmp_path: Path, exporter) -> None:
    # A directory is an invalid output file; failure must not mutate stored data.
    with open_database(seeded_database_url) as conn:
        before = require_row(conn.execute("SELECT COUNT(*) FROM games"))[0]
        with pytest.raises(OSError):
            exporter(conn, output=tmp_path)
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == before
    with open_database(seeded_database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == before


@pytest.mark.parametrize("exporter", [export_games_jsonl, export_users_jsonl, export_graph_csv])
def test_output_interruption_releases_stream_and_snapshot(seeded_database_url: str, monkeypatch, exporter) -> None:
    import io
    from chess_crawl.export import writers

    class FailingOutput(io.StringIO):
        writes = 0
        def write(self, text):
            self.writes += 1
            # CSV writes the header first; interrupt after a row is fetched.
            if self.writes >= 2:
                raise OSError("output interrupted")
            return super().write(text)

    output = FailingOutput()
    monkeypatch.setattr(writers.sys, "stdout", output)
    with open_database(seeded_database_url) as conn:
        with pytest.raises(OSError, match="interrupted"):
            exporter(conn)
        assert not conn.in_transaction
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
