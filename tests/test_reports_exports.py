from __future__ import annotations

from chess_crawl.storage.db import open_database, require_row

import json

import pytest

from support import seed_game
from chess_crawl.providers.base import RawRecord
from chess_crawl.api import compat
from chess_crawl.api.compat import _export_chunks
from chess_crawl.application import list_opponents
from chess_crawl.storage.api_views import months_page
from test_working_sets import client
from chess_crawl.storage.queries import (
    archive_freshness,
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

    opponents = list_opponents(conn, "chess.com", "SameName")["items"]
    assert opponents is not None
    assert [(row["provider"], row["opponent_username"], row["unfinished"]) for row in opponents] == [
        ("chess.com", "opponent", 1)
    ]

    months = months_page(conn, provider="chess.com", after="", limit=100)["items"]
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


def test_exports_preserve_provider_and_omit_raw_payloads(seeded_database_url: str) -> None:
    with client(seeded_database_url) as api:
        games = api.get("/v1/exports/games.jsonl")
        users = api.get("/v1/exports/users.jsonl")
    assert games.status_code == users.status_code == 200
    game_rows = [json.loads(line) for line in games.text.splitlines()]
    assert len(game_rows) == 2
    assert {row["provider"] for row in game_rows} == {"chess.com", "lichess"}
    assert all("raw_body" not in row and "ply_count" not in row for row in game_rows)
    user_rows = [json.loads(line) for line in users.text.splitlines()]
    assert sorted((row["provider"], row["username_normalized"]) for row in user_rows) == [
        ("chess.com", "opponent"), ("chess.com", "samename"),
        ("lichess", "opponent"), ("lichess", "samename"),
    ]
    # Graph membership and CSV correctness are exercised against owned run
    # memberships in test_archive_api_parity, not legacy global edge exports.


@pytest.mark.parametrize("kind", ["games", "users", "graph"])
def test_export_output_failure_preserves_database(seeded_database_url: str, monkeypatch, kind) -> None:
    import io

    class FailingOutput(io.StringIO):
        def write(self, text):
            raise OSError("output interrupted")

    output = FailingOutput()
    monkeypatch.setattr(compat.tempfile, "TemporaryFile", lambda **kwargs: output)
    with pytest.raises(OSError, match="interrupted"):
        list(_export_chunks(seeded_database_url, kind, None, "alpha"))
    assert output.closed
    with open_database(seeded_database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2


@pytest.mark.parametrize("kind", ["games", "users", "graph"])
def test_output_interruption_releases_spool(seeded_database_url: str, monkeypatch, kind) -> None:
    files = []
    original = compat.tempfile.TemporaryFile

    def tracked(**kwargs):
        file = original(**kwargs)
        files.append(file)
        return file

    monkeypatch.setattr(compat.tempfile, "TemporaryFile", tracked)
    chunks = _export_chunks(seeded_database_url, kind, None, "alpha")
    next(chunks)
    chunks.close()
    assert len(files) == 1 and files[0].closed
    with open_database(seeded_database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
