from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path

import pytest

from chess_crawl import cli
import chess_crawl.ingest as ingest_module
from chess_crawl.jobs import store
from chess_crawl.jobs.discovery import CrawlBounds, create_opponent_crawl
from chess_crawl.providers.base import EndpointType, RawRecord
from chess_crawl.storage.db import connect
from chess_crawl.storage.migrations import initialize_database


pytestmark = pytest.mark.workflow

SINCE = 1704067200
UNTIL = 1704153600


def _json_bytes(data: object) -> bytes:
    return json.dumps(data, separators=(",", ":")).encode("utf-8")


def _ndjson_bytes(games: list[dict[str, object]]) -> bytes:
    return b"\n".join(_json_bytes(game) for game in games)


def _lichess_game(
    game_id: str,
    *,
    white: str,
    black: str,
    winner: str = "black",
    created_at_ms: int = 1704067200000,
    last_move_at_ms: int = 1704067500000,
) -> dict[str, object]:
    return {
        "id": game_id,
        "rated": True,
        "variant": "standard",
        "speed": "blitz",
        "perf": "blitz",
        "createdAt": created_at_ms,
        "lastMoveAt": last_move_at_ms,
        "status": "mate",
        "winner": winner,
        "players": {
            "white": {
                "user": {"id": white.lower(), "name": white},
                "rating": 1700,
                "ratingDiff": -6,
            },
            "black": {
                "user": {"id": black.lower(), "name": black},
                "rating": 1710,
                "ratingDiff": 6,
            },
        },
        "clock": {"initial": 300, "increment": 0},
        "opening": {"eco": "C20", "name": "King's Pawn Game", "ply": 2},
        "pgn": (
            '[Event "Rated Blitz game"]\n'
            f'[Site "https://lichess.org/{game_id}"]\n'
            f'[White "{white}"]\n'
            f'[Black "{black}"]\n'
            '[Result "0-1"]\n\n'
            "1. e4 e5 0-1"
        ),
    }


def _raw_record(
    *,
    provider: str,
    endpoint_type: EndpointType,
    canonical_source_key: str,
    body: bytes | None,
    status: int = 200,
    media_type: str = "application/json",
    request_url: str | None = None,
    target_username: str | None = None,
    archive_unit: str | None = None,
    request_params: dict[str, object] | None = None,
    etag: str | None = None,
) -> RawRecord:
    return RawRecord(
        provider=provider,
        endpoint_type=endpoint_type,
        request_url=request_url or f"https://fixture.invalid/{canonical_source_key}",
        canonical_source_key=canonical_source_key,
        request_params=request_params or {},
        http_status=status,
        fetched_at=1704068000,
        body=body,
        media_type=media_type,
        etag=etag,
        target_username=target_username,
        archive_unit=archive_unit,
    )


class _FixtureProviderFactory:
    def __init__(
        self,
        fixtures_dir: Path,
        *,
        lichess_games: dict[str, list[dict[str, object]]],
        lichess_failures: dict[str, int] | None = None,
    ) -> None:
        self.chesscom_player_body = (fixtures_dir / "chesscom/player.json").read_bytes()
        self.chesscom_archive_body = (fixtures_dir / "chesscom/archive_2024_01.json").read_bytes()
        self.lichess_user_body = (fixtures_dir / "lichess/user.json").read_bytes()
        self.lichess_games = lichess_games
        self.lichess_failures = dict(lichess_failures or {})
        self.chesscom_month_calls: list[tuple[str, int, int, str | None]] = []
        self.lichess_game_calls: list[tuple[str, int | None, int | None, int]] = []

    def __call__(self, key: str, *_args: object, **_kwargs: object):
        if key == "chess.com":
            return _ChessComFixtureClient(self)
        if key == "lichess":
            return _LichessFixtureClient(self)
        raise AssertionError(f"unexpected provider: {key}")


class _ChessComFixtureClient:
    def __init__(self, factory: _FixtureProviderFactory) -> None:
        self.factory = factory

    def get_user_profile(
        self,
        username: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> RawRecord:
        del last_modified
        normalized = username.strip().lower()
        key = f"chess.com/player/{normalized}/profile"
        if etag == '"cc-profile-v1"':
            return _raw_record(
                provider="chess.com",
                endpoint_type="user_profile",
                canonical_source_key=key,
                body=None,
                status=304,
                target_username=normalized,
                etag='"cc-profile-v1"',
            )
        return _raw_record(
            provider="chess.com",
            endpoint_type="user_profile",
            canonical_source_key=key,
            body=self.factory.chesscom_player_body,
            target_username=normalized,
            etag='"cc-profile-v1"',
        )

    def get_monthly_archive(
        self,
        username: str,
        year: int,
        month: int,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> RawRecord:
        del last_modified
        normalized = username.strip().lower()
        self.factory.chesscom_month_calls.append((normalized, year, month, etag))
        key = f"chess.com/player/{normalized}/games/{year:04d}/{month:02d}"
        if etag == '"cc-month-v1"':
            return _raw_record(
                provider="chess.com",
                endpoint_type="monthly_archive",
                canonical_source_key=key,
                body=None,
                status=304,
                target_username=normalized,
                archive_unit=f"{year:04d}/{month:02d}",
                etag='"cc-month-v1"',
            )
        return _raw_record(
            provider="chess.com",
            endpoint_type="monthly_archive",
            canonical_source_key=key,
            body=self.factory.chesscom_archive_body,
            target_username=normalized,
            archive_unit=f"{year:04d}/{month:02d}",
            etag='"cc-month-v1"',
        )

    def close(self) -> None:
        return None


class _LichessFixtureClient:
    def __init__(self, factory: _FixtureProviderFactory) -> None:
        self.factory = factory

    def get_user_profile(self, username: str) -> RawRecord:
        normalized = username.strip().lower()
        return _raw_record(
            provider="lichess",
            endpoint_type="user_profile",
            canonical_source_key=f"lichess/user/{normalized}/profile",
            body=self.factory.lichess_user_body,
            target_username=normalized,
        )

    def get_user_games(
        self,
        username: str,
        *,
        since: int | None,
        until: int | None,
        limit: int,
    ) -> RawRecord:
        normalized = username.strip().lower()
        self.factory.lichess_game_calls.append((normalized, since, until, limit))
        unit = f"since-{since or 'none'}-until-{until or 'none'}-max-{limit}"
        key = f"lichess/games/user/{normalized}/{unit}"
        if self.factory.lichess_failures.get(normalized, 0) > 0:
            self.factory.lichess_failures[normalized] -= 1
            return _raw_record(
                provider="lichess",
                endpoint_type="user_games_stream",
                canonical_source_key=key,
                body=None,
                status=500,
                media_type="application/json",
                target_username=normalized,
                archive_unit=unit,
                request_params={"since": since, "until": until, "max": limit},
            )

        available = self.factory.lichess_games.get(normalized, [])
        body = _ndjson_bytes(available[:limit])
        return _raw_record(
            provider="lichess",
            endpoint_type="user_games_stream",
            canonical_source_key=key,
            body=body,
            media_type="application/x-ndjson",
            target_username=normalized,
            archive_unit=unit,
            request_params={"since": since, "until": until, "max": limit},
        )

    def close(self) -> None:
        return None


@pytest.fixture
def provider_factory(
    fixtures_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> _FixtureProviderFactory:
    factory = _FixtureProviderFactory(
        fixtures_dir,
        lichess_games={
            "samename": [
                _lichess_game("lich-workflow-1", white="SameName", black="OpponentTwo"),
                _lichess_game(
                    "lich-workflow-2",
                    white="OpponentThree",
                    black="SameName",
                    winner="white",
                    created_at_ms=1704067800000,
                    last_move_at_ms=1704068100000,
                ),
                _lichess_game(
                    "lich-workflow-3",
                    white="SameName",
                    black="OpponentFour",
                    created_at_ms=1704068400000,
                    last_move_at_ms=1704068700000,
                ),
            ]
        },
    )
    monkeypatch.setattr(ingest_module, "create_provider_client", factory)
    return factory


def test_cli_fresh_archive_rerun_query_export_and_provider_boundaries(
    tmp_path: Path,
    provider_factory: _FixtureProviderFactory,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "archive.sqlite"

    assert cli.run(["init", "--db", str(db_path)]) == 0
    assert "Schema version: 1" in capsys.readouterr().out

    assert cli.run(["fetch", "user", "chess.com", "SameName", "--db", str(db_path)]) == 0
    assert "Provider: chess.com" in capsys.readouterr().out
    assert cli.run(["fetch", "user", "lichess", "SameName", "--db", str(db_path)]) == 0
    assert "Provider: lichess" in capsys.readouterr().out

    assert cli.run(["fetch", "games", "chess.com", "SameName", "--month", "2024-01", "--db", str(db_path)]) == 0
    first_chesscom_out = capsys.readouterr().out
    assert "HTTP status: 200" in first_chesscom_out
    assert "Normalized rows: 1" in first_chesscom_out

    assert cli.run(["fetch", "games", "chess.com", "SameName", "--month", "2024-01", "--db", str(db_path)]) == 0
    second_chesscom_out = capsys.readouterr().out
    assert "HTTP status: 304" in second_chesscom_out
    assert "not modified" in second_chesscom_out

    lichess_fetch = [
        "fetch",
        "games",
        "lichess",
        "SameName",
        "--since",
        "2024-01-01",
        "--until",
        "2024-01-02",
        "--limit",
        "2",
        "--db",
        str(db_path),
    ]
    assert cli.run(lichess_fetch) == 0
    assert "Normalized rows: 2" in capsys.readouterr().out
    assert cli.run(lichess_fetch) == 0
    assert "Normalized rows: 2" in capsys.readouterr().out

    assert provider_factory.chesscom_month_calls == [
        ("samename", 2024, 1, None),
        ("samename", 2024, 1, '"cc-month-v1"'),
    ]
    assert provider_factory.lichess_game_calls == [
        ("samename", SINCE, UNTIL, 2),
        ("samename", SINCE, UNTIL, 2),
    ]

    assert cli.run(["query", "user", "chess.com", "SameName", "--db", str(db_path)]) == 0
    assert "Provider: chess.com" in capsys.readouterr().out
    assert cli.run(["query", "user", "lichess", "SameName", "--db", str(db_path)]) == 0
    assert "Provider: lichess" in capsys.readouterr().out
    assert cli.run(
        [
            "query",
            "game",
            "chess.com",
            "00000000-0000-4000-8000-000000000001",
            "--db",
            str(db_path),
        ]
    ) == 0
    assert "Provider: chess.com" in capsys.readouterr().out
    assert cli.run(["query", "raw", "--provider", "lichess", "--limit", "5", "--db", str(db_path)]) == 0
    assert "user_games_stream" in capsys.readouterr().out

    assert cli.run(["report", "summary", "--db", str(db_path)]) == 0
    assert "Raw payloads: 4" in capsys.readouterr().out

    games_path = tmp_path / "games.jsonl"
    users_path = tmp_path / "users.jsonl"
    assert cli.run(["export", "games", "--format", "jsonl", "--output", str(games_path), "--db", str(db_path)]) == 0
    assert cli.run(["export", "users", "--format", "jsonl", "--output", str(users_path), "--db", str(db_path)]) == 0
    game_rows = [json.loads(line) for line in games_path.read_text().splitlines()]
    user_rows = [json.loads(line) for line in users_path.read_text().splitlines()]

    assert [(row["provider"], row["provider_game_id"]) for row in game_rows] == [
        ("chess.com", "00000000-0000-4000-8000-000000000001"),
        ("lichess", "lich-workflow-1"),
        ("lichess", "lich-workflow-2"),
    ]
    assert ("chess.com", "samename") in {
        (row["provider"], row["username_normalized"]) for row in user_rows
    }
    assert ("lichess", "samename") in {(row["provider"], row["username_normalized"]) for row in user_rows}

    with closing(connect(db_path)) as conn:
        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("raw_payloads", "provider_users", "games", "discovery_jobs")
        }
        raw_endpoints = conn.execute(
            """
            SELECT provider, endpoint_type, body_bytes, normalization_status
              FROM raw_payloads
             ORDER BY provider, endpoint_type, canonical_source_key
            """
        ).fetchall()
        same_name_rows = conn.execute(
            """
            SELECT provider, id
              FROM provider_users
             WHERE username_normalized = 'samename'
             ORDER BY provider
            """
        ).fetchall()
        game_source_count = conn.execute(
            """
            SELECT COUNT(DISTINCT g.id)
              FROM games g
              JOIN source_records sr
                ON sr.entity_type = 'game'
               AND sr.entity_id = g.id
              JOIN raw_payloads rp
                ON rp.id = sr.raw_payload_id
               AND rp.provider = g.provider
            """
        ).fetchone()[0]
        provider_game_counts = conn.execute(
            "SELECT provider, COUNT(*) AS count FROM games GROUP BY provider ORDER BY provider"
        ).fetchall()

    assert counts == {"raw_payloads": 4, "provider_users": 5, "games": 3, "discovery_jobs": 0}
    assert [(row["provider"], row["endpoint_type"], row["normalization_status"]) for row in raw_endpoints] == [
        ("chess.com", "monthly_archive", "parsed"),
        ("chess.com", "user_profile", "parsed"),
        ("lichess", "user_games_stream", "parsed"),
        ("lichess", "user_profile", "parsed"),
    ]
    assert all(row["body_bytes"] > 0 for row in raw_endpoints)
    assert [row["provider"] for row in same_name_rows] == ["chess.com", "lichess"]
    assert same_name_rows[0]["id"] != same_name_rows[1]["id"]
    assert len(provider_factory.lichess_games["samename"]) == 3
    assert game_source_count == 3
    assert [(row["provider"], row["count"]) for row in provider_game_counts] == [
        ("chess.com", 1),
        ("lichess", 2),
    ]


def test_cli_resume_finishes_interrupted_crawl_without_refetching_done_jobs(
    tmp_path: Path,
    fixtures_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    factory = _FixtureProviderFactory(
        fixtures_dir,
        lichess_games={
            "samename": [_lichess_game("resume-root-1", white="SameName", black="OpponentOne")],
            "opponentone": [],
        },
    )
    monkeypatch.setattr(ingest_module, "create_provider_client", factory)
    db_path = tmp_path / "archive.sqlite"
    initialize_database(db_path)

    with closing(connect(db_path)) as conn:
        run_id, root_job_id = create_opponent_crawl(
            conn,
            provider="lichess",
            username="SameName",
            since=SINCE,
            until=UNTIL,
            bounds=CrawlBounds(max_depth=1, max_users=5, max_games=5, max_jobs=5),
        )

    assert cli.run(["jobs", "resume", "--run", str(run_id), "--max-jobs", "1", "--db", str(db_path)]) == 0
    first_resume = capsys.readouterr().out
    assert "Jobs: 1 done" in first_resume

    with closing(connect(db_path)) as conn:
        root = store.get_job(conn, root_job_id)
        interrupted = store.claim_next_job(conn, crawl_run_id=run_id)
        assert root is not None
        assert root.state == "done"
        assert interrupted is not None
        assert interrupted.target == "opponentone"

    assert cli.run(["jobs", "status", "--run", str(run_id), "--db", str(db_path)]) == 0
    assert "in_progress" in capsys.readouterr().out

    assert cli.run(["jobs", "resume", "--run", str(run_id), "--max-jobs", "5", "--db", str(db_path)]) == 0
    second_resume = capsys.readouterr().out
    assert "Stale in_progress -> pending: 1" in second_resume
    assert "Jobs: 1 done" in second_resume

    with closing(connect(db_path)) as conn:
        states = dict(
            conn.execute(
                """
                SELECT state, COUNT(*) AS count
                  FROM discovery_jobs
                 WHERE crawl_run_id = ?
                 GROUP BY state
                """,
                (run_id,),
            ).fetchall()
        )
        run = conn.execute("SELECT status FROM crawl_runs WHERE id = ?", (run_id,)).fetchone()
        games = conn.execute("SELECT provider, provider_game_id FROM games").fetchall()

    assert [call[0] for call in factory.lichess_game_calls] == ["samename", "opponentone"]
    assert states == {"done": 2}
    assert run["status"] == "done"
    assert [(row["provider"], row["provider_game_id"]) for row in games] == [("lichess", "resume-root-1")]


def test_cli_provider_failure_leaves_no_normalized_partial_and_rerun_recovers(
    tmp_path: Path,
    fixtures_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    factory = _FixtureProviderFactory(
        fixtures_dir,
        lichess_games={"samename": [_lichess_game("recovery-1", white="SameName", black="OpponentTwo")]},
        lichess_failures={"samename": 1},
    )
    monkeypatch.setattr(ingest_module, "create_provider_client", factory)
    db_path = tmp_path / "archive.sqlite"
    assert cli.run(["init", "--db", str(db_path)]) == 0
    capsys.readouterr()

    fetch_args = [
        "fetch",
        "games",
        "lichess",
        "SameName",
        "--since",
        "2024-01-01",
        "--until",
        "2024-01-02",
        "--limit",
        "1",
        "--db",
        str(db_path),
    ]
    assert cli.run(fetch_args) == 1
    failure_out = capsys.readouterr().out
    assert "HTTP status: 500" in failure_out
    assert "no raw payload stored" in failure_out

    with closing(connect(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM games").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM source_records").fetchone()[0] == 0
        assert conn.execute("SELECT status_code FROM fetch_logs").fetchone()[0] == 500

    assert cli.run(fetch_args) == 0
    assert "HTTP status: 200" in capsys.readouterr().out

    with closing(connect(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM games").fetchone()[0] == 1
        assert conn.execute(
            """
            SELECT COUNT(*)
              FROM source_records sr
              JOIN raw_payloads rp ON rp.id = sr.raw_payload_id
             WHERE sr.entity_type = 'game'
               AND rp.provider = 'lichess'
            """
        ).fetchone()[0] == 1
        assert [row[0] for row in conn.execute("SELECT status_code FROM fetch_logs ORDER BY id")] == [500, 200]

    assert [call[0] for call in factory.lichess_game_calls] == ["samename", "samename"]
