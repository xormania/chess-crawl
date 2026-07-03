from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import (
    _store_and_normalize,
    fetch_chesscom_month,
    fetch_chesscom_stats,
    fetch_lichess_games,
    fetch_user_profile,
)
import chess_crawl.normalize.games as games_module
from chess_crawl.normalize.games import normalize_games_payload
from chess_crawl.providers.base import FetchPolicy, RawRecord
from chess_crawl.providers.chesscom import endpoints as chesscom_endpoints
from chess_crawl.providers.lichess import endpoints as lichess_endpoints
from chess_crawl.providers.http import HttpClient
from chess_crawl.storage.raw import store_raw_payload


def _fixture(fixtures_dir: Path, relative: str) -> bytes:
    return (fixtures_dir / relative).read_bytes()


def _config() -> Config:
    return Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=1)


def test_chesscom_endpoint_construction() -> None:
    assert chesscom_endpoints.player_profile("SameName") == "https://api.chess.com/pub/player/samename"
    assert chesscom_endpoints.player_stats("Same Name") == "https://api.chess.com/pub/player/same%20name/stats"
    assert (
        chesscom_endpoints.archives_index("SameName")
        == "https://api.chess.com/pub/player/samename/games/archives"
    )
    assert (
        chesscom_endpoints.monthly_archive("SameName", 2024, 1)
        == "https://api.chess.com/pub/player/samename/games/2024/01"
    )


def test_lichess_endpoint_construction() -> None:
    assert lichess_endpoints.user_profile("SameName") == "https://lichess.org/api/user/samename"
    assert (
        lichess_endpoints.user_games("SameName", since=1704067200000, until=1704153600000, max=1)
        == "https://lichess.org/api/games/user/samename?since=1704067200000&until=1704153600000&max=1"
    )
    assert lichess_endpoints.game("abc123") == "https://lichess.org/api/game/abc123"


def test_http_attempt_records_only_sanitized_request_headers() -> None:
    outbound_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        outbound_headers.update({key.lower(): value for key, value in request.headers.items()})
        return httpx.Response(200, headers={"content-type": "application/json"}, content=b"{}")

    client = HttpClient(
        provider="lichess",
        user_agent="chess-crawl/test",
        policy=FetchPolicy(
            min_delay_s=0,
            supports_conditional=False,
            honor_retry_after=False,
            fixed_429_backoff_s=None,
            max_retries=0,
        ),
        transport=httpx.MockTransport(handler),
    )
    try:
        result = client.request(
            "GET",
            "https://example.test/user",
            endpoint_type="user_profile",
            headers={
                "Authorization": "Bearer secret-token",
                "Cookie": "session=secret",
                "X-Api-Key": "secret-key",
                "Accept": "application/json",
            },
        )
    finally:
        client.close()

    assert outbound_headers["authorization"] == "Bearer secret-token"
    assert "cookie" not in outbound_headers
    assert "x-api-key" not in outbound_headers
    recorded_headers = {key.lower(): value for key, value in result.attempts[0].request_headers.items()}
    assert recorded_headers["user-agent"] == "chess-crawl/test"
    assert recorded_headers["accept"] == "application/json"
    assert "authorization" not in recorded_headers
    assert "cookie" not in recorded_headers
    assert "x-api-key" not in recorded_headers


def test_chesscom_200_stores_raw_before_user_normalization(fixtures_dir: Path, initialized_conn) -> None:
    conn = initialized_conn
    body = _fixture(fixtures_dir, "chesscom/player.json")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["User-Agent"].startswith("chess-crawl/")
        return httpx.Response(200, headers={"etag": '"profile-v1"', "content-type": "application/json"}, content=body)

    result = fetch_user_profile(
        conn,
        "chess.com",
        "SameName",
        config=_config(),
        transport=httpx.MockTransport(handler),
    )

    assert result.raw_payload_id is not None
    assert result.normalized_ids
    raw = conn.execute("SELECT response_headers, normalization_status FROM raw_payloads").fetchone()
    assert json.loads(raw["response_headers"])["etag"] == '"profile-v1"'
    assert raw["normalization_status"] == "parsed"
    assert conn.execute("SELECT COUNT(*) FROM provider_users WHERE provider = 'chess.com'").fetchone()[0] == 1
    assert conn.execute("SELECT raw_payload_id FROM user_snapshots").fetchone()[0] == result.raw_payload_id
    assert conn.execute("SELECT raw_payload_id FROM fetch_logs WHERE status_code = 200").fetchone()[0] == result.raw_payload_id


def test_raw_payload_exists_before_normalizer_runs(initialized_conn) -> None:
    conn = initialized_conn
    seen: list[int] = []
    record = RawRecord(
        provider="chess.com",
        endpoint_type="archives_index",
        request_url="https://api.chess.com/pub/player/samename/games/archives",
        canonical_source_key="chess.com/player/samename/games/archives",
        http_status=200,
        fetched_at=123,
        body=b'{"archives":[]}',
        media_type="application/json",
    )

    def normalizer(inner_conn, raw_payload_id: int) -> list[int]:
        seen.append(inner_conn.execute("SELECT COUNT(*) FROM raw_payloads WHERE id = ?", (raw_payload_id,)).fetchone()[0])
        return []

    result = _store_and_normalize(conn, record, normalizer=normalizer)

    assert result.raw_payload_id is not None
    assert seen == [1]


def test_chesscom_304_uses_conditional_headers_without_new_raw(fixtures_dir: Path, initialized_conn) -> None:
    conn = initialized_conn
    body = _fixture(fixtures_dir, "chesscom/player.json")

    first = fetch_user_profile(
        conn,
        "chess.com",
        "SameName",
        config=_config(),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"etag": '"profile-v1"', "content-type": "application/json"},
                content=body,
            )
        ),
    )

    def second_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["If-None-Match"] == '"profile-v1"'
        return httpx.Response(304, headers={"etag": '"profile-v1"'})

    second = fetch_user_profile(
        conn,
        "chess.com",
        "SameName",
        config=_config(),
        transport=httpx.MockTransport(second_handler),
    )

    assert first.raw_payload_id is not None
    assert second.raw_payload_id is None
    assert second.status_code == 304
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM fetch_logs WHERE status_code = 304 AND from_cache = 1").fetchone()[0] == 1


def test_404_and_410_are_logged_without_raw_payload(initialized_conn) -> None:
    conn = initialized_conn
    statuses = iter([404, 410])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(next(statuses), headers={"content-type": "application/json"})

    transport = httpx.MockTransport(handler)
    first = fetch_user_profile(conn, "chess.com", "Missing", config=_config(), transport=transport)
    second = fetch_chesscom_stats(conn, "Missing", config=_config(), transport=transport)

    assert first.status_code == 404
    assert second.status_code == 410
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM errors WHERE error_kind IN ('http_404','http_410')").fetchone()[0] == 2


def test_chesscom_stats_store_raw_and_snapshot(fixtures_dir: Path, initialized_conn) -> None:
    conn = initialized_conn
    body = _fixture(fixtures_dir, "chesscom/stats.json")

    result = fetch_chesscom_stats(
        conn,
        "SameName",
        config=_config(),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={"content-type": "application/json"}, content=body)
        ),
    )

    assert result.raw_payload_id is not None
    raw = conn.execute("SELECT endpoint_type, normalization_status FROM raw_payloads").fetchone()
    assert raw["endpoint_type"] == "user_stats"
    assert raw["normalization_status"] == "parsed"
    snapshot = conn.execute("SELECT count_win, count_loss, count_draw, perfs_or_stats FROM user_snapshots").fetchone()
    assert (snapshot["count_win"], snapshot["count_loss"], snapshot["count_draw"]) == (13, 6, 3)
    assert "chess_blitz" in snapshot["perfs_or_stats"]


def test_lichess_429_waits_60_seconds_then_retries(fixtures_dir: Path, initialized_conn) -> None:
    conn = initialized_conn
    body = _fixture(fixtures_dir, "lichess/user.json")
    sleeps: list[float] = []
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        assert request.headers["User-Agent"].startswith("chess-crawl/")
        if calls["count"] == 1:
            return httpx.Response(429, headers={"retry-after": "1"})
        return httpx.Response(200, headers={"content-type": "application/json"}, content=body)

    result = fetch_user_profile(
        conn,
        "lichess",
        "SameName",
        config=_config(),
        transport=httpx.MockTransport(handler),
        sleeper=sleeps.append,
    )

    assert result.status_code == 200
    assert sleeps == [60.0]
    assert [row["status_code"] for row in conn.execute("SELECT status_code FROM fetch_logs ORDER BY id")] == [429, 200]
    assert conn.execute("SELECT retry_after FROM fetch_logs WHERE status_code = 429").fetchone()[0] == 1


def test_chesscom_monthly_archive_normalizes_game(fixtures_dir: Path, initialized_conn) -> None:
    conn = initialized_conn
    body = _fixture(fixtures_dir, "chesscom/archive_2024_01.json")
    result = fetch_chesscom_month(
        conn,
        "SameName",
        2024,
        1,
        config=_config(),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={"content-type": "application/json"}, content=body)
        ),
    )

    assert result.normalized_ids
    game = conn.execute("SELECT * FROM games WHERE provider = 'chess.com'").fetchone()
    assert game["provider_game_id"] == "00000000-0000-4000-8000-000000000001"
    assert game["outcome"] == "white_win"
    assert game["ended_at"] == 1704067500
    assert conn.execute("SELECT COUNT(*) FROM game_participants WHERE game_id = ?", (game["id"],)).fetchone()[0] == 2
    assert conn.execute("SELECT rating FROM ratings_at_game WHERE game_id = ? AND color = 'white'", (game["id"],)).fetchone()[0] == 1510


def test_game_normalization_failure_rolls_back_partial_normalized_rows(
    fixtures_dir: Path,
    initialized_conn,
    monkeypatch,
) -> None:
    conn = initialized_conn
    archive = json.loads(_fixture(fixtures_dir, "chesscom/archive_2024_01.json"))
    second_game = dict(archive["games"][0])
    second_game["uuid"] = "00000000-0000-4000-8000-000000000002"
    second_game["url"] = "https://www.chess.com/game/live/1000000002"
    archive["games"].append(second_game)
    record = RawRecord(
        provider="chess.com",
        endpoint_type="monthly_archive",
        request_url="https://api.chess.com/pub/player/samename/games/2024/01",
        canonical_source_key="chess.com/player/samename/games/2024/01",
        fetched_at=123,
        body=json.dumps(archive).encode(),
        media_type="application/json",
    )
    raw_payload_id = store_raw_payload(conn, record)
    real_normalize_game = games_module._normalize_game

    def fail_on_second_game(*args, **kwargs):
        if kwargs["json_pointer"] == "/games/1":
            raise RuntimeError("injected normalization failure")
        return real_normalize_game(*args, **kwargs)

    monkeypatch.setattr(games_module, "_normalize_game", fail_on_second_game)

    with pytest.raises(RuntimeError, match="injected normalization failure"):
        normalize_games_payload(conn, raw_payload_id)

    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 1
    assert conn.execute("SELECT normalization_status FROM raw_payloads").fetchone()[0] == "pending"
    assert conn.execute("SELECT COUNT(*) FROM games").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM provider_users").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM source_records").fetchone()[0] == 0


def test_lichess_games_ndjson_normalizes_ms_timestamps(fixtures_dir: Path, initialized_conn) -> None:
    conn = initialized_conn
    body = _fixture(fixtures_dir, "lichess/games.ndjson")
    result = fetch_lichess_games(
        conn,
        "SameName",
        since=1704067200,
        until=1704153600,
        limit=1,
        config=_config(),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"content-type": "application/x-ndjson"},
                content=body,
            )
        ),
    )

    assert result.normalized_ids
    game = conn.execute("SELECT * FROM games WHERE provider = 'lichess'").fetchone()
    assert game["provider_game_id"] == "lichgame1"
    assert game["outcome"] == "black_win"
    assert game["created_at"] == 1704067200
    assert game["ended_at"] == 1704067500
    assert b"1704067200000" in conn.execute("SELECT raw_body FROM raw_payloads").fetchone()[0]


def test_provider_scoped_same_username_across_providers(fixtures_dir: Path, initialized_conn) -> None:
    conn = initialized_conn
    chess_body = _fixture(fixtures_dir, "chesscom/player.json")
    lichess_body = _fixture(fixtures_dir, "lichess/user.json")

    fetch_user_profile(
        conn,
        "chess.com",
        "SameName",
        config=_config(),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=chess_body)),
    )
    fetch_user_profile(
        conn,
        "lichess",
        "SameName",
        config=_config(),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=lichess_body)),
    )

    rows = conn.execute("SELECT provider, username_normalized FROM provider_users ORDER BY provider").fetchall()
    assert [(row["provider"], row["username_normalized"]) for row in rows] == [
        ("chess.com", "samename"),
        ("lichess", "samename"),
    ]
