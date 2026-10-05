from __future__ import annotations

from chess_crawl.storage.db import require_row

import json
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path

import httpx
import pytest

from chess_crawl.config import Config
import chess_crawl.ingest as ingest
from chess_crawl.providers.base import FetchPolicy, RawRecord
from chess_crawl.providers.http import HttpClient
from chess_crawl.providers.registry import ProviderSession
import chess_crawl.providers.registry as registry
from chess_crawl.storage.raw import (
    latest_raw_payload_id,
    latest_validators,
    read_raw_payload,
    store_raw_payload,
)
from support import Clock


def config(**kwargs) -> Config:
    return Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0, **kwargs)


def profile_record(fixtures_dir: Path) -> RawRecord:
    return RawRecord(
        provider="chess.com", endpoint_type="user_profile",
        request_url="https://api.chess.com/pub/player/samename",
        canonical_source_key="chess.com/player/samename/profile",
        fetched_at=123, body=(fixtures_dir / "chesscom/player.json").read_bytes(),
        media_type="application/json", etag='"profile-v1"',
    )


@pytest.mark.parametrize("status", ["pending", "failed", "stale", "parsed"])
def test_304_replays_unfinished_or_outdated_cached_normalization(initialized_conn, fixtures_dir, status) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, profile_record(fixtures_dir), normalization_status=status, parser_version="old")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["If-None-Match"] == '"profile-v1"'
        return httpx.Response(304)

    result = ingest.fetch_user_profile(
        conn, "chess.com", "SameName", config=config(), transport=httpx.MockTransport(handler),
    )
    assert result.status_code == 304
    assert result.raw_payload_id == raw_id
    assert result.normalized_ids
    assert read_raw_payload(conn, raw_id).normalization_status == "parsed"
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    evidence = conn.execute("SELECT status_code, raw_payload_id, from_cache FROM fetch_logs").fetchone()
    assert tuple(evidence.values()) == (304, raw_id, 1)


def test_normalization_failure_preserves_response_and_can_be_replayed_offline(
    initialized_conn, fixtures_dir, monkeypatch,
) -> None:
    conn = initialized_conn
    normalizer = ingest.normalize_user_payload

    def fail(conn, raw_id, **kwargs):
        assert not conn.in_transaction
        assert require_row(conn.execute("SELECT raw_payload_id FROM fetch_logs"))[0] == raw_id
        raise RuntimeError("normalizer unavailable")

    monkeypatch.setattr(ingest, "normalize_user_payload", fail)
    with pytest.raises(RuntimeError, match="normalizer unavailable"):
        ingest.fetch_user_profile(
            conn, "chess.com", "SameName", config=config(),
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=profile_record(fixtures_dir).body)),
        )
    raw_id = require_row(conn.execute("SELECT id FROM raw_payloads"))[0]
    assert read_raw_payload(conn, raw_id).normalization_status == "pending"
    monkeypatch.setattr(ingest, "normalize_user_payload", normalizer)
    result = ingest.replay_raw_payload(conn, raw_id)
    assert result.normalized_ids
    assert require_row(conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM user_snapshots"))[0] == 1


def test_304_without_cached_representation_fails_but_keeps_fetch_evidence(initialized_conn) -> None:
    with pytest.raises(ValueError, match="without a stored raw payload"):
        ingest.fetch_user_profile(
            initialized_conn, "chess.com", "Missing", config=config(),
            transport=httpx.MockTransport(lambda request: httpx.Response(304)),
        )
    assert require_row(initialized_conn.execute("SELECT status_code FROM fetch_logs"))[0] == 304


def test_cache_follows_latest_observation_when_older_body_reappears(initialized_conn, fixtures_dir) -> None:
    conn = initialized_conn
    old_body = profile_record(fixtures_dir).body
    assert old_body is not None
    updated = json.loads(old_body)
    updated["followers"] = 100
    bodies = iter([(old_body, '"v1"'), (json.dumps(updated).encode(), '"v2"'), (old_body, '"v3"')])
    clock = Clock()

    def handler(request):
        body, etag = next(bodies)
        return httpx.Response(200, content=body, headers={"etag": etag})

    with ProviderSession(config(), transport=httpx.MockTransport(handler), clock=clock, sleeper=clock.sleep) as session:
        first = ingest.fetch_user_profile(conn, "chess.com", "samename", session=session)
        clock.now += 1
        ingest.fetch_user_profile(conn, "chess.com", "samename", session=session)
        clock.now += 1
        third = ingest.fetch_user_profile(conn, "chess.com", "samename", session=session)
    source = "chess.com/player/samename/profile"
    assert third.raw_payload_id == first.raw_payload_id == latest_raw_payload_id(conn, source)
    assert latest_validators(conn, source) == ('"v3"', None)
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 2
    ingest.fetch_user_profile(
        conn, "chess.com", "samename", config=config(),
        transport=httpx.MockTransport(lambda request: httpx.Response(304)),
    )
    assert latest_validators(conn, source) == ('"v3"', None)


@pytest.mark.parametrize("exception", [httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError, httpx.ReadTimeout])
def test_network_errors_are_retryable_and_recorded_without_exception_secrets(initialized_conn, exception) -> None:
    conn = initialized_conn
    calls = []
    clock = Clock()

    def handler(request):
        calls.append(request)
        raise exception("Bearer deliberately-secret-value", request=request)

    settings = Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=1, lichess_token="deliberately-secret-value")
    with ProviderSession(settings, transport=httpx.MockTransport(handler), clock=clock, sleeper=clock.sleep) as session:
        result = ingest.fetch_user_profile(conn, "lichess", "samename", session=session)
    assert result.status_code == 0
    assert len(calls) == 2
    assert clock.sleeps == [1]
    errors = conn.execute("SELECT error_kind, message, is_dead FROM errors").fetchall()
    assert len(errors) == 2
    expected = "timeout" if issubclass(exception, httpx.TimeoutException) else "other"
    assert [tuple(row.values()) for row in errors] == [(expected, "provider request failed", 0)] * 2
    for table in ("errors", "fetch_logs"):
        assert "deliberately-secret-value" not in repr([tuple(row.values()) for row in conn.execute(f"SELECT * FROM {table}")])


@pytest.mark.parametrize(("header", "expected"), [("seconds", 7), ("date", 7), ("expired", 0), ("invalid", None)])
def test_retry_after_handles_seconds_and_http_dates(header, expected) -> None:
    clock = Clock()
    headers = {
        "seconds": "7", "date": format_datetime(datetime.fromtimestamp(clock.now + 7, UTC), usegmt=True),
        "expired": format_datetime(datetime.fromtimestamp(clock.now - 7, UTC), usegmt=True), "invalid": "later",
    }
    responses = iter([httpx.Response(429, headers={"Retry-After": headers[header]}), httpx.Response(200, content=b"{}")])
    client = HttpClient(
        provider="chess.com", user_agent="test", policy=FetchPolicy(0, True, True, None, 1),
        transport=httpx.MockTransport(lambda request: next(responses)), sleeper=clock.sleep, clock=clock,
    )
    try:
        result = client.request("GET", "https://example.invalid/profile", endpoint_type="user_profile")
    finally:
        client.close()
    assert result.status_code == 200
    assert result.attempts[0].retry_after == expected
    assert clock.sleeps == [expected or 0]


def test_shared_session_retains_pacing_and_closes_transport(initialized_conn, fixtures_dir) -> None:
    clock = Clock()
    requested_at = []
    closed = []

    def handler(request):
        requested_at.append(clock.now)
        return httpx.Response(200, content=profile_record(fixtures_dir).body)

    class Transport(httpx.MockTransport):
        def close(self):
            closed.append(True)

    settings = Config(chesscom_delay_s=3, lichess_delay_s=0, max_retries=0)
    with ProviderSession(settings, transport=Transport(handler), sleeper=clock.sleep, clock=clock) as session:
        ingest.fetch_user_profile(initialized_conn, "chess.com", "samename", session=session)
        ingest.fetch_user_profile(initialized_conn, "chess.com", "samename", session=session)
        assert closed == []
    assert requested_at == [1_700_000_000, 1_700_000_003]
    assert clock.sleeps == [3]
    assert closed == [True]
    with pytest.raises(RuntimeError, match="closed"):
        session.client("chess.com")


@pytest.mark.parametrize(("retry_after", "expected"), [(None, 60), ("120", 120)])
def test_exhausted_429_cooldown_applies_to_next_job_in_session(
    initialized_conn, fixtures_dir, retry_after, expected,
) -> None:
    clock = Clock()
    headers = {"Retry-After": retry_after} if retry_after else {}
    responses = iter([httpx.Response(429, headers=headers), httpx.Response(200, content=(fixtures_dir / "lichess/user.json").read_bytes())])
    with ProviderSession(
        config(), transport=httpx.MockTransport(lambda request: next(responses)), sleeper=clock.sleep, clock=clock,
    ) as session:
        first = ingest.fetch_user_profile(initialized_conn, "lichess", "samename", session=session)
        second = ingest.fetch_user_profile(initialized_conn, "lichess", "samename", session=session)
    assert first.retry_after == expected
    assert second.status_code == 200
    assert clock.sleeps == [expected]


def test_session_closes_all_clients_even_if_one_close_fails(monkeypatch) -> None:
    closed = []

    class Client:
        def __init__(self, provider):
            self.provider = provider

        def close(self):
            closed.append(self.provider)
            if self.provider == "lichess":
                raise RuntimeError("close failed")

    monkeypatch.setattr(registry, "create_provider_client", lambda provider, *args, **kwargs: Client(provider))
    session = ProviderSession(config())
    session.client("chess.com")
    session.client("lichess")
    with pytest.raises(RuntimeError, match="close failed"):
        session.close()
    assert closed == ["lichess", "chess.com"]
    session.close()
    assert len(closed) == 2


def test_single_lichess_game_uses_documented_export_endpoint(initialized_conn, fixtures_dir) -> None:
    def handler(request):
        assert str(request.url.copy_with(query=None)) == "https://lichess.org/game/export/lichgame1"
        assert dict(request.url.params) == {"clocks": "true", "evals": "true", "accuracy": "true"}
        assert request.headers["Accept"] == "application/json"
        return httpx.Response(200, content=(fixtures_dir / "lichess/games.ndjson").read_bytes().splitlines()[0])

    result = ingest.fetch_lichess_game(initialized_conn, "lichgame1", config=config(), transport=httpx.MockTransport(handler))
    assert result.normalized_ids


@pytest.mark.parametrize("status", [429, 503, 0])
def test_stop_during_retry_backoff_preserves_last_attempt_without_another_request(initialized_conn, status) -> None:
    clock = Clock()
    stopped = False
    requests = []

    def handler(request):
        requests.append(request)
        if status == 0:
            raise httpx.ConnectError("connection unavailable", request=request)
        return httpx.Response(status, headers={"Retry-After": "120"})

    def interrupted_sleep(delay):
        nonlocal stopped
        clock.sleeps.append(delay)
        clock.now += 0.25
        stopped = True

    settings = Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=3)
    with ProviderSession(
        settings, transport=httpx.MockTransport(handler), clock=clock,
        sleeper=interrupted_sleep, stop_requested=lambda: stopped,
    ) as session:
        result = ingest.fetch_user_profile(initialized_conn, "lichess", "samename", session=session)
    assert len(requests) == 1
    assert result.status_code == status
    assert result.retry_after == (120 if status else None)
    assert clock.sleeps == [120 if status else 1]
    evidence = initialized_conn.execute("SELECT status_code, retry_after, attempt FROM fetch_logs").fetchall()
    assert [tuple(row.values()) for row in evidence] == [(status or None, 120 if status else None, 1)]


@pytest.mark.parametrize("stop_before_first", [True, False])
def test_stop_before_network_does_not_fabricate_a_fetch(initialized_conn, fixtures_dir, stop_before_first) -> None:
    from chess_crawl.providers.base import ProviderRequestStopped

    clock = Clock()
    stopped = stop_before_first
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=profile_record(fixtures_dir).body)

    def interrupted_sleep(delay):
        nonlocal stopped
        stopped = True

    settings = Config(chesscom_delay_s=3, lichess_delay_s=0, max_retries=0)
    with ProviderSession(
        settings, transport=httpx.MockTransport(handler), clock=clock,
        sleeper=interrupted_sleep, stop_requested=lambda: stopped,
    ) as session:
        if not stop_before_first:
            ingest.fetch_user_profile(initialized_conn, "chess.com", "samename", session=session)
        with pytest.raises(ProviderRequestStopped):
            ingest.fetch_user_profile(initialized_conn, "chess.com", "samename", session=session)
    assert len(requests) == (0 if stop_before_first else 1)
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == len(requests)
