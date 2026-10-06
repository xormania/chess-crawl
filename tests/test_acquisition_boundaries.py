"""Accepted date windows must be representable before any provider request."""

from __future__ import annotations

from chess_crawl.storage.db import require_row

import json
from pathlib import Path

import httpx
import pytest

from chess_crawl.application import ImportRequest, submit_import
from chess_crawl.config import Config
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.providers.lichess import client as lichess_client
from chess_crawl.providers.lichess.client import LichessClient
from chess_crawl.storage.raw import read_raw_payload


@pytest.mark.parametrize("provider", ["chess.com", "lichess"])
def test_last_supported_day_is_acquired_and_preserved(initialized_conn, fixtures_dir: Path, provider: str) -> None:
    until = 253402300800
    submission = submit_import(
        initialized_conn, ImportRequest(provider, "SameName", until - 86400, until, 1),
        idempotency_key="last-supported-day",
    )
    requested: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url)
        if request.url.path in {"/pub/player/samename", "/api/user/samename"}:
            fixture = "chesscom/player.json" if provider == "chess.com" else "lichess/user.json"
            body = (fixtures_dir / fixture).read_bytes()
            media_type = "application/json"
        else:
            body = b'{"games":[]}' if provider == "chess.com" else b""
            media_type = "application/json" if provider == "chess.com" else "application/x-ndjson"
        return httpx.Response(200, content=body, headers={"content-type": media_type})

    result = JobRunner(
        initialized_conn, config=Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0),
        transport=httpx.MockTransport(handler),
    ).run(crawl_run_id=submission["run_id"])
    assert result.done == 3 and result.errors == 0
    assert len(requested) == 2
    raw_id = require_row(initialized_conn.execute(
        "SELECT id FROM raw_payloads WHERE endpoint_type IN ('monthly_archive', 'user_games_stream')"
    ))[0]
    raw = read_raw_payload(initialized_conn, raw_id)
    if provider == "chess.com":
        assert requested[-1].path.endswith("/9999/12")
        assert raw.body == b'{"games":[]}'
    else:
        assert requested[-1].params["until"] == str(until * 1000)
        assert raw.request_params is not None
        assert json.loads(raw.request_params)["until"] == until * 1000
        assert str(until) in raw.canonical_source_key
        assert raw.body == b""


def test_lichess_request_metadata_is_prepared_before_network(monkeypatch: pytest.MonkeyPatch) -> None:
    requested: list[httpx.Request] = []

    def metadata_failure(*args):
        raise ValueError("invalid local request metadata")

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        return httpx.Response(200, content=b"")

    monkeypatch.setattr(lichess_client, "_range_unit_id", metadata_failure)
    client = LichessClient(Config(lichess_delay_s=0).provider("lichess"), transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(ValueError, match="invalid local request metadata"):
            client.get_user_games("SameName", since=1704067200, until=1704153600, limit=1)
    finally:
        client.close()
    assert requested == []


@pytest.mark.parametrize("invalid", [{"since": True}, {"until": "tomorrow"}, {"limit": 0}])
def test_invalid_request_metadata_never_reaches_transport(invalid) -> None:
    requested: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        return httpx.Response(200, content=b"")

    client = LichessClient(Config(lichess_delay_s=0).provider("lichess"), transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(ValueError):
            client.get_user_games("SameName", **{"since": 1704067200, "until": 1704153600, "limit": 1, **invalid})
    finally:
        client.close()
    assert requested == []


def test_distinct_subday_windows_have_distinct_source_keys() -> None:
    client = LichessClient(
        Config(lichess_delay_s=0).provider("lichess"),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"")),
    )
    try:
        first = client.get_user_games("SameName", since=1704067200, until=1704067201, limit=1)
        second = client.get_user_games("SameName", since=1704067201, until=1704067202, limit=1)
    finally:
        client.close()
    assert first.canonical_source_key != second.canonical_source_key
