from __future__ import annotations

from chess_crawl.storage.db import open_database, require_row

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import fetch_user_profile
import chess_crawl.ingest as ingest
from chess_crawl.jobs.state import provider_ready_at
from chess_crawl.providers.registry import ProviderSession


@pytest.mark.parametrize(
    ("provider", "status", "retry_after", "expected_deadline"),
    [
        ("lichess", 429, "45", 160),
        ("chess.com", 429, "45", 145),
        ("chess.com", 503, "45", 145),
        ("chess.com", 503, None, None),
        ("chess.com", 404, None, None),
    ],
)
def test_direct_acquisition_commits_provider_floor_with_fetch_evidence(
    database_url, provider, status, retry_after, expected_deadline,
) -> None:
    headers = {"Retry-After": retry_after} if retry_after else {}
    settings = Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0)
    with open_database(database_url, writable=True) as conn, ProviderSession(
        settings, clock=lambda: 100,
        transport=httpx.MockTransport(lambda request: httpx.Response(status, headers=headers)),
    ) as session:
        result = fetch_user_profile(conn, provider, "samename", session=session)
        assert result.status_code == status
    with open_database(database_url) as conn:
        assert provider_ready_at(conn, provider) == expected_deadline
        assert require_row(conn.execute("SELECT status_code FROM fetch_logs"))[0] == status


def test_provider_floor_and_response_evidence_commit_atomically(initialized_conn, monkeypatch) -> None:
    def fail(*args, **kwargs):
        raise RuntimeError("could not persist provider deadline")

    monkeypatch.setattr(ingest, "defer_provider", fail)
    settings = Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0)
    with ProviderSession(
        settings, clock=lambda: 100,
        transport=httpx.MockTransport(lambda request: httpx.Response(429, headers={"Retry-After": "45"})),
    ) as session, pytest.raises(RuntimeError, match="provider deadline"):
        fetch_user_profile(initialized_conn, "chess.com", "samename", session=session)
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 0
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM errors"))[0] == 0
    assert provider_ready_at(initialized_conn, "chess.com") is None
