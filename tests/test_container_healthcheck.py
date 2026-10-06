from __future__ import annotations

import http.client
import runpy
import sys
from pathlib import Path

import pytest


@pytest.fixture
def probe(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["healthcheck.py", "api"])
    return runpy.run_path(str(Path(__file__).resolve().parents[1] / "docker" / "healthcheck.py"))


@pytest.fixture
def requests(monkeypatch):
    calls: list[tuple] = []

    class LocalClient:
        def __init__(self, host, port, *, timeout):
            calls.append((host, port, timeout))

        def request(self, method, path, *, headers):
            calls.append((method, path, headers))

        def getresponse(self):
            return type("Response", (), {"status": 200})()

        def close(self):
            calls.append(("closed",))

    monkeypatch.setattr(http.client, "HTTPConnection", LocalClient)
    return calls


def test_database_health_probe_uses_dedicated_credential_and_fixed_local_route(probe, requests, monkeypatch, capsys) -> None:
    monkeypatch.setenv("CHESS_CRAWL_API_AUTH_MODE", "database")
    monkeypatch.setenv("CHESS_CRAWL_HEALTHCHECK_TOKEN", "opaque-readiness-credential")
    assert probe["main"]() == 0
    assert requests == [
        ("127.0.0.1", 8000, 3),
        ("GET", "/health/ready", {"Authorization": "Bearer opaque-readiness-credential"}),
        ("closed",),
    ]
    assert capsys.readouterr().out == ""


def test_database_health_probe_never_falls_back_to_static_api_token(probe, requests, monkeypatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_API_AUTH_MODE", "database")
    monkeypatch.setenv("CHESS_CRAWL_API_TOKEN", "obsolete-static-credential")
    assert probe["main"]() == 1
    assert requests == []


def test_static_health_probe_retains_api_secret_file_fallback(probe, requests, monkeypatch, tmp_path) -> None:
    secret = tmp_path / "secret"
    secret.write_text("local-service-token\n", encoding="utf-8")
    monkeypatch.setenv("CHESS_CRAWL_API_TOKEN_FILE", str(secret))
    assert probe["main"]() == 0
    assert requests[1][2] == {"Authorization": "Bearer local-service-token"}


@pytest.mark.parametrize("kind", ["conflict", "empty", "missing", "whitespace", "invalid-mode"])
def test_invalid_health_credentials_fail_without_request_or_secret_logging(probe, requests, monkeypatch, tmp_path, capsys, kind) -> None:
    secret = tmp_path / "secret"
    secret.write_text("private-probe-token\n", encoding="utf-8")
    monkeypatch.setenv("CHESS_CRAWL_API_TOKEN", "fallback-must-not-hide-errors")
    if kind == "conflict":
        monkeypatch.setenv("CHESS_CRAWL_HEALTHCHECK_TOKEN", "private-probe-token")
        monkeypatch.setenv("CHESS_CRAWL_HEALTHCHECK_TOKEN_FILE", str(secret))
    elif kind == "empty":
        secret.write_text("", encoding="utf-8")
        monkeypatch.setenv("CHESS_CRAWL_HEALTHCHECK_TOKEN_FILE", str(secret))
    elif kind == "missing":
        monkeypatch.setenv("CHESS_CRAWL_HEALTHCHECK_TOKEN_FILE", str(tmp_path / "missing"))
    elif kind == "whitespace":
        monkeypatch.setenv("CHESS_CRAWL_HEALTHCHECK_TOKEN", "private-probe-token\nheader: injected")
    else:
        monkeypatch.setenv("CHESS_CRAWL_API_AUTH_MODE", "unknown")
    assert probe["main"]() == 1
    assert requests == []
    output = capsys.readouterr()
    assert not output.out and not output.err
