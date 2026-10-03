"""Verify the offline default using an isolated synthetic pytest collection."""

from __future__ import annotations

import socket
from pathlib import Path

import pytest


pytest_plugins = ["pytester"]


@pytest.mark.parametrize("run_live", [False, True], ids=["offline-default", "explicit-live"])
def test_live_opt_in_preserves_offline_isolation_for_unmarked_tests(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, run_live: bool,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_MAX_GAMES", "1")
    monkeypatch.setenv("CHESS_CRAWL_API_TOKEN_FILE", "/unused/test-token")
    monkeypatch.setenv("UNRELATED_TEST_ENV", "preserved")

    def sentinel_connect(*args: object, **kwargs: object) -> None:
        raise RuntimeError("sentinel prevents real networking")

    # Test both connection entry points without opening a socket.
    for method in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, method, sentinel_connect)
    pytester.makeconftest(Path(__file__).with_name("conftest.py").read_text())
    pytester.makeini("[pytest]\nmarkers = live: requires explicit network opt-in\n")
    pytester.makepyfile('''
        import os
        import socket
        import pytest
        from chess_crawl.application import Limits

        def test_offline(monkeypatch):
            assert not any(key.startswith("CHESS_CRAWL_") for key in os.environ)
            assert os.environ["UNRELATED_TEST_ENV"] == "preserved"
            for method in ("connect", "connect_ex"):
                with pytest.raises(AssertionError, match="must not open network sockets"):
                    getattr(socket.socket, method)(None)
            monkeypatch.setenv("CHESS_CRAWL_MAX_GAMES", "5")
            assert Limits.from_env().max_games == 5

        @pytest.mark.live
        def test_live():
            assert os.environ["CHESS_CRAWL_MAX_GAMES"] == "1"
            assert os.environ["CHESS_CRAWL_API_TOKEN_FILE"] == "/unused/test-token"
            for method in ("connect", "connect_ex"):
                with pytest.raises(RuntimeError, match="sentinel prevents real networking"):
                    getattr(socket.socket, method)(None)
    ''')
    result = pytester.runpytest("-q", *(["--run-live"] if run_live else []))
    result.assert_outcomes(passed=2 if run_live else 1, skipped=0 if run_live else 1)
