from __future__ import annotations

import socket
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from chess_crawl.storage.db import open_database
from support import seed_game
from chess_crawl.storage.discovery import OpponentEdge, record_discovery_edges


@pytest.fixture(autouse=True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def guard(*args: object, **kwargs: object) -> None:
        raise AssertionError("tests must not open network sockets")

    monkeypatch.setattr(socket.socket, "connect", guard)


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


@pytest.fixture
def initialized_conn() -> Iterator[sqlite3.Connection]:
    with open_database(":memory:", writable=True) as conn:
        yield conn


@pytest.fixture
def archive_path(tmp_path: Path) -> Path:
    path = tmp_path / "archive.sqlite"
    with open_database(path, writable=True):
        pass
    return path


@pytest.fixture
def seeded_archive(tmp_path: Path) -> Path:
    """Two providers with a shared username and one recorded discovery edge."""
    archive_path = tmp_path / "archive.sqlite"
    with open_database(archive_path, writable=True) as conn:
        game_id, same_id, opponent_id = seed_game(
            conn, provider="chess.com", game_key="cc-1", white="SameName", black="Opponent",
        )
        seed_game(
            conn, provider="lichess", game_key="li-1", white="SameName", black="Opponent",
            outcome="black_win",
        )
        record_discovery_edges(
            conn, crawl_run_id=None, provider="chess.com", from_user_id=same_id, depth=1,
            edges=[OpponentEdge(opponent_id, "opponent", game_id, 1)],
        )
    return archive_path
