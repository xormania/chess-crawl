from __future__ import annotations

import os
import socket
from collections.abc import Callable, Iterator
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from chess_crawl.storage.db import Connection, open_database
from chess_crawl.storage.discovery import OpponentEdge, record_discovery_edges
from support import seed_game

# Capture the explicitly selected test cluster before application environment
# isolation. Every database fixture creates and drops only its own random name.
_TEST_DATABASE_URL = os.environ.get("CHESS_CRAWL_TEST_DATABASE_URL")
_TEST_DATABASE_PASSWORD = os.environ.get("CHESS_CRAWL_TEST_DATABASE_PASSWORD")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-live", action="store_true", default=False,
        help="Allow explicitly marked live tests to use configured provider APIs",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-live"):
        return
    skip_live = pytest.mark.skip(reason="Live tests require explicit --run-live opt-in")
    for item in items:
        if item.get_closest_marker("live") is not None:
            item.add_marker(skip_live)


def _live_enabled(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--run-live") and request.node.get_closest_marker("live"))


@pytest.fixture(autouse=True)
def isolated_environment(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if not _live_enabled(request):
        for name in tuple(os.environ):
            if name.startswith("CHESS_CRAWL_"):
                monkeypatch.delenv(name)


@pytest.fixture(autouse=True)
def no_network(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if _live_enabled(request):
        return

    def guard(*args: object, **kwargs: object) -> None:
        raise AssertionError("tests must not open provider network sockets")

    # libpq establishes the explicitly configured disposable database connection
    # in native code. Python HTTP/provider traffic remains blocked.
    monkeypatch.setattr(socket.socket, "connect", guard)
    monkeypatch.setattr(socket.socket, "connect_ex", guard)


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="session")
def postgres_admin() -> Iterator[psycopg.Connection]:
    if not _TEST_DATABASE_URL:
        pytest.fail("Database tests require CHESS_CRAWL_TEST_DATABASE_URL pointing to a disposable PostgreSQL cluster")
    try:
        options = conninfo_to_dict(_TEST_DATABASE_URL)
    except psycopg.Error:
        pytest.fail("CHESS_CRAWL_TEST_DATABASE_URL must contain PostgreSQL connection settings")
    if options.get("service"):
        pytest.fail("The disposable test database must be explicitly configured, without a libpq service")
    if _TEST_DATABASE_PASSWORD is not None:
        options["password"] = _TEST_DATABASE_PASSWORD
    with psycopg.connect(make_conninfo("", **options), autocommit=True) as conn:
        yield conn


@pytest.fixture
def database_factory(
    postgres_admin: psycopg.Connection, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Callable[..., str]]:
    names: list[str] = []
    password = _TEST_DATABASE_PASSWORD
    if password is None:
        embedded_password = conninfo_to_dict(_TEST_DATABASE_URL or "").get("password")
        password = str(embedded_password) if embedded_password is not None else None
    if password is not None:
        monkeypatch.setenv("CHESS_CRAWL_DATABASE_PASSWORD", str(password))

    def create(*, initialized: bool = True) -> str:
        name = f"chess_crawl_test_{uuid4().hex}"
        postgres_admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
        names.append(name)
        options = conninfo_to_dict(_TEST_DATABASE_URL or "")
        options.pop("password", None)
        options["dbname"] = name
        target = make_conninfo("", **options)
        if initialized:
            with open_database(target, writable=True):
                pass
        return target

    try:
        yield create
    finally:
        # FORCE cleans up leaked child-process connections after a failed test,
        # while the random names confine cleanup to databases this fixture created.
        for name in reversed(names):
            postgres_admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture
def uninitialized_database_url(database_factory: Callable[..., str]) -> str:
    return database_factory(initialized=False)


@pytest.fixture
def database_url(uninitialized_database_url: str) -> str:
    with open_database(uninitialized_database_url, writable=True):
        pass
    return uninitialized_database_url


@pytest.fixture
def initialized_conn(database_url: str) -> Iterator[Connection]:
    with open_database(database_url, writable=True) as conn:
        yield conn


@pytest.fixture
def seeded_database_url(database_url: str) -> str:
    """Two providers with a shared username and one recorded discovery edge."""
    with open_database(database_url, writable=True) as conn:
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
    return database_url
