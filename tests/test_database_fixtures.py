"""Fast initialized clones retain independent state and pristine migration inputs."""
from collections.abc import Callable

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from chess_crawl.storage.db import connection, require_row
from chess_crawl.storage.migrations import SCHEMA_VERSION


def test_initialized_clones_have_isolated_data_and_schema(database_factory: Callable[..., str]) -> None:
    first, second = database_factory(), database_factory()
    with connection(first, mode="rw") as conn:
        conn.execute("CREATE TABLE fixture_only(value INTEGER)")
        conn.execute("DELETE FROM providers")
    for target in (second, database_factory()):
        with connection(target) as conn:
            assert require_row(conn.execute("SELECT to_regclass('fixture_only')"))[0] is None
            assert require_row(conn.execute("SELECT COUNT(*) FROM providers"))[0] > 0
            assert require_row(conn.execute("SELECT MAX(version) FROM schema_migrations"))[0] == SCHEMA_VERSION


def test_template_rejects_connections_and_has_no_open_sessions(
    postgres_template: str, postgres_admin: psycopg.Connection,
) -> None:
    assert postgres_admin.execute(
        "SELECT datallowconn FROM pg_database WHERE datname=%s", (postgres_template,),
    ).fetchone() == (False,)
    assert postgres_admin.execute(
        "SELECT COUNT(*) FROM pg_stat_activity WHERE datname=%s", (postgres_template,),
    ).fetchone() == (0,)
    with pytest.raises(psycopg.OperationalError, match="not currently accepting connections"):
        with psycopg.connect(make_conninfo(postgres_admin.info.dsn, dbname=postgres_template)):
            pass


def test_uninitialized_database_remains_unmigrated(uninitialized_database_url: str) -> None:
    with connection(uninitialized_database_url) as conn:
        assert require_row(conn.execute("SELECT to_regclass('schema_migrations')"))[0] is None
