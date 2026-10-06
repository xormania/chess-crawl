"""Cloud runtime roles can use data while schema/role management stays privileged."""
from __future__ import annotations

from uuid import uuid4
from typing import cast

import psycopg
import pytest
from psycopg import sql

from chess_crawl.storage.cloud_bootstrap import bootstrap_runtime_role, main
from chess_crawl.storage.db import Connection, connection, require_row


def test_runtime_role_is_idempotent_and_has_data_privileges_only(database_url) -> None:
    username = "runtime_" + uuid4().hex
    password = "a-long-disposable-test-password-123456"
    with connection(database_url, mode="rw") as conn:
        try:
            bootstrap_runtime_role(conn, username=username, password=password)
            bootstrap_runtime_role(conn, username=username, password=password)
            role = sql.Identifier(username)
            with conn.transaction():
                conn.execute(sql.SQL("SET LOCAL ROLE {}").format(role))
                assert require_row(conn.execute("SELECT COUNT(*) FROM schema_migrations"))[0] > 0
                conn.execute(
                    "INSERT INTO provider_users(provider,username_normalized,display_username,first_seen_at,updated_at) "
                    "VALUES ('lichess','cloud-runtime','cloud-runtime',1,1)"
                )
                conn.execute("UPDATE provider_users SET title='GM' WHERE username_normalized='cloud-runtime'")
                conn.execute("DELETE FROM provider_users WHERE username_normalized='cloud-runtime'")
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    with conn.transaction():
                        conn.execute("CREATE TABLE forbidden_ddl(id INTEGER)")
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    with conn.transaction():
                        conn.execute("CREATE TEMP TABLE forbidden_temp(id INTEGER)")
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    with conn.transaction():
                        conn.execute("CREATE SCHEMA forbidden_schema")
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    with conn.transaction():
                        conn.execute("UPDATE schema_migrations SET applied_at=0")
                assert require_row(conn.execute("SELECT COUNT(*) FROM workspace_credentials"))[0] == 0
                for mutation in (
                    "INSERT INTO workspace_credentials(id,workspace_id,token_digest,created_at) VALUES('forbidden','local',repeat('a',64),1)",
                    "UPDATE workspace_credentials SET revoked_at=1",
                    "DELETE FROM workspace_credentials",
                ):
                    with pytest.raises(psycopg.errors.InsufficientPrivilege):
                        with conn.transaction():
                            conn.execute(mutation)
                conn.execute(
                    "INSERT INTO workspaces(id,created_at) VALUES('runtime-history',1)"
                )
                conn.execute(
                    "INSERT INTO workspace_policy_history(workspace_id,version,policy,managed,changed_at) "
                    "VALUES('runtime-history',1,'{}',false,1)"
                )
                assert require_row(conn.execute("SELECT COUNT(*) FROM workspace_policy_history WHERE workspace_id='runtime-history'"))[0] == 1
                for mutation in (
                    "UPDATE workspace_policy_history SET changed_at=2 WHERE workspace_id='runtime-history'",
                    "DELETE FROM workspace_policy_history WHERE workspace_id='runtime-history'",
                ):
                    with pytest.raises(psycopg.errors.InsufficientPrivilege):
                        with conn.transaction():
                            conn.execute(mutation)
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    with conn.transaction():
                        conn.execute("CREATE ROLE forbidden_role")
        finally:
            conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(username)))
            conn.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(username)))


@pytest.mark.parametrize("username", ["postgres", "public", "pg_runtime", "bad;role", "A", ""])
def test_bootstrap_rejects_unsafe_or_reserved_role_names(username) -> None:
    class NoDatabase:
        def __getattr__(self, name):
            pytest.fail(f"Invalid role names must be rejected before database access: {name}")

    with pytest.raises(ValueError):
        bootstrap_runtime_role(cast(Connection, NoDatabase()), username=username, password="x" * 32)


def test_bootstrap_rejects_short_password_and_migration_role(initialized_conn) -> None:
    conn = initialized_conn
    with pytest.raises(ValueError, match="password"):
        bootstrap_runtime_role(conn, username="valid_role", password="short")
    current = str(require_row(conn.execute("SELECT current_user"))[0])
    with pytest.raises(ValueError):
        bootstrap_runtime_role(conn, username=current, password="x" * 32)


def test_bootstrap_will_not_reuse_an_administrative_role(initialized_conn) -> None:
    conn = initialized_conn
    username = "admin_" + uuid4().hex
    conn.execute(sql.SQL("CREATE ROLE {} CREATEDB").format(sql.Identifier(username)))
    try:
        with pytest.raises(ValueError, match="administrative"):
            bootstrap_runtime_role(conn, username=username, password="x" * 32)
    finally:
        conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(username)))


def test_failed_role_bootstrap_rolls_back_schema_and_sanitizes_logs(
    uninitialized_database_url, monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", uninitialized_database_url)
    monkeypatch.setenv("CHESS_CRAWL_APPLICATION_DATABASE_USER", "unsafe;role")
    secret = "never-log-this-private-password-123456"
    monkeypatch.setenv("CHESS_CRAWL_APPLICATION_DATABASE_PASSWORD", secret)
    assert main() == 1
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    assert "unsafe;role" not in output.out + output.err
    with connection(uninitialized_database_url) as conn:
        assert require_row(conn.execute("SELECT to_regclass('schema_migrations')"))[0] is None


@pytest.mark.parametrize("privilege", ["schema", "function", "table", "grant_option"])
def test_bootstrap_rejects_unexpected_existing_grants(initialized_conn, privilege) -> None:
    conn = initialized_conn
    username = "unexpected_" + uuid4().hex
    role = sql.Identifier(username)
    conn.execute(sql.SQL("CREATE ROLE {}").format(role))
    try:
        if privilege == "schema":
            conn.execute(sql.SQL("GRANT USAGE ON SCHEMA pg_catalog TO {}").format(role))
        elif privilege == "function":
            conn.execute(sql.SQL("GRANT EXECUTE ON FUNCTION pg_catalog.current_database() TO {}").format(role))
        elif privilege == "table":
            conn.execute(sql.SQL("GRANT TRUNCATE ON provider_users TO {}").format(role))
        else:
            conn.execute(sql.SQL("GRANT SELECT ON provider_users TO {} WITH GRANT OPTION").format(role))
        with pytest.raises(ValueError, match="unexpected grants"):
            bootstrap_runtime_role(conn, username=username, password="x" * 32)
    finally:
        conn.execute(sql.SQL("DROP OWNED BY {}").format(role))
        conn.execute(sql.SQL("DROP ROLE {}").format(role))


def test_non_superuser_database_owner_can_bootstrap_and_rotate_runtime_role(uninitialized_database_url) -> None:
    """Model RDS master flags rather than assuming unrestricted local postgres."""
    from chess_crawl.storage.migrations import initialize

    master_name = "migration_" + uuid4().hex
    runtime_name = "runtime_" + uuid4().hex
    master, runtime = sql.Identifier(master_name), sql.Identifier(runtime_name)
    with connection(uninitialized_database_url, mode="rw") as conn:
        database = sql.Identifier(str(require_row(conn.execute("SELECT current_database()"))[0]))
        original_owner = sql.Identifier(str(require_row(conn.execute("SELECT current_user"))[0]))
        conn.execute(sql.SQL("CREATE ROLE {} NOSUPERUSER CREATEDB CREATEROLE NOREPLICATION NOBYPASSRLS").format(master))
        conn.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(database, master))
        try:
            conn.execute(sql.SQL("SET ROLE {}").format(master))
            initialize(conn)
            bootstrap_runtime_role(conn, username=runtime_name, password="first-disposable-runtime-password-12345")
            bootstrap_runtime_role(conn, username=runtime_name, password="rotated-disposable-runtime-password-123")
            flags = require_row(conn.execute(
                "SELECT rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=%s",
                (runtime_name,),
            ))
            assert not any(flags.values())
            conn.execute("RESET ROLE")
            with conn.transaction():
                conn.execute(sql.SQL("SET LOCAL ROLE {}").format(runtime))
                assert require_row(conn.execute("SELECT COUNT(*) FROM providers"))[0] > 0
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    with conn.transaction():
                        conn.execute("CREATE TEMP TABLE forbidden_temp(id INTEGER)")
        finally:
            conn.execute("RESET ROLE")
            if conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (runtime_name,)).fetchone():
                conn.execute(sql.SQL("DROP OWNED BY {}").format(runtime))
                conn.execute(sql.SQL("DROP ROLE {}").format(runtime))
            conn.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(database, original_owner))
            conn.execute(sql.SQL("DROP OWNED BY {}").format(master))
            conn.execute(sql.SQL("DROP ROLE {}").format(master))
