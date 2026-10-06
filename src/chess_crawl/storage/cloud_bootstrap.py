"""One-shot schema migration and restricted cloud runtime-role bootstrap."""
from __future__ import annotations

from chess_crawl.settings import setting

import re
import sys

from psycopg import sql

from chess_crawl.storage.db import Connection, connection, database_url, require_row, transaction
from chess_crawl.storage.migrations import initialize


def bootstrap_runtime_role(conn: Connection, *, username: str, password: str) -> None:
    if not isinstance(username, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", username):
        raise ValueError("Runtime database user must be a lowercase PostgreSQL identifier")
    if username.startswith("pg_") or username in {"postgres", "public"}:
        raise ValueError("Reserved database role cannot be the application runtime")
    if not isinstance(password, str) or len(password) < 32 or "\x00" in password:
        raise ValueError("Runtime database password must contain at least 32 characters")
    role = sql.Identifier(username)
    with transaction(conn):
        owner = str(require_row(conn.execute("SELECT current_user"))[0])
        if username == owner:
            raise ValueError("Migration credentials must differ from runtime credentials")
        existing = conn.execute(
            """SELECT r.rolsuper, r.rolcreatedb, r.rolcreaterole, r.rolreplication, r.rolbypassrls,
                     EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) AS member_of_role,
                     EXISTS(SELECT 1 FROM pg_database WHERE datdba=r.oid) AS owns_database,
                     EXISTS(SELECT 1 FROM pg_namespace WHERE nspowner=r.oid) AS owns_schema,
                     EXISTS(SELECT 1 FROM pg_shdepend
                            WHERE refclassid='pg_authid'::regclass AND refobjid=r.oid AND deptype='o') AS owns_object,
                     EXISTS(SELECT 1 FROM pg_namespace n, aclexplode(n.nspacl) a
                            WHERE a.grantee=r.oid AND
                              (n.nspname<>'public' OR a.privilege_type<>'USAGE' OR a.is_grantable))
                       OR EXISTS(SELECT 1 FROM pg_proc p, aclexplode(p.proacl) a WHERE a.grantee=r.oid)
                       OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                                 CROSS JOIN LATERAL aclexplode(c.relacl) a
                                 WHERE a.grantee=r.oid AND
                                   (n.nspname<>'public' OR a.is_grantable OR
                                    a.privilege_type NOT IN ('SELECT','INSERT','UPDATE','DELETE','USAGE')))
                       OR EXISTS(SELECT 1 FROM pg_attribute t, aclexplode(t.attacl) a WHERE a.grantee=r.oid)
                       OR EXISTS(SELECT 1 FROM pg_database d, aclexplode(d.datacl) a
                                 WHERE a.grantee=r.oid AND
                                   (d.datname<>current_database() OR a.is_grantable OR
                                    a.privilege_type<>'CONNECT')) AS unexpected_grants
                FROM pg_roles r WHERE r.rolname=%s""", (username,),
        ).fetchone()
        if existing is not None and any(bool(value) for value in existing.values()):
            raise ValueError("Existing runtime role has administrative privileges, ownership, or unexpected grants")
        if existing is None:
            conn.execute(sql.SQL(
                "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            ).format(role))
        # RDS masters are CREATEROLE database owners, not PostgreSQL superusers.
        # Even unchanged negative privileged attributes cannot be specified in
        # ALTER ROLE by those owners. Creation defaults and the refusal above
        # already establish the runtime boundary; only rotate permitted settings.
        conn.execute(sql.SQL("ALTER ROLE {} LOGIN NOINHERIT PASSWORD {}").format(role, sql.Literal(password)))
        database = sql.Identifier(str(require_row(conn.execute("SELECT current_database()"))[0]))
        # PUBLIC initially grants TEMPORARY, independently of direct role grants.
        # This is a dedicated application database; runtime workloads need no DDL.
        conn.execute(sql.SQL("REVOKE TEMPORARY ON DATABASE {} FROM PUBLIC").format(database))
        conn.execute(sql.SQL("REVOKE CREATE, TEMPORARY ON DATABASE {} FROM {}").format(database, role))
        conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database, role))
        # PostgreSQL's initial public schema grant can permit DDL independently
        # of role-specific grants; revoke it for this dedicated application DB.
        conn.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        conn.execute(sql.SQL("REVOKE CREATE ON SCHEMA public FROM {}").format(role))
        conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
        conn.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {}").format(role))
        conn.execute(sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {}").format(role))
        conn.execute(sql.SQL("REVOKE INSERT, UPDATE, DELETE ON schema_migrations FROM {}").format(role))


def main() -> int:
    try:
        username = setting("CHESS_CRAWL_APPLICATION_DATABASE_USER")
        password = setting("CHESS_CRAWL_APPLICATION_DATABASE_PASSWORD")
        if not username or not password:
            raise ValueError("Set application database username and password before bootstrap")
        with connection(database_url(), mode="rw") as conn:
            # Reuse the atomic schema+role boundary rather than initializing on
            # connection entry. External connections default to verified TLS.
            with transaction(conn):
                initialize(conn)
                bootstrap_runtime_role(conn, username=username, password=password)
        print("Schema and restricted application role are ready")
        return 0
    except Exception:
        # Credentials and server error/SQL text must not enter task logs.
        print("Cloud database bootstrap failed; inspect configuration and operator access", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
