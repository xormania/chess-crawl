from __future__ import annotations

from chess_crawl.storage.db import Connection, connection, require_row

import psycopg

import pytest

from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.migrations import SCHEMA_VERSION, initialize, migration_resources
from chess_crawl.storage.raw import compute_body_hash, insert_source_record, read_raw_payload, store_raw_payload
from chess_crawl.storage.repository import list_providers, upsert_provider_user


CANONICAL_TABLES = {
    "providers",
    "provider_users",
    "user_snapshots",
    "games",
    "game_participants",
    "ratings_at_game",
    "time_controls",
    "variants",
    "raw_payloads",
    "source_records",
    "fetch_logs",
    "discovery_jobs",
    "discovery_edges",
    "crawl_runs",
    "errors",
    "schema_migrations",
}


def test_schema_creation_and_providers_seeded(uninitialized_database_url: str) -> None:
    with connection(uninitialized_database_url, mode="rwc") as conn:
        result = initialize(conn)

        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'public'"
            )
        }

        assert CANONICAL_TABLES <= tables
        assert result.version == SCHEMA_VERSION
        assert list_providers(conn) == ("chess.com", "lichess")


def test_init_is_idempotent(uninitialized_database_url: str) -> None:
    with connection(uninitialized_database_url, mode="rwc") as conn:
        first = initialize(conn)
        second = initialize(conn)

        assert first.applied == tuple(name for _, name, _ in migration_resources())
        assert second.applied == ()
        assert require_row(conn.execute("SELECT COUNT(*) FROM schema_migrations"))[0] == len(migration_resources())
        assert require_row(conn.execute("SELECT COUNT(*) FROM providers"))[0] == 2


def test_provider_scoped_users_can_share_username(initialized_conn: Connection) -> None:
    conn = initialized_conn
    chesscom_id = upsert_provider_user(conn, provider="chess.com", username="SameName")
    lichess_id = upsert_provider_user(conn, provider="lichess", username="SameName")

    assert chesscom_id != lichess_id
    assert require_row(conn.execute("SELECT COUNT(*) FROM provider_users"))[0] == 2

    upsert_provider_user(conn, provider="chess.com", username="samename")
    assert require_row(conn.execute("SELECT COUNT(*) FROM provider_users"))[0] == 2


def test_foreign_keys_and_nullable_game_outcome(initialized_conn: Connection) -> None:
    conn = initialized_conn
    with pytest.raises(psycopg.IntegrityError):
        conn.execute(
            """
            INSERT INTO game_participants(game_id, color)
            VALUES (999, 'white')
            """
        )

    variant_id = require_row(conn.execute(
        """
        INSERT INTO variants(canonical_name, provider, provider_native_name)
        VALUES ('standard', 'chess.com', 'chess') RETURNING id
        """
    ))[0]
    time_control_id = require_row(conn.execute(
        """
        INSERT INTO time_controls(kind, initial_seconds, increment_seconds, days, time_class, raw_label)
        VALUES ('clock', 300, 0, NULL, 'blitz', '300') RETURNING id
        """
    ))[0]
    conn.execute(
        """
        INSERT INTO games(
          provider, content_hash, variant_id, time_control_id, rated,
          outcome, is_live, first_seen_at
        )
        VALUES ('chess.com', 'sha256:game', %s, %s, 1, NULL, 1, 123)
        """,
        (variant_id, time_control_id),
    )

    row = require_row(conn.execute("SELECT outcome, is_live FROM games"))
    assert row["outcome"] is None
    assert row["is_live"] == 1


@pytest.fixture
def archive_record() -> RawRecord:
    return RawRecord(
        provider="chess.com",
        endpoint_type="monthly_archive",
        request_url="https://api.chess.com/pub/player/test/games/2024/01",
        canonical_source_key="chess.com/player/test/games/2024/01",
        fetched_at=123,
        body=b'{"games":[]}',
        media_type="application/json",
    )


def test_raw_payload_idempotency_and_body_hash_round_trip(
    initialized_conn: Connection, archive_record: RawRecord,
) -> None:
    conn = initialized_conn
    record = archive_record
    body = record.body
    assert body is not None

    first_id = store_raw_payload(conn, record)
    second_id = store_raw_payload(conn, record)
    stored = read_raw_payload(conn, first_id)

    assert first_id == second_id
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert stored.body == body
    assert stored.body_hash == compute_body_hash(body)
    assert stored.normalization_status == "pending"


def test_duplicate_source_record_returns_original_id_after_another_insert(
    initialized_conn: Connection, archive_record: RawRecord,
) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, archive_record)
    first_user = upsert_provider_user(conn, provider="chess.com", username="First")
    second_user = upsert_provider_user(conn, provider="chess.com", username="Second")
    source_ids = [
        insert_source_record(
            conn, entity_type="user", entity_id=user_id, provider="chess.com",
            endpoint_type="monthly_archive", raw_payload_id=raw_id,
        )
        for user_id in (first_user, second_user, first_user)
    ]

    assert source_ids[0] == source_ids[2]
    assert source_ids[0] != source_ids[1]
    assert require_row(conn.execute("SELECT COUNT(*) FROM source_records"))[0] == 2
