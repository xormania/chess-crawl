"""Small repository helpers over the Phase 1 schema."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass

from chess_crawl.providers.registry import list_provider_infos
from chess_crawl.storage.migrations import current_version


@dataclass(frozen=True)
class DatabaseSummary:
    schema_version: int
    migration_count: int
    table_count: int
    providers: tuple[str, ...]


def seed_providers(conn: sqlite3.Connection) -> None:
    now = int(time.time())
    for provider in list_provider_infos():
        conn.execute(
            """
            INSERT INTO providers(key, name, base_url, docs_url, added_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(key) DO NOTHING
            """,
            (provider.key, provider.name, provider.base_url, provider.docs_url, now),
        )
    conn.commit()


def list_providers(conn: sqlite3.Connection) -> tuple[str, ...]:
    return tuple(row["key"] for row in conn.execute("SELECT key FROM providers ORDER BY key"))


def database_summary(conn: sqlite3.Connection) -> DatabaseSummary:
    migration_count = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
    table_count = conn.execute(
        """
        SELECT COUNT(*) FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        """
    ).fetchone()[0]
    return DatabaseSummary(
        schema_version=current_version(conn),
        migration_count=int(migration_count),
        table_count=int(table_count),
        providers=list_providers(conn),
    )


def upsert_provider_user(
    conn: sqlite3.Connection,
    *,
    provider: str,
    username: str,
    provider_user_id: str | None = None,
    display_username: str | None = None,
    account_status: str | None = None,
    title: str | None = None,
    now: int | None = None,
    commit: bool = True,
) -> int:
    timestamp = now or int(time.time())
    username_normalized = username.strip().lower()
    display = display_username or username

    if provider_user_id is not None:
        existing = conn.execute(
            """
            SELECT id FROM provider_users
            WHERE provider = ? AND provider_user_id = ?
            """,
            (provider, provider_user_id),
        ).fetchone()
        if existing is not None:
            conn.execute(
                """
                UPDATE provider_users
                   SET username_normalized = ?,
                       display_username = ?,
                       account_status = COALESCE(?, account_status),
                       title = COALESCE(?, title),
                       updated_at = ?
                 WHERE id = ?
                """,
                (username_normalized, display, account_status, title, timestamp, int(existing["id"])),
            )
            if commit:
                conn.commit()
            return int(existing["id"])

    conn.execute(
        """
        INSERT INTO provider_users(
          provider, provider_user_id, username_normalized, display_username,
          account_status, title, first_seen_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider, username_normalized) DO UPDATE SET
          provider_user_id = COALESCE(excluded.provider_user_id, provider_users.provider_user_id),
          display_username = excluded.display_username,
          account_status = COALESCE(excluded.account_status, provider_users.account_status),
          title = COALESCE(excluded.title, provider_users.title),
          updated_at = excluded.updated_at
        """,
        (
            provider,
            provider_user_id,
            username_normalized,
            display,
            account_status,
            title,
            timestamp,
            timestamp,
        ),
    )
    if commit:
        conn.commit()

    row = conn.execute(
        """
        SELECT id FROM provider_users
        WHERE provider = ? AND username_normalized = ?
        """,
        (provider, username_normalized),
    ).fetchone()
    if row is None:
        raise RuntimeError("provider user upsert did not return a row")
    return int(row["id"])


def upsert_user_snapshot(
    conn: sqlite3.Connection,
    *,
    provider_user_id: int,
    captured_at: int,
    observed_username: str,
    content_hash: str,
    raw_payload_id: int,
    status: str | None = None,
    title: str | None = None,
    country: str | None = None,
    followers: int | None = None,
    patron: bool | None = None,
    count_all: int | None = None,
    count_rated: int | None = None,
    count_win: int | None = None,
    count_loss: int | None = None,
    count_draw: int | None = None,
    perfs_or_stats: object | None = None,
    commit: bool = True,
) -> int:
    perfs_text = (
        None
        if perfs_or_stats is None
        else json.dumps(perfs_or_stats, sort_keys=True, separators=(",", ":"))
    )
    conn.execute(
        """
        INSERT INTO user_snapshots(
          provider_user_id, captured_at, observed_username, status, title,
          country, followers, patron, count_all, count_rated, count_win,
          count_loss, count_draw, perfs_or_stats, content_hash, raw_payload_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_user_id, content_hash) DO UPDATE SET
          captured_at = excluded.captured_at,
          raw_payload_id = excluded.raw_payload_id
        """,
        (
            provider_user_id,
            captured_at,
            observed_username,
            status,
            title,
            country,
            followers,
            None if patron is None else int(patron),
            count_all,
            count_rated,
            count_win,
            count_loss,
            count_draw,
            perfs_text,
            content_hash,
            raw_payload_id,
        ),
    )
    if commit:
        conn.commit()
    row = conn.execute(
        """
        SELECT id FROM user_snapshots
        WHERE provider_user_id = ? AND content_hash = ?
        """,
        (provider_user_id, content_hash),
    ).fetchone()
    if row is None:
        raise RuntimeError("user snapshot upsert did not return a row")
    return int(row["id"])


def get_or_create_variant(
    conn: sqlite3.Connection,
    *,
    provider: str,
    provider_native_name: str,
    canonical_name: str,
    mapped: bool = True,
) -> int:
    conn.execute(
        """
        INSERT INTO variants(canonical_name, provider, provider_native_name, mapped)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(provider, provider_native_name) DO NOTHING
        """,
        (canonical_name, provider, provider_native_name, int(mapped)),
    )
    row = conn.execute(
        "SELECT id FROM variants WHERE provider = ? AND provider_native_name = ?",
        (provider, provider_native_name),
    ).fetchone()
    if row is None:
        raise RuntimeError("variant upsert did not return a row")
    return int(row["id"])


def get_or_create_time_control(
    conn: sqlite3.Connection,
    *,
    kind: str,
    initial_seconds: int | None,
    increment_seconds: int | None,
    days: int | None,
    time_class: str,
    raw_label: str,
) -> int:
    conn.execute(
        """
        INSERT OR IGNORE INTO time_controls(
          kind, initial_seconds, increment_seconds, days, time_class, raw_label
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (kind, initial_seconds, increment_seconds, days, time_class, raw_label),
    )
    row = conn.execute(
        """
        SELECT id FROM time_controls
        WHERE kind = ?
          AND COALESCE(initial_seconds,-1) = COALESCE(?, -1)
          AND COALESCE(increment_seconds,-1) = COALESCE(?, -1)
          AND COALESCE(days,-1) = COALESCE(?, -1)
          AND time_class = ?
          AND raw_label = ?
        """,
        (kind, initial_seconds, increment_seconds, days, time_class, raw_label),
    ).fetchone()
    if row is None:
        raise RuntimeError("time control upsert did not return a row")
    return int(row["id"])


def upsert_game_stub(
    conn: sqlite3.Connection,
    *,
    provider: str,
    content_hash: str,
    variant_id: int,
    time_control_id: int,
    rated: bool,
    provider_game_id: str | None = None,
    canonical_url: str | None = None,
    outcome: str | None = None,
    is_live: bool = False,
    now: int | None = None,
) -> int:
    conn.execute(
        """
        INSERT INTO games(
          provider, provider_game_id, canonical_url, content_hash, variant_id,
          time_control_id, rated, outcome, is_live, first_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_hash) DO UPDATE SET
          provider_game_id = COALESCE(games.provider_game_id, excluded.provider_game_id),
          canonical_url = COALESCE(games.canonical_url, excluded.canonical_url)
        """,
        (
            provider,
            provider_game_id,
            canonical_url,
            content_hash,
            variant_id,
            time_control_id,
            int(rated),
            outcome,
            int(is_live),
            now or int(time.time()),
        ),
    )
    row = conn.execute("SELECT id FROM games WHERE content_hash = ?", (content_hash,)).fetchone()
    if row is None:
        raise RuntimeError("game upsert did not return a row")
    return int(row["id"])


def upsert_game(
    conn: sqlite3.Connection,
    *,
    provider: str,
    content_hash: str,
    variant_id: int,
    time_control_id: int,
    rated: bool,
    provider_game_id: str | None = None,
    canonical_url: str | None = None,
    outcome: str | None = None,
    is_live: bool = False,
    status_raw: str | None = None,
    created_at: int | None = None,
    ended_at: int | None = None,
    ply_count: int | None = None,
    eco: str | None = None,
    opening_name: str | None = None,
    opening_ply: int | None = None,
    tournament_ref: str | None = None,
    now: int | None = None,
) -> int:
    timestamp = now or int(time.time())
    existing = _find_existing_game(conn, provider, provider_game_id, canonical_url, content_hash)
    if existing is None:
        conn.execute(
            """
            INSERT INTO games(
              provider, provider_game_id, canonical_url, content_hash,
              variant_id, time_control_id, rated, outcome, is_live, status_raw,
              created_at, ended_at, ply_count, eco, opening_name, opening_ply,
              tournament_ref, first_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                provider,
                provider_game_id,
                canonical_url,
                content_hash,
                variant_id,
                time_control_id,
                int(rated),
                outcome,
                int(is_live),
                status_raw,
                created_at,
                ended_at,
                ply_count,
                eco,
                opening_name,
                opening_ply,
                tournament_ref,
                timestamp,
            ),
        )
        row = conn.execute("SELECT id FROM games WHERE content_hash = ?", (content_hash,)).fetchone()
        if row is None:
            raise RuntimeError("game insert did not return a row")
        return int(row["id"])

    conn.execute(
        """
        UPDATE games
           SET provider_game_id = COALESCE(?, provider_game_id),
               canonical_url = COALESCE(?, canonical_url),
               content_hash = ?,
               variant_id = ?,
               time_control_id = ?,
               rated = ?,
               outcome = ?,
               is_live = ?,
               status_raw = ?,
               created_at = COALESCE(?, created_at),
               ended_at = COALESCE(?, ended_at),
               ply_count = COALESCE(?, ply_count),
               eco = COALESCE(?, eco),
               opening_name = COALESCE(?, opening_name),
               opening_ply = COALESCE(?, opening_ply),
               tournament_ref = COALESCE(?, tournament_ref)
         WHERE id = ?
        """,
        (
            provider_game_id,
            canonical_url,
            content_hash,
            variant_id,
            time_control_id,
            int(rated),
            outcome,
            int(is_live),
            status_raw,
            created_at,
            ended_at,
            ply_count,
            eco,
            opening_name,
            opening_ply,
            tournament_ref,
            int(existing["id"]),
        ),
    )
    return int(existing["id"])


def upsert_game_participant(
    conn: sqlite3.Connection,
    *,
    game_id: int,
    color: str,
    provider_user_id: int | None = None,
    username_normalized: str | None = None,
    result_raw: str | None = None,
    is_winner: bool | None = None,
    is_ai: bool = False,
) -> int:
    conn.execute(
        """
        INSERT INTO game_participants(
          game_id, color, provider_user_id, username_normalized,
          result_raw, is_winner, is_ai
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, color) DO UPDATE SET
          provider_user_id = COALESCE(excluded.provider_user_id, game_participants.provider_user_id),
          username_normalized = COALESCE(excluded.username_normalized, game_participants.username_normalized),
          result_raw = COALESCE(excluded.result_raw, game_participants.result_raw),
          is_winner = excluded.is_winner,
          is_ai = excluded.is_ai
        """,
        (
            game_id,
            color,
            provider_user_id,
            username_normalized,
            result_raw,
            None if is_winner is None else int(is_winner),
            int(is_ai),
        ),
    )
    row = conn.execute(
        "SELECT id FROM game_participants WHERE game_id = ? AND color = ?",
        (game_id, color),
    ).fetchone()
    if row is None:
        raise RuntimeError("participant upsert did not return a row")
    return int(row["id"])


def upsert_rating_at_game(
    conn: sqlite3.Connection,
    *,
    game_id: int,
    color: str,
    rating: int | None = None,
    rating_diff: int | None = None,
    rd: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO ratings_at_game(game_id, color, rating, rating_diff, rd)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(game_id, color) DO UPDATE SET
          rating = excluded.rating,
          rating_diff = excluded.rating_diff,
          rd = excluded.rd
        """,
        (game_id, color, rating, rating_diff, rd),
    )


def _find_existing_game(
    conn: sqlite3.Connection,
    provider: str,
    provider_game_id: str | None,
    canonical_url: str | None,
    content_hash: str,
) -> sqlite3.Row | None:
    if provider_game_id is not None:
        row = conn.execute(
            "SELECT id FROM games WHERE provider = ? AND provider_game_id = ?",
            (provider, provider_game_id),
        ).fetchone()
        if row is not None:
            return row
    if canonical_url is not None:
        row = conn.execute("SELECT id FROM games WHERE canonical_url = ?", (canonical_url,)).fetchone()
        if row is not None:
            return row
    return conn.execute("SELECT id FROM games WHERE content_hash = ?", (content_hash,)).fetchone()
