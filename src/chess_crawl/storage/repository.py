"""Repository helpers over normalized archive tables."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass

from chess_crawl.storage.db import atomic
from chess_crawl.storage.migrations import current_version


@dataclass(frozen=True)
class DatabaseSummary:
    schema_version: int
    migration_count: int
    table_count: int
    providers: tuple[str, ...]


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


@atomic
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
    profile_raw_payload_id: int | None = None,
) -> int:
    """Merge sparse observations; full profiles replace metadata in observation order."""
    timestamp = int(time.time()) if now is None else now
    username_normalized = username.strip().lower()
    display = display_username or username
    named = conn.execute(
        "SELECT * FROM provider_users WHERE provider = ? AND username_normalized = ?",
        (provider, username_normalized),
    ).fetchone()
    identified = None if provider_user_id is None else conn.execute(
        "SELECT * FROM provider_users WHERE provider = ? AND provider_user_id = ?",
        (provider, provider_user_id),
    ).fetchone()
    existing = identified if identified is not None else named
    # A username match is not evidence that two different stable IDs refer to
    # the same account. Check before the stale-profile fast path as well.
    if identified is None and named is not None and provider_user_id is not None:
        if named["provider_user_id"] not in {None, provider_user_id}:
            raise ValueError(f"Cannot reconcile {provider}/{username_normalized}: conflicting provider user IDs")
    if existing is not None:
        user_id = int(existing["id"])
        if profile_raw_payload_id is not None and not _is_latest_profile(conn, user_id, profile_raw_payload_id):
            return user_id
        if identified is not None and profile_raw_payload_id is None and timestamp < int(existing["updated_at"]):
            return user_id
        if named is not None and int(named["id"]) != user_id:
            if named["provider_user_id"] is not None:
                raise ValueError(f"Cannot reconcile {provider}/{username_normalized}: conflicting provider user IDs")
            if profile_raw_payload_id is not None and not _is_latest_profile(conn, int(named["id"]), profile_raw_payload_id):
                # A historical name observation must not absorb a placeholder
                # that may represent a later holder of that username.
                return user_id
            _merge_user_placeholder(conn, user_id, int(named["id"]))
        conn.execute(
            """
            UPDATE provider_users
               SET provider_user_id = COALESCE(?, provider_user_id),
                   username_normalized = ?, display_username = ?,
                   account_status = CASE WHEN ? THEN ? ELSE COALESCE(?, account_status) END,
                   title = CASE WHEN ? THEN ? ELSE COALESCE(?, title) END,
                   updated_at = MAX(updated_at, ?)
             WHERE id = ?
            """,
            (provider_user_id, username_normalized, display,
             profile_raw_payload_id is not None, account_status, account_status,
             profile_raw_payload_id is not None, title, title, timestamp, user_id),
        )
        return user_id

    cursor = conn.execute(
        """
        INSERT INTO provider_users(
          provider, provider_user_id, username_normalized, display_username,
          account_status, title, first_seen_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (provider, provider_user_id, username_normalized, display, account_status, title, timestamp, timestamp),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("provider user insert did not return a row id")
    return int(cursor.lastrowid)


def _merge_source_records(conn: sqlite3.Connection, entity_type: str, survivor: int, replaced: int) -> None:
    conn.execute(
        """INSERT INTO source_records(entity_type, entity_id, provider, endpoint_type, source_key,
                     json_pointer, raw_payload_id, first_seen_at)
           SELECT entity_type, ?, provider, endpoint_type, source_key, json_pointer, raw_payload_id, first_seen_at
             FROM source_records WHERE entity_type = ? AND entity_id = ?
           ON CONFLICT(entity_type, entity_id, raw_payload_id) DO UPDATE SET
             first_seen_at = MIN(source_records.first_seen_at, excluded.first_seen_at),
             source_key = COALESCE(source_records.source_key, excluded.source_key),
             json_pointer = COALESCE(source_records.json_pointer, excluded.json_pointer)""",
        (survivor, entity_type, replaced),
    )
    conn.execute("DELETE FROM source_records WHERE entity_type = ? AND entity_id = ?", (entity_type, replaced))


def _merge_user_placeholder(conn: sqlite3.Connection, survivor: int, replaced: int) -> None:
    """Reconcile a certified same-provider placeholder inside the caller's transaction."""
    conn.execute("UPDATE game_participants SET provider_user_id = ? WHERE provider_user_id = ?", (survivor, replaced))
    for snapshot in conn.execute("SELECT * FROM user_snapshots WHERE provider_user_id = ?", (replaced,)).fetchall():
        matching = conn.execute(
            "SELECT id, captured_at FROM user_snapshots WHERE provider_user_id = ? AND content_hash = ?",
            (survivor, snapshot["content_hash"]),
        ).fetchone()
        if matching is None:
            conn.execute("UPDATE user_snapshots SET provider_user_id = ? WHERE id = ?", (survivor, snapshot["id"]))
        else:
            if snapshot["captured_at"] >= matching["captured_at"]:
                fields = (
                    "captured_at", "observed_username", "status", "title", "country", "followers", "patron",
                    "count_all", "count_rated", "count_win", "count_loss", "count_draw", "perfs_or_stats", "raw_payload_id",
                )
                conn.execute(
                    f"UPDATE user_snapshots SET {', '.join(f'{field} = ?' for field in fields)} WHERE id = ?",
                    (*[snapshot[field] for field in fields], matching["id"]),
                )
            _merge_source_records(conn, "user_snapshot", int(matching["id"]), int(snapshot["id"]))
            conn.execute("DELETE FROM user_snapshots WHERE id = ?", (snapshot["id"],))
    _merge_source_records(conn, "user", survivor, replaced)
    affected_runs = _merge_user_edges(conn, survivor, replaced)
    conn.execute(
        """UPDATE provider_users SET
             first_seen_at = MIN(first_seen_at, (SELECT first_seen_at FROM provider_users WHERE id = ?)),
             updated_at = MAX(updated_at, (SELECT updated_at FROM provider_users WHERE id = ?))
           WHERE id = ?""",
        (replaced, replaced, survivor),
    )
    conn.execute("DELETE FROM provider_users WHERE id = ?", (replaced,))
    if affected_runs:
        from chess_crawl.jobs.state import run_counters, update_crawl_run

        for run_id in sorted(affected_runs):
            # Reconciliation changes graph counts, not a run's lifecycle.
            update_crawl_run(conn, run_id, counters=run_counters(conn, run_id))


def _merge_user_edges(conn: sqlite3.Connection, survivor: int, replaced: int) -> set[int]:
    affected_runs: set[int] = set()
    edges = conn.execute(
        "SELECT * FROM discovery_edges WHERE from_user_id = ? OR to_user_id = ? ORDER BY id", (replaced, replaced),
    ).fetchall()
    for edge in edges:
        source = survivor if edge["from_user_id"] == replaced else edge["from_user_id"]
        target = survivor if edge["to_user_id"] == replaced else edge["to_user_id"]
        matching = conn.execute(
            "SELECT * FROM discovery_edges WHERE provider = ? AND from_user_id = ? AND to_user_id = ? AND id <> ?",
            (edge["provider"], source, target, edge["id"]),
        ).fetchone()
        if matching is None:
            conn.execute("UPDATE discovery_edges SET from_user_id = ?, to_user_id = ? WHERE id = ?", (source, target, edge["id"]))
            continue
        affected_runs.update(int(row[0]) for row in conn.execute(
            "SELECT crawl_run_id FROM run_edges WHERE discovery_edge_id IN (?, ?)", (edge["id"], matching["id"]),
        ))
        known_games = conn.execute(
            """SELECT COUNT(DISTINCT a.game_id) FROM game_participants a
                 JOIN game_participants b ON b.game_id = a.game_id AND b.color <> a.color
                 JOIN games g ON g.id = a.game_id
                WHERE a.provider_user_id = ? AND b.provider_user_id = ? AND g.provider = ?""",
            (source, target, edge["provider"]),
        ).fetchone()[0]
        earlier, later = sorted((edge, matching), key=lambda row: (row["first_seen_at"], row["id"]))
        conn.execute(
            """UPDATE discovery_edges SET game_count = MAX(game_count, ?, ?), depth = MIN(depth, ?),
                 first_seen_at = MIN(first_seen_at, ?),
                 crawl_run_id = ?, via_game_id = ?
               WHERE id = ?""",
            (edge["game_count"], known_games, edge["depth"], edge["first_seen_at"],
             earlier["crawl_run_id"] if earlier["crawl_run_id"] is not None else later["crawl_run_id"],
             earlier["via_game_id"] if earlier["via_game_id"] is not None else later["via_game_id"], matching["id"]),
        )
        conn.execute(
            """INSERT INTO run_edges(crawl_run_id, discovery_edge_id)
               SELECT crawl_run_id, ? FROM run_edges WHERE discovery_edge_id = ? ON CONFLICT DO NOTHING""",
            (matching["id"], edge["id"]),
        )
        conn.execute("DELETE FROM run_edges WHERE discovery_edge_id = ?", (edge["id"],))
        conn.execute("DELETE FROM discovery_edges WHERE id = ?", (edge["id"],))
    return affected_runs


def _is_latest_profile(conn: sqlite3.Connection, user_id: int, raw_payload_id: int) -> bool:
    # Full-profile recency is independent of later game/stats observations.
    # Fetch IDs break second-resolution ties when an older body reappears.
    row = conn.execute(
        """
        SELECT r.id
          FROM raw_payloads r
          LEFT JOIN fetch_logs f ON f.raw_payload_id = r.id AND f.status_code IN (200, 304)
         WHERE r.id = ? OR r.id IN (
           SELECT raw_payload_id FROM source_records
            WHERE entity_type = 'user' AND entity_id = ? AND endpoint_type = 'user_profile'
         )
         ORDER BY COALESCE(f.attempted_at, r.fetched_at) DESC, f.id DESC, r.id DESC
         LIMIT 1
        """,
        (raw_payload_id, user_id),
    ).fetchone()
    return row is not None and int(row["id"]) == raw_payload_id


@atomic
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
          observed_username = excluded.observed_username,
          status = excluded.status,
          title = excluded.title,
          country = excluded.country,
          followers = excluded.followers,
          patron = excluded.patron,
          count_all = excluded.count_all,
          count_rated = excluded.count_rated,
          count_win = excluded.count_win,
          count_loss = excluded.count_loss,
          count_draw = excluded.count_draw,
          perfs_or_stats = excluded.perfs_or_stats,
          raw_payload_id = excluded.raw_payload_id
        WHERE excluded.captured_at >= user_snapshots.captured_at
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


@atomic
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


@atomic
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


@atomic
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
    existing = find_existing_game(conn, provider, provider_game_id, canonical_url, content_hash)
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


@atomic
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


@atomic
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


def find_existing_game(
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


@atomic
def insert_error(
    conn: sqlite3.Connection,
    *,
    provider: str | None,
    error_kind: str,
    message: str,
    status_code: int | None = None,
    url: str | None = None,
    endpoint_type: str | None = None,
    retry_count: int = 0,
    is_dead: bool = True,
    occurred_at: int | None = None,
) -> int:
    """Record fetch and job failures through the same persistence operation."""
    cursor = conn.execute(
        """
        INSERT INTO errors(provider, url, endpoint_type, error_kind, status_code,
                           message, occurred_at, retry_count, is_dead)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (provider, url, endpoint_type, error_kind, status_code, message,
         int(time.time()) if occurred_at is None else occurred_at, retry_count, int(is_dead)),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("error insert did not return a row id")
    return int(cursor.lastrowid)
