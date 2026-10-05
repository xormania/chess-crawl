"""Queryable source facts and observation history for provider accounts."""

from __future__ import annotations

from datetime import date
from typing import Any

from psycopg.types.json import Jsonb

from chess_crawl.normalize.codes import canonical_hash
from chess_crawl.storage.db import Connection, atomic, require_row, operation_lock, operation_locks
from chess_crawl.providers.base import RawRecord


@atomic
def quarantine_unowned_profile(conn: Connection, raw_payload_id: int) -> None:
    """Preserve legacy OAuth evidence while removing it from public archive reads."""
    conn.execute(
        "UPDATE raw_payloads SET owner_scope = 'unassigned:legacy-profile' WHERE id = %s AND owner_scope = 'public'",
        (raw_payload_id,),
    )


@atomic
def publish_verified_legacy_profile(conn: Connection, raw_payload_id: int) -> None:
    """Only a successfully parsed, relationship-free Lichess profile is public."""
    conn.execute(
        """UPDATE raw_payloads SET owner_scope = 'public' WHERE id = %s
           AND provider = 'lichess' AND endpoint_type = 'user_profile'
           AND owner_scope = 'unassigned:legacy-profile'""",
        (raw_payload_id,),
    )


@atomic
def store_profile_facts(
    conn: Connection, snapshot_id: int, *, native_data: dict[str, Any], facts: dict[str, Any],
) -> None:
    conn.execute(
        """UPDATE user_snapshots SET native_data = %s, created_at = %s, last_seen_at = %s,
           real_name = %s, location = %s, avatar_url = %s, profile_url = %s,
           is_verified = %s, is_streamer = %s, fide_rating = %s WHERE id = %s""",
        (Jsonb(native_data), facts.get("created_at"), facts.get("last_seen_at"),
         facts.get("real_name"), facts.get("location"), facts.get("avatar_url"),
         facts.get("profile_url"), facts.get("is_verified"), facts.get("is_streamer"),
         facts.get("fide_rating"), snapshot_id),
    )


@atomic
def store_rating_records(conn: Connection, snapshot_id: int, records: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM user_rating_records WHERE snapshot_id = %s", (snapshot_id,))
    for record in records:
        conn.execute(
            """INSERT INTO user_rating_records(snapshot_id, performance, rating, best_rating, best_at, lowest_rating, lowest_at,
                rating_deviation, provisional, games, wins, losses, draws, progress, native_data)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (snapshot_id, record["performance"], record.get("rating"), record.get("best_rating"), record.get("best_at"),
             record.get("lowest_rating"), record.get("lowest_at"),
             record.get("rating_deviation"), record.get("provisional"), record.get("games"), record.get("wins"),
             record.get("losses"), record.get("draws"), record.get("progress"), Jsonb(record["native_data"])),
        )


@atomic
def record_alias(
    conn: Connection, user_id: int, username: str, *, observed_at: int, raw_payload_id: int | None,
    first_observed_at: int | None = None,
) -> None:
    conn.execute(
        """INSERT INTO provider_user_aliases(provider_user_id, username_normalized, display_username,
                  first_seen_at, last_seen_at, raw_payload_id)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT(provider_user_id, username_normalized) DO UPDATE SET
             display_username = CASE WHEN excluded.last_seen_at >= provider_user_aliases.last_seen_at
               THEN excluded.display_username ELSE provider_user_aliases.display_username END,
             first_seen_at = LEAST(provider_user_aliases.first_seen_at, excluded.first_seen_at),
             last_seen_at = GREATEST(provider_user_aliases.last_seen_at, excluded.last_seen_at),
             raw_payload_id = CASE WHEN excluded.last_seen_at >= provider_user_aliases.last_seen_at
               THEN excluded.raw_payload_id ELSE provider_user_aliases.raw_payload_id END""",
        (user_id, username.strip().lower(), username,
         observed_at if first_observed_at is None else min(first_observed_at, observed_at), observed_at, raw_payload_id),
    )
    conn.execute(
        """UPDATE provider_users SET first_seen_at = LEAST(first_seen_at, %s),
           updated_at = GREATEST(updated_at, %s) WHERE id = %s""",
        (observed_at if first_observed_at is None else min(first_observed_at, observed_at), observed_at, user_id),
    )


@atomic
def record_profile_observations(
    conn: Connection, user_id: int, snapshot_id: int, raw_payload_id: int, *, fetch_log_id: int | None = None,
) -> None:
    conn.execute(
        """INSERT INTO user_observations(provider_user_id, snapshot_id, raw_payload_id, fetch_log_id,
                   observation_key, captured_at, endpoint_type)
           SELECT %s, %s, f.raw_payload_id, f.id, 'fetch:' || f.id, f.attempted_at, r.endpoint_type
           FROM fetch_logs f JOIN raw_payloads r ON r.id = f.raw_payload_id
           WHERE f.raw_payload_id = %s AND f.status_code IN (200,304)
             AND (%s::bigint IS NULL OR f.id = %s)
             AND (f.provider_user_id IS NULL OR f.provider_user_id = %s)
           ON CONFLICT(observation_key) DO UPDATE SET provider_user_id = excluded.provider_user_id,
             snapshot_id = excluded.snapshot_id
           WHERE user_observations.snapshot_id <> excluded.snapshot_id
             AND user_observations.provider_user_id = excluded.provider_user_id""",
        (user_id, snapshot_id, raw_payload_id, fetch_log_id, fetch_log_id, user_id),
    )
    conn.execute(
        """INSERT INTO user_observations(provider_user_id, snapshot_id, raw_payload_id,
                   observation_key, captured_at, endpoint_type)
           SELECT %s, %s, r.id, 'raw:' || r.id, r.fetched_at, r.endpoint_type FROM raw_payloads r
           WHERE r.id = %s AND NOT EXISTS(SELECT 1 FROM fetch_logs f WHERE f.raw_payload_id = r.id AND f.status_code IN (200,304))
           AND %s::bigint IS NULL
           ON CONFLICT(observation_key) DO UPDATE SET provider_user_id = excluded.provider_user_id,
             snapshot_id = excluded.snapshot_id
           WHERE user_observations.snapshot_id <> excluded.snapshot_id
             AND user_observations.provider_user_id = excluded.provider_user_id""",
        (user_id, snapshot_id, raw_payload_id, fetch_log_id),
    )


@atomic
def refresh_profile_observations(conn: Connection, raw_payload_id: int) -> None:
    """Record a conditional refresh without reparsing or inventing a new body."""
    snapshot = conn.execute(
        """SELECT us.id, us.provider_user_id FROM user_snapshots us
           LEFT JOIN source_records s ON s.entity_type = 'user_snapshot' AND s.entity_id = us.id
           WHERE s.raw_payload_id = %s OR us.raw_payload_id = %s ORDER BY us.id DESC LIMIT 1""",
        (raw_payload_id, raw_payload_id),
    ).fetchone()
    if snapshot is not None:
        record_profile_observations(conn, int(snapshot["provider_user_id"]), int(snapshot["id"]), raw_payload_id)


@atomic
def store_resource_snapshot(
    conn: Connection, *, user_id: int, resource_key: str, parameters: dict[str, Any],
    native_data: object, coverage_status: str, coverage_note: str | None, parser_version: str,
    raw_payload_id: int, rating_points: list[tuple[int, int, str, date, int, str]],
    owner_scope: str = "public", fetch_log_id: int | None = None,
) -> int:
    snapshot = require_row(conn.execute(
        """INSERT INTO user_resource_snapshots(provider_user_id, owner_scope, resource_key, parameters, parameters_hash,
                 content_hash, native_data, coverage_status, coverage_note, parser_version)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT(provider_user_id, owner_scope, resource_key, parameters_hash, content_hash) DO UPDATE SET
             coverage_status = excluded.coverage_status, coverage_note = excluded.coverage_note,
             parser_version = excluded.parser_version
           RETURNING id""",
        (user_id, owner_scope, resource_key, Jsonb(parameters), canonical_hash(parameters), canonical_hash({
            "native_data": native_data, "coverage_status": coverage_status, "coverage_note": coverage_note,
        }),
         Jsonb(native_data), coverage_status, coverage_note, parser_version),
    ))
    snapshot_id = int(snapshot["id"])
    conn.execute("DELETE FROM rating_history_points WHERE snapshot_id = %s", (snapshot_id,))
    if rating_points:
        with conn.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO rating_history_points(snapshot_id, perf_index, point_index, performance,
                    rating_date, rating, source_pointer) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                [(snapshot_id, *point) for point in rating_points],
            )
    record_resource_observations(conn, snapshot_id, raw_payload_id, fetch_log_id=fetch_log_id)
    return snapshot_id


@atomic
def record_resource_observations(
    conn: Connection, snapshot_id: int, raw_payload_id: int, *, fetch_log_id: int | None = None,
) -> None:
    conn.execute(
        """INSERT INTO user_resource_observations(snapshot_id, raw_payload_id, fetch_log_id, observation_key, captured_at)
           SELECT %s, f.raw_payload_id, f.id, 'fetch:' || f.id, f.attempted_at FROM fetch_logs f
           WHERE f.raw_payload_id = %s AND f.status_code IN (200,304)
             AND (%s::bigint IS NULL OR f.id = %s)
             AND (f.provider_user_id IS NULL OR f.provider_user_id = (
                 SELECT provider_user_id FROM user_resource_snapshots WHERE id = %s))
           ON CONFLICT(observation_key) DO UPDATE SET snapshot_id = excluded.snapshot_id
           WHERE user_resource_observations.snapshot_id <> excluded.snapshot_id
             AND (SELECT provider_user_id FROM user_resource_snapshots
                  WHERE id = user_resource_observations.snapshot_id) =
                 (SELECT provider_user_id FROM user_resource_snapshots WHERE id = excluded.snapshot_id)""",
        (snapshot_id, raw_payload_id, fetch_log_id, fetch_log_id, snapshot_id),
    )
    conn.execute(
        """INSERT INTO user_resource_observations(snapshot_id, raw_payload_id, observation_key, captured_at)
           SELECT %s, r.id, 'raw:' || r.id, r.fetched_at FROM raw_payloads r WHERE r.id = %s
           AND %s::bigint IS NULL
           AND NOT EXISTS(SELECT 1 FROM fetch_logs f WHERE f.raw_payload_id = r.id AND f.status_code IN (200,304))
           ON CONFLICT(observation_key) DO UPDATE SET snapshot_id = excluded.snapshot_id
           WHERE user_resource_observations.snapshot_id <> excluded.snapshot_id
             AND (SELECT provider_user_id FROM user_resource_snapshots
                  WHERE id = user_resource_observations.snapshot_id) =
                 (SELECT provider_user_id FROM user_resource_snapshots WHERE id = excluded.snapshot_id)""",
        (snapshot_id, raw_payload_id, fetch_log_id),
    )


@atomic
def refresh_resource_observations(conn: Connection, raw_payload_id: int) -> None:
    snapshot = conn.execute(
        "SELECT snapshot_id FROM user_resource_observations WHERE raw_payload_id = %s ORDER BY id DESC LIMIT 1",
        (raw_payload_id,),
    ).fetchone()
    if snapshot is not None:
        record_resource_observations(conn, int(snapshot["snapshot_id"]), raw_payload_id)


@atomic
def resolve_capture_account(
    conn: Connection, *, provider: str, username: str, observed_at: int, owner_scope: str = "public",
) -> int:
    """Bind the request target without replacing supplied public identity facts."""
    from chess_crawl.storage.repository import upsert_provider_user, user_identity_transaction

    with user_identity_transaction(conn, provider, username, None):
        normalized = username.strip().lower()
        current = conn.execute(
            "SELECT id FROM provider_users WHERE provider = %s AND username_normalized = %s", (provider, normalized),
        ).fetchone()
        if current is not None:
            return int(current["id"])
        if owner_scope == "public":
            return upsert_provider_user(conn, provider=provider, username=username, now=observed_at)
        return int(require_row(conn.execute(
            """INSERT INTO provider_users(provider, username_normalized, display_username, first_seen_at, updated_at)
               VALUES (%s, %s, %s, NULL, NULL) RETURNING id""", (provider, normalized, username),
        ))["id"])


@atomic
def record_resource_attempt(conn: Connection, record: RawRecord, raw_payload_id: int | None) -> int:
    """Keep failed/unavailable resources distinguishable from an empty listing."""
    params = dict(record.request_params)
    values = params.get("parameters") or {}
    if not record.target_username:
        raise ValueError("player resource attempt is missing its target username")
    user_id = resolve_capture_account(
        conn, provider=record.provider, username=record.target_username,
        observed_at=record.fetched_at, owner_scope=record.owner_scope,
    )
    conn.execute(
        """INSERT INTO user_resource_acquisition(provider_user_id, provider, username_normalized, owner_scope, resource_key,
                   parameters, parameters_hash, source_key, attempted_at, status_code, raw_payload_id)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(provider_user_id, owner_scope, resource_key, parameters_hash) DO UPDATE SET
             provider = excluded.provider, username_normalized = excluded.username_normalized,
             source_key = excluded.source_key, attempted_at = excluded.attempted_at,
             status_code = excluded.status_code, raw_payload_id = excluded.raw_payload_id
           WHERE excluded.attempted_at >= user_resource_acquisition.attempted_at""",
        (user_id, record.provider, record.target_username, record.owner_scope, params["resource_key"], Jsonb(values),
         canonical_hash(values), record.canonical_source_key, record.fetched_at, record.http_status, raw_payload_id),
    )
    return user_id


@atomic
def lock_capture_accounts(conn: Connection, provider: str, user_ids: list[int], requested_username: str) -> None:
    """Lock a replay's bound identities together while reconciliation is gated."""
    from chess_crawl.storage.repository import user_identity_resources

    operation_lock(conn, "reconciliation-provider", provider, shared=True)
    resources = user_identity_resources(provider, requested_username, None)
    for account in conn.execute(
        "SELECT username_normalized,provider_user_id FROM provider_users WHERE provider=%s AND id=ANY(%s)",
        (provider, user_ids),
    ).fetchall():
        resources.extend(user_identity_resources(provider, account["username_normalized"], account["provider_user_id"]))
    operation_locks(conn, resources)


def resource_attempts(conn: Connection, provider_user_id: int, *, owner_scope: str = "public") -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(
        """SELECT * FROM user_resource_acquisition WHERE provider_user_id = %s
           AND owner_scope IN ('public', %s) ORDER BY resource_key, parameters_hash""",
        (provider_user_id, owner_scope),
    )]


@atomic
def merge_player_evidence(conn: Connection, survivor: int, replaced: int) -> None:
    """Carry new evidence tables through the existing stable-ID reconciliation."""
    conn.execute("UPDATE fetch_logs SET provider_user_id = %s WHERE provider_user_id = %s", (survivor, replaced))
    for alias in conn.execute("SELECT * FROM provider_user_aliases WHERE provider_user_id = %s", (replaced,)).fetchall():
        record_alias(conn, survivor, alias["display_username"], observed_at=alias["first_seen_at"], raw_payload_id=alias["raw_payload_id"])
        record_alias(conn, survivor, alias["display_username"], observed_at=alias["last_seen_at"], raw_payload_id=alias["raw_payload_id"])
    conn.execute("DELETE FROM provider_user_aliases WHERE provider_user_id = %s", (replaced,))
    conn.execute("UPDATE user_observations SET provider_user_id = %s WHERE provider_user_id = %s", (survivor, replaced))
    conn.execute(
        """INSERT INTO user_resource_acquisition(provider_user_id, provider, username_normalized, owner_scope,
                   resource_key, parameters, parameters_hash, source_key, attempted_at, status_code, raw_payload_id)
           SELECT %s, provider, username_normalized, owner_scope, resource_key, parameters, parameters_hash,
                  source_key, attempted_at, status_code, raw_payload_id
           FROM user_resource_acquisition WHERE provider_user_id = %s
           ON CONFLICT(provider_user_id, owner_scope, resource_key, parameters_hash) DO UPDATE SET
             provider = excluded.provider, username_normalized = excluded.username_normalized,
             source_key = excluded.source_key, attempted_at = excluded.attempted_at,
             status_code = excluded.status_code, raw_payload_id = excluded.raw_payload_id
           WHERE excluded.attempted_at >= user_resource_acquisition.attempted_at""",
        (survivor, replaced),
    )
    conn.execute("DELETE FROM user_resource_acquisition WHERE provider_user_id = %s", (replaced,))
    for snapshot in conn.execute("SELECT * FROM user_resource_snapshots WHERE provider_user_id = %s", (replaced,)).fetchall():
        matching = conn.execute(
            """SELECT id FROM user_resource_snapshots WHERE provider_user_id = %s AND owner_scope = %s AND resource_key = %s
               AND parameters_hash = %s AND content_hash = %s""",
            (survivor, snapshot["owner_scope"], snapshot["resource_key"], snapshot["parameters_hash"], snapshot["content_hash"]),
        ).fetchone()
        if matching is None:
            conn.execute("UPDATE user_resource_snapshots SET provider_user_id = %s WHERE id = %s", (survivor, snapshot["id"]))
        else:
            from chess_crawl.storage.repository import _merge_source_records

            conn.execute("UPDATE user_resource_observations SET snapshot_id = %s WHERE snapshot_id = %s", (matching["id"], snapshot["id"]))
            _merge_source_records(conn, "user_resource", int(matching["id"]), int(snapshot["id"]))
            conn.execute("DELETE FROM rating_history_points WHERE snapshot_id = %s", (snapshot["id"],))
            conn.execute("DELETE FROM user_resource_snapshots WHERE id = %s", (snapshot["id"],))


def resource_account(
    conn: Connection, provider: str, username: str, raw_payload_id: int, *,
    prefer_observed_identity: bool = True,
) -> int | None:
    """Replay follows existing source identity rather than recreating a renamed username."""
    if prefer_observed_identity:
        observed = conn.execute(
            """SELECT s.provider_user_id FROM user_resource_observations o
               JOIN user_resource_snapshots s ON s.id = o.snapshot_id
               WHERE o.raw_payload_id = %s ORDER BY o.id DESC LIMIT 1""", (raw_payload_id,),
        ).fetchone()
        if observed is not None:
            return int(observed["provider_user_id"])
    current = conn.execute(
        "SELECT id FROM provider_users WHERE provider = %s AND username_normalized = %s", (provider, username.lower()),
    ).fetchone()
    if current is not None:
        return int(current["id"])
    if not prefer_observed_identity:
        return None
    aliases = conn.execute(
        """SELECT a.provider_user_id FROM provider_user_aliases a JOIN provider_users p ON p.id = a.provider_user_id
           WHERE p.provider = %s AND a.username_normalized = %s LIMIT 2""", (provider, username.lower()),
    ).fetchall()
    return int(aliases[0]["provider_user_id"]) if len(aliases) == 1 else None


def resource_accounts(conn: Connection, raw_payload_id: int) -> list[int]:
    """Accounts already bound to retained occurrences of one resource body."""
    return [int(row["provider_user_id"]) for row in conn.execute(
        """SELECT provider_user_id, MAX(observed_at) AS last_observed_at FROM (
             SELECT s.provider_user_id, o.captured_at AS observed_at
             FROM user_resource_observations o
             JOIN user_resource_snapshots s ON s.id = o.snapshot_id
             WHERE o.raw_payload_id = %s
             UNION ALL
             SELECT f.provider_user_id, f.attempted_at FROM fetch_logs f
             WHERE f.raw_payload_id = %s AND f.status_code IN (200,304)
               AND f.provider_user_id IS NOT NULL
           ) evidence GROUP BY provider_user_id ORDER BY MAX(observed_at) DESC, provider_user_id""",
        (raw_payload_id, raw_payload_id),
    )]


def stats_account(
    conn: Connection, provider: str, username: str, raw_payload_id: int, *,
    prefer_observed_identity: bool = True,
) -> dict[str, Any] | None:
    if prefer_observed_identity:
        observed = conn.execute(
            """SELECT p.* FROM user_observations o JOIN provider_users p ON p.id = o.provider_user_id
               WHERE o.raw_payload_id = %s ORDER BY o.id DESC LIMIT 1""", (raw_payload_id,),
        ).fetchone()
        if observed is not None:
            return dict(observed)
    current = conn.execute(
        "SELECT * FROM provider_users WHERE provider = %s AND username_normalized = %s", (provider, username.lower()),
    ).fetchone()
    if current is not None:
        return dict(current)
    if not prefer_observed_identity:
        return None
    aliases = conn.execute(
        """SELECT p.* FROM provider_user_aliases a JOIN provider_users p ON p.id = a.provider_user_id
           WHERE p.provider = %s AND a.username_normalized = %s LIMIT 2""", (provider, username.lower()),
    ).fetchall()
    return dict(aliases[0]) if len(aliases) == 1 else None


def stats_accounts(conn: Connection, raw_payload_id: int) -> list[dict[str, Any]]:
    """Accounts already bound to retained occurrences of one statistics body."""
    return [dict(row) for row in conn.execute(
        """SELECT p.* FROM provider_users p JOIN (
             SELECT provider_user_id, MAX(observed_at) AS last_observed_at FROM (
               SELECT o.provider_user_id, o.captured_at AS observed_at FROM user_observations o
               WHERE o.raw_payload_id = %s AND o.endpoint_type = 'user_stats'
               UNION ALL
               SELECT f.provider_user_id, f.attempted_at FROM fetch_logs f
               WHERE f.raw_payload_id = %s AND f.endpoint_type = 'user_stats'
                 AND f.status_code IN (200,304) AND f.provider_user_id IS NOT NULL
             ) evidence GROUP BY provider_user_id
           ) bound ON bound.provider_user_id = p.id
           ORDER BY bound.last_observed_at DESC, p.id""",
        (raw_payload_id, raw_payload_id),
    )]


def captured_fetch_account(
    conn: Connection, raw_payload_id: int, fetch_log_id: int,
) -> tuple[dict[str, Any] | None, int]:
    """A parser must honor the account and clock committed with its own fetch."""
    row = conn.execute(
        """SELECT p.*, f.provider_user_id AS captured_account_id, f.attempted_at AS capture_at
           FROM fetch_logs f JOIN raw_payloads r ON r.id = f.raw_payload_id
           LEFT JOIN provider_users p ON p.id = f.provider_user_id AND p.provider = r.provider
           WHERE f.id = %s AND f.raw_payload_id = %s AND f.status_code IN (200,304)
             AND f.provider = r.provider AND f.endpoint_type = r.endpoint_type""",
        (fetch_log_id, raw_payload_id),
    ).fetchone()
    if row is None or (row["captured_account_id"] is not None and row["id"] is None):
        raise ValueError("Successful fetch evidence does not match this source payload")
    account = None if row["captured_account_id"] is None else {
        key: value for key, value in row.items() if key not in {"captured_account_id", "capture_at"}
    }
    return account, int(row["capture_at"])


def account_observation_times(conn: Connection, raw_payload_id: int, user_id: int) -> tuple[int, int]:
    """Replay clocks come from this account's occurrences, never another owner."""
    row = require_row(conn.execute(
        """SELECT MIN(observed_at) AS first_at, MAX(observed_at) AS last_at FROM (
             SELECT captured_at AS observed_at FROM user_observations
               WHERE raw_payload_id = %s AND provider_user_id = %s
             UNION ALL
             SELECT o.captured_at FROM user_resource_observations o
               JOIN user_resource_snapshots s ON s.id = o.snapshot_id
               WHERE o.raw_payload_id = %s AND s.provider_user_id = %s
             UNION ALL
             SELECT f.attempted_at FROM fetch_logs f
               WHERE f.raw_payload_id = %s AND f.status_code IN (200,304)
                 AND (f.provider_user_id = %s OR (f.provider_user_id IS NULL
                   AND NOT EXISTS(SELECT 1 FROM user_observations o WHERE o.fetch_log_id = f.id
                     AND o.provider_user_id <> %s)
                   AND NOT EXISTS(SELECT 1 FROM user_resource_observations o
                     JOIN user_resource_snapshots s ON s.id = o.snapshot_id
                     WHERE o.fetch_log_id = f.id AND s.provider_user_id <> %s)))
           ) occurrences""",
        (raw_payload_id, user_id, raw_payload_id, user_id, raw_payload_id, user_id, user_id, user_id),
    ))
    if row["first_at"] is None:
        raw = require_row(conn.execute("SELECT fetched_at FROM raw_payloads WHERE id = %s", (raw_payload_id,)))
        return int(raw["fetched_at"]), int(raw["fetched_at"])
    return int(row["first_at"]), int(row["last_at"])


def player_profile(conn: Connection, provider: str, username: str, *, owner_scope: str = "public") -> dict[str, Any] | None:
    user = conn.execute(
        "SELECT * FROM provider_users WHERE provider = %s AND username_normalized = %s",
        (provider, username.strip().lower()),
    ).fetchone()
    if user is None:
        aliases = conn.execute(
            """SELECT p.* FROM provider_user_aliases a JOIN provider_users p ON p.id = a.provider_user_id
               WHERE p.provider = %s AND a.username_normalized = %s LIMIT 2""", (provider, username.strip().lower()),
        ).fetchall()
        if len(aliases) != 1:
            return None
        user = aliases[0]
    user_id = int(user["id"])
    result = dict(user)
    snapshot = conn.execute(
        """SELECT s.*, o.captured_at AS observed_at, o.fetch_log_id FROM user_observations o
           JOIN user_snapshots s ON s.id = o.snapshot_id JOIN raw_payloads raw ON raw.id = o.raw_payload_id
           WHERE o.provider_user_id = %s AND raw.owner_scope = 'public'
           AND o.endpoint_type = 'user_profile' ORDER BY o.captured_at DESC,
             o.fetch_log_id DESC NULLS LAST, o.id DESC LIMIT 1""",
        (user_id,),
    ).fetchone()
    result["profile"] = None if snapshot is None else dict(snapshot)
    stats = conn.execute(
        """SELECT s.*, o.captured_at AS observed_at, o.fetch_log_id FROM user_observations o
           JOIN user_snapshots s ON s.id = o.snapshot_id JOIN raw_payloads raw ON raw.id = o.raw_payload_id
           WHERE o.provider_user_id = %s AND raw.owner_scope = 'public'
           AND o.endpoint_type = 'user_stats' ORDER BY o.captured_at DESC,
             o.fetch_log_id DESC NULLS LAST, o.id DESC LIMIT 1""", (user_id,),
    ).fetchone()
    result["statistics"] = None if stats is None else dict(stats)
    result["aliases"] = [dict(row) for row in conn.execute(
        """SELECT a.* FROM provider_user_aliases a LEFT JOIN raw_payloads r ON r.id = a.raw_payload_id
           WHERE a.provider_user_id = %s AND (r.owner_scope = 'public' OR a.raw_payload_id IS NULL)
           ORDER BY a.first_seen_at, a.username_normalized""", (user_id,),
    )]
    result["resources"] = resource_current(conn, user_id, owner_scope=owner_scope)
    result["resource_attempts"] = resource_attempts(conn, user_id, owner_scope=owner_scope)
    result["ratings"] = [dict(row) for row in conn.execute(
        """SELECT DISTINCT ON(r.performance) r.*, o.captured_at AS observed_at, o.raw_payload_id
           FROM user_rating_records r JOIN user_observations o ON o.snapshot_id = r.snapshot_id
           JOIN raw_payloads raw ON raw.id = o.raw_payload_id
           WHERE o.provider_user_id = %s AND raw.owner_scope = 'public'
           ORDER BY r.performance, o.captured_at DESC,
             o.fetch_log_id DESC NULLS LAST, o.id DESC""", (user_id,),
    )]
    if result["first_seen_at"] is None and result["updated_at"] is None and not any((
        result["profile"], result["statistics"], result["aliases"], result["resources"],
        result["resource_attempts"], result["ratings"],
    )):
        return None
    return result


def profile_history(conn: Connection, user_id: int, *, limit: int = 100, after_id: int = 0) -> list[dict[str, Any]]:
    _page(limit, after_id)
    return [dict(row) for row in conn.execute(
        """SELECT o.id AS observation_id, o.captured_at AS observed_at, o.fetch_log_id, o.endpoint_type,
           s.* FROM user_observations o JOIN user_snapshots s ON s.id = o.snapshot_id
           JOIN raw_payloads raw ON raw.id = o.raw_payload_id
           WHERE o.provider_user_id = %s AND raw.owner_scope = 'public' AND o.id > %s ORDER BY o.id LIMIT %s""",
        (user_id, after_id, limit),
    )]


def resource_history(
    conn: Connection, user_id: int, *, resource_key: str | None = None, limit: int = 100, after_id: int = 0,
    owner_scope: str = "public",
) -> list[dict[str, Any]]:
    _page(limit, after_id)
    return [dict(row) for row in conn.execute(
        """SELECT o.id AS observation_id, o.captured_at AS observed_at, o.raw_payload_id, o.fetch_log_id, s.*
           FROM user_resource_observations o JOIN user_resource_snapshots s ON s.id = o.snapshot_id
           WHERE s.provider_user_id = %s AND s.owner_scope IN ('public', %s)
             AND (%s::text IS NULL OR s.resource_key = %s) AND o.id > %s
           ORDER BY o.id LIMIT %s""",
        (user_id, owner_scope, resource_key, resource_key, after_id, limit),
    )]


def resource_current(conn: Connection, user_id: int, *, owner_scope: str = "public") -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(
        """SELECT DISTINCT ON(s.owner_scope, s.resource_key, s.parameters_hash) s.*, o.id AS observation_id,
             o.captured_at AS observed_at, o.raw_payload_id, o.fetch_log_id
           FROM user_resource_observations o JOIN user_resource_snapshots s ON s.id = o.snapshot_id
           WHERE s.provider_user_id = %s AND s.owner_scope IN ('public', %s)
           ORDER BY s.owner_scope, s.resource_key, s.parameters_hash,
             o.captured_at DESC, o.fetch_log_id DESC NULLS LAST, o.id DESC""",
        (user_id, owner_scope),
    )]


def _page(limit: int, after_id: int) -> None:
    if type(limit) is not int or not 1 <= limit <= 1000 or type(after_id) is not int or after_id < 0:
        raise ValueError("history requires limit 1..1000 and a nonnegative observation cursor")
