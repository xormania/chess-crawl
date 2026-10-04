"""Durable acquisition coverage, pagination, and unfinished-game follow-ups."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from psycopg.types.json import Jsonb

from chess_crawl.storage.db import Connection, atomic


def checkpoint(conn: Connection, job_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT cursor FROM collection_checkpoints WHERE job_id = %s", (job_id,),
    ).fetchone()
    return dict(row["cursor"]) if row is not None else None


@atomic
def save_checkpoint(conn: Connection, job_id: int, cursor: Mapping[str, Any], *, now: int) -> None:
    conn.execute(
        """INSERT INTO collection_checkpoints(job_id, cursor, updated_at)
             VALUES (%s, %s, %s)
             ON CONFLICT(job_id) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = EXCLUDED.updated_at""",
        (job_id, Jsonb(dict(cursor)), now),
    )


def coverage(conn: Connection, provider: str, username: str, unit: str) -> dict[str, Any] | None:
    row = conn.execute(
        """SELECT * FROM collection_coverage
             WHERE provider = %s AND username_normalized = %s AND unit_key = %s""",
        (provider, username.lower(), unit),
    ).fetchone()
    return dict(row) if row is not None else None


def coverage_rows(conn: Connection, provider: str, username: str) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(
        """SELECT * FROM collection_coverage
             WHERE provider = %s AND username_normalized = %s ORDER BY unit_key""",
        (provider, username.lower()),
    )]


def covered_source_high_water(conn: Connection, username: str, until_ms: int, since_ms: int | None = None) -> int:
    row = conn.execute(
        """SELECT COALESCE(MAX(r.id),0) FROM collection_source_ranges c
             JOIN raw_payloads r ON r.id=c.raw_payload_id
             WHERE c.provider='lichess' AND c.username_normalized=%s
               AND (c.covered_since_ms IS NULL OR c.covered_since_ms<%s)
               AND (%s::bigint IS NULL OR c.covered_until_ms>%s::bigint)
               AND r.response_status=200 AND r.owner_scope='public'""",
        (username.lower(), until_ms, since_ms, since_ms),
    ).fetchone()
    return int(row[0]) if row is not None else 0


def covered_sources(
    conn: Connection, username: str, *, until_ms: int, after: int, high_water: int, limit: int,
    since_ms: int | None = None,
) -> list[int]:
    """Page preserved source versions underlying a completed history baseline."""
    return [int(row[0]) for row in conn.execute(
        """SELECT r.id FROM collection_source_ranges c
             JOIN raw_payloads r ON r.id=c.raw_payload_id
             WHERE c.provider='lichess' AND c.username_normalized=%s
               AND (c.covered_since_ms IS NULL OR c.covered_since_ms<%s)
               AND (%s::bigint IS NULL OR c.covered_until_ms>%s::bigint)
               AND r.response_status=200 AND r.owner_scope='public'
               AND r.id>%s AND r.id<=%s ORDER BY r.id LIMIT %s""",
        (username.lower(), until_ms, since_ms, since_ms, after, high_water, limit),
    )]


@atomic
def record_source_range(
    conn: Connection, username: str, raw_id: int, *, since_ms: int | None, until_ms: int,
) -> None:
    conn.execute(
        """INSERT INTO collection_source_ranges(provider,username_normalized,raw_payload_id,covered_since_ms,covered_until_ms)
             VALUES('lichess',%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (username.lower(), raw_id, since_ms, until_ms),
    )


def missing_windows(conn: Connection, username: str, *, since_ms: int | None, until_ms: int) -> list[dict[str, Any]]:
    """Subtract only proven complete intervals; saturated boundary ties remain missing."""
    return [dict(row) for row in conn.execute(
        """WITH covered AS (
             SELECT int8range(covered_since_ms,covered_until_ms,'[)') AS span
               FROM collection_source_ranges WHERE provider='lichess' AND username_normalized=%s
             UNION ALL
             SELECT int8range(NULL,window_until_ms,'[)') FROM collection_coverage
               WHERE provider='lichess' AND username_normalized=%s AND unit_key='history'
                 AND state='complete' AND window_since_ms IS NULL AND window_until_ms IS NOT NULL
           )
           SELECT lower(fragment) AS since_ms,upper(fragment) AS until_ms
             FROM unnest(int8multirange(int8range(%s::bigint,%s::bigint,'[)'))
                         - COALESCE((SELECT range_agg(span) FROM covered),'{}'::int8multirange)) AS fragment
             ORDER BY upper(fragment) DESC""",
        (username.lower(), username.lower(), since_ms, until_ms),
    )]


def history_high_water(conn: Connection, username: str) -> int | None:
    row = conn.execute(
        """WITH covered AS (
             SELECT int8range(covered_since_ms,covered_until_ms,'[)') AS span
               FROM collection_source_ranges WHERE provider='lichess' AND username_normalized=%s
             UNION ALL
             SELECT int8range(NULL,window_until_ms,'[)') FROM collection_coverage
               WHERE provider='lichess' AND username_normalized=%s AND unit_key='history'
                 AND state='complete' AND window_since_ms IS NULL AND window_until_ms IS NOT NULL
           )
           SELECT upper(fragment) FROM unnest((SELECT range_agg(span) FROM covered)) AS fragment
             WHERE lower_inf(fragment) LIMIT 1""",
        (username.lower(), username.lower()),
    ).fetchone()
    return int(row[0]) if row is not None and row[0] is not None else None


@atomic
def record_coverage(
    conn: Connection, *, provider: str, username: str, unit: str, state: str,
    raw_payload_id: int | None = None, parser_version: str | None = None,
    error: str | None = None, since_ms: int | None = None, until_ms: int | None = None,
    sealed: bool = False, now: int,
) -> None:
    conn.execute(
        """INSERT INTO collection_coverage(
             provider, username_normalized, unit_key, state, raw_payload_id,
             parser_version, fetched_at, error, window_since_ms, window_until_ms, sealed)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
             ON CONFLICT(provider, username_normalized, unit_key) DO UPDATE SET
               state = EXCLUDED.state, raw_payload_id = COALESCE(EXCLUDED.raw_payload_id,collection_coverage.raw_payload_id),
               parser_version = EXCLUDED.parser_version, fetched_at = EXCLUDED.fetched_at,
               error = EXCLUDED.error, window_since_ms = EXCLUDED.window_since_ms,
               window_until_ms = EXCLUDED.window_until_ms, sealed = EXCLUDED.sealed
             WHERE CASE WHEN EXCLUDED.unit_key = 'history' THEN
               EXCLUDED.state = 'complete' AND EXCLUDED.window_since_ms IS NULL
               AND EXCLUDED.window_until_ms IS NOT NULL
               AND (collection_coverage.state <> 'complete'
                    OR collection_coverage.window_since_ms IS NOT NULL
                    OR collection_coverage.window_until_ms IS NULL
                    OR EXCLUDED.window_until_ms > collection_coverage.window_until_ms
                    OR (EXCLUDED.window_until_ms = collection_coverage.window_until_ms
                        AND collection_coverage.fetched_at <= EXCLUDED.fetched_at))
             ELSE collection_coverage.fetched_at <= EXCLUDED.fetched_at END""",
        (provider, username.lower(), unit, state, raw_payload_id, parser_version,
         now, error, since_ms, until_ms, sealed),
    )


@atomic
def track_followup(
    conn: Connection, *, username: str, game_ref: str, created_ms: int,
    finished: bool, now: int,
) -> None:
    if finished:
        conn.execute(
            """DELETE FROM collection_followups
                 WHERE provider = 'lichess' AND username_normalized = %s AND game_ref = %s""",
            (username.lower(), game_ref),
        )
    else:
        conn.execute(
            """INSERT INTO collection_followups(
                 provider, username_normalized, game_ref, created_ms, updated_at)
                 VALUES ('lichess', %s, %s, %s, %s)
                 ON CONFLICT(provider, username_normalized, game_ref)
                 DO UPDATE SET updated_at = EXCLUDED.updated_at""",
            (username.lower(), game_ref, created_ms, now),
        )


def followups(conn: Connection, username: str, *, after: str, limit: int) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(
        """SELECT game_ref, created_ms FROM collection_followups
             WHERE provider = 'lichess' AND username_normalized = %s AND game_ref > %s
             ORDER BY game_ref LIMIT %s""",
        (username.lower(), after, limit),
    )]


def latest_successful_source(conn: Connection, key: str) -> int | None:
    row = conn.execute(
        """SELECT r.id FROM raw_payloads r
             LEFT JOIN fetch_logs f ON f.raw_payload_id = r.id AND f.status_code IN (200, 304)
             WHERE r.canonical_source_key = %s AND r.response_status = 200
             ORDER BY COALESCE(f.attempted_at, r.fetched_at) DESC,
                      f.id DESC NULLS LAST, r.id DESC LIMIT 1""",
        (key,),
    ).fetchone()
    return int(row["id"]) if row is not None else None
