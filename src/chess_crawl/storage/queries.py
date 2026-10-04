"""Read-side queries over normalized archive tables."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, cast

from chess_crawl.storage.db import Connection, Row, require_row


_FIRST_REPORT_EPOCH = -62135596800  # 0001-01-01T00:00:00Z
_AFTER_LAST_REPORT_EPOCH = 253402300800  # 10000-01-01T00:00:00Z


@dataclass(frozen=True)
class UserReport:
    id: int
    provider: str
    provider_user_id: str | None
    username: str
    display_username: str
    account_status: str | None
    title: str | None
    snapshots: int
    games: int


@dataclass(frozen=True)
class GameReport:
    id: int
    provider: str
    provider_game_id: str | None
    canonical_url: str | None
    outcome: str | None
    is_live: bool
    status_raw: str | None
    ended_at: int | None
    variant: str
    time_class: str
    white: str | None
    black: str | None


def query_user(conn: Connection, provider: str, username: str) -> UserReport | None:
    normalized = username.strip().lower()
    row = conn.execute(
        """
        SELECT pu.*,
               (SELECT COUNT(*) FROM user_snapshots us WHERE us.provider_user_id = pu.id) AS snapshots,
               (SELECT COUNT(*)
                  FROM game_participants gp
                  JOIN games g ON g.id = gp.game_id
                 WHERE gp.provider_user_id = pu.id AND g.provider = pu.provider) AS games
          FROM provider_users pu
         WHERE pu.provider = %s AND pu.username_normalized = %s
        """,
        (provider, normalized),
    ).fetchone()
    if row is None:
        return None
    return UserReport(
        id=int(row["id"]),
        provider=row["provider"],
        provider_user_id=row["provider_user_id"],
        username=row["username_normalized"],
        display_username=row["display_username"],
        account_status=row["account_status"],
        title=row["title"],
        snapshots=int(row["snapshots"]),
        games=int(row["games"]),
    )


def query_game(conn: Connection, provider: str, game_id: str) -> GameReport | None:
    row = conn.execute(
        """
        SELECT g.*, v.canonical_name AS variant, tc.time_class,
               wp.username_normalized AS white_username,
               bp.username_normalized AS black_username
          FROM games g
          JOIN variants v ON v.id = g.variant_id
          JOIN time_controls tc ON tc.id = g.time_control_id
          LEFT JOIN game_participants wp ON wp.game_id = g.id AND wp.color = 'white'
          LEFT JOIN game_participants bp ON bp.game_id = g.id AND bp.color = 'black'
         WHERE g.provider = %s
           AND (g.provider_game_id = %s OR g.canonical_url = %s OR g.content_hash = %s)
         ORDER BY g.id
         LIMIT 1
        """,
        (provider, game_id, game_id, game_id),
    ).fetchone()
    if row is None:
        return None
    return GameReport(
        id=int(row["id"]),
        provider=row["provider"],
        provider_game_id=row["provider_game_id"],
        canonical_url=row["canonical_url"],
        outcome=row["outcome"],
        is_live=bool(row["is_live"]),
        status_raw=row["status_raw"],
        ended_at=row["ended_at"],
        variant=row["variant"],
        time_class=row["time_class"],
        white=row["white_username"],
        black=row["black_username"],
    )


def query_raw(conn: Connection, provider: str, limit: int) -> list[Row]:
    return list(
        conn.execute(
            """
            SELECT id, provider, endpoint_type, canonical_source_key, response_status,
                   content_type, body_hash, body_bytes, normalization_status, fetched_at
              FROM raw_payloads
             WHERE provider = %s
             ORDER BY fetched_at DESC, id DESC
             LIMIT %s
            """,
            (provider, limit),
        )
    )


def summary_report(conn: Connection) -> dict[str, Any]:
    providers = list(
        conn.execute(
            """
            SELECT p.key AS provider,
                   (SELECT COUNT(*) FROM provider_users pu WHERE pu.provider = p.key) AS users,
                   (SELECT COUNT(*) FROM games g WHERE g.provider = p.key) AS games
              FROM providers p
             ORDER BY p.key COLLATE "C"
            """
        )
    )
    raw_payloads = int(require_row(conn.execute(
        "SELECT COUNT(*) FROM raw_payloads WHERE owner_scope = 'public'",
    ))[0])
    runs = list(
        conn.execute(
            """
            SELECT status, COUNT(*) AS count
              FROM crawl_runs
             GROUP BY status
             ORDER BY status COLLATE "C"
            """
        )
    )
    jobs = list(
        conn.execute(
            """
            SELECT state, COUNT(*) AS count
              FROM discovery_jobs
             GROUP BY state
             ORDER BY state COLLATE "C"
            """
        )
    )
    return {
        "providers": providers,
        "raw_payloads": raw_payloads,
        "runs": runs,
        "jobs": jobs,
    }


def user_game_summary(conn: Connection, provider: str, username: str) -> Row | None:
    """Count results and known activity independently; unfinished is a legacy no_result alias."""
    user = query_user(conn, provider, username)
    if user is None:
        return None
    return conn.execute(
        """
        WITH mine AS (
          SELECT g.id, gp.color, g.outcome, g.rated, g.ended_at, g.is_live
            FROM games g
            JOIN game_participants gp ON gp.game_id = g.id AND gp.provider_user_id = %s
           WHERE g.provider = %s
        ),
        opp AS (
          SELECT DISTINCT gp_o.provider_user_id AS opponent_id
            FROM games g
            JOIN game_participants gp_m ON gp_m.game_id = g.id AND gp_m.provider_user_id = %s
            JOIN game_participants gp_o ON gp_o.game_id = g.id AND gp_o.color <> gp_m.color
           WHERE g.provider = %s AND gp_o.provider_user_id IS NOT NULL
        )
        SELECT
          %s::bigint AS user_id,
          %s::text AS provider,
          %s::text AS username,
          %s::text AS display_username,
          %s::text AS account_status,
          COUNT(mine.id) AS games,
          COUNT(*) FILTER (WHERE mine.rated = 1) AS rated_games,
          COUNT(*) FILTER (WHERE mine.rated = 0) AS unrated_games,
          COUNT(*) FILTER (WHERE (mine.color='white' AND mine.outcome='white_win')
                    OR (mine.color='black' AND mine.outcome='black_win')) AS wins,
          COUNT(*) FILTER (WHERE mine.outcome='draw') AS draws,
          COUNT(*) FILTER (WHERE (mine.color='white' AND mine.outcome='black_win')
                    OR (mine.color='black' AND mine.outcome='white_win')) AS losses,
          COUNT(*) FILTER (WHERE mine.outcome IS NULL) AS unfinished,
          COUNT(*) FILTER (WHERE mine.outcome IS NULL) AS no_result,
          COUNT(*) FILTER (WHERE mine.is_live = 1) AS in_progress,
          MIN(mine.ended_at) AS first_game_ts,
          MAX(mine.ended_at) AS last_game_ts,
          (SELECT COUNT(*) FROM opp) AS distinct_opponents
        FROM mine
        """,
        (
            user.id,
            provider,
            user.id,
            provider,
            user.id,
            provider,
            user.username,
            user.display_username,
            user.account_status,
        ),
    ).fetchone()


def opponent_report(conn: Connection, provider: str, username: str) -> list[Row] | None:
    user = query_user(conn, provider, username)
    if user is None:
        return None
    return list(
        conn.execute(
            """
            WITH opp AS (
              SELECT gp_o.provider_user_id AS opponent_id,
                     gp_m.color AS my_color,
                     g.outcome, g.is_live
                FROM games g
                JOIN game_participants gp_m
                  ON gp_m.game_id = g.id AND gp_m.provider_user_id = %s
                JOIN game_participants gp_o
                  ON gp_o.game_id = g.id AND gp_o.color <> gp_m.color
               WHERE g.provider = %s
                 AND gp_o.provider_user_id IS NOT NULL
            )
            SELECT pu.provider AS provider,
                   pu.username_normalized AS opponent_username,
                   pu.display_username AS opponent_display,
                   COUNT(*) AS games,
                   COUNT(*) FILTER (WHERE (my_color='white' AND outcome='white_win')
                             OR (my_color='black' AND outcome='black_win')) AS my_wins,
                   COUNT(*) FILTER (WHERE outcome='draw') AS draws,
                   COUNT(*) FILTER (WHERE (my_color='white' AND outcome='black_win')
                             OR (my_color='black' AND outcome='white_win')) AS my_losses,
                   COUNT(*) FILTER (WHERE outcome IS NULL) AS unfinished,
                   COUNT(*) FILTER (WHERE outcome IS NULL) AS no_result,
                   COUNT(*) FILTER (WHERE is_live = 1) AS in_progress
              FROM opp
              JOIN provider_users pu ON pu.id = opp.opponent_id AND pu.provider = %s
             GROUP BY pu.id
             ORDER BY games DESC, pu.username_normalized COLLATE "C"
            """,
            (user.id, provider, provider),
        )
    )


def games_by_month(conn: Connection, *, provider: str) -> list[Row]:
    """Bucket UTC Gregorian years 0001–9999; other preserved times are unknown."""
    return list(
        conn.execute(
            """
            SELECT CASE WHEN ended_at >= %s AND ended_at < %s
                        THEN to_char(to_timestamp(ended_at) AT TIME ZONE 'UTC', 'YYYY-MM')
                        ELSE 'unknown' END AS month,
                   COUNT(*) AS games,
                   COUNT(*) FILTER (WHERE outcome='white_win') AS white_wins,
                   COUNT(*) FILTER (WHERE outcome='black_win') AS black_wins,
                   COUNT(*) FILTER (WHERE outcome='draw') AS draws,
                   COUNT(*) FILTER (WHERE outcome IS NULL) AS unfinished,
                   COUNT(*) FILTER (WHERE outcome IS NULL) AS no_result,
                   COUNT(*) FILTER (WHERE is_live = 1) AS in_progress
              FROM games
             WHERE provider = %s
             GROUP BY month
             ORDER BY month
            """,
            (_FIRST_REPORT_EPOCH, _AFTER_LAST_REPORT_EPOCH, provider),
        )
    )


def iter_games(
    conn: Connection, *, provider: str | None = None
) -> Generator[Row, None, None]:
    """Stream normalized game records in deterministic export order."""
    with conn.cursor() as cursor:
        stream = cast(Generator[Row, None, None], cursor.stream(
            """
            SELECT g.provider, g.provider_game_id, g.canonical_url, g.outcome, g.is_live,
                   g.status_raw, g.rated, g.created_at, g.ended_at,
                   v.canonical_name AS variant, v.provider_native_name AS variant_raw,
                   tc.time_class, tc.raw_label AS time_control,
                   wp.username_normalized AS white_username,
                   bp.username_normalized AS black_username
              FROM games g
              JOIN variants v ON v.id = g.variant_id
              JOIN time_controls tc ON tc.id = g.time_control_id
              LEFT JOIN game_participants wp ON wp.game_id = g.id AND wp.color = 'white'
              LEFT JOIN game_participants bp ON bp.game_id = g.id AND bp.color = 'black'
             WHERE (%s::text IS NULL OR g.provider = %s)
             ORDER BY g.provider COLLATE "C", g.ended_at NULLS FIRST, g.provider_game_id COLLATE "C" NULLS FIRST, g.id
            """,
            (provider, provider),
        ))
        try:
            yield from stream
        finally:
            stream.close()


def iter_users(
    conn: Connection, *, provider: str | None = None
) -> Generator[Row, None, None]:
    """Stream normalized user records without loading the archive into memory."""
    with conn.cursor() as cursor:
        stream = cast(Generator[Row, None, None], cursor.stream(
            """
            SELECT provider, provider_user_id, username_normalized, display_username,
                   account_status, title, first_seen_at, updated_at
              FROM provider_users
             WHERE (%s::text IS NULL OR provider = %s)
             ORDER BY provider COLLATE "C", username_normalized COLLATE "C"
            """,
            (provider, provider),
        ))
        try:
            yield from stream
        finally:
            stream.close()


def iter_graph_edges(
    conn: Connection, *, provider: str | None = None
) -> Generator[Row, None, None]:
    """Stream provider-scoped discovery edges and their endpoint usernames."""
    with conn.cursor() as cursor:
        stream = cast(Generator[Row, None, None], cursor.stream(
            """
            SELECT e.provider,
                   e.crawl_run_id,
                   fu.username_normalized AS from_username,
                   tu.username_normalized AS to_username,
                   e.from_user_id,
                   e.to_user_id,
                   e.via_game_id,
                   e.game_count,
                   e.depth,
                   e.edge_kind
              FROM discovery_edges e
              JOIN provider_users fu ON fu.id = e.from_user_id AND fu.provider = e.provider
              JOIN provider_users tu ON tu.id = e.to_user_id AND tu.provider = e.provider
             WHERE (%s::text IS NULL OR e.provider = %s)
             ORDER BY e.provider COLLATE "C", e.depth, fu.username_normalized COLLATE "C", tu.username_normalized COLLATE "C"
            """,
            (provider, provider),
        ))
        try:
            yield from stream
        finally:
            stream.close()


def archive_freshness(conn: Connection, *, provider: str | None = None) -> dict[str, Any]:
    """Describe preserved and normalized observations, without implying live data."""
    row = require_row(conn.execute(
        """
        SELECT (SELECT MAX(f.attempted_at) FROM fetch_logs f
                 LEFT JOIN raw_payloads checked_raw ON checked_raw.id = f.raw_payload_id
                WHERE (f.raw_payload_id IS NULL OR checked_raw.owner_scope = 'public')
                  AND (%s::text IS NULL OR f.provider = %s)
                  AND f.status_code IN (200, 304)) AS last_checked_at,
               MAX(fetched_at) AS last_fetched_at,
               MAX(normalized_at) AS last_normalized_at,
               COUNT(*) FILTER (WHERE normalization_status IN ('pending', 'stale')) AS pending_payloads,
               COUNT(*) FILTER (WHERE normalization_status = 'failed') AS failed_payloads
          FROM raw_payloads
         WHERE owner_scope = 'public' AND (%s::text IS NULL OR provider = %s)
        """,
        (provider, provider, provider, provider),
    ))
    return {"provider": provider, **dict(row)}


def game_page(
    conn: Connection, *, provider: str | None, after: int, limit: int
) -> tuple[list[Row], int]:
    """Read a bounded page ordered by immutable storage ID, plus matching count."""
    total = int(require_row(conn.execute(
        "SELECT COUNT(*) FROM games WHERE (%s::text IS NULL OR provider = %s)", (provider, provider),
    ))[0])
    rows = list(conn.execute(
        """
        SELECT g.id, g.provider, g.provider_game_id, g.canonical_url, g.outcome,
               g.is_live, g.status_raw, g.rated, g.created_at, g.ended_at, g.first_seen_at,
               v.canonical_name AS variant, tc.time_class,
               wp.username_normalized AS white_username, bp.username_normalized AS black_username
          FROM games g
          JOIN variants v ON v.id = g.variant_id
          JOIN time_controls tc ON tc.id = g.time_control_id
          LEFT JOIN game_participants wp ON wp.game_id = g.id AND wp.color = 'white'
          LEFT JOIN game_participants bp ON bp.game_id = g.id AND bp.color = 'black'
         WHERE g.id > %s AND (%s::text IS NULL OR g.provider = %s)
         ORDER BY g.id
         LIMIT %s
        """,
        (after, provider, provider, limit),
    ))
    return rows, total


def user_page(
    conn: Connection, *, provider: str | None, after: int, limit: int
) -> tuple[list[Row], int]:
    total = int(require_row(conn.execute(
        "SELECT COUNT(*) FROM provider_users WHERE (%s::text IS NULL OR provider = %s)", (provider, provider),
    ))[0])
    rows = list(conn.execute(
        """
        SELECT pu.id, pu.provider, pu.provider_user_id, pu.username_normalized,
               pu.display_username, pu.account_status, pu.title, pu.first_seen_at, pu.updated_at,
               (SELECT COUNT(*) FROM user_snapshots us WHERE us.provider_user_id = pu.id) AS snapshots,
               (SELECT COUNT(*) FROM game_participants gp
                  JOIN games g ON g.id = gp.game_id
                 WHERE gp.provider_user_id = pu.id AND g.provider = pu.provider) AS games
          FROM provider_users pu
         WHERE pu.id > %s AND (%s::text IS NULL OR pu.provider = %s)
         ORDER BY pu.id
         LIMIT %s
        """,
        (after, provider, provider, limit),
    ))
    return rows, total


def opponent_page(
    conn: Connection, *, provider: str, user_id: int, after: int, limit: int
) -> tuple[list[Row], int]:
    # Limit applies to opponent groups, never individual games within a group.
    source = """
      FROM games g
      JOIN game_participants mine ON mine.game_id = g.id AND mine.provider_user_id = %s
      JOIN game_participants other ON other.game_id = g.id AND other.color <> mine.color
      JOIN provider_users pu ON pu.id = other.provider_user_id AND pu.provider = %s
     WHERE g.provider = %s AND pu.id <> %s
    """
    params = (user_id, provider, provider, user_id)
    total = int(require_row(conn.execute("SELECT COUNT(DISTINCT pu.id) " + source, params))[0])
    rows = list(conn.execute(
        """
        SELECT pu.id, pu.provider, pu.username_normalized AS opponent_username,
               pu.display_username AS opponent_display, COUNT(*) AS games,
               COUNT(*) FILTER (WHERE (mine.color='white' AND g.outcome='white_win')
                         OR (mine.color='black' AND g.outcome='black_win')) AS my_wins,
               COUNT(*) FILTER (WHERE g.outcome='draw') AS draws,
               COUNT(*) FILTER (WHERE (mine.color='white' AND g.outcome='black_win')
                         OR (mine.color='black' AND g.outcome='white_win')) AS my_losses,
               COUNT(*) FILTER (WHERE g.outcome IS NULL) AS unfinished,
               COUNT(*) FILTER (WHERE g.outcome IS NULL) AS no_result,
               COUNT(*) FILTER (WHERE g.is_live = 1) AS in_progress
        """ + source + " AND pu.id > %s GROUP BY pu.id ORDER BY pu.id LIMIT %s",
        (*params, after, limit),
    ))
    return rows, total
