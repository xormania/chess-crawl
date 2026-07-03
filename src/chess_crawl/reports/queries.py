"""Small read-side queries available in Phase 1."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


def provider_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        row["provider"]: int(row["count"])
        for row in conn.execute(
            """
            SELECT provider, COUNT(*) AS count
            FROM provider_users
            GROUP BY provider
            ORDER BY provider
            """
        )
    }


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


def query_user(conn: sqlite3.Connection, provider: str, username: str) -> UserReport | None:
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
         WHERE pu.provider = ? AND pu.username_normalized = ?
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


def query_game(conn: sqlite3.Connection, provider: str, game_id: str) -> GameReport | None:
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
         WHERE g.provider = ?
           AND (g.provider_game_id = ? OR g.canonical_url = ? OR g.content_hash = ?)
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


def query_raw(conn: sqlite3.Connection, provider: str, limit: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT id, provider, endpoint_type, canonical_source_key, response_status,
                   content_type, body_hash, body_bytes, normalization_status, fetched_at
              FROM raw_payloads
             WHERE provider = ?
             ORDER BY fetched_at DESC, id DESC
             LIMIT ?
            """,
            (provider, limit),
        )
    )


def summary_report(conn: sqlite3.Connection) -> dict[str, object]:
    providers = list(
        conn.execute(
            """
            SELECT p.key AS provider,
                   COUNT(DISTINCT pu.id) AS users,
                   COUNT(DISTINCT g.id) AS games
              FROM providers p
              LEFT JOIN provider_users pu ON pu.provider = p.key
              LEFT JOIN games g ON g.provider = p.key
             GROUP BY p.key
             ORDER BY p.key
            """
        )
    )
    raw_payloads = int(conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0])
    runs = list(
        conn.execute(
            """
            SELECT status, COUNT(*) AS count
              FROM crawl_runs
             GROUP BY status
             ORDER BY status
            """
        )
    )
    jobs = list(
        conn.execute(
            """
            SELECT state, COUNT(*) AS count
              FROM discovery_jobs
             GROUP BY state
             ORDER BY state
            """
        )
    )
    return {
        "providers": providers,
        "raw_payloads": raw_payloads,
        "runs": runs,
        "jobs": jobs,
    }


def user_game_summary(conn: sqlite3.Connection, provider: str, username: str) -> sqlite3.Row | None:
    user = query_user(conn, provider, username)
    if user is None:
        return None
    return conn.execute(
        """
        WITH mine AS (
          SELECT g.id, gp.color, g.outcome, g.rated, g.ended_at
            FROM games g
            JOIN game_participants gp ON gp.game_id = g.id AND gp.provider_user_id = ?
           WHERE g.provider = ?
        ),
        opp AS (
          SELECT DISTINCT gp_o.provider_user_id AS opponent_id
            FROM games g
            JOIN game_participants gp_m ON gp_m.game_id = g.id AND gp_m.provider_user_id = ?
            JOIN game_participants gp_o ON gp_o.game_id = g.id AND gp_o.color <> gp_m.color
           WHERE g.provider = ? AND gp_o.provider_user_id IS NOT NULL
        )
        SELECT
          ? AS user_id,
          ? AS provider,
          ? AS username,
          ? AS display_username,
          ? AS account_status,
          COUNT(mine.id) AS games,
          COALESCE(SUM(mine.rated), 0) AS rated_games,
          COALESCE(SUM(1 - mine.rated), 0) AS unrated_games,
          COALESCE(SUM((mine.color='white' AND mine.outcome='white_win')
                    OR (mine.color='black' AND mine.outcome='black_win')), 0) AS wins,
          COALESCE(SUM(mine.outcome='draw'), 0) AS draws,
          COALESCE(SUM((mine.color='white' AND mine.outcome='black_win')
                    OR (mine.color='black' AND mine.outcome='white_win')), 0) AS losses,
          COALESCE(SUM(mine.outcome IS NULL), 0) AS unfinished,
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


def opponent_report(conn: sqlite3.Connection, provider: str, username: str) -> list[sqlite3.Row] | None:
    user = query_user(conn, provider, username)
    if user is None:
        return None
    return list(
        conn.execute(
            """
            WITH opp AS (
              SELECT gp_o.provider_user_id AS opponent_id,
                     gp_m.color AS my_color,
                     g.outcome
                FROM games g
                JOIN game_participants gp_m
                  ON gp_m.game_id = g.id AND gp_m.provider_user_id = ?
                JOIN game_participants gp_o
                  ON gp_o.game_id = g.id AND gp_o.color <> gp_m.color
               WHERE g.provider = ?
                 AND gp_o.provider_user_id IS NOT NULL
            )
            SELECT pu.provider AS provider,
                   pu.username_normalized AS opponent_username,
                   pu.display_username AS opponent_display,
                   COUNT(*) AS games,
                   COALESCE(SUM((my_color='white' AND outcome='white_win')
                             OR (my_color='black' AND outcome='black_win')), 0) AS my_wins,
                   COALESCE(SUM(outcome='draw'), 0) AS draws,
                   COALESCE(SUM((my_color='white' AND outcome='black_win')
                             OR (my_color='black' AND outcome='white_win')), 0) AS my_losses,
                   COALESCE(SUM(outcome IS NULL), 0) AS unfinished
              FROM opp
              JOIN provider_users pu ON pu.id = opp.opponent_id AND pu.provider = ?
             GROUP BY pu.id
             ORDER BY games DESC, pu.username_normalized
            """,
            (user.id, provider, provider),
        )
    )


def games_by_month(conn: sqlite3.Connection, *, provider: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT COALESCE(strftime('%Y-%m', ended_at, 'unixepoch'), 'unknown') AS month,
                   COUNT(*) AS games,
                   COALESCE(SUM(outcome='white_win'), 0) AS white_wins,
                   COALESCE(SUM(outcome='black_win'), 0) AS black_wins,
                   COALESCE(SUM(outcome='draw'), 0) AS draws,
                   COALESCE(SUM(outcome IS NULL), 0) AS unfinished
              FROM games
             WHERE provider = ?
             GROUP BY month
             ORDER BY month
            """,
            (provider,),
        )
    )
