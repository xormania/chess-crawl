"""Local bounded exports over normalized archive tables."""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO


def export_games_jsonl(
    conn: sqlite3.Connection,
    *,
    output: Path | None = None,
    provider: str | None = None,
) -> int:
    rows = conn.execute(
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
         WHERE (? IS NULL OR g.provider = ?)
         ORDER BY g.provider, g.ended_at, g.provider_game_id, g.id
        """,
        (provider, provider),
    )
    with _open_output(output) as handle:
        return _write_jsonl(handle, (_row_dict(row) for row in rows))


def export_users_jsonl(
    conn: sqlite3.Connection,
    *,
    output: Path | None = None,
    provider: str | None = None,
) -> int:
    rows = conn.execute(
        """
        SELECT provider, provider_user_id, username_normalized, display_username,
               account_status, title, first_seen_at, updated_at
          FROM provider_users
         WHERE (? IS NULL OR provider = ?)
         ORDER BY provider, username_normalized
        """,
        (provider, provider),
    )
    with _open_output(output) as handle:
        return _write_jsonl(handle, (_row_dict(row) for row in rows))


def export_graph_csv(
    conn: sqlite3.Connection,
    *,
    output: Path | None = None,
    provider: str | None = None,
) -> int:
    rows = conn.execute(
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
         WHERE (? IS NULL OR e.provider = ?)
         ORDER BY e.provider, e.depth, from_username, to_username
        """,
        (provider, provider),
    )
    with _open_output(output) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "provider",
                "crawl_run_id",
                "from_username",
                "to_username",
                "from_user_id",
                "to_user_id",
                "via_game_id",
                "game_count",
                "depth",
                "edge_kind",
            ),
        )
        writer.writeheader()
        count = 0
        for row in rows:
            writer.writerow(_row_dict(row))
            count += 1
        return count


def _write_jsonl(handle: TextIO, rows: Iterable[dict[str, object]]) -> int:
    count = 0
    for row in rows:
        handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")))
        handle.write("\n")
        count += 1
    return count


def _row_dict(row: sqlite3.Row) -> dict[str, object]:
    return {key: row[key] for key in row.keys()}


@contextmanager
def _open_output(output: Path | None):
    if output is None:
        yield sys.stdout
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        yield handle
