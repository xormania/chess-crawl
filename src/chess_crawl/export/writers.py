"""Local bounded exports over normalized archive tables."""

from __future__ import annotations

import csv
import json
import sys
from collections.abc import Iterable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import TextIO

from chess_crawl.storage.db import Connection, Row, consistent_read
from chess_crawl.storage.queries import iter_games, iter_graph_edges, iter_users


@consistent_read
def export_games_jsonl(
    conn: Connection,
    *,
    output: Path | None = None,
    provider: str | None = None,
) -> int:
    with closing(iter_games(conn, provider=provider)) as rows, _open_output(output) as handle:
        return _write_jsonl(handle, (_row_dict(row) for row in rows))


@consistent_read
def export_users_jsonl(
    conn: Connection,
    *,
    output: Path | None = None,
    provider: str | None = None,
) -> int:
    with closing(iter_users(conn, provider=provider)) as rows, _open_output(output) as handle:
        return _write_jsonl(handle, (_row_dict(row) for row in rows))


@consistent_read
def export_graph_csv(
    conn: Connection,
    *,
    output: Path | None = None,
    provider: str | None = None,
) -> int:
    with closing(iter_graph_edges(conn, provider=provider)) as rows, _open_output(output) as handle:
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


def _row_dict(row: Row) -> dict[str, object]:
    return {key: row[key] for key in row.keys()}


@contextmanager
def _open_output(output: Path | None) -> Iterator[TextIO]:
    if output is None:
        yield sys.stdout
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        yield handle
