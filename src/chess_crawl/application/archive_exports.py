"""Shared bounded archive serialization for immediate and background exports."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, closing, contextmanager
from dataclasses import dataclass
from typing import TextIO, Protocol

from chess_crawl.application.errors import ValidationError
from chess_crawl.jobs.budget import QuotaExceeded
from chess_crawl.storage import api_views, queries
from chess_crawl.storage.db import Connection, connection, transaction


class RenderLimits(Protocol):
    @property
    def max_rows(self) -> int: ...
    @property
    def max_bytes(self) -> int: ...
    @property
    def workspace_slots(self) -> int: ...


@dataclass(frozen=True)
class ExportRenderLimits:
    max_rows: int
    max_bytes: int
    workspace_slots: int


@dataclass(frozen=True)
class ExportSnapshot:
    rows: int
    body_bytes: int
    content_hash: str
    snapshot_started_at: int


def check_export_bounds(*, limits: RenderLimits, rows: int, bytes_written: int,
                        deadline: float, now: float | None = None) -> None:
    if rows > limits.max_rows or bytes_written > limits.max_bytes or (time.monotonic() if now is None else now) >= deadline:
        raise ValidationError(
            "The export exceeds the operator's row, byte or preparation-time limit; narrow the provider filter",
            code="export_limit_exceeded",
        )


@contextmanager
def _export_snapshot(conn: Connection) -> Iterator[Connection]:
    if conn.in_transaction:
        raise ValueError("Export snapshot requires an idle owned session")
    with transaction(conn, write=False):
        yield conn


def write_export_snapshot(archive: str, kind: str, provider: str|None, workspace_id: str, *,
                          spool: TextIO, limits: RenderLimits, deadline: float,
                          connect: Callable[..., AbstractContextManager[Connection]] = connection,
                          clock: Callable[[], float] = time.monotonic,
                          on_rows: Callable[[int], None] | None = None) -> ExportSnapshot:
    rows_written = bytes_written = 0
    hash_state = hashlib.sha256()
    snapshot_started_at = int(time.time())

    def write(chunk: str, *, record: bool) -> None:
        nonlocal rows_written,bytes_written
        if record and on_rows is not None:
            on_rows(1)
        rows_written += int(record)
        bytes_written += len(chunk.encode("utf-8"))
        check_export_bounds(limits=limits,rows=rows_written,bytes_written=bytes_written,deadline=deadline,now=clock())
        spool.write(chunk)
        hash_state.update(chunk.encode("utf-8"))

    with connect(archive) as conn, _export_snapshot(conn):
        remaining_ms = max(1,int((deadline-clock())*1000))
        if not api_views.admit_export_snapshot(conn,workspace_id=workspace_id,
                                              slots=limits.workspace_slots,timeout_ms=remaining_ms):
            raise QuotaExceeded("export_preparations", remaining=0)
        api_views.check_export_schema(conn)
        check_export_bounds(limits=limits,rows=0,bytes_written=0,deadline=deadline,now=clock())
        api_views.set_export_timeout(conn,max(1,int((deadline-clock())*1000)))
        rows = queries.iter_games(conn,provider=provider) if kind=="games" else (
            queries.iter_users(conn,provider=provider) if kind=="users" else
            api_views.iter_owned_graph(conn,workspace_id=workspace_id,provider=provider)
        )
        with closing(rows):
            if kind!="graph":
                for row in rows:
                    write(json.dumps(dict(row),sort_keys=True,separators=(",",":"))+"\n",record=True)
                return ExportSnapshot(rows_written, bytes_written, "sha256:" + hash_state.hexdigest(), snapshot_started_at)
            buffer = io.StringIO(newline="")
            writer = csv.DictWriter(buffer,fieldnames=(
                "provider","crawl_run_id","from_username","to_username","from_user_id","to_user_id",
                "via_game_id","game_count","depth","edge_kind",
            ))
            writer.writeheader()
            write(buffer.getvalue(),record=False)
            for row in rows:
                buffer.seek(0)
                buffer.truncate(0)
                values = dict(row)
                # Provider-native usernames must not become spreadsheet formulas.
                for key in ("from_username","to_username"):
                    value = values[key]
                    if isinstance(value,str) and value.startswith(("=","+","-","@","\t","\r")):
                        values[key] = "'"+value
                writer.writerow(values)
                write(buffer.getvalue(),record=True)

    return ExportSnapshot(rows_written, bytes_written, "sha256:" + hash_state.hexdigest(), snapshot_started_at)
