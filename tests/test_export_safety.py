"""Real snapshot admission and resource bounds for slow export consumers."""
from __future__ import annotations

import asyncio
import tempfile
from types import SimpleNamespace
from contextlib import contextmanager
from typing import Any

import pytest
from psycopg.errors import QueryCanceled
from starlette.requests import ClientDisconnect

from chess_crawl.api import compat, exports
from chess_crawl.api.compat import ArchiveExportResponse, _prepare_export
from chess_crawl.api.exports import ExportLimits, ExportSpool
from chess_crawl.application import ValidationError
from chess_crawl.storage import api_views
from chess_crawl.storage.db import connection, transaction
from support import Clock, seed_game
from helpers.api import client


def test_row_and_byte_overflow_fail_before_http_headers(database_url: str,monkeypatch: pytest.MonkeyPatch) -> None:
    with connection(database_url,mode="rw") as conn:
        for name in ("first","second"):
            seed_game(conn,provider="lichess",game_key=name,white="alice",black="bob")
    for name,value in (("MAX_ROWS","1"),("MAX_BYTES","1")):
        with monkeypatch.context() as patch:
            patch.setenv("CHESS_CRAWL_EXPORT_"+name,value)
            with client(database_url) as api:
                response = api.get("/v1/exports/games.jsonl")
                assert response.status_code==422
                assert response.json()["error"]["code"]=="export_limit_exceeded"
                assert "Content-Disposition" not in response.headers


def test_database_admission_is_cross_connection_and_workspace_scoped(database_url: str) -> None:
    with connection(database_url) as first,transaction(first,write=False):
        assert api_views.admit_export_snapshot(first,workspace_id="alpha",slots=1,timeout_ms=1000)
        with connection(database_url) as second,transaction(second,write=False):
            assert not api_views.admit_export_snapshot(second,workspace_id="alpha",slots=1,timeout_ms=1000)
            assert api_views.admit_export_snapshot(second,workspace_id="beta",slots=1,timeout_ms=1000)
    with connection(database_url) as third,transaction(third,write=False):
        assert api_views.admit_export_snapshot(third,workspace_id="alpha",slots=1,timeout_ms=1000)


def test_http_preparation_limit_rejects_only_busy_workspace(database_url: str) -> None:
    with connection(database_url) as first,transaction(first,write=False), connection(database_url) as second,transaction(second,write=False):
        assert api_views.admit_export_snapshot(first,workspace_id="alpha",slots=2,timeout_ms=5000)
        assert api_views.admit_export_snapshot(second,workspace_id="alpha",slots=2,timeout_ms=5000)
        with client(database_url) as api:
            assert api.get("/v1/exports/games.jsonl").status_code==429
            assert api.get("/v1/exports/games.jsonl",headers={"Authorization":"Bearer beta-secret"}).status_code==200


def test_prepared_snapshot_is_closed_and_immutable_before_slow_delivery(database_url: str,monkeypatch: pytest.MonkeyPatch) -> None:
    with connection(database_url,mode="rw") as conn:
        seed_game(conn,provider="lichess",game_key="before",white="alice",black="bob")
    observed=[]
    original=compat.connection
    @contextmanager
    def tracked(target: str):
        with original(target) as conn:
            observed.append(conn)
            yield conn
    monkeypatch.setattr(compat,"connection",tracked)
    spool=_prepare_export(database_url,"games",None,"alpha",limits=ExportLimits())
    assert len(observed)==1 and observed[0].closed
    with connection(database_url,mode="rw") as conn:
        seed_game(conn,provider="lichess",game_key="after",white="alice",black="bob")
    result="".join(spool)
    assert '"provider_game_id":"before"' in result and '"provider_game_id":"after"' not in result
    assert spool.file.closed


def test_overflow_closes_temporary_file(database_url: str,monkeypatch: pytest.MonkeyPatch) -> None:
    with connection(database_url,mode="rw") as conn:
        seed_game(conn,provider="lichess",game_key="one",white="alice",black="bob")
    files=[]
    original=tempfile.TemporaryFile
    def tracked(*args: Any,**kwargs: Any):
        file=original(*args,**kwargs)
        files.append(file)
        return file
    monkeypatch.setattr(compat.tempfile,"TemporaryFile",tracked)
    with pytest.raises(ValidationError):
        _prepare_export(database_url,"games",None,"alpha",limits=ExportLimits(max_bytes=1))
    assert len(files)==1 and files[0].closed


@pytest.mark.parametrize("headers_fail",[True,False])
def test_asgi_timeout_or_early_send_failure_closes_spool(headers_fail: bool) -> None:
    file=tempfile.TemporaryFile(mode="w+t")
    file.write("one\n")
    file.seek(0)
    chunks=ExportSpool(file)
    response=ArchiveExportResponse(chunks,media_type="application/x-ndjson",headers={},download_seconds=1)
    async def receive():
        await asyncio.Event().wait()
    async def send(message):
        if headers_fail:
            raise OSError("closed before any body read")
        await asyncio.Event().wait()
    with pytest.raises((ClientDisconnect,TimeoutError)):
        asyncio.run(response({"type":"http","asgi":{"spec_version":"2.4"}},receive,send))
    assert file.closed


def test_total_deadline_cancels_serial_database_delays_even_for_empty_export(database_url: str,monkeypatch: pytest.MonkeyPatch) -> None:
    clock = Clock(now=100)
    monkeypatch.setattr(compat, "time", SimpleNamespace(monotonic=clock))
    monkeypatch.setattr(exports, "time", SimpleNamespace(monotonic=clock))
    original = api_views.check_export_schema

    def delayed_schema(conn):
        original(conn)
        # Schema admission consumed part of the total preparation budget.
        # Advance only the application clock; SQL cancellation remains real.
        clock.now += 0.75

    observed = []
    def empty_rows(conn, *, provider):
        observed.append(conn.execute("SHOW statement_timeout").fetchone()[0])
        conn.execute("SELECT pg_sleep(1)")
        yield from ()

    monkeypatch.setattr(api_views, "check_export_schema", delayed_schema)
    monkeypatch.setattr(compat.queries, "iter_games", empty_rows)
    with pytest.raises(QueryCanceled):
        _prepare_export(database_url,"games",None,"alpha",limits=ExportLimits(prepare_seconds=1))
    assert observed == ["250ms"]


@pytest.mark.parametrize("value",["0","-1","bad","100000000"])
def test_operator_export_limits_are_finite(value: str,monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_EXPORT_WORKSPACE_SLOTS",value)
    with pytest.raises(ValueError):
        ExportLimits.from_env()
