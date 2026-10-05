"""All retained export files count against capacity until actual close."""
from __future__ import annotations

import asyncio
import tempfile
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

import pytest
from fastapi import HTTPException
from starlette.requests import ClientDisconnect

from chess_crawl.api import compat
from chess_crawl.api.compat import ArchiveExportResponse, _prepare_export
from chess_crawl.api.exports import ExportCapacity, ExportLimits, ExportSpool
from test_working_sets import client


def test_capacity_counts_slow_downloads_across_workspaces(database_url, monkeypatch):
    monkeypatch.setattr(compat, "export_capacity", ExportCapacity())
    limits = ExportLimits(max_bytes=1024, outstanding_bytes=2048, outstanding_spools=4)
    first = _prepare_export(database_url, "graph", None, "alpha", limits=limits)
    second = _prepare_export(database_url, "graph", None, "beta", limits=limits)
    try:
        with pytest.raises(HTTPException) as full:
            _prepare_export(database_url, "graph", None, "alpha", limits=limits)
        assert full.value.status_code == 429
        # A fully prepared, unread file keeps its reservation. Reading the last
        # chunk also keeps it until EOF/close because its bytes still exist.
        next(first)
        with pytest.raises(HTTPException):
            _prepare_export(database_url, "graph", None, "gamma", limits=limits)
        first.close()
        first.close()  # Never subtract a reservation twice.
        with pytest.raises(StopIteration):
            next(first)
        replacement = _prepare_export(database_url, "graph", None, "gamma", limits=limits)
        replacement.close()
    finally:
        first.close()
        second.close()


def test_process_capacity_cannot_be_oversubscribed_by_parallel_requests():
    capacity = ExportCapacity()
    limits = ExportLimits(max_bytes=100, outstanding_bytes=300, outstanding_spools=2)
    barrier = Barrier(8)
    def reserve():
        barrier.wait(timeout=10)
        try:
            return capacity.reserve(limits)
        except HTTPException:
            return None
    with ThreadPoolExecutor(max_workers=8) as pool:
        reservations = list(pool.map(lambda _: reserve(), range(8)))
    acquired = [release for release in reservations if release is not None]
    assert len(acquired) == 2
    for release in acquired:
        release()
    capacity.reserve(limits)()


def test_http_storage_capacity_rejects_before_attachment_headers(database_url, monkeypatch):
    capacity = ExportCapacity()
    monkeypatch.setattr(compat, "export_capacity", capacity)
    monkeypatch.setenv("CHESS_CRAWL_EXPORT_OUTSTANDING_SPOOLS", "1")
    release = capacity.reserve(ExportLimits.from_env())
    try:
        with client(database_url) as api:
            response = api.get("/v1/exports/games.jsonl")
        assert response.status_code == 429
        assert response.headers["retry-after"] == "5"
        assert "content-disposition" not in response.headers
    finally:
        release()


@pytest.mark.parametrize("stage", ["open", "prepare"])
def test_preparation_failure_releases_reservation(database_url, monkeypatch, stage):
    capacity = ExportCapacity()
    monkeypatch.setattr(compat, "export_capacity", capacity)
    limits = ExportLimits(outstanding_spools=1)
    def fail(*args, **kwargs):
        raise OSError("spool failure")
    with monkeypatch.context() as patch:
        if stage == "open":
            patch.setattr(compat.tempfile, "TemporaryFile", fail)
        else:
            patch.setattr(compat, "_write_export", fail)
        with pytest.raises(OSError, match="spool failure"):
            _prepare_export(database_url, "games", None, "alpha", limits=limits)
    spool = _prepare_export(database_url, "games", None, "alpha", limits=limits)
    assert list(spool) == []
    assert spool.file.closed
    capacity.reserve(limits)()


def test_unconsumed_spool_expires_and_releases_actual_file():
    capacity = ExportCapacity()
    limits = ExportLimits(outstanding_spools=1)
    release = capacity.reserve(limits)
    closed = Event()
    def on_close():
        release()
        closed.set()
    file = tempfile.TemporaryFile(mode="w+t")
    spool = ExportSpool(file, on_close=on_close, lifetime_seconds=0.01)
    try:
        assert closed.wait(10), "spool expiry callback never ran"
        assert file.closed
        with pytest.raises(TimeoutError, match="lifetime expired"):
            next(spool)
        capacity.reserve(limits)()
    finally:
        spool.close()


@pytest.mark.parametrize("reason", ["disconnect", "timeout", "cancel"])
def test_interrupted_delivery_releases_capacity(reason):
    capacity = ExportCapacity()
    limits = ExportLimits(outstanding_spools=1)
    file = tempfile.TemporaryFile(mode="w+t")
    file.write("row\n")
    file.seek(0)
    spool = ExportSpool(file, on_close=capacity.reserve(limits))
    response = ArchiveExportResponse(spool, media_type="application/x-ndjson", headers={}, download_seconds=1)
    async def exercise():
        sending = asyncio.Event()
        async def receive():
            await asyncio.Event().wait()
        async def send(message):
            if message["type"] == "http.response.start":
                return
            sending.set()
            if reason == "disconnect":
                raise OSError("closed")
            await asyncio.Event().wait()
        task = asyncio.create_task(response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send))
        await asyncio.wait_for(sending.wait(), timeout=10)
        if reason == "cancel":
            task.cancel()
        with pytest.raises((ClientDisconnect, TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(task, timeout=10)
    asyncio.run(exercise())
    assert file.closed
    capacity.reserve(limits)()


def test_expiry_waits_for_active_read_before_closing_and_releasing():
    read_started, finish_read, closed = Event(), Event(), Event()
    class BlockingFile:
        closed = False
        def read(self, size):
            read_started.set()
            assert finish_read.wait(10)
            assert not self.closed
            return "row\n"
        def close(self):
            self.closed = True
    file = BlockingFile()
    spool = ExportSpool(file, on_close=closed.set)
    with ThreadPoolExecutor(max_workers=2) as pool:
        reading = pool.submit(next, spool)
        assert read_started.wait(10)
        expiring = pool.submit(spool._expire)
        try:
            assert not closed.is_set()
        finally:
            finish_read.set()
        assert reading.result(timeout=10) == "row\n"
        expiring.result(timeout=10)
    assert file.closed and closed.is_set()


@pytest.mark.parametrize("name,value", [
    ("OUTSTANDING_SPOOLS", "0"), ("OUTSTANDING_SPOOLS", "129"),
    ("OUTSTANDING_BYTES", "0"), ("OUTSTANDING_BYTES", str(16 * 1024**3 + 1)),
    ("OUTSTANDING_BYTES", "bad"), ("OUTSTANDING_BYTES", "100"),
])
def test_outstanding_limits_are_finite_and_admit_one_full_export(monkeypatch, name, value):
    monkeypatch.setenv("CHESS_CRAWL_EXPORT_" + name, value)
    with pytest.raises(ValueError):
        ExportLimits.from_env()
