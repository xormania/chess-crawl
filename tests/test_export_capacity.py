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
from helpers.api import client


def test_capacity_counts_slow_downloads_across_workspaces(database_url, monkeypatch):
    monkeypatch.setattr(compat, "export_capacity", ExportCapacity())
    limits = ExportLimits(max_bytes=1024, outstanding_bytes=2048, outstanding_spools=4,
                          workspace_outstanding_spools=1, workspace_outstanding_bytes=1024)
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
    limits = ExportLimits(max_bytes=100, outstanding_bytes=300, outstanding_spools=2,
                          workspace_outstanding_spools=1, workspace_outstanding_bytes=100)
    barrier = Barrier(8)
    def reserve(index):
        barrier.wait(timeout=10)
        try:
            return capacity.reserve(limits, workspace_id=f"owner-{index}")
        except HTTPException:
            return None
    with ThreadPoolExecutor(max_workers=8) as pool:
        reservations = list(pool.map(reserve, range(8)))
    acquired = [release for release in reservations if release is not None]
    assert len(acquired) == 2
    for release in acquired:
        release()
    capacity.reserve(limits, workspace_id="alpha")()


def test_http_storage_capacity_rejects_before_attachment_headers(database_url, monkeypatch):
    capacity = ExportCapacity()
    monkeypatch.setattr(compat, "export_capacity", capacity)
    monkeypatch.setenv("CHESS_CRAWL_EXPORT_OUTSTANDING_SPOOLS", "2")
    monkeypatch.setenv("CHESS_CRAWL_EXPORT_WORKSPACE_OUTSTANDING_SPOOLS", "1")
    limits = ExportLimits.from_env()
    release = capacity.reserve(limits, workspace_id="beta")
    other_release = capacity.reserve(limits, workspace_id="gamma")
    try:
        with client(database_url) as api:
            response = api.get("/v1/exports/games.jsonl")
        assert response.status_code == 429
        assert response.headers["retry-after"] == "5"
        assert "content-disposition" not in response.headers
    finally:
        release()
        other_release()


@pytest.mark.parametrize("stage", ["open", "prepare"])
def test_preparation_failure_releases_reservation(database_url, monkeypatch, stage):
    capacity = ExportCapacity()
    monkeypatch.setattr(compat, "export_capacity", capacity)
    limits = ExportLimits(outstanding_spools=2, workspace_outstanding_spools=1)
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
    capacity.reserve(limits, workspace_id="alpha")()


def test_unconsumed_spool_expires_and_releases_actual_file():
    capacity = ExportCapacity()
    limits = ExportLimits(outstanding_spools=2, workspace_outstanding_spools=1)
    release = capacity.reserve(limits, workspace_id="alpha")
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
        capacity.reserve(limits, workspace_id="alpha")()
    finally:
        spool.close()


@pytest.mark.parametrize("reason", ["disconnect", "timeout", "cancel"])
def test_interrupted_delivery_releases_capacity(reason):
    capacity = ExportCapacity()
    limits = ExportLimits(outstanding_spools=2, workspace_outstanding_spools=1)
    file = tempfile.TemporaryFile(mode="w+t")
    file.write("row\n")
    file.seek(0)
    spool = ExportSpool(file, on_close=capacity.reserve(limits, workspace_id="alpha"))
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
    capacity.reserve(limits, workspace_id="alpha")()


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


@pytest.mark.parametrize("quota", ["spools", "bytes"])
def test_one_workspace_cannot_fill_storage_with_slow_downloads(database_url, monkeypatch, quota):
    capacity = ExportCapacity()
    monkeypatch.setattr(compat, "export_capacity", capacity)
    limits = ExportLimits(
        max_bytes=1024, outstanding_spools=4, outstanding_bytes=4096,
        workspace_outstanding_spools=2 if quota == "spools" else 3,
        workspace_outstanding_bytes=3072 if quota == "spools" else 2048,
    )
    alpha = [_prepare_export(database_url, "graph", None, "alpha", limits=limits) for _ in range(2)]
    beta = []
    try:
        # Retain both files after preparation, as an unread/slow download does.
        next(alpha[0])
        with pytest.raises(HTTPException, match="workspace") as busy:
            _prepare_export(database_url, "graph", None, "alpha", limits=limits)
        assert busy.value.status_code == 429
        assert busy.value.headers == {"Retry-After": "5"}
        beta = [_prepare_export(database_url, "graph", None, "beta", limits=limits) for _ in range(2)]
        assert all(not spool.file.closed for spool in [*alpha, *beta])
        with pytest.raises(HTTPException) as full:
            _prepare_export(database_url, "graph", None, "gamma", limits=limits)
        assert "workspace" not in str(full.value.detail)
        alpha[0].close()
        alpha[0].close()
        # Releasing alpha cannot consume or release beta's quota.
        with pytest.raises(HTTPException, match="workspace"):
            _prepare_export(database_url, "graph", None, "beta", limits=limits)
        replacement = _prepare_export(database_url, "graph", None, "alpha", limits=limits)
        replacement.close()
    finally:
        for spool in [*alpha, *beta]:
            spool.close()
    assert capacity._workspaces == {}


def test_workspace_capacity_is_atomic_during_parallel_requests():
    capacity = ExportCapacity()
    limits = ExportLimits()
    barrier = Barrier(8)
    def reserve(index):
        owner = "alpha" if index % 2 else "beta"
        barrier.wait(timeout=10)
        try:
            return owner, capacity.reserve(limits, workspace_id=owner)
        except HTTPException:
            return owner, None
    with ThreadPoolExecutor(max_workers=8) as pool:
        reservations = list(pool.map(reserve, range(8)))
    acquired = [(owner, release) for owner, release in reservations if release is not None]
    assert [owner for owner, _ in acquired].count("alpha") == 2
    assert [owner for owner, _ in acquired].count("beta") == 2
    for _, release in acquired:
        release()
    assert capacity._workspaces == {}


@pytest.mark.parametrize("settings", [
    {"outstanding_spools": 2},
    {"outstanding_bytes": 128 * 1024 * 1024},
    {"workspace_outstanding_spools": 4},
    {"workspace_outstanding_bytes": 256 * 1024 * 1024},
    {"workspace_outstanding_bytes": 1},
    {"workspace_outstanding_spools": 0},
    {"workspace_outstanding_bytes": 0},
    {"workspace_outstanding_spools": True},
    {"max_bytes": 100, "outstanding_bytes": 350, "outstanding_spools": 8,
     "workspace_outstanding_spools": 7, "workspace_outstanding_bytes": 340},
])
def test_workspace_limits_must_leave_room_for_another_full_export(settings, monkeypatch):
    with pytest.raises(ValueError):
        ExportLimits(**settings)
    for field, value in settings.items():
        monkeypatch.setenv("CHESS_CRAWL_EXPORT_" + field.upper(), str(value))
    with pytest.raises(ValueError):
        ExportLimits.from_env()


def test_workspace_quota_accepts_small_coherent_operator_limits(monkeypatch):
    settings = {"max_bytes": 100, "outstanding_spools": 2, "outstanding_bytes": 200,
                "workspace_outstanding_spools": 1, "workspace_outstanding_bytes": 100}
    for field, value in settings.items():
        monkeypatch.setenv("CHESS_CRAWL_EXPORT_" + field.upper(), str(value))
    limits = ExportLimits.from_env()
    capacity = ExportCapacity()
    first = capacity.reserve(limits, workspace_id="alpha")
    with pytest.raises(HTTPException, match="workspace"):
        capacity.reserve(limits, workspace_id="alpha")
    second = capacity.reserve(limits, workspace_id="beta")
    first()
    second()
    assert capacity._workspaces == {}
