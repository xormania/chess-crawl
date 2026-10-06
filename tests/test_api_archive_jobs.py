"""Async archive API ownership, finite downloads, and immediate-export parity."""
from __future__ import annotations

import hashlib
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from helpers.api import TOKENS, client
from support import seed_game
from chess_crawl.api import create_app
from chess_crawl.api import artifact_downloads as downloads
from chess_crawl.api.exports import ExportCapacity, ExportLimits
from fastapi import HTTPException
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs import state
from chess_crawl.storage import artifacts
from chess_crawl.storage import working_sets
from chess_crawl.storage.db import Connection, connection, require_row
from chess_crawl.storage.discovery import OpponentEdge, record_discovery_edges
from chess_crawl.storage.object_store import object_key


@pytest.fixture(autouse=True)
def archive_job_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_JOBS_ENABLED", "true")
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def submit_export(api: TestClient, *, kind: str = "games", key: str = "export") -> dict[str, Any]:
    response = api.post("/v1/archive-jobs/exports", json={"kind": kind}, headers={"Idempotency-Key": key})
    assert response.status_code == 202, response.text
    assert response.headers["Location"] == response.json()["status_url"]
    return response.json()


def process(archive: str, job_id: int) -> None:
    with connection(archive, mode="rw") as conn:
        result = JobRunner(conn, stage="processing").run(max_jobs=1, job_id=job_id)
        job = state.get_job(conn, job_id)
        assert result.done == 1, (result, job.reason if job else None)


def test_disabled_admission_and_invalid_requests_never_open_the_database(monkeypatch: pytest.MonkeyPatch) -> None:
    from chess_crawl.api import archive_jobs

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Disabled or invalid admission must not access the archive")

    monkeypatch.setattr(archive_jobs, "connection", forbidden)
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_JOBS_ENABLED", "false")
    with client("postgresql://test@127.0.0.1:1/unavailable") as api:
        response = api.post("/v1/archive-jobs/exports", json={"kind": "games"}, headers={"Idempotency-Key": "off"})
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "archive_jobs_disabled"
        monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_JOBS_ENABLED", "true")
        for body in ({"kind": "private"}, {"kind": "games", "workspace_id": "beta"},
                     {"kind": "games", "max_rows": 999999999}, {"kind": "games", "provider": "invalid"}):
            assert api.post("/v1/archive-jobs/exports", json=body, headers={"Idempotency-Key": "bad"}).status_code == 422
        assert api.post("/v1/archive-jobs/working-sets", json={"name": "bad", "filters": {"username": "alice"}},
                        headers={"Idempotency-Key": "bad"}).status_code == 422


def test_async_submissions_are_owned_and_replay_at_artifact_capacity(database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_MAX_COUNT", "1")
    with client(database_url) as alpha, client(database_url, "beta") as beta:
        first = submit_export(alpha)
        replay = submit_export(alpha)
        assert replay["replayed"] is True and replay["job_ids"] == first["job_ids"]
        assert alpha.post("/v1/archive-jobs/exports", json={"kind": "users"}, headers={"Idempotency-Key": "export"}).status_code == 409
        excess = alpha.post("/v1/archive-jobs/exports", json={"kind": "games"}, headers={"Idempotency-Key": "second"})
        assert excess.status_code == 429 and excess.json()["error"]["quota"]["dimension"] == "artifacts"
        other = submit_export(beta)
        assert other["job_ids"] != first["job_ids"]
        assert beta.get(first["status_url"]).status_code == 404
        assert beta.get(first["status_url"] + "/download").status_code == 404
        assert alpha.get(other["status_url"]).status_code == 404
    with connection(database_url) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM archive_artifacts"))[0] == 2
        assert require_row(conn.execute("SELECT COUNT(*) FROM archive_jobs"))[0] == 2


@pytest.mark.parametrize(("kind", "extension"), [("games", "jsonl"), ("users", "jsonl"), ("graph", "csv")])
def test_background_download_matches_immediate_serialization_and_closes_its_lease(
    seeded_database_url: str, kind: str, extension: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = seeded_database_url
    active = 0
    original_connection = downloads.connection
    original_read = downloads.read_archive_reference

    @contextmanager
    def tracked_connection(*args: Any, **kwargs: Any) -> Iterator[Connection]:
        nonlocal active
        with original_connection(*args, **kwargs) as conn:
            active += 1
            try:
                yield conn
            finally:
                active -= 1

    def checked_read(row: Any) -> bytes:
        assert active == 0, "Object reads must not hold a database connection"
        return original_read(row)

    monkeypatch.setattr(downloads, "connection", tracked_connection)
    monkeypatch.setattr(downloads, "read_archive_reference", checked_read)
    with client(archive) as alpha:
        immediate = alpha.get(f"/v1/exports/{kind}.{extension}")
        assert immediate.status_code == 200
        submission = submit_export(alpha, kind=kind)
        process(archive, submission["job_ids"][0])
        descriptor = alpha.get(submission["status_url"])
        assert descriptor.status_code == 200, descriptor.text
        manifest = descriptor.json()["artifact"]["manifest"]
        assert "object_key" not in descriptor.text and "location" not in descriptor.text
        # Disabling new admission preserves status and retained downloads.
        monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_JOBS_ENABLED", "false")
        downloaded = alpha.get(submission["status_url"] + "/download")
        assert downloaded.status_code == 200, downloaded.text
        assert downloaded.content == immediate.content
        assert downloaded.headers["X-Content-Hash"] == "sha256:" + hashlib.sha256(downloaded.content).hexdigest()
        assert int(downloaded.headers["Content-Length"]) == manifest["body_bytes"]
        assert downloaded.headers["Cache-Control"] == "private, no-store"
    with connection(archive) as conn:
        assert require_row(conn.execute("SELECT COUNT(*) FROM artifact_downloads"))[0] == 0


def test_empty_export_and_working_set_status_remain_owned_after_completion(database_url: str) -> None:
    with client(database_url) as alpha, client(database_url, "beta") as beta:
        body = {"name": "empty analysis", "filters": {}, "settings": {"model": "test-v1"}}
        submitted = alpha.post("/v1/archive-jobs/working-sets", json=body, headers={"Idempotency-Key": "build"})
        assert submitted.status_code == 202
        result = submitted.json()
        process(database_url, result["job_ids"][0])
        saved = alpha.get(result["status_url"]).json()
        assert saved["job"]["state"] == "done" and saved["working_set"]["member_count"] == 0
        assert alpha.post("/v1/archive-jobs/working-sets", json=body, headers={"Idempotency-Key": "build"}).json()["replayed"] is True
        assert beta.get(result["status_url"]).status_code == 404
        assert beta.get(f"/v1/working-sets/{saved['working_set']['id']}").status_code == 404
        assert alpha.get(result["status_url"] + "/download").status_code == 404
        exported = submit_export(alpha)
        process(database_url, exported["job_ids"][0])
        response = alpha.get(exported["status_url"] + "/download")
        assert response.status_code == 200 and response.content == b""


def test_foreign_workspace_download_never_reads_an_object(seeded_database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with client(seeded_database_url) as alpha, client(seeded_database_url, "beta") as beta:
        submitted = submit_export(alpha)
        process(seeded_database_url, submitted["job_ids"][0])

        def forbidden(*args: Any, **kwargs: Any) -> bytes:
            pytest.fail("Foreign workspace attempted an artifact object read")

        monkeypatch.setattr(downloads, "read_archive_reference", forbidden)
        assert beta.get(submitted["status_url"] + "/download").status_code == 404
        assert beta.get(submitted["status_url"]).status_code == 404


def test_async_graph_contains_only_owned_edges_and_keeps_csv_formula_escaping(database_url: str) -> None:
    with client(database_url) as alpha, client(database_url, "beta") as beta:
        owners = [(alpha, "owned", "=formula"), (beta, "private", "private-player")]
        for api, key, username in owners:
            response = api.post("/v1/crawls", json={
                "provider": "lichess", "username": "alice", "since": 1704067200, "until": 1706745600,
                "max_games": 2, "max_depth": 1, "max_users": 2, "max_jobs": 2,
            }, headers={"Idempotency-Key": key})
            assert response.status_code == 202, response.text
            with connection(database_url, mode="rw") as conn:
                game, source, target = seed_game(conn, provider="lichess", game_key=key, white=username, black=key)
                record_discovery_edges(conn, crawl_run_id=response.json()["run_id"], provider="lichess",
                                       from_user_id=source, depth=1, edges=[OpponentEdge(target, key, game, 1)])
        submitted = submit_export(alpha, kind="graph")
        process(database_url, submitted["job_ids"][0])
        body = alpha.get(submitted["status_url"] + "/download").text
        assert "'=formula" in body and "private-player" not in body
        assert body == alpha.get("/v1/exports/graph.csv").text


def test_artifact_ttl_and_pending_state_prevent_downloads(database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with client(database_url) as alpha:
        submitted = submit_export(alpha)
        assert alpha.get(submitted["status_url"] + "/download").status_code == 404
        process(database_url, submitted["job_ids"][0])
        future = time.time() + 90000
        monkeypatch.setattr(artifacts.time, "time", lambda: future)
        descriptor = alpha.get(submitted["status_url"]).json()["artifact"]
        assert descriptor["state"] == "expired" and descriptor["download_url"] is None
        assert alpha.get(submitted["status_url"] + "/download").status_code == 404


def test_corrupt_first_chunk_fails_before_success_headers_and_releases_capacity(
    seeded_database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = seeded_database_url
    with client(archive) as alpha:
        submitted = submit_export(alpha)
        process(archive, submitted["job_ids"][0])
        with connection(archive) as conn:
            row = artifacts.chunk_page(conn, submitted["job_ids"][0], limit=1)[0]
        path = Path(row["location"]) / row["object_key"]
        encoded = path.read_bytes()
        path.write_bytes(bytes([encoded[0] ^ 1]) + encoded[1:])
        with TestClient(create_app(archive, workspace_tokens=TOKENS), headers={"Authorization": "Bearer alpha-secret"},
                        raise_server_exceptions=False) as bounded_api:
            response = bounded_api.get(submitted["status_url"] + "/download")
            assert response.status_code == 500
        with connection(archive) as conn:
            assert require_row(conn.execute("SELECT COUNT(*) FROM artifact_downloads"))[0] == 0
        path.write_bytes(encoded)
        assert alpha.get(submitted["status_url"] + "/download").status_code == 200


def download_record(body: bytes) -> dict[str, Any]:
    manifest = {
        "contract_version": 1, "renderer_version": "archive-export-v1", "kind": "games", "provider": None,
        "rows": 0, "body_bytes": len(body), "content_hash": "sha256:" + hashlib.sha256(body).hexdigest(),
        "snapshot_started_at": 1, "selection_time": "processing",
        "chunk_count": (len(body) + artifacts.CHUNK_BYTES - 1) // artifacts.CHUNK_BYTES,
        "filename": "games.jsonl", "media_type": "application/x-ndjson",
    }
    manifest["artifact_signature"] = working_sets.digest(manifest)
    return {"manifest": manifest, "reserved_bytes": max(1, len(body))}


def reference(body: bytes, ordinal: int) -> dict[str, Any]:
    body_hash = "sha256:" + hashlib.sha256(body).hexdigest()
    return {"ordinal": ordinal, "body_bytes": len(body), "stored_bytes": 1, "body_hash": body_hash, "stored_hash": body_hash,
            "object_key": artifacts.artifact_prefix("alpha", 1) + "attempt-1/" + object_key(body_hash), "body": body}


def test_multichunk_download_verifies_the_whole_content_and_closes_once(monkeypatch: pytest.MonkeyPatch) -> None:
    body = b"a" * artifacts.CHUNK_BYTES + b"tail"
    rows = [reference(body[:artifacts.CHUNK_BYTES], 1), reference(body[artifacts.CHUNK_BYTES:], 2)]
    closed: list[bool] = []
    stream = downloads.ArtifactDownload("unused", 1, "alpha", download_record(body),
                                        deadline=time.time() + 60, on_close=lambda: closed.append(True))
    monkeypatch.setattr(stream, "_page", lambda: rows[stream._ordinal:stream._ordinal + 1])
    monkeypatch.setattr(downloads, "read_archive_reference", lambda row: row["body"])
    stream.prime()
    assert b"".join(stream) == body
    stream.close()
    assert closed == [True]


@pytest.mark.parametrize("corruption", ["prefix", "attempt", "ordinal", "stored_bytes", "body_bytes", "hash"])
def test_download_rejects_invalid_metadata_and_checksums_and_closes(
    monkeypatch: pytest.MonkeyPatch, corruption: str,
) -> None:
    body = b"one verified chunk"
    record = download_record(body)
    row = reference(body, 1)
    if corruption == "prefix":
        row["object_key"] = object_key(row["body_hash"])
    elif corruption == "attempt":
        row["object_key"] = row["object_key"].replace("attempt-1/", "attempt-0/")
    elif corruption == "ordinal":
        row["ordinal"] = 2
    elif corruption == "stored_bytes":
        row["stored_bytes"] = artifacts.CHUNK_BYTES + 65537
    elif corruption == "body_bytes":
        row["body_bytes"] = artifacts.CHUNK_BYTES + 1
    else:
        manifest = record["manifest"]
        manifest["content_hash"] = "sha256:" + "0" * 64
        manifest["artifact_signature"] = working_sets.digest({key: value for key, value in manifest.items()
                                                              if key != "artifact_signature"})
    closed: list[bool] = []
    stream = downloads.ArtifactDownload("unused", 1, "alpha", record,
                                        deadline=time.time() + 60, on_close=lambda: closed.append(True))
    monkeypatch.setattr(stream, "_page", lambda: [row] if stream._ordinal == 0 else [])
    reads: list[bool] = []

    def read(row: Any) -> bytes:
        reads.append(True)
        return row["body"]

    monkeypatch.setattr(downloads, "read_archive_reference", read)
    with pytest.raises(ValueError, match="Artifact"):
        stream.prime()
    assert bool(reads) is (corruption == "hash")
    assert closed == [True]


def test_download_cannot_mix_chunks_from_different_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    body = b"a" * artifacts.CHUNK_BYTES + b"tail"
    rows = [reference(body[:artifacts.CHUNK_BYTES], 1), reference(body[artifacts.CHUNK_BYTES:], 2)]
    rows[1]["object_key"] = rows[1]["object_key"].replace("attempt-1/", "attempt-2/")
    closed: list[bool] = []
    stream = downloads.ArtifactDownload("unused", 1, "alpha", download_record(body),
                                        deadline=time.time() + 60, on_close=lambda: closed.append(True))
    monkeypatch.setattr(stream, "_page", lambda: rows[stream._ordinal:stream._ordinal + 1])
    monkeypatch.setattr(downloads, "read_archive_reference", lambda row: row["body"])
    stream.prime()
    assert next(stream) == body[:artifacts.CHUNK_BYTES]
    with pytest.raises(ValueError, match="private namespace"):
        next(stream)
    assert closed == [True]


def test_download_expiry_releases_resources_without_waiting_for_iteration() -> None:
    closed: list[bool] = []
    stream = downloads.ArtifactDownload("unused", 1, "alpha", download_record(b""),
                                        deadline=time.time() + 60, on_close=lambda: closed.append(True))
    stream._expire()
    with pytest.raises(TimeoutError):
        next(stream)
    stream.close()
    assert closed == [True]


def test_download_memory_capacity_bounds_unconsumed_streams_and_releases_on_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(downloads, "export_capacity", ExportCapacity())
    limits = ExportLimits()
    first = downloads.reserve_download_memory(limits, "alpha")
    second = downloads.reserve_download_memory(limits, "alpha")
    with pytest.raises(HTTPException) as error:
        downloads.reserve_download_memory(limits, "alpha")
    assert error.value.status_code == 429
    first()
    replacement = downloads.reserve_download_memory(limits, "alpha")
    replacement()
    second()
    tiny = ExportLimits(max_bytes=1024, workspace_outstanding_bytes=4096, outstanding_bytes=8192)
    with pytest.raises(HTTPException) as error:
        downloads.reserve_download_memory(tiny, "alpha")
    assert error.value.status_code == 429
