"""Immutable backups survive retries, rollback, migration and corrupt objects."""
from __future__ import annotations

import gzip
import base64
import hashlib
import io
import os
import stat
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from chess_crawl.providers.base import RawRecord
from chess_crawl.ingest import _persist_response
from chess_crawl.storage import archives, migrations
from chess_crawl.storage.archives import (
    prepare_archive_object, read_archive_object, read_import_backup, relocate_raw_payloads,
    store_archive_object, store_import_backup,
)
from chess_crawl.storage.db import connection, require_row, transaction
from chess_crawl.storage.object_store import LocalObjectStore, S3ObjectStore, configured_store, digest, object_key
from chess_crawl.storage.raw import read_raw_payload, store_raw_payload
from chess_crawl.storage.repository import upsert_provider_user


def record(body: bytes = b'{"username":"alice"}') -> RawRecord:
    return RawRecord(
        provider="lichess", endpoint_type="user_profile", canonical_source_key="lichess:user:alice",
        body=body, fetched_at=123, http_status=200, request_url="https://lichess.org/api/user/alice",
    )


def test_local_publication_is_immutable_and_rejects_escape(tmp_path: Path) -> None:
    store = LocalObjectStore(str(tmp_path))
    key = object_key(digest(b"compressed"))
    store.put(key, b"compressed")
    store.put(key, b"compressed")
    with pytest.raises(ValueError, match="differs"):
        store.put(key, b"xxxxxxxxxx")
    assert store.read(key, expected_size=10) == b"compressed"
    with pytest.raises(ValueError, match="key"):
        store.put("../../escaped.gz", b"x")
    with pytest.raises(ValueError, match="size"):
        store.read(key, expected_size=9)


def test_local_directory_symlink_cannot_escape_archive(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    outside = tmp_path / "elsewhere"
    root.mkdir()
    outside.mkdir()
    (root / "sha256").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="escapes"):
        LocalObjectStore(str(root)).put(object_key(digest(b"x")), b"x")
    assert list(outside.iterdir()) == []


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory durability contract")
def test_reused_local_object_syncs_publication_before_database_reference(tmp_path, monkeypatch) -> None:
    store = LocalObjectStore(str(tmp_path))
    key = object_key(digest(b"object"))
    store.put(key, b"object")
    directory_syncs: list[int] = []
    original = os.fsync

    def sync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            directory_syncs.append(fd)
        original(fd)

    monkeypatch.setattr(os, "fsync", sync)
    store.put(key, b"object")
    assert directory_syncs


def test_object_payload_reads_without_current_write_configuration(initialized_conn, tmp_path, monkeypatch) -> None:
    conn = initialized_conn
    store = LocalObjectStore(str(tmp_path))
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(tmp_path))
    raw_id = store_raw_payload(conn, record(), parser_version="v1")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "database")
    row = require_row(conn.execute("SELECT raw_body, archive_object_id FROM raw_payloads WHERE id=%s", (raw_id,)))
    assert row["raw_body"] is None and row["archive_object_id"] is not None
    assert read_raw_payload(conn, raw_id).body == record().body
    assert store_raw_payload(conn, record(), store=store) == raw_id
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1


def test_failed_database_commit_leaves_reusable_object(initialized_conn, tmp_path) -> None:
    conn = initialized_conn
    store = LocalObjectStore(str(tmp_path))
    prepared = prepare_archive_object(record().body or b"", store=store)
    with pytest.raises(RuntimeError, match="abort"):
        with transaction(conn):
            store_raw_payload(conn, record(), store=store, prepared_object=prepared)
            raise RuntimeError("abort database commit")
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 0
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 0
    assert len(list(tmp_path.rglob("*.gz"))) == 1
    raw_id = store_raw_payload(conn, record(), store=store)
    assert read_raw_payload(conn, raw_id).body == record().body
    assert len(list(tmp_path.rglob("*.gz"))) == 1


@pytest.mark.parametrize("operation", ["raw", "response", "import", "relocate"])
def test_blocked_object_publication_does_not_block_database_writes(
    database_url, tmp_path, monkeypatch, operation,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    store = LocalObjectStore(str(tmp_path))
    original_put = LocalObjectStore.put
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(tmp_path))
    if operation == "relocate":
        monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "database")
        with connection(database_url, mode="rw") as conn:
            store_raw_payload(conn, record())

    def blocked_put(self, key, body):
        entered.set()
        if not release.wait(10):
            raise TimeoutError("test publication release not signaled")
        original_put(self, key, body)

    monkeypatch.setattr(LocalObjectStore, "put", blocked_put)

    def publish():
        with connection(database_url, mode="rw") as conn:
            if operation == "raw":
                return store_raw_payload(conn, record(), store=store)
            if operation == "response":
                return _persist_response(conn, record(), job_id=None, crawl_run_id=None)
            if operation == "import":
                return store_import_backup(conn, b"PGN", workspace_id="a", source_name="game.pgn", store=store)
            return relocate_raw_payloads(conn, store=store).moved

    def mutate():
        with connection(database_url, mode="rw") as conn:
            return upsert_provider_user(conn, provider="lichess", username="independent")

    with ThreadPoolExecutor(max_workers=2) as executor:
        publication = executor.submit(publish)
        try:
            assert entered.wait(5)
            mutation = executor.submit(mutate)
            assert mutation.result(timeout=3) > 0
            assert not publication.done()
        finally:
            release.set()
        assert publication.result(timeout=10) > 0


def test_external_writes_in_outer_transaction_require_preparation(initialized_conn, tmp_path, monkeypatch) -> None:
    conn = initialized_conn
    store = LocalObjectStore(str(tmp_path))

    def forbidden_put(self, key, body):
        pytest.fail("External I/O must never run inside a caller-owned transaction")

    monkeypatch.setattr(LocalObjectStore, "put", forbidden_put)
    with transaction(conn):
        with pytest.raises(ValueError, match="prepared object"):
            store_raw_payload(conn, record(), store=store)
        with pytest.raises(ValueError, match="prepared object"):
            store_archive_object(conn, b"PGN", store=store)
        with pytest.raises(ValueError, match="prepared object"):
            store_import_backup(conn, b"PGN", workspace_id="a", source_name="game.pgn", store=store)
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 0


def test_existing_raw_payload_deduplication_avoids_object_io(initialized_conn, tmp_path, monkeypatch) -> None:
    conn = initialized_conn
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(tmp_path))
    raw_id = _persist_response(conn, record(), job_id=None, crawl_run_id=None)

    def forbidden(*args, **kwargs):
        pytest.fail("Deduplicated source must not re-upload or download objects")

    monkeypatch.setattr(LocalObjectStore, "put", forbidden)
    monkeypatch.setattr(LocalObjectStore, "read", forbidden)
    assert _persist_response(conn, record(), job_id=None, crawl_run_id=None) == raw_id
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 2


def test_shared_objects_are_reverified_across_imports_and_sources(initialized_conn, tmp_path, monkeypatch) -> None:
    conn = initialized_conn
    store = LocalObjectStore(str(tmp_path))
    body = b"shared evidence"
    archive_id = store_archive_object(conn, body, store=store)

    original_read = LocalObjectStore.read
    reads: list[str] = []

    def read(self, key, *, expected_size):
        assert not conn.in_transaction
        reads.append(key)
        return original_read(self, key, expected_size=expected_size)

    monkeypatch.setattr(LocalObjectStore, "read", read)
    assert store_archive_object(conn, body, store=store) == archive_id
    assert reads
    reads.clear()
    store_import_backup(conn, body, workspace_id="a", source_name="study.pgn", store=store)
    assert reads
    reads.clear()
    store_import_backup(conn, body, workspace_id="b", source_name="study.pgn", store=store)
    assert reads
    reads.clear()
    store_raw_payload(conn, record(body), store=store)
    assert reads
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_imports"))[0] == 2
    assert len(list(tmp_path.rglob("*.gz"))) == 1


def test_failed_publication_never_releases_inline_body(initialized_conn, tmp_path, monkeypatch) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, record())
    store = LocalObjectStore(str(tmp_path))

    def failed_put(self, key, body):
        raise OSError("disk unavailable")

    monkeypatch.setattr(LocalObjectStore, "put", failed_put)
    with pytest.raises(OSError, match="unavailable"):
        relocate_raw_payloads(conn, store=store)
    assert read_raw_payload(conn, raw_id).body == record().body
    assert require_row(conn.execute("SELECT archive_object_id FROM raw_payloads WHERE id=%s", (raw_id,)))[0] is None
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 0


def test_failed_readback_verification_keeps_inline_body(initialized_conn, tmp_path, monkeypatch) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, record())
    original_read = LocalObjectStore.read

    def corrupt_read(self, key, *, expected_size):
        body = original_read(self, key, expected_size=expected_size)
        return bytes([body[0] ^ 1]) + body[1:]

    monkeypatch.setattr(LocalObjectStore, "read", corrupt_read)
    with pytest.raises(ValueError, match="checksum"):
        relocate_raw_payloads(conn, store=LocalObjectStore(str(tmp_path)))
    assert read_raw_payload(conn, raw_id).body == record().body
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 0


def test_partial_relocation_resumes_without_provider_calls(initialized_conn, tmp_path, monkeypatch) -> None:
    conn = initialized_conn
    ids = [store_raw_payload(conn, record(str(number).encode())) for number in range(3)]
    store = LocalObjectStore(str(tmp_path))
    original_put = LocalObjectStore.put
    count = 0

    def interrupted(self, key, body):
        nonlocal count
        count += 1
        if count == 2:
            raise OSError("interrupted")
        original_put(self, key, body)

    monkeypatch.setattr(LocalObjectStore, "put", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        relocate_raw_payloads(conn, store=store)
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads WHERE archive_object_id IS NOT NULL"))[0] == 1
    monkeypatch.setattr(LocalObjectStore, "put", original_put)
    result = relocate_raw_payloads(conn, store=store, batch_size=1, count_remaining=True)
    assert (result.moved, result.remaining) == (1, 1)
    result = relocate_raw_payloads(conn, store=store)
    assert (result.moved, result.remaining) == (1, 0)
    assert relocate_raw_payloads(conn, store=store).moved == 0
    assert [read_raw_payload(conn, value).body for value in ids] == [b"0", b"1", b"2"]


def test_original_schema_payload_survives_migration(uninitialized_database_url, tmp_path) -> None:
    with connection(uninitialized_database_url, mode="rwc") as conn:
        with transaction(conn):
            migrations._execute_schema(conn, migrations.read_schema_sql())
            conn.execute("INSERT INTO schema_migrations VALUES (1, '0001_init', 123)")
            conn.execute(
                """INSERT INTO raw_payloads(provider,endpoint_type,canonical_source_key,fetched_at,
                     body_hash,raw_body,body_bytes,response_status)
                   VALUES ('lichess','user_profile','original',123,%s,%s,%s,200)""",
                (digest(b"legacy"), b"legacy", 6),
            )
        migrations.initialize(conn)
        assert read_raw_payload(conn, 1).body == b"legacy"
        assert relocate_raw_payloads(conn, store=LocalObjectStore(str(tmp_path))).moved == 1
        assert read_raw_payload(conn, 1).body == b"legacy"
        assert migrations.initialize(conn).applied == ()


def test_corruption_missing_object_and_bad_original_hash_fail_explicitly(initialized_conn, tmp_path) -> None:
    conn = initialized_conn
    store = LocalObjectStore(str(tmp_path))
    archive_id = store_archive_object(conn, b"evidence", store=store)
    row = require_row(conn.execute("SELECT object_key FROM archive_objects WHERE id=%s", (archive_id,)))
    path = tmp_path / row[0]
    encoded = path.read_bytes()
    path.write_bytes(bytes([encoded[0] ^ 1]) + encoded[1:])
    with pytest.raises(ValueError, match="checksum"):
        read_archive_object(conn, archive_id)
    path.unlink()
    with pytest.raises(FileNotFoundError):
        read_archive_object(conn, archive_id)
    with pytest.raises(ValueError, match="hash"):
        store_raw_payload(conn, RawRecord(
            provider="lichess", endpoint_type="user_profile", canonical_source_key="wrong", body=b"x",
            body_hash=digest(b"y"), request_url="https://lichess.org/api/user/wrong",
        ))


def test_decompression_is_bounded_by_recorded_original_size(initialized_conn, tmp_path) -> None:
    conn = initialized_conn
    archive_id = store_archive_object(conn, b"x" * 10000, store=LocalObjectStore(str(tmp_path)))
    with transaction(conn):
        conn.execute("UPDATE archive_objects SET body_bytes=10 WHERE id=%s", (archive_id,))
    with pytest.raises(ValueError, match="size"):
        read_archive_object(conn, archive_id)


def test_import_backup_preserves_bytes_and_separate_observations(initialized_conn, tmp_path) -> None:
    conn = initialized_conn
    store = LocalObjectStore(str(tmp_path))
    pgn = b'[Event "Imported"]\n\n1. e4 *'
    first = store_import_backup(conn, pgn, workspace_id="workspace-a", store=store, source_name="study.pgn", captured_at=1)
    second = store_import_backup(conn, pgn, workspace_id="workspace-b", store=store, source_name="study.pgn", captured_at=2)
    assert first != second
    ids = [row[0] for row in conn.execute("SELECT archive_object_id FROM archive_imports ORDER BY id")]
    assert ids[0] == ids[1]
    assert read_archive_object(conn, ids[0]) == pgn
    assert read_import_backup(conn, first, workspace_id="workspace-a").body == pgn
    assert read_import_backup(conn, second, workspace_id="workspace-b").body == pgn
    with pytest.raises(KeyError, match="not found"):
        read_import_backup(conn, first, workspace_id="workspace-b")
    with pytest.raises(ValueError, match="workspace"):
        store_import_backup(conn, pgn, workspace_id=" ", store=store, source_name="study.pgn")
    with pytest.raises(ValueError, match="workspace"):
        read_import_backup(conn, first, workspace_id="")


class FakeS3Error(Exception):
    def __init__(self, code: str):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.requests: list[dict[str, Any]] = []
        self.failure: str | None = None

    def put_object(self, **kwargs):
        self.requests.append(kwargs)
        if self.failure:
            raise FakeS3Error(self.failure)
        key = (kwargs["Bucket"], kwargs["Key"])
        if key in self.objects:
            raise FakeS3Error("PreconditionFailed")
        self.objects[key] = kwargs["Body"]

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(self.objects[(kwargs["Bucket"], kwargs["Key"])])}


def test_s3_conditional_publication_checksum_and_retry() -> None:
    client = FakeS3()
    store = S3ObjectStore("private-archive", client=client)
    body = gzip.compress(b"remote", mtime=0)
    key = object_key(digest(body))
    store.put(key, body)
    store.put(key, body)
    assert len(client.objects) == 1
    assert client.requests[0]["IfNoneMatch"] == "*"
    assert "ChecksumSHA256" in client.requests[0]
    assert not {"ACL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}.intersection(client.requests[0])
    client.failure = "ConditionalRequestConflict"
    with pytest.raises(FakeS3Error):
        store.put(key, body)


def test_s3_sdk_accepts_conditional_checksum_request() -> None:
    boto3 = pytest.importorskip("boto3")
    stub_module = pytest.importorskip("botocore.stub")
    response_module = pytest.importorskip("botocore.response")
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="offline-testing",
        aws_secret_access_key="offline-testing",
    )
    body = gzip.compress(b"SDK contract", mtime=0)
    key = object_key(digest(body))
    with stub_module.Stubber(client) as stub:
        stub.add_response("put_object", {}, {
            "Bucket": "private-archive", "Key": key, "Body": body, "IfNoneMatch": "*",
            "ContentType": "application/gzip",
            "ChecksumSHA256": base64.b64encode(hashlib.sha256(body).digest()).decode("ascii"),
        })
        stub.add_response("get_object", {
            "Body": response_module.StreamingBody(io.BytesIO(body), len(body)), "ContentLength": len(body),
        }, {"Bucket": "private-archive", "Key": key})
        store = S3ObjectStore("private-archive", client=client)
        store.put(key, body)
        assert store.read(key, expected_size=len(body)) == body
        stub.assert_no_pending_responses()


def test_s3_object_failed_read_and_publication_do_not_publish_reference(initialized_conn, monkeypatch) -> None:
    conn = initialized_conn
    client = FakeS3()
    store = S3ObjectStore("private-archive", client=client)
    monkeypatch.setattr(archives, "store_for_reference", lambda backend, location: store)
    client.failure = "AccessDenied"
    with pytest.raises(FakeS3Error):
        store_raw_payload(conn, record(), store=store)
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 0
    client.failure = None
    raw_id = store_raw_payload(conn, record(), store=store)
    assert read_raw_payload(conn, raw_id).body == record().body
    key = next(iter(client.objects))
    client.objects[key] = b"bad"
    with pytest.raises(ValueError, match="size"):
        read_raw_payload(conn, raw_id)


def test_archive_configuration_is_local_by_default_and_never_includes_credentials(tmp_path, monkeypatch) -> None:
    assert configured_store() is None
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "not-an-archive-setting")
    assert configured_store() == LocalObjectStore(str(tmp_path))
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "other")
    with pytest.raises(ValueError, match="database, local, or s3"):
        configured_store()


@pytest.mark.parametrize("directory", [None, "", "  ", "relative/archive"])
def test_local_archive_configuration_requires_explicit_absolute_directory(tmp_path, monkeypatch, directory) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    if directory is None:
        monkeypatch.delenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", raising=False)
    else:
        monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", directory)
    with pytest.raises(ValueError, match="CHESS_CRAWL_ARCHIVE_DIRECTORY"):
        configured_store()
    assert not (tmp_path / "data").exists()


def test_local_archive_configuration_uses_same_location_after_chdir(tmp_path, monkeypatch) -> None:
    root = tmp_path / "durable-archive"
    elsewhere = tmp_path / "other-process-directory"
    elsewhere.mkdir()
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(root))
    first = configured_store()
    monkeypatch.chdir(elsewhere)
    assert configured_store() == first == LocalObjectStore(str(root))
