"""Object publication must allow unrelated database writers to commit."""
from __future__ import annotations

import pytest

from chess_crawl.ingest import _persist_response
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.archives import (
    prepare_archive_object, read_import_backup, relocate_raw_payloads, store_archive_object, store_import_backup,
)
from chess_crawl.storage.db import connection, require_row, transaction
from chess_crawl.storage.object_store import LocalObjectStore
from chess_crawl.storage.raw import prepare_raw_payload, read_raw_payload, store_raw_payload


@pytest.mark.parametrize("operation", ["raw", "object", "import", "relocation", "ingest"])
@pytest.mark.parametrize("phase", ["put", "read"])
def test_archive_io_allows_unrelated_writes(
    initialized_conn, database_url, tmp_path, monkeypatch, operation, phase,
) -> None:
    conn = initialized_conn
    store = LocalObjectStore(str(tmp_path))
    body = b'{"username":"alice"}'
    record = RawRecord(
        provider="lichess", endpoint_type="user_profile", canonical_source_key="archive-lock-test",
        body=body, fetched_at=123, http_status=200, request_url="https://lichess.org/api/user/alice",
    )
    if operation == "relocation":
        store_raw_payload(conn, record)
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(tmp_path))
    original = getattr(LocalObjectStore, phase)
    writes = 0

    def probe(self, *args, **kwargs):
        nonlocal writes
        # Simulate a slow adapter waiting on an independent API/worker write.
        # A short real PG timeout detects lock retention without a timed sleep.
        with connection(database_url, mode="rw") as other:
            other.execute("SET lock_timeout = '100ms'")
            store_raw_payload(other, RawRecord(
                provider="lichess", endpoint_type="user_profile", canonical_source_key=f"other-{writes}",
                body=b"independent writer", fetched_at=124, request_url="https://lichess.org/api/user/other",
            ), store=None)
        writes += 1
        return original(self, *args, **kwargs)

    # The other writer must use inline storage even though ingestion is configured
    # for objects; patch its configuration only while performing that write.
    def inline_probe(self, *args, **kwargs):
        with monkeypatch.context() as patch:
            patch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "database")
            return probe(self, *args, **kwargs)

    monkeypatch.setattr(LocalObjectStore, phase, inline_probe)
    if operation == "raw":
        store_raw_payload(conn, record, store=store)
    elif operation == "object":
        store_archive_object(conn, body, store=store)
    elif operation == "import":
        store_import_backup(conn, body, workspace_id="w", source_name="game.pgn", store=store)
    elif operation == "relocation":
        assert relocate_raw_payloads(conn, store=store).moved == 1
    else:
        _persist_response(conn, record, job_id=None, crawl_run_id=None)
    assert writes > 0
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1


def sample_record() -> RawRecord:
    return RawRecord(
        provider="lichess", endpoint_type="user_profile", canonical_source_key="concurrent-payload",
        body=b'{"username":"alice"}', fetched_at=123, http_status=200,
        request_url="https://lichess.org/api/user/alice",
    )


@pytest.mark.parametrize("operation", ["raw", "object", "import"])
def test_unprepared_object_io_inside_transaction_is_rejected(initialized_conn, tmp_path, monkeypatch, operation):
    store = LocalObjectStore(str(tmp_path))

    def unexpected_io(*args, **kwargs):
        pytest.fail("Object I/O must never start inside the transaction")

    monkeypatch.setattr(LocalObjectStore, "put", unexpected_io)
    with transaction(initialized_conn):
        with pytest.raises(ValueError, match="prepared object"):
            if operation == "raw":
                store_raw_payload(initialized_conn, sample_record(), store=store)
            elif operation == "object":
                store_archive_object(initialized_conn, b"evidence", store=store)
            else:
                store_import_backup(initialized_conn, b"evidence", store=store, workspace_id="w", source_name="g.pgn")
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 0


def test_prepared_import_commits_with_caller_transaction(initialized_conn, tmp_path, monkeypatch):
    store = LocalObjectStore(str(tmp_path))
    published = prepare_archive_object(b"evidence", store=store)

    def unexpected_io(*args, **kwargs):
        pytest.fail("Prepared imports must only write database metadata")

    with monkeypatch.context() as patch:
        patch.setattr(LocalObjectStore, "put", unexpected_io)
        patch.setattr(LocalObjectStore, "read", unexpected_io)
        with transaction(initialized_conn):
            import_id = store_import_backup(
                initialized_conn, b"evidence", store=store, workspace_id="w", source_name="g.pgn", prepared_object=published,
            )
    assert read_import_backup(initialized_conn, import_id, workspace_id="w").body == b"evidence"


def test_concurrent_same_payload_is_rechecked_after_publication(initialized_conn, database_url, tmp_path, monkeypatch):
    original = LocalObjectStore.put
    other_id = None

    def put(self, key, body):
        nonlocal other_id
        original(self, key, body)
        with connection(database_url, mode="rw") as other:
            other_id = store_raw_payload(other, sample_record())

    monkeypatch.setattr(LocalObjectStore, "put", put)
    raw_id = store_raw_payload(initialized_conn, sample_record(), store=LocalObjectStore(str(tmp_path)))
    assert raw_id == other_id
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 0


def test_relocation_rechecks_another_workers_completed_reference(initialized_conn, database_url, tmp_path, monkeypatch):
    raw_id = store_raw_payload(initialized_conn, sample_record())
    store = LocalObjectStore(str(tmp_path))
    original = LocalObjectStore.put
    competing = False

    def put(self, key, body):
        nonlocal competing
        original(self, key, body)
        if not competing:
            competing = True
            with connection(database_url, mode="rw") as other:
                assert relocate_raw_payloads(other, store=store).moved == 1

    monkeypatch.setattr(LocalObjectStore, "put", put)
    result = relocate_raw_payloads(initialized_conn, store=store)
    assert (result.moved, result.remaining) == (0, 0)
    assert read_raw_payload(initialized_conn, raw_id).body == sample_record().body
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1


def test_response_and_fetch_evidence_still_roll_back_together(initialized_conn, tmp_path, monkeypatch):
    from chess_crawl import ingest

    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(tmp_path))

    def failed_log(*args, **kwargs):
        raise RuntimeError("fetch evidence failure")

    with monkeypatch.context() as patch:
        patch.setattr(ingest, "_log_attempts", failed_log)
        with pytest.raises(RuntimeError, match="fetch evidence"):
            _persist_response(initialized_conn, sample_record(), job_id=None, crawl_run_id=None)
    for table in ("archive_objects", "raw_payloads", "fetch_logs"):
        assert require_row(initialized_conn.execute(f"SELECT COUNT(*) FROM {table}"))[0] == 0
    assert len(list(tmp_path.rglob("*.gz"))) == 1
    raw_id = _persist_response(initialized_conn, sample_record(), job_id=None, crawl_run_id=None)
    assert read_raw_payload(initialized_conn, raw_id).body == sample_record().body
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 1
    assert len(list(tmp_path.rglob("*.gz"))) == 1


def test_prepared_object_cannot_be_attached_to_different_body(initialized_conn, tmp_path):
    published = prepare_raw_payload(initialized_conn, sample_record(), store=LocalObjectStore(str(tmp_path)))
    different = RawRecord(
        provider="lichess", endpoint_type="user_profile", canonical_source_key="different",
        body=b"other bytes", request_url="https://lichess.org/api/user/other",
    )
    with pytest.raises(ValueError, match="does not match"):
        store_raw_payload(initialized_conn, different, prepared_object=published)
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 0
