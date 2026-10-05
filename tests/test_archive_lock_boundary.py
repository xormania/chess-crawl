"""Object publication must allow unrelated database writers to commit."""
from __future__ import annotations

import pytest
import json

from chess_crawl.ingest import _persist_response
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.archives import (
    prepare_archive_object, read_import_backup, relocate_raw_payloads, store_archive_object, store_import_backup,
)
from chess_crawl.storage.db import Connection, connection, require_row, transaction
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
    raw_id, fetch_id = _persist_response(initialized_conn, sample_record(), job_id=None, crawl_run_id=None)
    assert fetch_id is not None
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


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_relocation_reverifies_registered_object_before_releasing_inline_copy(
    initialized_conn, tmp_path, damage,
):
    conn = initialized_conn
    store = LocalObjectStore(str(tmp_path))
    original = sample_record()
    store_raw_payload(conn, original, store=store)
    inline_record = RawRecord(
        provider=original.provider, endpoint_type=original.endpoint_type, canonical_source_key="inline-backup",
        body=original.body, fetched_at=124, request_url=original.request_url,
    )
    inline_id = store_raw_payload(conn, inline_record)
    path = next(tmp_path.rglob("*.gz"))
    if damage == "missing":
        path.unlink()
        # The valid inline copy can restore a missing immutable object.
        assert relocate_raw_payloads(conn, store=store).moved == 1
        assert path.exists()
    else:
        path.write_bytes(b"corrupted source backup")
        with pytest.raises(ValueError):
            relocate_raw_payloads(conn, store=store)
        row = require_row(conn.execute("SELECT raw_body, archive_object_id FROM raw_payloads WHERE id=%s", (inline_id,)))
        assert row["raw_body"] == original.body
        assert row["archive_object_id"] is None
    assert read_raw_payload(conn, inline_id).body == original.body


def test_relocation_selection_work_is_bounded_after_a_large_completed_prefix(initialized_conn, tmp_path) -> None:
    conn = initialized_conn
    body = b"evidence"
    archive_id = store_archive_object(conn, body, store=LocalObjectStore(str(tmp_path)))
    from chess_crawl.storage.raw import compute_body_hash

    with transaction(conn):
        conn.execute(
            """INSERT INTO raw_payloads(provider, endpoint_type, canonical_source_key, fetched_at, response_status,
                 body_hash, body_compression, raw_body, body_bytes, archive_object_id)
               SELECT 'lichess', 'user_profile', 'completed-' || n, 123, 200, %s, 'gzip', NULL, %s, %s
                 FROM generate_series(1, 10000) AS n""",
            (compute_body_hash(body), len(body), archive_id),
        )
        conn.execute(
            """INSERT INTO raw_payloads(provider, endpoint_type, canonical_source_key, fetched_at, response_status,
                 body_hash, body_compression, raw_body, body_bytes)
               SELECT 'lichess', 'user_profile', 'pending-' || n, 123, 200, %s, 'none', %s, %s
                 FROM generate_series(1, 20) AS n""",
            (compute_body_hash(body), body, len(body)),
        )
    conn.execute("ANALYZE raw_payloads")
    plan = require_row(conn.execute(
        "EXPLAIN (ANALYZE, FORMAT JSON) SELECT id FROM raw_payloads WHERE archive_object_id IS NULL ORDER BY id LIMIT 2",
    ))[0][0]["Plan"]

    def scanned_rows(node):
        here = node["Actual Rows"] + node.get("Rows Removed by Filter", 0) if "Scan" in node["Node Type"] else 0
        return here + sum(scanned_rows(child) for child in node.get("Plans", []))

    assert scanned_rows(plan) <= 2, plan


def test_relocation_default_continuation_avoids_full_count(initialized_conn, tmp_path, monkeypatch) -> None:
    conn = initialized_conn
    for number in range(3):
        store_raw_payload(conn, RawRecord(
            provider="lichess", endpoint_type="user_profile", canonical_source_key=f"batch-{number}",
            body=b"evidence", request_url="https://lichess.org/api/user/alice",
        ))
    commands: list[str] = []
    original = Connection.execute

    def execute(self, query, params=None, **kwargs):
        if self is conn and isinstance(query, str):
            commands.append(query)
        return original(self, query, params, **kwargs)

    monkeypatch.setattr(Connection, "execute", execute)
    store = LocalObjectStore(str(tmp_path))
    first = relocate_raw_payloads(conn, store=store, batch_size=1)
    assert (first.moved, first.has_more, first.remaining) == (1, True, None)
    assert not any("COUNT(*)" in command for command in commands)
    commands.clear()
    counted = relocate_raw_payloads(conn, store=store, batch_size=1, count_remaining=True)
    assert (counted.moved, counted.has_more, counted.remaining) == (1, True, 1)
    assert any("COUNT(*)" in command for command in commands)
    commands.clear()
    last = relocate_raw_payloads(conn, store=store, batch_size=1)
    assert (last.moved, last.has_more, last.remaining) == (1, False, 0)
    assert not any("COUNT(*)" in command for command in commands)


def test_relocation_entrypoint_exposes_continuation_and_optional_count(database_url, tmp_path, monkeypatch, capsys):
    from chess_crawl.storage.archive_migration import main

    with connection(database_url, mode="rw") as conn:
        for number in range(3):
            store_raw_payload(conn, RawRecord(
                provider="lichess", endpoint_type="user_profile", canonical_source_key=f"cli-{number}",
                body=b"evidence", request_url="https://lichess.org/api/user/alice",
            ))
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", database_url)
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", str(tmp_path))
    assert main(["--batch-size", "1"]) == 0
    assert json.loads(capsys.readouterr().out) == {"moved": 1, "has_more": True, "remaining": None}
    assert main(["--batch-size", "1", "--count-remaining"]) == 0
    assert json.loads(capsys.readouterr().out) == {"moved": 1, "has_more": True, "remaining": 1}
    assert main(["--batch-size", "1"]) == 0
    assert json.loads(capsys.readouterr().out) == {"moved": 1, "has_more": False, "remaining": 0}


def test_same_source_registration_serializes_before_read_without_blocking_other_sources(
    initialized_conn, database_url, monkeypatch,
) -> None:
    from concurrent.futures import ThreadPoolExecutor
    import threading
    import time
    from chess_crawl.storage import raw

    first_read = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    second_selected = threading.Event()
    tags: dict[int, str] = {}
    second_pids = []
    original = raw._existing_payload

    def captured_read(conn, record, body_hash):
        existing = original(conn, record, body_hash)
        if conn.in_transaction and record.canonical_source_key == sample_record().canonical_source_key:
            tag = tags[conn.pgconn.backend_pid]
            if tag == "first" and existing is None:
                first_read.set()
                assert release_first.wait(10)
            elif tag == "second":
                second_selected.set()
        return existing

    def register(tag):
        with connection(database_url, mode="rw") as conn:
            tags[conn.pgconn.backend_pid] = tag
            if tag == "second":
                second_pids.append(conn.pgconn.backend_pid)
                second_started.set()
            return store_raw_payload(conn, sample_record())

    monkeypatch.setattr(raw, "_existing_payload", captured_read)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(register, "first")
        try:
            assert first_read.wait(10)
            second = pool.submit(register, "second")
            assert second_started.wait(10)
            with connection(database_url, mode="rw") as other:
                other.execute("SET lock_timeout='1s'")
                independent_id = store_raw_payload(other, RawRecord(
                    provider="lichess", endpoint_type="user_profile", canonical_source_key="independent-source",
                    body=b"independent source", fetched_at=124, request_url="https://lichess.org/api/user/other",
                ))
            deadline = time.monotonic() + 10
            while True:
                assert not second_selected.is_set(), "Duplicate read raced before the first source committed"
                wait_type = require_row(initialized_conn.execute(
                    "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s", (second_pids[0],),
                ))[0]
                if wait_type == "Lock":
                    break
                assert time.monotonic() < deadline, "Second registration did not reach its source lock"
                second_selected.wait(0.01)
            release_first.set()
            assert first.result(timeout=10) == second.result(timeout=10)
            assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 2
            assert independent_id != first.result()
        finally:
            release_first.set()
