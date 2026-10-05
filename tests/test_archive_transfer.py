"""Operational transfer preserves evidence and identities across partial failures."""
from __future__ import annotations

import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from chess_crawl.providers.base import RawRecord
from chess_crawl.storage import archive_transfer, archives
from chess_crawl.storage.archive_transfer import main, transfer_archive_objects
from chess_crawl.storage.archives import read_import_backup, store_archive_object, store_import_backup
from chess_crawl.storage.db import connection, require_row, transaction
from chess_crawl.storage.object_store import LocalObjectStore, S3ObjectStore, digest
from chess_crawl.storage.raw import read_raw_payload, store_raw_payload
from chess_crawl.storage.repository import upsert_provider_user


class ConditionalFailure(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class OfflineS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts = 0
        self.reads = 0
        self.on_put: Any = None
        self.corrupt_read = False
        self.lock = threading.Lock()

    def put_object(self, **kwargs):
        self.puts += 1
        if self.on_put is not None:
            self.on_put()
        with self.lock:
            key = kwargs["Bucket"], kwargs["Key"]
            if key in self.objects:
                raise ConditionalFailure
            self.objects[key] = kwargs["Body"]

    def get_object(self, **kwargs):
        self.reads += 1
        encoded = self.objects[kwargs["Bucket"], kwargs["Key"]]
        if self.corrupt_read:
            encoded = bytes([encoded[0] ^ 1]) + encoded[1:]
        return {"Body": io.BytesIO(encoded)}


@pytest.fixture
def target(monkeypatch):
    client = OfflineS3()
    store = S3ObjectStore("offline-transfer-target", client=client)
    original = archive_transfer.store_for_reference

    def resolve(backend, location):
        return store if (backend, location) == (store.backend, store.location) else original(backend, location)

    monkeypatch.setattr(archive_transfer, "store_for_reference", resolve)
    monkeypatch.setattr(archives, "store_for_reference", resolve)
    return store, client


def record(key: str, body: bytes = b"shared evidence") -> RawRecord:
    return RawRecord(provider="lichess", endpoint_type="user_profile", canonical_source_key=key, body=body, request_url="https://lichess.org/api/user/offline")


def source_id(conn, raw_id: int) -> int:
    return int(require_row(conn.execute("SELECT archive_object_id FROM raw_payloads WHERE id=%s", (raw_id,)))[0])


def test_transfer_keeps_exact_bytes_ids_scope_and_old_metadata(initialized_conn, tmp_path, target) -> None:
    conn = initialized_conn
    source = LocalObjectStore(str(tmp_path))
    raw_ids = [store_raw_payload(conn, record(key), store=source) for key in ("first", "second")]
    imports = [store_import_backup(conn, b"shared evidence", workspace_id=scope, source_name="original.pgn",
                                  captured_at=17, store=source) for scope in ("one", "two")]
    old_id = source_id(conn, raw_ids[0])
    before = dict(require_row(conn.execute("SELECT * FROM archive_objects WHERE id=%s", (old_id,))))
    original_bytes = (tmp_path / before["object_key"]).read_bytes()
    store, client = target
    result = transfer_archive_objects(conn, store=store, batch_size=1)
    assert (result.objects_moved, result.raw_payloads_moved, result.imports_moved, result.has_more) == (1, 2, 2, False)
    assert result.next_after_object_id == old_id
    assert dict(require_row(conn.execute("SELECT * FROM archive_objects WHERE id=%s", (old_id,)))) == before
    assert (tmp_path / before["object_key"]).read_bytes() == original_bytes
    assert next(iter(client.objects.values())) == original_bytes
    assert source_id(conn, raw_ids[0]) != old_id
    assert [read_raw_payload(conn, value).body for value in raw_ids] == [b"shared evidence"] * 2
    for value, workspace in zip(imports, ("one", "two")):
        backup = read_import_backup(conn, value, workspace_id=workspace)
        assert backup.id == value and backup.captured_at == 17 and backup.source_name == "original.pgn"
        assert backup.body == b"shared evidence"
    with pytest.raises(KeyError):
        read_import_backup(conn, imports[0], workspace_id="two")
    calls = client.puts, client.reads
    assert transfer_archive_objects(conn, store=store).objects_moved == 0
    assert (client.puts, client.reads) == calls


@pytest.mark.parametrize("damage", ["missing", "encoded_checksum", "body_checksum", "body_size"])
def test_bad_source_never_publishes_or_repoints(initialized_conn, tmp_path, target, damage) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, record("source"), store=LocalObjectStore(str(tmp_path)))
    old_id = source_id(conn, raw_id)
    row = require_row(conn.execute("SELECT * FROM archive_objects WHERE id=%s", (old_id,)))
    path = tmp_path / row["object_key"]
    if damage == "missing":
        path.unlink()
    elif damage == "encoded_checksum":
        body = path.read_bytes()
        path.write_bytes(bytes([body[0] ^ 1]) + body[1:])
    elif damage == "body_checksum":
        conn.execute("UPDATE archive_objects SET body_hash=%s WHERE id=%s", (digest(b"different"), old_id))
    else:
        conn.execute("UPDATE archive_objects SET body_bytes=1 WHERE id=%s", (old_id,))
    store, client = target
    with pytest.raises((FileNotFoundError, ValueError)):
        transfer_archive_objects(conn, store=store)
    assert source_id(conn, raw_id) == old_id and client.puts == 0
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1


@pytest.mark.parametrize("failure", ["publication", "readback"])
def test_bad_target_keeps_source_readable(initialized_conn, tmp_path, target, failure) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, record("source"), store=LocalObjectStore(str(tmp_path)))
    old_id = source_id(conn, raw_id)
    store, client = target
    if failure == "publication":
        def fail():
            raise OSError("private operator diagnostic")
        client.on_put = fail
    else:
        client.corrupt_read = True
    with pytest.raises((OSError, ValueError)):
        transfer_archive_objects(conn, store=store)
    assert source_id(conn, raw_id) == old_id
    assert read_raw_payload(conn, raw_id).body == b"shared evidence"
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1
    client.on_put = None
    client.corrupt_read = False
    assert transfer_archive_objects(conn, store=store).objects_moved == 1


def test_partial_batches_resume_after_failure_without_exact_counts(initialized_conn, tmp_path, target) -> None:
    conn = initialized_conn
    source = LocalObjectStore(str(tmp_path))
    ids = [store_raw_payload(conn, record(str(value), str(value).encode()), store=source) for value in range(3)]
    store, client = target

    def fail_second():
        if client.puts == 2:
            raise OSError("interrupted")

    client.on_put = fail_second
    with pytest.raises(OSError):
        transfer_archive_objects(conn, store=store, batch_size=3)
    assert require_row(conn.execute(
        "SELECT COUNT(*) FROM raw_payloads r JOIN archive_objects a ON a.id=r.archive_object_id WHERE a.backend='s3'",
    ))[0] == 1
    client.on_put = None
    first = transfer_archive_objects(conn, store=store, batch_size=1)
    assert first.objects_moved == 1 and first.has_more
    last = transfer_archive_objects(conn, store=store, batch_size=1, after_object_id=first.next_after_object_id)
    assert last.objects_moved == 1 and not last.has_more
    assert [read_raw_payload(conn, value).body for value in ids] == [b"0", b"1", b"2"]
    assert transfer_archive_objects(conn, store=store).objects_moved == 0


def test_failed_import_cutover_rolls_back_raw_and_target_registration(initialized_conn, tmp_path, target) -> None:
    conn = initialized_conn
    source = LocalObjectStore(str(tmp_path))
    raw_id = store_raw_payload(conn, record("source"), store=source)
    store_import_backup(conn, b"shared evidence", workspace_id="one", source_name="original", store=source)
    old_id = source_id(conn, raw_id)
    conn.execute("""CREATE FUNCTION reject_import_transfer() RETURNS trigger LANGUAGE plpgsql AS $$
                   BEGIN RAISE EXCEPTION 'disposable transfer failure'; END $$""")
    conn.execute("CREATE TRIGGER reject_import_transfer BEFORE UPDATE ON archive_imports "
                 "FOR EACH ROW EXECUTE FUNCTION reject_import_transfer()")
    store, client = target
    with pytest.raises(Exception, match="disposable transfer failure"):
        transfer_archive_objects(conn, store=store)
    assert source_id(conn, raw_id) == old_id
    assert require_row(conn.execute("SELECT archive_object_id FROM archive_imports"))[0] == old_id
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1
    assert len(client.objects) == 1
    conn.execute("DROP TRIGGER reject_import_transfer ON archive_imports")
    assert transfer_archive_objects(conn, store=store).objects_moved == 1


def test_refs_created_during_upload_join_cutover_without_blocking_writes(database_url, tmp_path, target) -> None:
    source = LocalObjectStore(str(tmp_path))
    store, client = target
    entered, release = threading.Event(), threading.Event()

    def block():
        entered.set()
        if not release.wait(15):
            raise TimeoutError("test upload was not released")

    with connection(database_url, mode="rw") as conn:
        raw_id = store_raw_payload(conn, record("original"), store=source)
    client.on_put = block

    def transfer():
        with connection(database_url, mode="rw") as conn:
            return transfer_archive_objects(conn, store=store)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(transfer)
        try:
            assert entered.wait(5)
            with connection(database_url, mode="rw") as conn:
                upsert_provider_user(conn, provider="lichess", username="independent")
                another = store_raw_payload(conn, record("new-reference"), store=source)
                imported = store_import_backup(conn, b"shared evidence", workspace_id="new", source_name="new", store=source)
            assert not future.done()
        finally:
            release.set()
        result = future.result(timeout=15)
    assert (result.raw_payloads_moved, result.imports_moved) == (2, 1)
    with connection(database_url) as conn:
        assert source_id(conn, raw_id) == source_id(conn, another)
        assert read_import_backup(conn, imported, workspace_id="new").body == b"shared evidence"


def test_concurrent_reference_repoint_is_not_overwritten(database_url, tmp_path, target) -> None:
    source = LocalObjectStore(str(tmp_path / "source"))
    alternate = LocalObjectStore(str(tmp_path / "alternate"))
    store, client = target
    entered, release = threading.Event(), threading.Event()
    with connection(database_url, mode="rw") as conn:
        raw_id = store_raw_payload(conn, record("original"), store=source)
        other_id = store_archive_object(conn, b"shared evidence", store=alternate)
        store_import_backup(conn, b"shared evidence", workspace_id="old", source_name="old", store=source)

    def block():
        entered.set()
        if not release.wait(15):
            raise TimeoutError("test upload was not released")

    client.on_put = block

    def transfer():
        with connection(database_url, mode="rw") as conn:
            return transfer_archive_objects(conn, store=store)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(transfer)
        try:
            assert entered.wait(5)
            with connection(database_url, mode="rw") as conn:
                with transaction(conn):
                    conn.execute("UPDATE raw_payloads SET archive_object_id=%s WHERE id=%s", (other_id, raw_id))
        finally:
            release.set()
        result = future.result(timeout=15)
    assert (result.raw_payloads_moved, result.imports_moved) == (0, 1)
    with connection(database_url) as conn:
        assert source_id(conn, raw_id) == other_id
        assert read_raw_payload(conn, raw_id).body == b"shared evidence"


def test_two_transfers_recheck_after_source_lock(database_url, tmp_path, target) -> None:
    store, client = target
    with connection(database_url, mode="rw") as conn:
        store_raw_payload(conn, record("source"), store=LocalObjectStore(str(tmp_path)))
    barrier = threading.Barrier(2)
    client.on_put = lambda: barrier.wait(timeout=10)

    def transfer():
        with connection(database_url, mode="rw") as conn:
            return transfer_archive_objects(conn, store=store)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = executor.submit(transfer), executor.submit(transfer)
        results = [first.result(timeout=15), second.result(timeout=15)]
    assert sorted(result.objects_moved for result in results) == [0, 1]
    assert len(client.objects) == 1


def test_transfer_rejects_outer_transaction_and_bad_limits(initialized_conn, target) -> None:
    conn = initialized_conn
    store, client = target
    with transaction(conn):
        with pytest.raises(ValueError, match="own per-object"):
            transfer_archive_objects(conn, store=store)
    for value in (0, 10001, True):
        with pytest.raises(ValueError, match="batch"):
            transfer_archive_objects(conn, store=store, batch_size=value)
    for value in (-1, 2**63, True):
        with pytest.raises(ValueError, match="cursor"):
            transfer_archive_objects(conn, store=store, after_object_id=value)
    assert client.puts == client.reads == 0


def test_operational_module_reports_progress_and_sanitizes_failures(database_url, tmp_path, target, monkeypatch, capsys) -> None:
    store, client = target
    with connection(database_url, mode="rw") as conn:
        store_raw_payload(conn, record("source"), store=LocalObjectStore(str(tmp_path)))
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", database_url)
    monkeypatch.setattr(archive_transfer, "configured_store", lambda: store)

    def fail():
        raise OSError("secret-private-password")

    client.on_put = fail
    assert main([]) == 1
    output = capsys.readouterr()
    assert "secret-private-password" not in output.out + output.err
    client.on_put = None
    assert main(["--batch-size", "1", "--after-object-id", "0"]) == 0
    assert json.loads(capsys.readouterr().out)["objects_moved"] == 1


def test_metadata_changed_during_upload_is_rechecked(initialized_conn, tmp_path, target) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, record("source"), store=LocalObjectStore(str(tmp_path)))
    old_id = source_id(conn, raw_id)
    store, client = target

    def change_source_metadata():
        conn.execute("UPDATE archive_objects SET body_hash=%s WHERE id=%s", (digest(b"changed"), old_id))

    client.on_put = change_source_metadata
    with pytest.raises(ValueError, match="metadata changed"):
        transfer_archive_objects(conn, store=store)
    assert source_id(conn, raw_id) == old_id
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1
    conn.execute("UPDATE archive_objects SET body_hash=%s WHERE id=%s", (digest(b"shared evidence"), old_id))
    assert read_raw_payload(conn, raw_id).body == b"shared evidence"


def test_inconsistent_raw_reference_aborts_entire_cutover(initialized_conn, tmp_path, target) -> None:
    conn = initialized_conn
    source = LocalObjectStore(str(tmp_path))
    raw_id = store_raw_payload(conn, record("source"), store=source)
    imported = store_import_backup(conn, b"shared evidence", workspace_id="one", source_name="pgn", store=source)
    old_id = source_id(conn, raw_id)
    conn.execute("UPDATE raw_payloads SET body_hash=%s WHERE id=%s", (digest(b"changed"), raw_id))
    with pytest.raises(ValueError, match="conflicts"):
        transfer_archive_objects(conn, store=target[0])
    assert source_id(conn, raw_id) == old_id
    assert require_row(conn.execute("SELECT archive_object_id FROM archive_imports WHERE id=%s", (imported,)))[0] == old_id
    assert require_row(conn.execute("SELECT COUNT(*) FROM archive_objects"))[0] == 1


def test_transfer_to_another_local_archive_is_supported(initialized_conn, tmp_path) -> None:
    conn = initialized_conn
    source = LocalObjectStore(str(tmp_path / "old"))
    raw_id = store_raw_payload(conn, record("source"), store=source)
    destination = LocalObjectStore(str(tmp_path / "new"))
    assert transfer_archive_objects(conn, store=destination).objects_moved == 1
    assert read_raw_payload(conn, raw_id).body == b"shared evidence"
    assert list((tmp_path / "old").rglob("*.gz")) and list((tmp_path / "new").rglob("*.gz"))
