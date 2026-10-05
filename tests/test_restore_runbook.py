"""Execute the documented archive drill against preserved and damaged copies."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.archives import store_archive_object, store_import_backup
from chess_crawl.storage.db import Connection
from chess_crawl.storage.object_store import LocalObjectStore
from chess_crawl.storage.raw import store_raw_payload


RUNBOOK = Path(__file__).resolve().parents[1] / "docs/postgresql-operations.md"


def verifier() -> str:
    return RUNBOOK.read_text().split("cat > backups/verify-archive.py <<'PY'\n", 1)[1].split("\nPY\n", 1)[0]


@pytest.mark.parametrize("damage", [None, "missing", "corrupt"])
def test_runbook_verifies_recovered_raw_and_import_objects_without_live_copy_fallback(
    initialized_conn: Connection, database_url: str, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], damage: str | None,
) -> None:
    location = tmp_path / "archive"
    store = LocalObjectStore(str(location))
    store_raw_payload(initialized_conn, RawRecord(
        provider="lichess", endpoint_type="user_profile", canonical_source_key="lichess:user:alice",
        body=b'{"username":"alice"}', fetched_at=123, request_url="https://lichess.org/api/user/alice",
    ), store=store)
    store_import_backup(initialized_conn, b"1. e4 e5 *", workspace_id="analysis", source_name="game.pgn", store=store)
    # Unreferenced immutable objects do not belong to the required recovery set.
    store_archive_object(initialized_conn, b"unreferenced retained evidence", store=store)
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", database_url)
    code = compile(verifier(), str(RUNBOOK), "exec")
    exec(code, {})
    original = capsys.readouterr().out
    manifest = [json.loads(line) for line in original.splitlines()]
    assert len(manifest) == 2
    assert {row["body_bytes"] for row in manifest} == {len(b'{"username":"alice"}'), len(b"1. e4 e5 *")}

    backup = tmp_path / "backup"
    shutil.copytree(location, backup)
    live = tmp_path / "live-original"
    location.rename(live)
    shutil.copytree(backup, location)
    # The same recorded path now resolves only the restored copy, as the
    # runbook's isolated volume does inside its one-off verification container.
    target = location / manifest[-1]["object_key"]
    retained_bytes = target.read_bytes()
    if damage == "missing":
        target.unlink()
    elif damage == "corrupt":
        target.write_bytes(bytes(byte ^ 1 for byte in retained_bytes))
    if damage is None:
        exec(code, {})
        assert capsys.readouterr().out == original
    else:
        with pytest.raises(SystemExit, match=f"Archive object {manifest[-1]['id']} failed verification"):
            exec(code, {})
    assert (live / manifest[-1]["object_key"]).read_bytes() == retained_bytes
    assert (backup / manifest[-1]["object_key"]).read_bytes() == retained_bytes
