"""Typed archive limits use the shared source and valid cloud/local settings."""
from dataclasses import replace

import pytest

from chess_crawl.application.archive_jobs import ArchiveJobSettings
from chess_crawl.storage.artifacts import CHUNK_BYTES, validate_artifact_manifest
from chess_crawl.storage.working_sets import digest


def test_s3_empty_artifact_bucket_falls_back_before_enabled_validation(monkeypatch):
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_JOBS_ENABLED", "true")
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_BACKEND", "s3")
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_S3_BUCKET", "")
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_S3_BUCKET", "private-archive-test")
    assert ArchiveJobSettings.from_env().artifact_s3_bucket == "private-archive-test"
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_S3_BUCKET", "separate-artifact-test")
    assert ArchiveJobSettings.from_env().artifact_s3_bucket == "separate-artifact-test"


@pytest.mark.parametrize("changes", [
    {"artifact_directory": "relative"}, {"async_export_max_rows": 0},
    {"async_export_max_bytes": True}, {"async_export_prepare_seconds": 3601},
    {"artifact_ttl_seconds": 31536001}, {"artifact_max_count": 10001},
    {"artifact_max_bytes": 1}, {"artifact_backend": "database"},
])
def test_invalid_limits_are_rejected(changes):
    with pytest.raises(ValueError):
        replace(ArchiveJobSettings(), **changes)


def test_artifact_toml_and_env_share_source(monkeypatch, tmp_path):
    config = tmp_path / "settings.toml"
    config.write_text('[chess_crawl]\narchive_jobs_enabled=true\nartifact_directory="/tmp/test-artifacts"\nasync_export_max_rows=12\n', encoding="utf-8")
    monkeypatch.setenv("CHESS_CRAWL_CONFIG_FILE", str(config))
    monkeypatch.setenv("CHESS_CRAWL_ASYNC_EXPORT_MAX_ROWS", "14")
    settings = ArchiveJobSettings.from_env()
    assert settings.archive_jobs_enabled and settings.async_export_max_rows == 14
    assert settings.artifact_directory == "/tmp/test-artifacts"


def test_artifact_only_s3_configuration_requires_its_sdk(monkeypatch, capsys):
    from chess_crawl import configuration
    monkeypatch.setenv("CHESS_CRAWL_ARCHIVE_BACKEND", "database")
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_BACKEND", "s3")
    monkeypatch.setenv("CHESS_CRAWL_ARTIFACT_S3_BUCKET", "private-artifact-test")
    monkeypatch.setattr(configuration.importlib.util, "find_spec", lambda name: None)
    assert configuration.main(["validate"]) == 2
    assert "chess-crawl[s3] extra" in capsys.readouterr().err


def test_signed_manifest_cannot_authorize_unbounded_or_malformed_download():
    manifest = {"contract_version": 1, "renderer_version": "archive-export-v1", "kind": "users",
                "rows": 0, "body_bytes": 0, "chunk_count": 0, "selection_time": "processing",
                "content_hash": "sha256:" + "0" * 64, "filename": "users.jsonl", "media_type": "application/x-ndjson"}
    record = {"manifest": {**manifest, "artifact_signature": digest(manifest)}, "reserved_bytes": 1}
    assert validate_artifact_manifest(record)["chunk_count"] == 0
    for changes in ({"body_bytes": 4 * 1024**3 + 1}, {"chunk_count": 4097}, {"filename": "../source"},
                    {"content_hash": "bad"}, {"body_bytes": CHUNK_BYTES, "chunk_count": 0}):
        invalid = {**manifest, **changes}
        with pytest.raises(ValueError):
            validate_artifact_manifest({"manifest": {**invalid, "artifact_signature": digest(invalid)}, "reserved_bytes": 4 * 1024**3})
