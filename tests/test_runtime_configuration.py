"""Configuration parity, explicit overrides, and safe offline inspection."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from chess_crawl import operations
from chess_crawl.application.export_limits import ExportLimits
from chess_crawl.application.models import Limits
from chess_crawl.config import Config
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.settings import SettingsSource, setting
from chess_crawl.storage.object_store import ArchiveSettings


def configure_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str) -> Path:
    path = tmp_path / "runtime.toml"
    path.write_text("[chess_crawl]\n" + body, encoding="utf-8")
    monkeypatch.setenv("CHESS_CRAWL_CONFIG_FILE", str(path))
    return path


def test_file_and_environment_share_existing_typed_settings(tmp_path, monkeypatch) -> None:
    configure_file(tmp_path, monkeypatch, '''
contact = "file@example.test"
chesscom_delay_s = 2.5
lichess_delay_s = 3.5
provider_max_retries = 7
lichess_evals = false
poll_interval = 2.0
heartbeat_interval = 8.0
retry_base = 50.0
max_games = 90
job_max_games = 200
export_max_rows = 300
archive_backend = "local"
archive_directory = "/var/lib/archive"
''')
    monkeypatch.setenv("CHESS_CRAWL_CONTACT", "environment@example.test")
    monkeypatch.setenv("CHESS_CRAWL_MAX_GAMES", "100")
    config = Config.from_env()
    assert config.contact == "environment@example.test"
    assert config.chesscom_delay_s == 2.5 and config.lichess_delay_s == 3.5
    assert config.provider("lichess").max_retries == 7
    assert config.provider("lichess").include_evals is False
    assert WorkerSettings.from_env() == WorkerSettings(poll_interval=2, heartbeat_interval=8,
                                                     heartbeat_max_age=32, job_retry_base_s=50)
    assert Limits.from_env().max_games == 100
    assert BudgetPolicy.from_env().job_max_games == 200
    assert ExportLimits.from_env().max_rows == 300
    assert ArchiveSettings.from_env().directory == "/var/lib/archive"
    assert SettingsSource.from_env().origin("CHESS_CRAWL_CONTACT") == "environment"
    assert SettingsSource.from_env().origin("CHESS_CRAWL_JOB_MAX_GAMES") == "file"


def test_explicit_empty_environment_value_overrides_file_without_mutating_environment(tmp_path, monkeypatch) -> None:
    configure_file(tmp_path, monkeypatch, 'lichess_token = "file-secret"\n')
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_TOKEN", "")
    before = dict(os.environ)
    assert Config.from_env().lichess_token == ""
    assert dict(os.environ) == before


@pytest.mark.parametrize("body", [
    'unknown = "private-secret"\n',
    'contact = ["private-secret"]\n',
    'max_games = "private-secret"\n',
    'provider_max_retries = "private-secret"\n',
    'lichess_delay_s = "private-secret"\n',
    'lichess_delay_s = nan\n',
    'retry_base = -1\n',
    'heartbeat_interval = 10\nheartbeat_max_age = 11\n',
    'archive_backend = "local"\narchive_directory = "relative"\n',
    'archive_backend = "s3"\narchive_s3_bucket = "private-secret!"\n',
])
def test_validation_rejects_bad_settings_without_echoing_values(tmp_path, monkeypatch, capsys, body) -> None:
    configure_file(tmp_path, monkeypatch, body)
    assert operations.main(["config", "validate"]) == 2
    captured = capsys.readouterr()
    assert "private-secret" not in captured.out + captured.err
    assert captured.err and not captured.out


def test_configuration_show_includes_effective_defaults_origins_and_redacts_secrets(tmp_path, monkeypatch, capsys) -> None:
    configure_file(tmp_path, monkeypatch, '''
lichess_token = "provider-secret"
api_token = "api-secret"
mercure_publisher_jwt = "publisher-secret"
database_url = "postgresql://postgres:database-secret@localhost/chess_crawl"
database_transport = "local"
poll_interval = 3.0
''')
    assert operations.main(["config", "show", "--role", "worker"]) == 0
    captured = capsys.readouterr()
    for secret in ("provider-secret", "api-secret", "publisher-secret", "database-secret"):
        assert secret not in captured.out + captured.err
    output = json.loads(captured.out)
    assert output["settings"]["lichess_token"] == "<redacted>"
    assert output["settings"]["database_url"] == "<redacted>"
    assert output["settings"]["poll_interval"] == 3.0
    assert output["sources"]["poll_interval"] == "file"
    assert output["sources"]["max_games"] == "default"
    assert output["settings"]["max_games"] == Limits().max_games
    assert "provider-secret" not in repr(Config.from_env())


@pytest.mark.parametrize("origin", ["file", "environment"])
def test_file_backed_mercure_credential_reports_its_configured_source(origin, tmp_path, monkeypatch, capsys) -> None:
    secret = tmp_path / "publisher-jwt"
    secret.write_text("private-publisher-secret", encoding="utf-8")
    configure_file(tmp_path, monkeypatch, '''
database_url = "postgresql://postgres@localhost/chess_crawl"
mercure_url = "https://hub.example/.well-known/mercure"
mercure_topic_prefix = "https://archive.example"
''' + (f'mercure_publisher_jwt_file = "{secret}"\n' if origin == "file" else ""))
    if origin == "environment":
        monkeypatch.setenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT_FILE", str(secret))
    assert operations.main(["config", "show", "--role", "events"]) == 0
    output = capsys.readouterr().out
    assert "private-publisher-secret" not in output
    result = json.loads(output)
    assert result["sources"]["mercure_publisher_jwt"] == origin
    assert result["settings"]["mercure_publisher_jwt"] == "<redacted>"


def test_role_validation_checks_database_policy_without_connecting(monkeypatch, capsys) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", "postgresql://postgres@external.example/chess_crawl")
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    assert operations.main(["config", "validate", "--role", "worker"]) == 2
    assert "verified transport" in capsys.readouterr().err
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "verified")
    assert operations.main(["config", "validate", "--role", "worker"]) == 0
    assert json.loads(capsys.readouterr().out)["valid"] is True


def test_stage_queue_configuration_is_checked_before_sdk_or_database_access(monkeypatch, capsys) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", "postgresql://postgres@localhost/chess_crawl")
    monkeypatch.setenv("CHESS_CRAWL_SQS_QUEUE_URL", "https://sqs.example/general")
    assert operations.main(["config", "validate", "--role", "processing"]) == 2
    assert "stage queue" in capsys.readouterr().err


@pytest.mark.parametrize("role", ["acquisition", "processing"])
def test_independent_stage_role_accepts_only_its_own_queue(role, monkeypatch, capsys) -> None:
    from chess_crawl import configuration
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", "postgresql://postgres@localhost/chess_crawl")
    monkeypatch.setenv(f"CHESS_CRAWL_SQS_{role.upper()}_QUEUE_URL", f"https://sqs.example/{role}")
    monkeypatch.setattr(configuration.importlib.util, "find_spec", lambda name: object())
    assert operations.main(["config", "validate", "--role", role]) == 0
    assert json.loads(capsys.readouterr().out)["valid"] is True


def test_settings_source_snapshot_is_stable_and_file_changes_are_seen_on_reload(tmp_path, monkeypatch) -> None:
    path = configure_file(tmp_path, monkeypatch, 'contact = "first@example.test"\n')
    snapshot = SettingsSource.from_env()
    path.write_text('[chess_crawl]\ncontact = "second@example.test"\n', encoding="utf-8")
    assert snapshot.get("CHESS_CRAWL_CONTACT") == "first@example.test"
    assert setting("CHESS_CRAWL_CONTACT") == "second@example.test"


def test_worker_cli_explicit_overrides_win_over_file_and_environment(tmp_path, monkeypatch) -> None:
    from chess_crawl.jobs import worker
    configure_file(tmp_path, monkeypatch, 'poll_interval = 4.0\nretry_base = 7.0\n')
    monkeypatch.setenv("CHESS_CRAWL_POLL_INTERVAL", "3.0")
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", "postgresql://postgres@localhost/chess_crawl")
    captured = {}

    class FakeWorker:
        def __init__(self, target, **kwargs):
            captured.update(kwargs)

        def run(self, **kwargs):
            return 0

        def request_stop(self):
            pass

    monkeypatch.setattr(worker, "Worker", FakeWorker)
    assert worker.main(["--once", "--poll-interval", "2.0"]) == 0
    assert captured["settings"].poll_interval == 2.0
    assert captured["settings"].job_retry_base_s == 7.0


def test_explicit_worker_setting_overrides_are_applied_before_validation(monkeypatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_POLL_INTERVAL", "invalid-secret")
    assert WorkerSettings.from_env(overrides={"poll_interval": 2.0}).poll_interval == 2.0


def test_api_file_credentials_are_validated_without_connecting(tmp_path, monkeypatch, capsys) -> None:
    configure_file(tmp_path, monkeypatch, 'database_url = "postgresql://postgres@localhost/chess_crawl"\napi_token = "private-secret"\n')
    assert operations.main(["config", "validate", "--role", "api"]) == 0
    assert "private-secret" not in capsys.readouterr().out
    monkeypatch.setenv("CHESS_CRAWL_API_TOKEN_FILE", str(tmp_path / "absent"))
    assert operations.main(["config", "validate", "--role", "api"]) == 2
    assert "private-secret" not in capsys.readouterr().err


def test_database_api_configuration_is_validated_without_app_or_database_access(tmp_path, monkeypatch, capsys) -> None:
    from chess_crawl.api import app
    from chess_crawl.storage.db import Connection
    configure_file(tmp_path, monkeypatch, '''
database_url = "postgresql://postgres@external.example/chess_crawl"
api_auth_mode = "database"
healthcheck_token = "private-health-secret"
''')

    def forbidden(*args, **kwargs):
        raise AssertionError("Configuration validation must not construct an app or connect to PostgreSQL")

    monkeypatch.setattr(app, "create_app", forbidden)
    monkeypatch.setattr(Connection, "connect", forbidden)
    assert operations.main(["config", "show", "--role", "api"]) == 0
    captured = capsys.readouterr()
    assert "private-health-secret" not in captured.out + captured.err
    output = json.loads(captured.out)
    assert output["settings"]["api_auth_mode"] == "database"
    assert output["settings"]["healthcheck_token"] == "<redacted>"
    assert output["sources"]["api_auth_mode"] == "file"


@pytest.mark.parametrize("static_setting", ["API_TOKEN", "API_TOKEN_FILE", "API_WORKSPACE_TOKENS_FILE"])
def test_database_api_validation_rejects_static_credentials(static_setting, monkeypatch, capsys) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", "postgresql://postgres@localhost/chess_crawl")
    monkeypatch.setenv("CHESS_CRAWL_API_AUTH_MODE", "database")
    monkeypatch.setenv("CHESS_CRAWL_" + static_setting, "private-secret")
    assert operations.main(["config", "validate", "--role", "api"]) == 2
    captured = capsys.readouterr()
    assert "static API credentials" in captured.err
    assert "private-secret" not in captured.out + captured.err


def test_api_validation_reports_missing_optional_framework(monkeypatch, capsys) -> None:
    from chess_crawl import configuration
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_URL", "postgresql://postgres@localhost/chess_crawl")
    monkeypatch.setenv("CHESS_CRAWL_API_AUTH_MODE", "database")
    monkeypatch.setattr(configuration.importlib.util, "find_spec", lambda name: None)
    assert operations.main(["config", "validate", "--role", "api"]) == 2
    assert "chess-crawl[api] extra" in capsys.readouterr().err


def test_authentication_adapter_import_does_not_load_optional_http_framework() -> None:
    project = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-c", '''
import sys
from chess_crawl.api.auth import configured_authenticator
auth = configured_authenticator("postgresql://postgres@localhost/chess_crawl", None, None, "database")
assert auth.credentials is None
assert not any(name.split(".")[0] in {"fastapi", "starlette", "uvicorn"} for name in sys.modules)
'''],
        cwd=project, env={**os.environ, "PYTHONPATH": str(project / "src")},
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr


def test_configuration_snapshot_mappings_cannot_be_mutated() -> None:
    source = SettingsSource({}, {"CHESS_CRAWL_CONTACT": "first@example.test"})
    with pytest.raises(TypeError):
        source.environment["CHESS_CRAWL_CONTACT"] = "second@example.test"  # type: ignore[index]


@pytest.mark.parametrize("enabled", ["yes", "on", "TRUE", "1"])
def test_usage_logging_uses_the_same_boolean_contract(enabled, monkeypatch, capsys) -> None:
    from chess_crawl.costs import UsageSample, emit_sample
    monkeypatch.setenv("CHESS_CRAWL_USAGE_LOG", enabled)
    emit_sample(UsageSample("acquisition", 0.0, 0.0))
    assert json.loads(capsys.readouterr().out)["stage"] == "acquisition"


def test_cloud_bootstrap_reads_file_configuration_without_exposing_password(tmp_path, monkeypatch, capsys) -> None:
    from contextlib import nullcontext
    from chess_crawl.storage import cloud_bootstrap
    configure_file(tmp_path, monkeypatch, 'database_url = "postgresql://postgres@localhost/chess_crawl"\napplication_database_user = "chess_crawl"\napplication_database_password = "bootstrap-secret"\n')
    supplied = {}
    monkeypatch.setattr(cloud_bootstrap, "connection", lambda *args, **kwargs: nullcontext(object()))
    monkeypatch.setattr(cloud_bootstrap, "transaction", lambda conn: nullcontext(conn))
    monkeypatch.setattr(cloud_bootstrap, "initialize", lambda conn: None)
    monkeypatch.setattr(cloud_bootstrap, "bootstrap_runtime_role", lambda conn, **kwargs: supplied.update(kwargs))
    assert cloud_bootstrap.main() == 0
    assert supplied == {"username": "chess_crawl", "password": "bootstrap-secret"}
    captured = capsys.readouterr()
    assert "bootstrap-secret" not in captured.out + captured.err


@pytest.mark.parametrize("name", ["CHESS_CRAWL_MERCURE_URL", "CHESS_CRAWL_SQS_QUEUE_URL", "CHESS_CRAWL_MERCURE_TOPIC_PREFIX"])
@pytest.mark.parametrize("value", ["https://user:private-secret@example.test/path", "https://example.test/path?token=private-secret", "https://example.test/path#private-secret"])
def test_settings_inspection_redacts_url_credentials_and_parameters(name, value, monkeypatch, capsys) -> None:
    monkeypatch.setenv(name, value)
    assert operations.main(["config", "show"]) == 0
    captured = capsys.readouterr()
    assert "private-secret" not in captured.out + captured.err
    assert json.loads(captured.out)["settings"][name.removeprefix("CHESS_CRAWL_").lower()] == "<redacted>"
