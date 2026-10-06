"""Shared configuration sources and parsing; value objects own their defaults.

TOML uses one ``[chess_crawl]`` table with lowercase environment-name suffixes.
Environment values override the file, including explicitly empty strings.
"""
from __future__ import annotations

import math
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, fields
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar, overload
from urllib.parse import urlsplit


# The public file contract lists names, not a second set of runtime defaults.
# Domain value objects remain responsible for validation and default values.
SETTING_KEYS = frozenset("""
contact user_agent lichess_token lichess_token_owner_scope lichess_clocks lichess_evals lichess_accuracy
chesscom_delay_s lichess_delay_s provider_max_retries
poll_interval heartbeat_interval heartbeat_max_age job_max_retries retry_base retry_max worker_identity_file
archive_backend archive_directory archive_s3_bucket
database_max_connections database_admission_timeout_s database_url database_password database_password_file database_transport database_trusted_host database_ssl_root_cert_file
application_database_user application_database_password
api_token api_token_file api_workspace_tokens_file api_auth_mode healthcheck_token healthcheck_token_file
max_games max_depth max_users max_jobs page_size max_date_span_days max_working_set_members max_analysis_results max_analysis_result_bytes
job_max_games job_max_normalization_units job_max_remote_bytes job_max_remote_requests max_response_bytes
workspace_max_games workspace_max_normalization_units workspace_max_remote_bytes workspace_max_remote_requests workspace_max_active_jobs workspace_max_queued_jobs
export_max_rows export_max_bytes export_prepare_seconds export_download_seconds export_workspace_slots
export_outstanding_spools export_outstanding_bytes export_workspace_outstanding_spools export_workspace_outstanding_bytes
sqs_queue_url sqs_acquisition_queue_url sqs_processing_queue_url
dispatch_retention_seconds dispatch_cleanup_interval_seconds dispatch_cleanup_batch_size
mercure_url mercure_publisher_jwt mercure_publisher_jwt_file mercure_topic_prefix
mercure_timeout_s mercure_retry_base_s mercure_retry_max_s
archive_jobs_enabled artifact_backend artifact_directory artifact_s3_bucket
async_max_working_set_members async_export_max_rows async_export_max_bytes async_export_prepare_seconds
artifact_max_count artifact_max_bytes artifact_ttl_seconds
usage_log events_enabled events_retention_seconds events_cleanup_batch_size
""".split())
_T = TypeVar("_T")


@lru_cache(maxsize=8)
def _read_file(path: str, identity: tuple[int, int, int]) -> Mapping[str, str]:
    try:
        with Path(path).open("rb") as stream:
            data = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError):
        raise ValueError("Could not read CHESS_CRAWL_CONFIG_FILE as valid TOML") from None
    if set(data) != {"chess_crawl"} or not isinstance(data["chess_crawl"], dict):
        raise ValueError("CHESS_CRAWL_CONFIG_FILE must contain only a [chess_crawl] table")
    values: dict[str, str] = {}
    for key, value in data["chess_crawl"].items():
        if key not in SETTING_KEYS:
            # Do not echo arbitrary untrusted keys or values, which may be secrets.
            raise ValueError("CHESS_CRAWL_CONFIG_FILE contains an unknown setting")
        if type(value) not in {str, int, float, bool}:
            raise ValueError(f"CHESS_CRAWL_{key.upper()} must be a scalar configuration value")
        values[f"CHESS_CRAWL_{key.upper()}"] = str(value).lower() if type(value) is bool else str(value)
    return MappingProxyType(values)


@dataclass(frozen=True)
class SettingsSource:
    """Immutable snapshot; loading never mutates the process environment."""

    file_values: Mapping[str, str]
    environment: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "file_values", MappingProxyType(dict(self.file_values)))
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))

    @classmethod
    def from_env(cls) -> SettingsSource:
        environment = dict(os.environ)
        configured = environment.get("CHESS_CRAWL_CONFIG_FILE")
        file_values: Mapping[str, str] = {}
        if configured:
            try:
                path = Path(configured).expanduser().resolve()
                stat = path.stat()
            except OSError:
                raise ValueError("Could not read CHESS_CRAWL_CONFIG_FILE") from None
            file_values = _read_file(str(path), (stat.st_mtime_ns, stat.st_size, stat.st_ino))
        return cls(file_values, MappingProxyType(environment))

    @overload
    def get(self, name: str, default: str) -> str: ...
    @overload
    def get(self, name: str, default: None = None) -> str | None: ...
    def get(self, name: str, default: str | None = None) -> str | None:
        return self.environment.get(name, self.file_values.get(name, default))

    def origin(self, name: str) -> str:
        return "environment" if name in self.environment else "file" if name in self.file_values else "default"


@overload
def setting(name: str, default: str) -> str: ...
@overload
def setting(name: str, default: None = None) -> str | None: ...
def setting(name: str, default: str | None = None) -> str | None:
    return SettingsSource.from_env().get(name, default)


def boolean(value: str, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true/false, 1/0, yes/no, or on/off")


def integer(value: str, name: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer") from None


def number(value: str, name: str) -> float:
    try:
        result = float(value)
    except ValueError:
        raise ValueError(f"{name} must be a finite number") from None
    if not math.isfinite(result):
        raise ValueError(f"{name} must be a finite number")
    return result


def dataclass_settings(
    cls: type[_T], *, source: SettingsSource | None = None,
    prefix: str = "CHESS_CRAWL_", aliases: Mapping[str, str] | None = None, defaults: _T | None = None,
) -> _T:
    """Reuse each value object's field defaults and constructor validation."""
    source = source or SettingsSource.from_env()
    defaults = defaults if defaults is not None else cls()
    values: dict[str, Any] = {field.name: getattr(defaults, field.name) for field in fields(defaults)}  # type: ignore[arg-type]
    for field in fields(defaults):  # type: ignore[arg-type]
        name = prefix + (aliases or {}).get(field.name, field.name.upper())
        raw = source.get(name)
        if raw is None:
            continue
        default = getattr(defaults, field.name)
        if type(default) is bool:
            values[field.name] = boolean(raw, name)
        elif type(default) is int:
            values[field.name] = integer(raw, name)
        elif type(default) is float:
            values[field.name] = number(raw, name)
        else:
            values[field.name] = raw
    return cls(**values)


def redacted(name: str, value: Any) -> Any:
    """Credentials and connection strings never appear in inspection output."""
    key = name.lower()
    secret = key == "database_url" or key.endswith(("token", "password", "publisher_jwt"))
    if value and (key.endswith("_url") or key == "mercure_topic_prefix"):
        try:
            parsed = urlsplit(str(value))
            secret = secret or bool(parsed.username is not None or parsed.password is not None
                                    or parsed.query or parsed.fragment)
        except ValueError:
            secret = True
    return "<redacted>" if secret and value is not None and value != "" else value
