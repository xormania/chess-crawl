"""Inspect and validate deployment settings without contacting services."""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import fields
from typing import Any

from chess_crawl.application.export_limits import ExportLimits
from chess_crawl.application.archive_jobs import ArchiveJobSettings
from chess_crawl.application.models import Limits
from chess_crawl.config import Config
from chess_crawl.events.settings import EventSettings
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.jobs.dispatch import DispatchMaintenance
from chess_crawl.jobs.settings import WorkerSettings
from chess_crawl.settings import SETTING_KEYS, SettingsSource, boolean, redacted
from chess_crawl.storage.db import DatabaseError, validate_database_settings
from chess_crawl.storage.object_store import ArchiveSettings


ROLES = ("settings", "admin", "api", "worker", "acquisition", "processing", "dispatcher", "events")


def inspect_configuration(role: str = "settings") -> dict[str, Any]:
    if role not in ROLES:
        raise ValueError("Unknown configuration role")
    source = SettingsSource.from_env()
    values: dict[str, Any] = {}

    def include(owner: Any, *, prefix: str = "", aliases: Mapping[str, str] | None = None) -> None:
        for field in fields(owner):
            key = prefix + (aliases or {}).get(field.name, field.name)
            values[key] = getattr(owner, field.name)

    include(Config.from_env(source=source), aliases={"max_retries": "provider_max_retries"})
    include(WorkerSettings.from_env(source=source), aliases={
        "job_retry_base_s": "retry_base", "job_retry_max_s": "retry_max",
    })
    include(Limits.from_env())
    include(BudgetPolicy.from_env())
    include(ExportLimits.from_env(), prefix="export_")
    events = EventSettings.from_env()
    include(events, prefix="events_")
    archive_jobs = ArchiveJobSettings.from_env()
    include(archive_jobs)
    archive = ArchiveSettings.from_env()
    include(archive, prefix="archive_")
    maintenance = DispatchMaintenance.from_env()
    values.update(dispatch_retention_seconds=maintenance.retention_seconds,
                  dispatch_cleanup_interval_seconds=maintenance.interval_seconds,
                  dispatch_cleanup_batch_size=maintenance.batch_size,
                  usage_log=boolean(source.get("CHESS_CRAWL_USAGE_LOG", "false"), "CHESS_CRAWL_USAGE_LOG"))
    # Include configured deployment settings without maintaining duplicate defaults.
    for key in SETTING_KEYS:
        raw = source.get("CHESS_CRAWL_" + key.upper())
        if raw is not None and key not in values:
            values[key] = raw
    database = None
    if role != "settings" or source.get("CHESS_CRAWL_DATABASE_URL") is not None:
        database = validate_database_settings()
    if role == "api":
        if importlib.util.find_spec("fastapi") is None:
            raise ValueError("API configuration validation requires the chess-crawl[api] extra")
        from chess_crawl.api.auth import configured_authenticator
        if database is None:
            raise ValueError("The HTTP API requires PostgreSQL connection settings")
        configured_authenticator(database, None, None, None)
    if role == "events":
        if not events.enabled:
            raise ValueError("Event delivery is disabled by CHESS_CRAWL_EVENTS_ENABLED")
        from chess_crawl.events.mercure import MercureSettings
        mercure = MercureSettings.from_env()
        include(mercure, prefix="mercure_", aliases={"hub_url": "url"})
    if role == "dispatcher" and not source.get("CHESS_CRAWL_SQS_QUEUE_URL"):
        raise ValueError("Set CHESS_CRAWL_SQS_QUEUE_URL for the dispatcher")
    acquisition = source.get("CHESS_CRAWL_SQS_ACQUISITION_QUEUE_URL")
    processing = source.get("CHESS_CRAWL_SQS_PROCESSING_QUEUE_URL")
    if role not in {"acquisition", "processing"} and bool(acquisition) != bool(processing):
        raise ValueError("Configure both acquisition and processing queues for stage routing")
    if role in {"acquisition", "processing"} and source.get("CHESS_CRAWL_SQS_QUEUE_URL"):
        if not source.get(f"CHESS_CRAWL_SQS_{role.upper()}_QUEUE_URL"):
            raise ValueError("Stage-specific SQS workers require the corresponding stage queue URL")
    uses_sqs = role in {"worker", "acquisition", "processing", "dispatcher"} and any(
        source.get(name) for name in (
            "CHESS_CRAWL_SQS_QUEUE_URL", "CHESS_CRAWL_SQS_ACQUISITION_QUEUE_URL", "CHESS_CRAWL_SQS_PROCESSING_QUEUE_URL",
        )
    )
    if (archive.backend == "s3" or archive_jobs.artifact_backend == "s3" or uses_sqs) and importlib.util.find_spec("boto3") is None:
        raise ValueError("S3 or SQS configuration requires the chess-crawl[s3] extra")
    origins = {key: source.origin("CHESS_CRAWL_" + key.upper()) for key in sorted(values)}
    jwt_file = "CHESS_CRAWL_MERCURE_PUBLISHER_JWT_FILE"
    if role == "events" and source.get(jwt_file):
        origins["mercure_publisher_jwt"] = source.origin(jwt_file)
    return {
        "version": 1, "role": role, "valid": True,
        "settings": {key: redacted(key, values[key]) for key in sorted(values)},
        "sources": origins,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate or inspect effective deployment settings without network access")
    parser.add_argument("action", choices=("validate", "show"))
    parser.add_argument("--role", choices=ROLES, default="settings")
    args = parser.parse_args(argv)
    try:
        output = inspect_configuration(args.role)
        if args.action == "validate":
            output = {"version": output["version"], "role": args.role, "valid": True}
        print(json.dumps(output, sort_keys=True))
        return 0
    except (ValueError, DatabaseError, OSError, UnicodeError) as exc:
        message = str(exc) if isinstance(exc, ValueError) else "Could not read a configured secret file"
        print(message, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
