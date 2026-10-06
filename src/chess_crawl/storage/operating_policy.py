"""Trusted versioned provider policy; acquisition replicas read it per request."""
from __future__ import annotations

import time
from dataclasses import replace
from typing import Any

from chess_crawl.config import ProviderSettings
from chess_crawl.providers.registry import get_provider_info
from chess_crawl.storage.db import Connection, atomic, operation_lock, require_row


class PolicyVersionConflict(ValueError):
    """The operator's observed policy version was superseded."""


def provider_policy(conn: Connection, provider: str) -> dict[str, Any] | None:
    get_provider_info(provider)
    row = conn.execute("SELECT * FROM provider_operating_policies WHERE provider=%s", (provider,)).fetchone()
    return dict(row) if row is not None else None


def effective_provider_settings(conn: Connection, provider: str, fallback: ProviderSettings) -> ProviderSettings:
    if fallback.key != provider:
        raise ValueError("Provider fallback settings do not match the selected provider")
    policy = provider_policy(conn, provider)
    return fallback if policy is None else replace(fallback, min_delay_s=policy["min_delay_s"], max_retries=policy["max_retries"])


@atomic
def set_provider_policy(
    conn: Connection, settings: ProviderSettings, *, expected_version: int, now: int | None = None,
) -> dict[str, Any]:
    get_provider_info(settings.key)
    if type(expected_version) is not int or not 0 <= expected_version < 2**63 - 1:
        raise ValueError("Expected policy version must be a nonnegative PostgreSQL bigint")
    if settings.max_retries >= 2**63:
        raise ValueError("Provider max_retries must fit a PostgreSQL bigint")
    operation_lock(conn, "provider-operating-policy", settings.key)
    current = provider_policy(conn, settings.key)
    version = int(current["version"]) if current is not None else 0
    if version != expected_version:
        raise PolicyVersionConflict("Provider policy changed; inspect its current version before updating")
    timestamp = int(time.time()) if now is None else now
    new_version = version + 1
    values = (settings.key, new_version, settings.min_delay_s, settings.max_retries, timestamp)
    conn.execute(
        """INSERT INTO provider_operating_policies(provider,version,min_delay_s,max_retries,updated_at)
           VALUES(%s,%s,%s,%s,%s) ON CONFLICT(provider) DO UPDATE SET
           version=EXCLUDED.version,min_delay_s=EXCLUDED.min_delay_s,max_retries=EXCLUDED.max_retries,
           updated_at=EXCLUDED.updated_at""", values,
    )
    from chess_crawl.jobs.state import defer_provider
    defer_provider(conn, settings.key, not_before=timestamp + settings.min_delay_s,
                   reason="provider request pacing", now=timestamp)
    conn.execute(
        """INSERT INTO provider_operating_policy_history(provider,version,min_delay_s,max_retries,updated_at)
           VALUES(%s,%s,%s,%s,%s)""", values,
    )
    return dict(require_row(conn.execute("SELECT * FROM provider_operating_policies WHERE provider=%s", (settings.key,))))


def provider_policy_history(conn: Connection, provider: str, *, after_version: int = 0, limit: int = 100) -> list[dict[str, Any]]:
    get_provider_info(provider)
    if type(after_version) is not int or not 0 <= after_version < 2**63:
        raise ValueError("History cursor must be a nonnegative PostgreSQL bigint")
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("History page limit must be between 1 and 1000")
    return [dict(row) for row in conn.execute(
        """SELECT * FROM provider_operating_policy_history WHERE provider=%s AND version>%s
           ORDER BY version LIMIT %s""", (provider, after_version, limit),
    )]
