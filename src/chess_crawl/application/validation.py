"""Validate requests before opening storage or scheduling provider work."""

from __future__ import annotations

import re
from dataclasses import replace

from chess_crawl.application.errors import ValidationError
from chess_crawl.application.models import CrawlRequest, ImportRequest, Limits
from chess_crawl.providers.registry import known_keys


_USERNAME = re.compile(r"[A-Za-z0-9_-]{1,100}\Z")
_MAX_TIMESTAMP = 253402300800  # Exclusive end of datetime's supported year 9999.


def validate_provider(provider: str) -> str:
    if not isinstance(provider, str) or provider.strip().lower() not in known_keys():
        raise ValidationError("Unknown provider", code="invalid_provider")
    return provider.strip().lower()


def validate_username(username: str) -> str:
    if not isinstance(username, str) or _USERNAME.fullmatch(username.strip()) is None:
        raise ValidationError(
            "Username must contain 1 to 100 letters, digits, underscores, or hyphens",
            code="invalid_username",
        )
    return username.strip().lower()


def validate_idempotency_key(key: str) -> str:
    if not isinstance(key, str) or not 1 <= len(key) <= 128 or any(not 33 <= ord(char) <= 126 for char in key):
        raise ValidationError(
            "Idempotency key must contain 1 to 128 visible ASCII characters without spaces",
            code="invalid_idempotency_key",
        )
    return key


def validate_player_refresh(provider: str, username: str, *, statistics: bool) -> tuple[str,str]:
    provider,username = validate_provider(provider),validate_username(username)
    if statistics and provider!="chess.com":
        raise ValidationError("Separate statistics refresh is supported only for chess.com",code="unsupported_resource")
    return provider,username


def validate_game_collection(provider: str, game_id: str) -> tuple[str,str]:
    provider = validate_provider(provider)
    if provider!="lichess":
        raise ValidationError("Direct game collection is supported only for lichess",code="unsupported_resource")
    if not isinstance(game_id,str) or not re.fullmatch(r"[A-Za-z0-9]{8}",game_id):
        raise ValidationError("A public Lichess game ID must have eight alphanumeric characters",code="invalid_game_id")
    return provider,game_id


def validate_import(request: ImportRequest, *, limits: Limits = Limits()) -> ImportRequest:
    provider, username = _validate_common(request, limits)
    if not isinstance(request.collection_mode,str) or request.collection_mode not in {"bounded", "full", "incremental", "backfill"}:
        raise ValidationError("Unknown collection mode", code="invalid_collection_mode")
    validate_integer(request.batch_size, "batch_size", minimum=1, maximum=12)
    if limits.max_jobs < 2:
        raise ValidationError("An import requires a profile job and a games job", code="invalid_max_jobs")
    return replace(request, provider=provider, username=username)


def validate_crawl(request: CrawlRequest, *, limits: Limits = Limits()) -> CrawlRequest:
    provider, username = _validate_common(request, limits)
    validate_integer(request.max_depth, "max_depth", minimum=0, maximum=limits.max_depth)
    validate_integer(request.max_users, "max_users", minimum=1, maximum=limits.max_users)
    validate_integer(request.max_jobs, "max_jobs", minimum=1, maximum=limits.max_jobs)
    return replace(request, provider=provider, username=username)


def _validate_common(request: ImportRequest | CrawlRequest, limits: Limits) -> tuple[str, str]:
    provider = validate_provider(request.provider)
    username = validate_username(request.username)
    if isinstance(request, ImportRequest) and (request.since is None or request.until is None):
        raise ValidationError(
            "HTTP and CLI imports require since and until in every collection mode",
            code="invalid_date_window",
        )
    if request.since is not None:
        validate_integer(request.since, "since", minimum=0, maximum=_MAX_TIMESTAMP - 1)
    if request.until is not None:
        validate_integer(request.until, "until", minimum=1, maximum=_MAX_TIMESTAMP)
    if request.since is not None and request.until is not None:
        if request.since >= request.until:
            raise ValidationError("since must be earlier than the exclusive until timestamp",code="invalid_date_window")
        if request.until-request.since > limits.max_date_span_days*86400:
            raise ValidationError("Date window exceeds the configured maximum span",code="invalid_date_window")
    validate_integer(request.max_games, "max_games", minimum=1, maximum=limits.max_games)
    return provider, username


def validate_integer(value: int, name: str, *, minimum: int, maximum: int | None = None) -> int:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        bounds = f"{minimum} to {maximum}" if maximum is not None else f">= {minimum}"
        raise ValidationError(f"{name} must be an integer {bounds}", code=f"invalid_{name}")
    return value


def validate_page(*, after: int | None, limit: int | None, limits: Limits) -> tuple[int, int]:
    cursor = 0 if after is None else validate_integer(after, "cursor", minimum=0, maximum=2**63 - 1)
    size = min(50, limits.page_size) if limit is None else validate_integer(
        limit, "limit", minimum=1, maximum=limits.page_size,
    )
    return cursor, size
