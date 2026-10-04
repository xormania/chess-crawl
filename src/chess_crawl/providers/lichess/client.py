"""Lichess public API client."""

from __future__ import annotations

from typing import Any, Mapping

import httpx

from chess_crawl.config import ProviderSettings
from chess_crawl.providers.base import FetchPolicy, RawRecord
from chess_crawl.providers.http import HttpClient, HttpFetchResult
from chess_crawl.providers.lichess import endpoints
from chess_crawl.providers.resources import get_resource, resource_owner_scope, resource_source_key


PROVIDER = "lichess"


class LichessClient:
    def __init__(
        self,
        settings: ProviderSettings,
        *,
        transport: httpx.BaseTransport | None = None,
        sleeper=None,
        clock=None,
        stop_requested=None,
        timeout_s: float = 60.0,
    ) -> None:
        self.settings = settings
        self._policy = FetchPolicy(
            min_delay_s=settings.min_delay_s,
            supports_conditional=False,
            honor_retry_after=False,
            fixed_429_backoff_s=60.0,
            max_retries=settings.max_retries,
        )
        kwargs = {}
        if sleeper is not None:
            kwargs["sleeper"] = sleeper
        if clock is not None:
            kwargs["clock"] = clock
        if stop_requested is not None:
            kwargs["stop_requested"] = stop_requested
        self.http = HttpClient(
            provider=PROVIDER,
            user_agent=settings.user_agent,
            policy=self._policy,
            timeout_s=timeout_s,
            transport=transport,
            **kwargs,
        )

    def key(self) -> str:
        return PROVIDER

    def display_name(self) -> str:
        return "Lichess"

    def user_agent(self) -> str:
        return self.settings.user_agent

    def policy(self) -> FetchPolicy:
        return self._policy

    def get_user_profile(self, username: str) -> RawRecord:
        normalized = _username(username)
        params = {"trophies": "true", "profile": "true", "rank": "true", "fideId": "true"}
        result = self.http.request(
            "GET",
            endpoints.user_profile(username, **params),
            endpoint_type="user_profile",
            # OAuth adds account-relative following/blocking information. This
            # collector captures the broad public profile independently.
            headers={"Accept": "application/json"},
        )
        return _raw_record(
            result,
            endpoint_type="user_profile",
            canonical_source_key=f"lichess/user/{normalized}/profile",
            target_username=normalized,
            request_params=params,
        )

    def get_user_stats(self, username: str) -> RawRecord:
        return self.get_user_profile(username)

    def get_user_games(
        self,
        username: str,
        *,
        since: int | None,
        until: int | None,
        limit: int,
    ) -> RawRecord:
        normalized = _username(username)
        unit = _range_unit_id(since, until, limit)
        source_key = f"lichess/games/user/{normalized}/{unit}"
        params = {
            "since": _seconds_to_ms(since),
            "until": _seconds_to_ms(until),
            "max": limit,
            "pgnInJson": "true",
            "opening": "true",
            **self._evidence_params(),
        }
        request_params = {key: value for key, value in params.items() if value is not None}
        result = self.http.request(
            "GET",
            endpoints.user_games(username, **params),
            endpoint_type="user_games_stream",
            headers=self._headers("application/x-ndjson"),
        )
        return _raw_record(
            result,
            endpoint_type="user_games_stream",
            canonical_source_key=source_key,
            target_username=normalized,
            archive_unit=unit,
            request_params=request_params,
        )

    def get_game(self, game_ref: str) -> RawRecord:
        game_id = game_ref.strip()
        source_key = f"lichess/game/{game_id}"
        params = self._evidence_params()
        result = self.http.request(
            "GET",
            endpoints.game(game_id, **params),
            endpoint_type="game",
            headers=self._headers("application/json"),
        )
        return _raw_record(
            result,
            endpoint_type="game",
            canonical_source_key=source_key,
            target_game_id=game_id,
            request_params=params,
        )

    def close(self) -> None:
        self.http.close()

    def get_user_resource(
        self, username: str, resource_key: str, *, parameters: dict[str, Any] | None = None,
        owner_scope: str = "public", etag: str | None = None, last_modified: str | None = None,
    ) -> RawRecord:
        resource = get_resource(PROVIDER, resource_key)
        values = resource.parameters(parameters)
        scope = resource_owner_scope(resource, owner_scope)
        if resource.authentication == "required" and not self.settings.oauth_token:
            raise ValueError(f"resource {resource_key} requires an OAuth token")
        if resource.access_scope == "workspace" and scope != self.settings.oauth_owner_scope:
            raise ValueError("provider OAuth credentials belong to a different workspace")
        result = self.http.request(
            "GET", resource.url(username, values), endpoint_type="user_resource", headers=self._headers("application/json"),
        )
        return _raw_record(
            result, endpoint_type="user_resource",
            canonical_source_key=resource_source_key(
                PROVIDER, username, resource_key, values, owner_scope=scope, authenticated=bool(self.settings.oauth_token),
            ),
            target_username=_username(username), request_params={
                "resource_key": resource_key, "parameters": values, "owner_scope": scope,
                "authenticated": bool(self.settings.oauth_token),
            },
            owner_scope=scope,
        )

    def _evidence_params(self) -> dict[str, str]:
        return {
            "clocks": str(self.settings.include_clocks).lower(),
            "evals": str(self.settings.include_evals).lower(),
            "accuracy": str(self.settings.include_accuracy).lower(),
        }

    def _headers(self, accept: str) -> Mapping[str, str]:
        headers = {"Accept": accept}
        if self.settings.oauth_token:
            headers["Authorization"] = f"Bearer {self.settings.oauth_token}"
        return headers


def _raw_record(
    result: HttpFetchResult,
    *,
    endpoint_type,
    canonical_source_key: str,
    target_username: str | None = None,
    target_game_id: str | None = None,
    archive_unit: str | None = None,
    request_params: Mapping[str, object] | None = None,
    owner_scope: str = "public",
) -> RawRecord:
    return RawRecord(
        provider=PROVIDER,
        endpoint_type=endpoint_type,
        request_url=result.url,
        canonical_source_key=canonical_source_key,
        request_params=request_params or {},
        http_status=result.status_code,
        fetched_at=result.fetched_at,
        body=result.body,
        media_type=result.content_type or ("application/x-ndjson" if endpoint_type == "user_games_stream" else "application/json"),
        etag=result.etag,
        last_modified=result.last_modified,
        body_hash=result.body_hash,
        target_username=target_username,
        target_game_id=target_game_id,
        archive_unit=archive_unit,
        response_headers=result.headers,
        fetch_attempts=result.attempts,
        owner_scope=owner_scope,
    )


def _username(username: str) -> str:
    return username.strip().lower()


def _seconds_to_ms(value: int | None) -> int | None:
    return None if value is None else value * 1000


def _range_unit_id(since: int | None, until: int | None, limit: int | None = None) -> str:
    def fmt(value: int | None) -> str:
        if value is None:
            return "open"
        if type(value) is not int:
            raise ValueError("Date window bounds must be integer Unix seconds")
        return str(value)

    if limit is not None and (type(limit) is not int or limit <= 0):
        raise ValueError("Game limit must be a positive integer")
    suffix = f"-limit-{limit}" if limit is not None else ""
    # Exact seconds distinguish subday windows and represent the exclusive end
    # of year 9999 without trying to construct an unsupported year-10000 date.
    return f"seconds-{fmt(since)}..{fmt(until)}{suffix}"
