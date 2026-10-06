"""Chess.com public API client."""

from __future__ import annotations

from typing import Any, Mapping

import httpx

from chess_crawl.config import ProviderSettings
from chess_crawl.providers.base import FetchPolicy, RawRecord
from chess_crawl.providers.chesscom import endpoints
from chess_crawl.providers.http import HttpClient, HttpFetchResult
from chess_crawl.providers.resources import get_resource, resource_source_key


PROVIDER = "chess.com"


class ChessComClient:
    def __init__(
        self,
        settings: ProviderSettings,
        *,
        transport: httpx.BaseTransport | None = None,
        sleeper=None,
        clock=None,
        stop_requested=None,
        timeout_s: float = 30.0,
    ) -> None:
        self.settings = settings
        self._policy = FetchPolicy(
            min_delay_s=settings.min_delay_s,
            supports_conditional=True,
            honor_retry_after=True,
            fixed_429_backoff_s=None,
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
        return "Chess.com"

    def user_agent(self) -> str:
        return self.settings.user_agent

    def policy(self) -> FetchPolicy:
        return self.http.policy

    def get_user_profile(
        self,
        username: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> RawRecord:
        normalized = _username(username)
        return self._get(
            "user_profile",
            endpoints.player_profile(username),
            f"chess.com/player/{normalized}/profile",
            target_username=normalized,
            etag=etag,
            last_modified=last_modified,
        )

    def get_user_stats(
        self,
        username: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> RawRecord:
        normalized = _username(username)
        return self._get(
            "user_stats",
            endpoints.player_stats(username),
            f"chess.com/player/{normalized}/stats",
            target_username=normalized,
            etag=etag,
            last_modified=last_modified,
        )

    def get_archives_index(
        self,
        username: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> RawRecord:
        normalized = _username(username)
        return self._get(
            "archives_index",
            endpoints.archives_index(username),
            f"chess.com/player/{normalized}/games/archives",
            target_username=normalized,
            etag=etag,
            last_modified=last_modified,
        )

    def get_monthly_archive(
        self,
        username: str,
        year: int,
        month: int,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> RawRecord:
        normalized = _username(username)
        return self._get(
            "monthly_archive",
            endpoints.monthly_archive(username, year, month),
            f"chess.com/player/{normalized}/games/{year:04d}/{month:02d}",
            target_username=normalized,
            archive_unit=f"{year:04d}/{month:02d}",
            etag=etag,
            last_modified=last_modified,
        )

    def get_game(self, game_ref: str) -> RawRecord:
        raise NotImplementedError("Chess.com has no single-game-by-id endpoint; fetch the owning monthly archive")

    def get_user_resource(
        self, username: str, resource_key: str, *, parameters: dict[str, Any] | None = None,
        owner_scope: str = "public", etag: str | None = None, last_modified: str | None = None,
    ) -> RawRecord:
        resource = get_resource(PROVIDER, resource_key)
        values = resource.parameters(parameters)
        result = self.http.request(
            "GET", resource.url(username, values), endpoint_type="user_resource",
            headers=_conditional_headers(etag, last_modified),
        )
        return RawRecord(
            provider=PROVIDER, endpoint_type="user_resource", request_url=result.url,
            canonical_source_key=resource_source_key(PROVIDER, username, resource_key, values, owner_scope=owner_scope),
            request_params={"resource_key": resource_key, "parameters": values, "owner_scope": "public", "authenticated": False},
            http_status=result.status_code, fetched_at=result.fetched_at, body=result.body,
            media_type=result.content_type or "application/json", etag=result.etag, last_modified=result.last_modified,
            body_hash=result.body_hash, target_username=_username(username), response_headers=result.headers,
            fetch_attempts=result.attempts,
        )

    def close(self) -> None:
        self.http.close()

    def _get(
        self,
        endpoint_type,
        url: str,
        canonical_source_key: str,
        *,
        target_username: str | None = None,
        archive_unit: str | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> RawRecord:
        headers = _conditional_headers(etag, last_modified)
        result = self.http.request("GET", url, endpoint_type=endpoint_type, headers=headers)
        return _raw_record(
            result,
            endpoint_type=endpoint_type,
            canonical_source_key=canonical_source_key,
            target_username=target_username,
            archive_unit=archive_unit,
        )


def _raw_record(
    result: HttpFetchResult,
    *,
    endpoint_type,
    canonical_source_key: str,
    target_username: str | None = None,
    archive_unit: str | None = None,
) -> RawRecord:
    return RawRecord(
        provider=PROVIDER,
        endpoint_type=endpoint_type,
        request_url=result.url,
        canonical_source_key=canonical_source_key,
        http_status=result.status_code,
        fetched_at=result.fetched_at,
        body=result.body,
        media_type=result.content_type or "application/json",
        etag=result.etag,
        last_modified=result.last_modified,
        body_hash=result.body_hash,
        target_username=target_username,
        archive_unit=archive_unit,
        response_headers=result.headers,
        fetch_attempts=result.attempts,
    )


def _conditional_headers(etag: str | None, last_modified: str | None) -> Mapping[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    return headers


def _username(username: str) -> str:
    return username.strip().lower()
