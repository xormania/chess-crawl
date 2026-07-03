"""Chess.com public API client."""

from __future__ import annotations

from typing import Mapping

import httpx

from chess_crawl.config import ProviderSettings
from chess_crawl.providers.base import ArchiveUnit, FetchPolicy, GameFilters, RawRecord
from chess_crawl.providers.chesscom import endpoints
from chess_crawl.providers.chesscom.parser import parse_archives_index
from chess_crawl.providers.http import HttpClient, HttpFetchResult


PROVIDER = "chess.com"


class ChessComClient:
    def __init__(
        self,
        settings: ProviderSettings,
        *,
        transport: httpx.BaseTransport | None = None,
        sleeper=None,
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
        return self._policy

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

    def list_archive_units(
        self,
        username: str,
        since: int | None,
        until: int | None,
    ) -> list[ArchiveUnit]:
        del since, until
        record = self.get_archives_index(username)
        if record.body is None:
            return []
        units: list[ArchiveUnit] = []
        for url in parse_archives_index(record.body):
            year, month = _archive_url_year_month(str(url))
            units.append(
                ArchiveUnit(
                    provider=PROVIDER,
                    username=_username(username),
                    unit_id=f"{year:04d}/{month:02d}",
                    url=str(url),
                    since=None,
                    until=None,
                    immutable=False,
                )
            )
        return units

    def iter_user_games(
        self,
        username: str,
        since: int | None,
        until: int | None,
        filters: GameFilters,
    ):
        del filters
        for unit in self.list_archive_units(username, since, until):
            year, month = (int(part) for part in unit.unit_id.split("/", 1))
            yield self.get_monthly_archive(username, year, month)

    def get_game(self, game_ref: str) -> RawRecord:
        raise NotImplementedError("Chess.com has no single-game-by-id endpoint; fetch the owning monthly archive")

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


def _archive_url_year_month(url: str) -> tuple[int, int]:
    parts = url.rstrip("/").split("/")
    return int(parts[-2]), int(parts[-1])
