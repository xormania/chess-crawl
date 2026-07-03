"""Shared synchronous HTTP helper for provider clients."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

import httpx

from chess_crawl.providers.base import EndpointType, FetchAttempt, FetchPolicy
from chess_crawl.storage.raw import compute_body_hash


SENSITIVE_REQUEST_HEADERS = {"authorization", "cookie", "x-api-key"}


@dataclass(frozen=True)
class HttpFetchResult:
    status_code: int
    url: str
    headers: Mapping[str, str]
    content_type: str | None
    body: bytes | None
    body_hash: str | None
    fetched_at: int
    attempts: tuple[FetchAttempt, ...]

    @property
    def etag(self) -> str | None:
        return self.headers.get("etag")

    @property
    def last_modified(self) -> str | None:
        return self.headers.get("last-modified")


class HttpClient:
    """Small serial HTTP wrapper around httpx.

    The helper sends provider headers as requested, but only exposes sanitized
    request metadata so bearer tokens cannot leak through this path.
    """

    def __init__(
        self,
        *,
        provider: str,
        user_agent: str,
        policy: FetchPolicy,
        timeout_s: float = 30.0,
        transport: httpx.BaseTransport | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.provider = provider
        self.user_agent = user_agent
        self.policy = policy
        self.timeout_s = timeout_s
        self._sleeper = sleeper
        self._clock = clock
        self._last_request_at: float | None = None
        self._client = httpx.Client(
            timeout=timeout_s,
            transport=transport,
            follow_redirects=False,
            headers={"User-Agent": user_agent},
        )

    def close(self) -> None:
        self._client.close()

    def request(
        self,
        method: str,
        url: str,
        *,
        endpoint_type: EndpointType,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        content: bytes | str | None = None,
    ) -> HttpFetchResult:
        attempts: list[FetchAttempt] = []
        max_attempts = self.policy.max_retries + 1
        request_headers = self._outbound_request_headers(headers)
        recorded_request_headers = self._request_headers(request_headers)
        method_upper = method.upper()

        for attempt_number in range(1, max_attempts + 1):
            self._respect_serial_delay()
            attempted_at = int(self._clock())
            started = self._clock()

            try:
                response = self._client.request(
                    method_upper,
                    url,
                    headers=request_headers,
                    params=params,
                    content=content,
                )
                duration_ms = int(max(0.0, self._clock() - started) * 1000)
                body = response.content if response.status_code == 200 else None
                response_headers = _cache_relevant_headers(response.headers)
                retry_after = _parse_retry_after(response.headers.get("retry-after"))
                final_url = str(response.url)
                attempts.append(
                    FetchAttempt(
                        provider=self.provider,
                        endpoint_type=endpoint_type,
                        url=final_url,
                        method=method_upper,
                        status_code=response.status_code,
                        attempted_at=attempted_at,
                        attempt=attempt_number,
                        request_headers=recorded_request_headers,
                        response_headers=response_headers,
                        retry_after=retry_after,
                        bytes_count=len(body) if body is not None else None,
                        duration_ms=duration_ms,
                        from_cache=response.status_code == 304,
                    )
                )
                if response.status_code in {200, 304, 404, 410}:
                    return HttpFetchResult(
                        status_code=response.status_code,
                        url=final_url,
                        headers=response_headers,
                        content_type=response.headers.get("content-type"),
                        body=body,
                        body_hash=compute_body_hash(body) if body is not None else None,
                        fetched_at=attempted_at,
                        attempts=tuple(attempts),
                    )
                if response.status_code == 429 or 500 <= response.status_code <= 599:
                    if attempt_number < max_attempts:
                        self._sleeper(self._retry_delay(response.status_code, retry_after, attempt_number))
                        continue
                    return HttpFetchResult(
                        status_code=response.status_code,
                        url=final_url,
                        headers=response_headers,
                        content_type=response.headers.get("content-type"),
                        body=None,
                        body_hash=None,
                        fetched_at=attempted_at,
                        attempts=tuple(attempts),
                    )

                return HttpFetchResult(
                    status_code=response.status_code,
                    url=final_url,
                    headers=response_headers,
                    content_type=response.headers.get("content-type"),
                    body=body,
                    body_hash=compute_body_hash(body) if body is not None else None,
                    fetched_at=attempted_at,
                    attempts=tuple(attempts),
                )
            except httpx.TimeoutException:
                duration_ms = int(max(0.0, self._clock() - started) * 1000)
                attempts.append(
                    FetchAttempt(
                        provider=self.provider,
                        endpoint_type=endpoint_type,
                        url=url,
                        method=method_upper,
                        status_code=None,
                        attempted_at=attempted_at,
                        attempt=attempt_number,
                        request_headers=recorded_request_headers,
                        duration_ms=duration_ms,
                    )
                )
                if attempt_number < max_attempts:
                    self._sleeper(self._retry_delay(None, None, attempt_number))
                    continue
                return HttpFetchResult(
                    status_code=0,
                    url=url,
                    headers={},
                    content_type=None,
                    body=None,
                    body_hash=None,
                    fetched_at=attempted_at,
                    attempts=tuple(attempts),
                )

        raise RuntimeError("unreachable HTTP retry state")

    def _respect_serial_delay(self) -> None:
        if self._last_request_at is not None:
            elapsed = self._clock() - self._last_request_at
            delay = self.policy.min_delay_s - elapsed
            if delay > 0:
                self._sleeper(delay)
        self._last_request_at = self._clock()

    def _retry_delay(
        self,
        status_code: int | None,
        retry_after: int | None,
        attempt_number: int,
    ) -> float:
        if status_code == 429:
            return self.policy.next_delay(429, retry_after)
        base = max(self.policy.min_delay_s, 1.0)
        delay = base * (2 ** (attempt_number - 1))
        if retry_after is not None:
            return max(delay, float(retry_after))
        return delay

    def _outbound_request_headers(self, headers: Mapping[str, str] | None) -> dict[str, str]:
        merged = {"User-Agent": self.user_agent}
        if headers:
            merged.update(headers)
        return {
            key: value
            for key, value in merged.items()
            if key.lower() not in SENSITIVE_REQUEST_HEADERS or key.lower() == "authorization"
        }

    def _request_headers(self, headers: Mapping[str, str] | None) -> dict[str, str]:
        if not headers:
            return {}
        return {key: value for key, value in headers.items() if key.lower() not in SENSITIVE_REQUEST_HEADERS}


def _cache_relevant_headers(headers: httpx.Headers) -> dict[str, str]:
    keep = {"etag", "last-modified", "content-type", "content-length", "retry-after"}
    return {key.lower(): value for key, value in headers.items() if key.lower() in keep}


def _parse_retry_after(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return max(0, parsed)
