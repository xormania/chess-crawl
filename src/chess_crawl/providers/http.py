"""Shared synchronous HTTP helper for provider clients."""

from __future__ import annotations

import math
import time
from collections.abc import Iterator
from datetime import UTC
from email.utils import parsedate_to_datetime
from dataclasses import dataclass
from typing import Any, Callable, Mapping

import httpx

from chess_crawl.providers.base import (
    EndpointType, FetchAttempt, FetchPolicy, ProviderRequestStopped,
    ProviderResponseDeadlineExceeded, ProviderResponseEncodingError, ProviderResponseTooLarge,
)
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
        stop_requested: Callable[[], bool] | None = None,
    ) -> None:
        self.provider = provider
        self.user_agent = user_agent
        self.policy = policy
        self.timeout_s = timeout_s
        self._sleeper = sleeper
        self._clock = clock
        self._stop_requested = stop_requested or (lambda: False)
        self._last_request_at: float | None = None
        self._not_before: float = 0
        self.before_request: Callable[[], None] | None = None
        self.persist_deadline: Callable[[float, str], None] | None = None
        self.reserve_request: Callable[[], int] | None = None
        self.finish_request: Callable[[int], None] | None = None
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
        reserve_request, finish_request = self.reserve_request, self.finish_request
        if (reserve_request is None) != (finish_request is None):
            raise ValueError("Request budget reservation and settlement must be configured together")
        if reserve_request is not None:
            request_headers = {key: value for key, value in request_headers.items() if key.lower() != "accept-encoding"}
            request_headers["Accept-Encoding"] = "identity"
        recorded_request_headers = self._request_headers(request_headers)
        method_upper = method.upper()

        last_result: HttpFetchResult | None = None
        for attempt_number in range(1, max_attempts + 1):
            if self.before_request is not None:
                self.before_request()
            if not self._stop_requested():
                self._respect_serial_delay()
            if self._stop_requested():
                if last_result is not None:
                    return last_result
                raise ProviderRequestStopped("Provider request stopped before network acquisition")
            attempted_at = int(self._clock())
            started = self._clock()
            retry_after = None
            received = 0
            limit = reserve_request() if reserve_request is not None else None
            response: httpx.Response | None = None

            try:
                if reserve_request is None:
                    response = self._client.request(
                        method_upper, url, headers=request_headers, params=params, content=content,
                    )
                    body = response.content if response.status_code == 200 else None
                else:
                    if type(limit) is not int or limit < 0:
                        raise ValueError("Request budget reservation must return a nonnegative integer")
                    if limit == 0:
                        raise ProviderResponseTooLarge(limit, 0, None, url)
                    with self._client.stream(
                        method_upper, url, headers=request_headers, params=params, content=content,
                    ) as response:
                        encoding = response.headers.get("content-encoding", "identity").strip().lower()
                        if encoding != "identity":
                            raise ProviderResponseEncodingError(encoding, response.status_code, str(response.url))
                        chunks = []
                        # Bill application reads, including one bounded read of
                        # headroom. Transport/TLS prefetch is outside this bound.
                        response_chunks = _bounded_response_chunks(response, min(65536, limit))
                        while True:
                            self._check_response_deadline(response, started, received)
                            try:
                                chunk = next(response_chunks)
                            except StopIteration:
                                self._check_response_deadline(response, started, received)
                                break
                            received += len(chunk)
                            if received > limit:
                                raise ProviderResponseTooLarge(limit, received, response.status_code, str(response.url))
                            self._check_response_deadline(response, started, received)
                            if response.status_code == 200:
                                chunks.append(bytes(chunk))
                        body = b"".join(chunks) if response.status_code == 200 else None
                duration_ms = int(max(0.0, self._clock() - started) * 1000)
                response_headers = _cache_relevant_headers(response.headers)
                retry_after = _parse_retry_after(response.headers.get("retry-after"), now=self._clock())
                final_url = str(response.url)
                attempts.append(
                    FetchAttempt(
                        provider=self.provider, endpoint_type=endpoint_type, url=final_url,
                        method=method_upper, status_code=response.status_code,
                        attempted_at=attempted_at, attempt=attempt_number,
                        request_headers=recorded_request_headers, response_headers=response_headers,
                        retry_after=retry_after,
                        bytes_count=received if reserve_request is not None else (len(body) if body is not None else None),
                        duration_ms=duration_ms, from_cache=response.status_code == 304,
                    )
                )
                last_result = HttpFetchResult(
                    status_code=response.status_code, url=final_url, headers=response_headers,
                    content_type=response.headers.get("content-type"), body=body,
                    body_hash=compute_body_hash(body) if body is not None else None,
                    fetched_at=attempted_at, attempts=tuple(attempts),
                )
            except httpx.RequestError as exc:
                if reserve_request is not None and self._clock() - started >= self.timeout_s:
                    raise ProviderResponseDeadlineExceeded(
                        self.timeout_s, received, None if response is None else response.status_code,
                        url if response is None else str(response.url),
                    ) from None
                duration_ms = int(max(0.0, self._clock() - started) * 1000)
                attempts.append(
                    FetchAttempt(
                        provider=self.provider, endpoint_type=endpoint_type, url=url,
                        method=method_upper, status_code=None, attempted_at=attempted_at,
                        attempt=attempt_number, request_headers=recorded_request_headers,
                        bytes_count=received if reserve_request is not None else None,
                        duration_ms=duration_ms,
                        error_kind="timeout" if isinstance(exc, httpx.TimeoutException) else "network_error",
                    )
                )
                last_result = HttpFetchResult(
                    status_code=0, url=url, headers={}, content_type=None, body=None,
                    body_hash=None, fetched_at=attempted_at, attempts=tuple(attempts),
                )
            finally:
                if finish_request is not None:
                    finish_request(received)

            status = last_result.status_code
            if status not in {0, 429} and not 500 <= status <= 599:
                if self.persist_deadline is not None:
                    self.persist_deadline(self._clock() + self.policy.min_delay_s, "provider request pacing")
                return last_result
            delay = self._retry_delay(status, retry_after, attempt_number)
            self._not_before = self._clock() + delay
            if self.persist_deadline is not None:
                self.persist_deadline(self._not_before, f"HTTP {status} backoff")
            if attempt_number == max_attempts or self._stop_requested():
                return last_result
            self._sleeper(delay)
            if self._stop_requested():
                # Return the actual last response so ingestion commits fetch
                # evidence and the runner persists the provider backoff floor.
                return last_result
            self._not_before = 0

        raise RuntimeError("unreachable HTTP retry state")

    def _check_response_deadline(self, response: httpx.Response, started: float, received: int) -> None:
        # A read timeout alone can be extended forever by a slow-dripping body.
        # Check the whole attempt before and after each application read. The
        # existing read timeout bounds a blocking transport read in progress.
        if self._clock() - started >= self.timeout_s:
            raise ProviderResponseDeadlineExceeded(self.timeout_s, received, response.status_code, str(response.url))

    def _respect_serial_delay(self) -> None:
        not_before = self._not_before
        if self._last_request_at is not None:
            not_before = max(not_before, self._last_request_at + self.policy.min_delay_s)
        delay = not_before - self._clock()
        if delay > 0:
            self._sleeper(delay)
        if not self._stop_requested():
            self._not_before = 0
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


def _bounded_response_chunks(response: httpx.Response, read_size: int) -> Iterator[memoryview]:
    """Expose bounded raw slices without copying a transport's complete chunk.

    HTTPX's optional chunker first buffers and slices an entire raw chunk.
    Lazy views avoid that extra allocation for a prebuffered custom transport.
    Identity encoding was verified before this iterator: no decoder is invoked.
    Already buffered responses (for example MockTransport fixtures) own their
    content before acquisition; their additional retention is still bounded.
    """
    raw_chunks = (response.content,) if response.is_stream_consumed else response.iter_raw()
    for raw_chunk in raw_chunks:
        view = memoryview(raw_chunk)
        for offset in range(0, len(view), read_size):
            yield view[offset:offset + read_size]


def _parse_retry_after(value: str | None, *, now: float | None = None) -> int | None:
    if value is None:
        return None
    try:
        return max(0, int(value))
    except ValueError:
        pass
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        return max(0, math.ceil(date.timestamp() - (time.time() if now is None else now)))
    except (ValueError, TypeError, OverflowError):
        return None
