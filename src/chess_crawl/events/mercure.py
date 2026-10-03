"""Mercure publishing adapter. It never owns crawl state or HTML rendering."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from chess_crawl.storage.events import PendingEvent


@dataclass(frozen=True)
class MercureSettings:
    hub_url: str
    publisher_jwt: str = field(repr=False)
    topic_prefix: str = "https://chess-crawl.local"
    timeout_s: float = 15.0
    retry_base_s: float = 1.0
    retry_max_s: float = 300.0

    def __post_init__(self) -> None:
        for name, value in (("hub_url", self.hub_url), ("topic_prefix", self.topic_prefix)):
            parsed = urlsplit(value)
            if (
                parsed.scheme not in {"http", "https"} or not parsed.netloc
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or any(c.isspace() for c in value)
            ):
                raise ValueError(f"Mercure {name} must be an HTTP(S) URL without credentials, query, or fragment")
        if (
            not self.publisher_jwt or not self.publisher_jwt.isascii()
            or any(c.isspace() or not c.isprintable() for c in self.publisher_jwt)
        ):
            raise ValueError("A nonempty Mercure publisher JWT without whitespace is required")
        if not all(math.isfinite(value) and value > 0 for value in (
            self.timeout_s, self.retry_base_s, self.retry_max_s,
        )) or self.retry_max_s < self.retry_base_s:
            raise ValueError("Mercure timeouts and retry intervals must be finite positive values")

    @classmethod
    def from_env(cls) -> MercureSettings:
        token = os.getenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT", "")
        token_file = os.getenv("CHESS_CRAWL_MERCURE_PUBLISHER_JWT_FILE", "")
        if token and token_file:
            raise ValueError("Configure only one Mercure publisher JWT source")
        if token_file:
            try:
                token = Path(token_file).read_text(encoding="utf-8").strip()
            except (OSError, UnicodeError):
                raise ValueError("Could not read the configured Mercure publisher JWT file") from None
        return cls(
            hub_url=os.getenv("CHESS_CRAWL_MERCURE_URL", ""),
            publisher_jwt=token,
            topic_prefix=os.getenv("CHESS_CRAWL_MERCURE_TOPIC_PREFIX", "https://chess-crawl.local").rstrip("/"),
        )


@dataclass(frozen=True)
class DeliveryResult:
    succeeded: bool
    error: str | None = None
    retry_after_s: float | None = None


class MercurePublisher:
    def __init__(self, settings: MercureSettings, *, transport: httpx.BaseTransport | None = None) -> None:
        self.settings = settings
        self._client = httpx.Client(
            timeout=settings.timeout_s,
            transport=transport,
            follow_redirects=False,
            headers={"Authorization": f"Bearer {settings.publisher_jwt}"},
        )

    def __enter__(self) -> MercurePublisher:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def publish(self, event: PendingEvent, *, now: float) -> DeliveryResult:
        try:
            response = self._client.post(
                self.settings.hub_url,
                data={
                    "topic": self.settings.topic_prefix.rstrip("/") + event.resource_path,
                    "data": json.dumps(event.payload, sort_keys=True, separators=(",", ":")),
                    "id": event.event_id,
                    "type": event.event_type,
                    "private": "on",
                },
            )
        except httpx.HTTPError:
            # Exception strings and response bodies may reflect credentials.
            return DeliveryResult(False, error="transport_error")
        if 200 <= response.status_code < 300:
            return DeliveryResult(True)
        return DeliveryResult(
            False,
            error=f"http_{response.status_code}",
            retry_after_s=retry_after_seconds(response.headers.get("Retry-After"), now=now),
        )

    def retry_delay(self, attempts: int, retry_after_s: float | None = None) -> float:
        backoff = min(self.settings.retry_max_s, self.settings.retry_base_s * 2 ** min(attempts, 30))
        # The exponential component is bounded. A valid server Retry-After is
        # a lower bound and must not be shortened by our backoff cap.
        return max(backoff, retry_after_s or 0.0)


def retry_after_seconds(value: str | None, *, now: float) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            timestamp = parsedate_to_datetime(value)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            seconds = timestamp.timestamp() - now
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0.0, seconds) if math.isfinite(seconds) else None
