"""Bound actual acquisition before source storage, regardless of response metadata."""
from __future__ import annotations

import gzip
from collections.abc import Iterator
from contextlib import contextmanager

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.providers.base import (
    FetchPolicy, ProviderRequestStopped, ProviderResponseDeadlineExceeded,
    ProviderResponseEncodingError, ProviderResponseTooLarge,
)
from chess_crawl.providers.http import HttpClient
from chess_crawl.providers.registry import ProviderSession
from support import Clock


class ObservedStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes], *, error: Exception | None = None) -> None:
        self.chunks = chunks
        self.error = error
        self.yielded = 0
        self.closed = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk
        if self.error is not None:
            raise self.error

    def close(self) -> None:
        self.closed += 1


@contextmanager
def client_for(handler, *, retries: int = 0, clock: Clock | None = None):
    clock = clock or Clock()
    client = HttpClient(
        provider="lichess", user_agent="bounded-tests", transport=httpx.MockTransport(handler),
        policy=FetchPolicy(0, False, False, None, retries), sleeper=clock.sleep, clock=clock,
    )
    try:
        yield client
    finally:
        client.close()


def acquire(client: HttpClient, **kwargs):
    return client.request("GET", "https://lichess.org/api/user/example", endpoint_type="user_profile", **kwargs)


def test_chunked_response_at_exact_limit_is_retained_and_settled_once() -> None:
    stream = ObservedStream([b"abc", b"defgh"])
    events: list[object] = []

    def handler(request):
        events.append("network")
        assert request.headers["accept-encoding"] == "identity"
        return httpx.Response(200, stream=stream)

    def reserve() -> int:
        events.append("reserve")
        return 8

    with client_for(handler) as client:
        client.reserve_request = reserve
        client.finish_request = lambda received: events.append(("finish", received))
        result = acquire(client, headers={"accept-encoding": "gzip", "Authorization": "Bearer secret"})
    assert result.body == b"abcdefgh" and result.attempts[0].bytes_count == 8
    assert events == ["reserve", "network", ("finish", 8)]
    assert "authorization" not in {key.lower() for key in result.attempts[0].request_headers}
    assert stream.yielded == 2 and stream.closed == 1


@pytest.mark.parametrize("status", [200, 500])
def test_excessive_unknown_length_response_stops_before_reading_tail(status) -> None:
    stream = ObservedStream([b"a" * 4, b"b" * 5, b"unread tail"])
    settled: list[int] = []
    with client_for(lambda request: httpx.Response(status, stream=stream), retries=3) as client:
        client.reserve_request = lambda: 8
        client.finish_request = settled.append
        with pytest.raises(ProviderResponseTooLarge) as failure:
            acquire(client)
    assert (failure.value.limit, failure.value.received, failure.value.status_code) == (8, 9, status)
    assert failure.value.url == "https://lichess.org/api/user/example"
    assert settled == [9] and stream.yielded == 2 and stream.closed == 1


def test_one_huge_chunk_is_rejected_before_buffering_and_tail_is_not_read() -> None:
    stream = ObservedStream([b"x" * (1024 * 1024), b"unread tail"])
    settled: list[int] = []
    with client_for(lambda request: httpx.Response(200, stream=stream)) as client:
        client.reserve_request = lambda: 16
        client.finish_request = settled.append
        with pytest.raises(ProviderResponseTooLarge):
            acquire(client)
    # At the application boundary, only the limit plus one bounded read is
    # exposed; the fixture transport already owned its prebuffered chunk.
    assert settled == [32] and stream.yielded == 1 and stream.closed == 1


@pytest.mark.parametrize("declared", ["1", "1000000", "invalid"])
def test_content_length_cannot_replace_actual_byte_accounting(declared) -> None:
    stream = ObservedStream([b"ab", b"cd"])
    settled: list[int] = []
    with client_for(lambda request: httpx.Response(200, headers={"content-length": declared}, stream=stream)) as client:
        client.reserve_request = lambda: 4
        client.finish_request = settled.append
        assert acquire(client).body == b"abcd"
    assert settled == [4] and stream.closed == 1


def test_small_false_content_length_does_not_allow_oversized_body() -> None:
    stream = ObservedStream([b"ab", b"cde", b"unread tail"])
    settled: list[int] = []
    with client_for(lambda request: httpx.Response(200, headers={"content-length": "1"}, stream=stream)) as client:
        client.reserve_request = lambda: 4
        client.finish_request = settled.append
        with pytest.raises(ProviderResponseTooLarge):
            acquire(client)
    assert settled == [5] and stream.yielded == 2 and stream.closed == 1


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br", "identity, gzip"])
def test_nonidentity_encoding_is_rejected_before_reading_or_decoding(encoding) -> None:
    stream = ObservedStream([b"invalid compressed data"])
    settled: list[int] = []
    with client_for(lambda request: httpx.Response(200, headers={"content-encoding": encoding}, stream=stream)) as client:
        client.reserve_request = lambda: 4
        client.finish_request = settled.append
        with pytest.raises(ProviderResponseEncodingError) as failure:
            acquire(client)
    assert failure.value.encoding == encoding and failure.value.status_code == 200
    assert settled == [0] and stream.yielded == 0 and stream.closed == 1


def test_interrupted_stream_settles_consumed_bytes_and_closes_response() -> None:
    stream = ObservedStream([b"abc"], error=httpx.ReadError("interrupted stream"))
    settled: list[int] = []
    with client_for(lambda request: httpx.Response(200, stream=stream)) as client:
        client.reserve_request = lambda: 10
        client.finish_request = settled.append
        result = acquire(client)
    assert result.status_code == 0 and result.body is None
    assert result.attempts[0].error_kind == "network_error" and result.attempts[0].bytes_count == 3
    assert settled == [3] and stream.closed == 1


def test_slow_dripping_response_hits_total_deadline_under_its_byte_cap() -> None:
    clock = Clock(0)

    class DripStream(ObservedStream):
        def __iter__(self) -> Iterator[bytes]:
            for chunk in super().__iter__():
                clock.sleep(4)
                yield chunk

    stream = DripStream([b"a", b"b", b"c", b"unread tail"])
    settled: list[int] = []
    with client_for(lambda request: httpx.Response(200, stream=stream), retries=3, clock=clock) as client:
        client.timeout_s = 10
        client.reserve_request = lambda: 100
        client.finish_request = settled.append
        with pytest.raises(ProviderResponseDeadlineExceeded) as failure:
            acquire(client)
    assert (failure.value.timeout_s, failure.value.received, failure.value.status_code) == (10, 3, 200)
    assert failure.value.url == "https://lichess.org/api/user/example"
    assert settled == [3] and stream.yielded == 3 and stream.closed == 1


def test_late_response_headers_expire_without_reading_body() -> None:
    clock = Clock(0)
    stream = ObservedStream([b"unread body"])
    settled: list[int] = []

    def handler(request):
        clock.sleep(11)
        return httpx.Response(500, stream=stream)

    with client_for(handler, clock=clock) as client:
        client.timeout_s = 10
        client.reserve_request = lambda: 100
        client.finish_request = settled.append
        with pytest.raises(ProviderResponseDeadlineExceeded) as failure:
            acquire(client)
    assert failure.value.received == 0 and failure.value.status_code == 500
    assert settled == [0] and stream.yielded == 0 and stream.closed == 1


def test_read_timeout_after_total_deadline_does_not_start_another_attempt() -> None:
    clock = Clock(0)

    class SlowFailure(ObservedStream):
        def __iter__(self) -> Iterator[bytes]:
            yield from super().__iter__()
            clock.sleep(11)
            raise httpx.ReadTimeout("stalled body")

    stream = SlowFailure([b"abc"])
    settled: list[int] = []
    with client_for(lambda request: httpx.Response(200, stream=stream), retries=3, clock=clock) as client:
        client.timeout_s = 10
        client.reserve_request = lambda: 100
        client.finish_request = settled.append
        with pytest.raises(ProviderResponseDeadlineExceeded) as failure:
            acquire(client)
    assert failure.value.received == 3 and failure.value.status_code == 200
    assert settled == [3] and stream.closed == 1


def test_each_retry_reserves_after_delay_and_settles_its_error_or_success_body() -> None:
    clock = Clock(0)
    streams = [ObservedStream([b"error"]), ObservedStream([b"ok"])]
    events: list[object] = []

    def handler(request):
        index = sum(item == "network" for item in events)
        events.append("network")
        return httpx.Response(500 if index == 0 else 200, stream=streams[index])

    def reserve() -> int:
        events.append(("reserve", clock()))
        return 10

    with client_for(handler, retries=1, clock=clock) as client:
        client.reserve_request = reserve
        client.finish_request = lambda received: events.append(("finish", received))
        result = acquire(client)
    assert result.body == b"ok" and [attempt.bytes_count for attempt in result.attempts] == [5, 2]
    assert events == [("reserve", 0), "network", ("finish", 5), ("reserve", 1), "network", ("finish", 2)]
    assert all(stream.closed == 1 for stream in streams)


def test_stream_failure_retry_settles_partial_attempt_separately() -> None:
    streams = [ObservedStream([b"abc"], error=httpx.ReadError("broken")), ObservedStream([b"ok"])]
    pending = iter(streams)
    settled: list[int] = []
    reservations: list[int] = []

    def reserve() -> int:
        reservations.append(1)
        return 10

    with client_for(lambda request: httpx.Response(200, stream=next(pending)), retries=1) as client:
        client.reserve_request = reserve
        client.finish_request = settled.append
        result = acquire(client)
    assert result.body == b"ok" and len(result.attempts) == 2
    assert reservations == [1, 1] and settled == [3, 2]
    assert all(stream.closed == 1 for stream in streams)


def test_stopped_attempt_does_not_reserve_or_contact_provider() -> None:
    def forbidden(*args):
        pytest.fail("Stopped acquisition contacted a provider or reserved capacity")

    with client_for(forbidden) as client:
        client._stop_requested = lambda: True
        client.reserve_request = forbidden
        client.finish_request = forbidden
        with pytest.raises(ProviderRequestStopped):
            acquire(client)


def test_stop_during_serial_delay_leaves_next_attempt_unreserved() -> None:
    clock = Clock(0)
    stopped = []

    def forbidden(*args):
        pytest.fail("Acquisition reserved or contacted a provider after stopping in its delay")

    def stop_while_waiting(seconds: float) -> None:
        clock.sleep(seconds)
        stopped.append(True)

    with client_for(forbidden, clock=clock) as client:
        client._not_before = 3
        client._stop_requested = lambda: bool(stopped)
        client._sleeper = stop_while_waiting
        client.reserve_request = forbidden
        client.finish_request = forbidden
        with pytest.raises(ProviderRequestStopped):
            acquire(client)
    assert clock() == 3


def test_reservation_failure_precedes_network_and_does_not_settle_unreserved_bytes() -> None:
    def forbidden(*args):
        pytest.fail("Failed reservation contacted a provider or settled unreserved capacity")

    def reserve():
        raise RuntimeError("durable request budget exhausted")

    with client_for(forbidden) as client:
        client.reserve_request = reserve
        client.finish_request = forbidden
        with pytest.raises(RuntimeError, match="durable request budget exhausted"):
            acquire(client)


def test_budget_callbacks_must_be_paired_before_network() -> None:
    with client_for(lambda request: pytest.fail("Invalid callback pair contacted a provider")) as client:
        client.reserve_request = lambda: 4
        with pytest.raises(ValueError, match="configured together"):
            acquire(client)


def test_nonbudget_client_keeps_existing_decoding_behavior() -> None:
    stream = ObservedStream([gzip.compress(b"decoded body")])
    with client_for(lambda request: httpx.Response(200, headers={"content-encoding": "gzip"}, stream=stream)) as client:
        assert acquire(client).body == b"decoded body"
    assert stream.closed == 1


def test_provider_session_routes_callbacks_and_can_disable_them_on_reused_client() -> None:
    config = Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0)
    events: list[object] = []
    observed_headers: list[str] = []

    def handler(request):
        observed_headers.append(request.headers["accept-encoding"])
        return httpx.Response(200, content=b"{}")

    def reserve(provider: str) -> int:
        events.append(("reserve", provider))
        return 4

    with ProviderSession(config, transport=httpx.MockTransport(handler)) as session:
        session.reserve_request = reserve
        session.finish_request = lambda provider, received: events.append(("finish", provider, received))
        session.client("chess.com").get_user_profile("example")
        session.client("lichess").get_user_profile("example")
        session.reserve_request = session.finish_request = None
        session.client("lichess").get_user_profile("example")
    assert events == [("reserve", "chess.com"), ("finish", "chess.com", 2),
                      ("reserve", "lichess"), ("finish", "lichess", 2)]
    assert observed_headers[:2] == ["identity", "identity"] and observed_headers[2] != "identity"
