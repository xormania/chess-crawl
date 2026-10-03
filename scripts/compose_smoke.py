#!/usr/bin/env python3
"""Exercise the Compose API, worker, SQLite outbox, and real Mercure hub offline.

Run after scripts/bootstrap_dev.py and `docker compose up --build -d --wait`.
The worker is stopped before submissions; synthetic completions never contact a
chess provider. Use this only with the disposable development/CI archive.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess  # nosec B404 # Controls only the operator's disposable Compose stack.
import time
import uuid
from collections.abc import Callable
from http.client import HTTPResponse
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def compose(*arguments: str, code: str | None = None) -> str:
    result = subprocess.run(  # nosec B603, B607 # Repository-controlled Docker argv and trusted PATH; no shell.
        ["docker", "compose", *arguments],
        input=code, text=True, capture_output=True, check=False, timeout=90,
    )
    if result.returncode:
        # Keep credentials, environment, and HTTP headers out of diagnostics.
        raise RuntimeError(f"Compose command failed ({' '.join(arguments[:3])}); inspect service logs")
    return result.stdout


def wait_until(check: Callable[[], bool], label: str, *, timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {label}")


class Api:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def request(
        self, path: str, *, body: dict[str, Any] | None = None,
        idempotency_key: str | None = None, authenticated: bool = True,
    ) -> tuple[int, Any]:
        headers = {"Accept": "application/json"}
        if authenticated:
            headers["Authorization"] = f"Bearer {self.token}"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, headers=headers, data=data)
        try:
            with urlopen(request, timeout=5) as response:  # nosec B310 # Operator-selected disposable API endpoint.
                return response.status, json.load(response)
        except HTTPError as response:
            return response.code, json.load(response)


def subscribe(hub_url: str, topic: str, token: str | None) -> HTTPResponse:
    headers = {"Accept": "text/event-stream"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = urlopen(Request(hub_url + "?" + urlencode({"topic": topic}), headers=headers), timeout=30)  # nosec B310 # Operator-selected disposable Mercure endpoint.
    if response.status != 200 or "text/event-stream" not in response.headers.get("Content-Type", ""):
        response.close()
        raise RuntimeError("Mercure did not open an SSE subscription")
    return response


def read_job_event(stream: HTTPResponse, *, job_id: int, status: str) -> dict[str, Any]:
    deadline = time.monotonic() + 40
    lines: list[str] = []
    while time.monotonic() < deadline:
        try:
            raw_line = stream.readline()
        except (TimeoutError, OSError) as exc:
            raise RuntimeError(f"Timed out waiting for Mercure job state {status}") from exc
        if not raw_line:
            raise RuntimeError("Mercure closed its event stream before the expected update")
        line = raw_line.decode("utf-8").rstrip("\r\n")
        if not line:
            if lines:
                event = json.loads("\n".join(lines))
                lines.clear()
                if event.get("job_id") == job_id and event.get("status") == status:
                    assert event["schema_version"] == 1
                    assert event["type"] == "job.updated"
                    assert event["revision"] >= 1
                    assert event["event_id"].startswith("urn:chess-crawl:")
                    return event
        elif line.startswith("data:"):
            lines.append(line[5:].lstrip())
    raise RuntimeError(f"Mercure job state {status} was not delivered")


def main() -> int:
    if not __debug__:
        raise RuntimeError("Compose smoke requires assertions; disable -O and PYTHONOPTIMIZE")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--hub-url", default="http://127.0.0.1:3000/.well-known/mercure")
    parser.add_argument("--secrets-dir", type=Path, default=Path(os.getenv("CHESS_CRAWL_SECRETS_DIR", "data/dev-secrets")))
    parser.add_argument("--topic-prefix", default=os.getenv("CHESS_CRAWL_MERCURE_TOPIC_PREFIX", "https://chess-crawl.local"))
    args = parser.parse_args()
    api = Api(args.api_url, (args.secrets_dir / "api_token").read_text().strip())
    subscriber_jwt = (args.secrets_dir / "mercure_subscriber_jwt").read_text().strip()

    wait_until(lambda: api.request("/health/ready")[0] == 200, "API readiness")
    wait_until(lambda: api.request("/v1/worker")[1].get("alive") is True, "idle worker heartbeat")
    assert api.request("/v1/games", authenticated=False)[0] == 401
    assert api.request("/health/ready", authenticated=False)[0] == 401
    assert api.request("/health/live", authenticated=False) == (200, {"status": "ok"})
    assert api.request("/v1/providers")[0] == 200

    # No acquisition can race the synthetic submissions or contact a provider.
    compose("stop", "-t", "20", "worker", "events")
    wait_until(lambda: api.request("/v1/worker")[1].get("alive") is False, "worker shutdown")
    body = {
        "provider": "chess.com", "username": "compose-smoke",
        "since": 1704067200, "until": 1704153600, "max_games": 1,
    }
    key = "compose-smoke-" + uuid.uuid4().hex
    status, submission = api.request("/v1/imports", body=body, idempotency_key=key)
    assert status == 202 and submission["replayed"] is False
    status, replay = api.request("/v1/imports", body=body, idempotency_key=key)
    assert status == 202 and replay["replayed"] is True
    assert replay["run_id"] == submission["run_id"] and replay["job_ids"] == submission["job_ids"]
    assert api.request("/v1/imports", body={**body, "max_games": 2}, idempotency_key=key)[0] == 409
    job_id = submission["job_ids"][0]
    status, snapshot = api.request(f"/v1/jobs/{job_id}")
    assert status == 200 and snapshot["state"] == "pending"
    for collection in ("games", "users"):
        status, page = api.request(f"/v1/{collection}?limit=1")
        assert status == 200 and page["items"] == [] and page["next_cursor"] is None
    topic = args.topic_prefix.rstrip("/") + f"/jobs/{job_id}"
    try:
        anonymous = subscribe(args.hub_url, topic, None)
    except HTTPError as response:
        assert response.code in {401, 403}
        response.close()
    else:
        anonymous.close()
        raise AssertionError("Development hub unexpectedly permits anonymous subscriptions")

    with subscribe(args.hub_url, topic, subscriber_jwt) as stream:
        # Initial insertion events remained durable while the publisher was down.
        compose("start", "events")
        pending = read_job_event(stream, job_id=job_id, status="pending")
        completion_code = f"""
import os
from chess_crawl.jobs import state
from chess_crawl.storage.db import connection, transaction
with connection(os.environ['CHESS_CRAWL_DB'], mode='rw') as conn:
    with transaction(conn):
        for job_id in {submission['job_ids']!r}:
            state.mark_done(conn, job_id, reason='offline Compose smoke check')
        state.refresh_run_status(conn, {submission['run_id']!r})
    assert conn.execute('SELECT COUNT(*) FROM fetch_logs').fetchone()[0] == 0
print('No provider requests were performed')
"""  # nosec B608 # This is Python fixture code; its SQL is literal, and IDs use repr.
        compose("exec", "-T", "api", "python", "-", code=completion_code)
        completed = read_job_event(stream, job_id=job_id, status="done")
        assert completed["event_id"] != pending["event_id"]
        assert completed["revision"] > pending["revision"]
        assert completed["archive_id"] == pending["archive_id"]

    status, snapshot = api.request(f"/v1/jobs/{job_id}")
    assert status == 200 and snapshot["state"] == "done"
    assert snapshot["revision"] == completed["revision"]
    assert snapshot["archive_id"] == completed["archive_id"]
    status, run = api.request(f"/v1/runs/{submission['run_id']}")
    assert status == 200 and run["status"] == "done"
    compose("start", "worker")
    wait_until(lambda: api.request("/v1/worker")[1].get("alive") is True, "restarted idle worker")
    print("Compose smoke passed: authenticated API, idempotent queue, worker liveness, durable private Mercure events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
