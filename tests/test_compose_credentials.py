"""Generated development grants must authorize the actual private event contract."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import runpy
import sys
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from chess_crawl.events.mercure import MercurePublisher, MercureSettings
from chess_crawl.storage.events import PendingEvent

BOOTSTRAP = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/bootstrap_dev.py"))


def claims(token: str, key: str) -> dict:
    header, payload, signature = token.split(".")
    expected = hmac.new(key.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
    decoded_signature = base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4))
    assert hmac.compare_digest(decoded_signature, expected), "Generated JWT signature is invalid"
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


def bootstrap(directory: Path, prefix: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["bootstrap_dev.py", "--directory", str(directory), "--topic-prefix", prefix])
    assert BOOTSTRAP["main"]() == 0


def granted(selectors: list[str], topic: str, resource_id: int) -> bool:
    # The bootstrap emits only the simple RFC6570 {id} expansion. Exercise
    # concrete topics sent by the production publisher, not the issuer helper.
    return topic in [selector.format(id=resource_id) for selector in selectors]


@pytest.mark.parametrize("event_type", ["job.updated", "run.updated"])
@pytest.mark.parametrize("prefix", ["https://chess-crawl.local", "https://archive.example/chess/"])
def test_development_credentials_authorize_private_local_events_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event_type: str, prefix: str,
) -> None:
    bootstrap(tmp_path, prefix, monkeypatch)
    key = (tmp_path / "mercure_signing_key").read_text().strip()
    publisher_token = (tmp_path / "mercure_publisher_jwt").read_text().strip()
    subscriber_token = (tmp_path / "mercure_subscriber_jwt").read_text().strip()
    publisher_grant = claims(publisher_token, key)["mercure"]
    subscriber_grant = claims(subscriber_token, key)["mercure"]
    assert set(publisher_grant) == {"publish"}
    assert set(subscriber_grant) == {"subscribe"}
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == f"Bearer {publisher_token}"
        form = parse_qs(request.content.decode())
        assert form["private"] == ["on"]
        requests.append(form)
        permitted = granted(publisher_grant["publish"], form["topic"][0], 7)
        return httpx.Response(201 if permitted else 403)

    settings = MercureSettings(
        hub_url="https://hub.example/.well-known/mercure", publisher_jwt=publisher_token,
        topic_prefix=prefix,
    )
    event = PendingEvent(1, "urn:chess-crawl:test:1", event_type, 7, {"workspace_id": "local"}, 0, 0)
    foreign = PendingEvent(2, "urn:chess-crawl:test:2", event_type, 7, {"workspace_id": "another-workspace"}, 0, 0)
    with MercurePublisher(settings, transport=httpx.MockTransport(respond)) as publisher:
        assert publisher.publish(event, now=0).succeeded, "Generated publisher grant excludes its local event"
        denied = publisher.publish(foreign, now=0)
        assert not denied.succeeded and denied.error == "http_403"
    assert granted(subscriber_grant["subscribe"], requests[0]["topic"][0], 7)
    assert not granted(subscriber_grant["subscribe"], requests[1]["topic"][0], 7)


def test_bootstrap_replaces_legacy_topic_grants_without_rotating_master_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = "https://chess-crawl.local"
    bootstrap(tmp_path, prefix, monkeypatch)
    retained = {name: (tmp_path / name).read_bytes() for name in ("postgres_password", "api_token", "mercure_signing_key")}
    key = retained["mercure_signing_key"].decode().strip()
    for permission in ("publish", "subscribe"):
        path = tmp_path / f"mercure_{'publisher' if permission == 'publish' else 'subscriber'}_jwt"
        path.chmod(0o600)
        path.write_text(BOOTSTRAP["_jwt"](key, permission, [f"{prefix}/jobs/{{id}}", f"{prefix}/runs/{{id}}"]))
    bootstrap(tmp_path, prefix, monkeypatch)
    assert retained == {name: (tmp_path / name).read_bytes() for name in retained}
    for permission, filename in (("publish", "mercure_publisher_jwt"), ("subscribe", "mercure_subscriber_jwt")):
        selectors = claims((tmp_path / filename).read_text().strip(), key)["mercure"][permission]
        assert granted(selectors, f"{prefix}/workspaces/local/jobs/1", 1)
        assert not granted(selectors, f"{prefix}/workspaces/another-workspace/jobs/1", 1)
