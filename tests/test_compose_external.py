"""Render actual local/external Compose graphs without contacting a Docker daemon."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import httpx

from chess_crawl.config import Config
from chess_crawl.providers.lichess.client import LichessClient


ROOT = Path(__file__).resolve().parents[1]
PYTHON_SERVICES = ("init", "api", "worker", "events")


@pytest.fixture
def render_compose(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> Callable[..., subprocess.CompletedProcess[str]]:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker Compose client is required to render the real application graph")
    for variable in ("COMPOSE_FILE", "COMPOSE_PROFILES", "CHESS_CRAWL_DATABASE_URL", "CHESS_CRAWL_DATABASE_CA_FILE"):
        monkeypatch.delenv(variable, raising=False)
    certificate = tmp_path / "server CA.pem"
    certificate.write_text("Public certificate fixture for Compose configuration only\n")
    certificate.chmod(0o444)

    def render(*, external: bool = False, url: bool = True, ca: bool = True) -> subprocess.CompletedProcess[str]:
        environment = dict(os.environ)
        if external:
            if url:
                environment["CHESS_CRAWL_DATABASE_URL"] = "postgresql://chess_crawl@external-db.example:5432/chess_crawl"
            if ca:
                environment["CHESS_CRAWL_DATABASE_CA_FILE"] = str(certificate)
        arguments = [docker, "compose", "--env-file", os.devnull, "--file", str(ROOT / "compose.yaml")]
        if external:
            arguments.extend(("--file", str(ROOT / "compose.external.yaml")))
        return subprocess.run(
            [*arguments, "config", "--format", "json"], cwd=ROOT,
            env=environment, text=True, capture_output=True, timeout=30,
        )

    return render


def parsed(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    assert result.returncode == 0, result.stderr
    value: Any = json.loads(result.stdout)
    assert isinstance(value, dict)
    return value


def test_default_compose_retains_private_bundled_database_and_migration_gate(
    render_compose: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    services = parsed(render_compose())["services"]
    assert services["postgres"]["image"] == "postgres:18"
    assert "ports" not in services["postgres"]
    assert services["init"]["depends_on"]["postgres"] == {"condition": "service_healthy", "required": True}
    for name in PYTHON_SERVICES:
        settings = services[name]["environment"]
        assert settings["CHESS_CRAWL_DATABASE_TRANSPORT"] == "local"
        assert settings["CHESS_CRAWL_DATABASE_TRUSTED_HOST"] == "postgres"
        assert settings["CHESS_CRAWL_DATABASE_URL"] == "postgresql://chess_crawl@postgres:5432/chess_crawl"
        if name != "init":
            assert services[name]["depends_on"]["init"] == {"condition": "service_completed_successfully", "required": True}


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("owner", [None, "workspace:analysis"])
def test_compose_worker_token_authorizes_only_the_configured_owner(
    render_compose: Callable[..., subprocess.CompletedProcess[str]], monkeypatch: pytest.MonkeyPatch,
    external: bool, owner: str | None,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_TOKEN", "private-fixture-token")
    if owner is not None:
        monkeypatch.setenv("CHESS_CRAWL_LICHESS_TOKEN_OWNER_SCOPE", owner)
    services = parsed(render_compose(external=external))["services"]
    environment = services["worker"]["environment"]
    # Apply only the rendered container environment: a host variable omitted by
    # Compose must not make a deployment regression pass accidentally.
    monkeypatch.delenv("CHESS_CRAWL_LICHESS_TOKEN_OWNER_SCOPE", raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, str(value))
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer private-fixture-token"
        return httpx.Response(200, json=[{"id": "private-team"}])

    client = LichessClient(Config.from_env().provider("lichess"), transport=httpx.MockTransport(respond))
    try:
        expected_owner = owner or "local"
        result = client.get_user_resource("Alice", "teams", owner_scope=expected_owner)
        assert result.http_status == 200 and result.owner_scope == expected_owner
        with pytest.raises(ValueError, match="different workspace"):
            client.get_user_resource("Alice", "teams", owner_scope="workspace:unrelated")
        assert len(requests) == 1
    finally:
        client.close()
    for name in ("api", "init", "events"):
        assert "CHESS_CRAWL_LICHESS_TOKEN" not in services[name]["environment"]


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("custom", [False, True])
def test_compose_passes_export_limits_only_to_api(
    render_compose: Callable[..., subprocess.CompletedProcess[str]], monkeypatch: pytest.MonkeyPatch,
    external: bool, custom: bool,
) -> None:
    defaults = {
        "MAX_ROWS": "100000", "MAX_BYTES": "67108864", "PREPARE_SECONDS": "60",
        "DOWNLOAD_SECONDS": "300", "WORKSPACE_SLOTS": "2", "OUTSTANDING_SPOOLS": "4",
        "OUTSTANDING_BYTES": "268435456", "WORKSPACE_OUTSTANDING_SPOOLS": "2",
        "WORKSPACE_OUTSTANDING_BYTES": "134217728",
    }
    custom_values = {
        "MAX_ROWS": "7", "MAX_BYTES": "1024", "PREPARE_SECONDS": "7", "DOWNLOAD_SECONDS": "7",
        "WORKSPACE_SLOTS": "7", "OUTSTANDING_SPOOLS": "6", "OUTSTANDING_BYTES": "8192",
        "WORKSPACE_OUTSTANDING_SPOOLS": "3", "WORKSPACE_OUTSTANDING_BYTES": "3072",
    }
    expected = {f"CHESS_CRAWL_EXPORT_{name}": value for name, value in (custom_values if custom else defaults).items()}
    if custom:
        for name, value in expected.items():
            monkeypatch.setenv(name, value)
    services = parsed(render_compose(external=external))["services"]
    for name, value in expected.items():
        assert services["api"]["environment"][name] == value
        for service in ("worker", "init", "events"):
            assert name not in services[service]["environment"]
    from chess_crawl.api.exports import ExportLimits
    for name in expected:
        monkeypatch.setenv(name, services["api"]["environment"][name])
    limits = ExportLimits.from_env()
    assert limits.workspace_outstanding_bytes + limits.max_bytes <= limits.outstanding_bytes


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("custom", [False, True])
def test_compose_passes_dispatch_cleanup_settings_only_to_worker(
    render_compose: Callable[..., subprocess.CompletedProcess[str]], monkeypatch: pytest.MonkeyPatch,
    external: bool, custom: bool,
) -> None:
    defaults = {"RETENTION_SECONDS": "86400", "CLEANUP_INTERVAL_SECONDS": "60", "CLEANUP_BATCH_SIZE": "256"}
    expected = {f"CHESS_CRAWL_DISPATCH_{name}": "17" if custom else value for name, value in defaults.items()}
    if custom:
        for name, value in expected.items():
            monkeypatch.setenv(name, value)
    services = parsed(render_compose(external=external))["services"]
    for name, value in expected.items():
        assert services["worker"]["environment"][name] == value
        for service in ("api", "init", "events"):
            assert name not in services[service]["environment"]


@pytest.mark.parametrize("external", [False, True])
def test_compose_scratch_is_private_and_owned_by_the_runtime_user(
    render_compose: Callable[..., subprocess.CompletedProcess[str]], external: bool,
) -> None:
    services = parsed(render_compose(external=external))["services"]
    for name in (*PYTHON_SERVICES, "archive-init"):
        assert services[name]["tmpfs"] == ["/tmp:uid=10001,gid=10001,mode=0700"]
        assert services[name]["read_only"] is True
    for name in PYTHON_SERVICES:
        assert services[name]["user"] == "10001:10001"


def test_external_compose_excludes_unused_database_and_preserves_migration_and_hub_gates(
    render_compose: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    services = parsed(render_compose(external=True))["services"]
    assert set(services) == {"archive-init", "init", "api", "worker", "events", "mercure"}
    assert services["init"].get("depends_on", {}) == {}
    for name in ("api", "worker", "events"):
        assert services[name]["depends_on"]["init"] == {"condition": "service_completed_successfully", "required": True}
    assert services["events"]["depends_on"]["mercure"] == {"condition": "service_healthy", "required": True}
    assert "mercure" not in services["api"]["depends_on"]
    assert "mercure" not in services["worker"]["depends_on"]
    for name in ("api", "worker"):
        assert services[name]["depends_on"]["archive-init"] == {
            "condition": "service_completed_successfully", "required": True,
        }
    assert services["archive-init"].get("depends_on", {}) == {}


def test_external_compose_enforces_verified_transport_and_mounts_ca_on_every_database_client(
    render_compose: Callable[..., subprocess.CompletedProcess[str]], monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A caller's inherited local exception must not override external mode.
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRUSTED_HOST", "external-db.example")
    model = parsed(render_compose(external=True))
    certificate = Path(model["secrets"]["postgres_ca"]["file"])
    assert certificate.is_file() and certificate.stat().st_mode & 0o777 == 0o444
    for name in PYTHON_SERVICES:
        service = model["services"][name]
        settings = service["environment"]
        assert settings["CHESS_CRAWL_DATABASE_URL"] == "postgresql://chess_crawl@external-db.example:5432/chess_crawl"
        assert settings["CHESS_CRAWL_DATABASE_TRANSPORT"] == "verified"
        assert settings["CHESS_CRAWL_DATABASE_TRUSTED_HOST"] == ""
        assert settings["CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE"] == "/run/secrets/postgres_ca"
        assert settings["CHESS_CRAWL_DATABASE_PASSWORD_FILE"] == "/run/secrets/postgres_password"
        mounted = {secret["target"]: secret["source"] for secret in service["secrets"]}
        assert mounted["/run/secrets/postgres_ca"] == "postgres_ca"
        assert mounted["/run/secrets/postgres_password"] == "postgres_password"
        assert service["read_only"] is True
        assert service["user"] == "10001:10001"
    assert any(secret["source"] == "api_token" for secret in model["services"]["api"]["secrets"])
    assert any(secret["source"] == "mercure_publisher_jwt" for secret in model["services"]["events"]["secrets"])


@pytest.mark.parametrize(("url", "ca", "missing"), [
    (False, True, "CHESS_CRAWL_DATABASE_URL"),
    (True, False, "CHESS_CRAWL_DATABASE_CA_FILE"),
])
def test_external_compose_requires_explicit_target_and_ca_without_bundled_fallback(
    render_compose: Callable[..., subprocess.CompletedProcess[str]], url: bool, ca: bool, missing: str,
) -> None:
    result = render_compose(external=True, url=url, ca=ca)
    assert result.returncode != 0
    assert missing in result.stderr
