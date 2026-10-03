"""Exercise the CI overlay contract; CI checks the real Compose merger once."""

from __future__ import annotations

import copy
import os
import runpy
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/check_compose_ci.py"
MODULE = runpy.run_path(str(SCRIPT))
validate_overlay = cast(
    Callable[[dict[str, Any], dict[str, Any]], None],
    MODULE["validate_overlay"],
)
configuration = cast(Callable[..., dict[str, Any]], MODULE["configuration"])


@pytest.fixture
def configurations() -> tuple[dict[str, Any], dict[str, Any]]:
    base: dict[str, Any] = {
        "name": "chess-crawl",
        "services": {
            name: {
                "healthcheck": {
                    "test": ["CMD", "probe", name], "interval": "5s", "timeout": "5s",
                    "retries": 5, "start_period": "10s",
                },
                "depends_on": {"init": {"condition": "service_completed_successfully"}},
                "read_only": True,
                "secrets": [{"source": "api_token", "target": "api_token"}],
            }
            for name in ("api", "worker", "mercure", "postgres")
        },
        "volumes": {"postgres_data": {"name": "postgres_data"}},
    }
    ci = copy.deepcopy(base)
    for service in ci["services"].values():
        service["healthcheck"]["start_interval"] = "1s"
    return base, ci


def test_startup_overlay_accepts_only_faster_initial_probes_without_mutating_inputs(
    configurations: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    base, ci = configurations
    before = copy.deepcopy(configurations)
    validate_overlay(base, ci)
    assert configurations == before


@pytest.mark.parametrize("field", ["test", "interval", "timeout", "retries", "start_period"])
def test_startup_overlay_rejects_changed_readiness_or_failure_budgets(
    configurations: tuple[dict[str, Any], dict[str, Any]], field: str,
) -> None:
    base, ci = configurations
    ci["services"]["api"]["healthcheck"][field] = "changed"
    with pytest.raises(ValueError, match="only override"):
        validate_overlay(base, ci)


@pytest.mark.parametrize("field", ["depends_on", "read_only", "secrets"])
def test_startup_overlay_rejects_changed_dependencies_and_permissions(
    configurations: tuple[dict[str, Any], dict[str, Any]], field: str,
) -> None:
    base, ci = configurations
    del ci["services"]["worker"][field]
    with pytest.raises(ValueError, match="only override"):
        validate_overlay(base, ci)


def test_startup_overlay_rejects_changed_database_storage(
    configurations: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    base, ci = configurations
    ci["volumes"].clear()
    with pytest.raises(ValueError, match="only override"):
        validate_overlay(base, ci)


def test_startup_overlay_rejects_changed_project_name(
    configurations: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    base, ci = configurations
    ci["name"] = "another-project"
    with pytest.raises(ValueError, match="only override"):
        validate_overlay(base, ci)


def test_configuration_preserves_project_name_and_file_order(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        assert kwargs["cwd"] == SCRIPT.parents[2]
        return subprocess.CompletedProcess(command, 0, stdout='{"name": "chess-crawl"}')

    monkeypatch.setattr(subprocess, "run", run)
    assert configuration("compose.yaml", ".github/compose.ci.yaml") == {"name": "chess-crawl"}
    assert calls == [[
        "docker", "compose", "--env-file", os.devnull,
        "--file", "compose.yaml", "--file", ".github/compose.ci.yaml",
        "config", "--format", "json",
    ]]


@pytest.mark.parametrize("service", ["api", "worker", "mercure", "postgres"])
def test_startup_overlay_rejects_missing_faster_probe(
    configurations: tuple[dict[str, Any], dict[str, Any]], service: str,
) -> None:
    base, ci = configurations
    del ci["services"][service]["healthcheck"]["start_interval"]
    with pytest.raises(ValueError, match="every second"):
        validate_overlay(base, ci)


def test_startup_overlay_rejects_missing_healthcheck(
    configurations: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    base, ci = configurations
    del ci["services"]["worker"]["healthcheck"]
    with pytest.raises(ValueError, match="missing or malformed"):
        validate_overlay(base, ci)
