"""Verify that CI only changes the initial healthcheck polling frequency."""

from __future__ import annotations

import copy
import json
import os
import subprocess  # nosec B404 # Renders the checked-out Compose configuration.
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
STARTUP_PROBES = ("api", "worker", "mercure")


def validate_overlay(base: dict[str, Any], ci: dict[str, Any]) -> None:
    """Compare rendered configurations without logging credentials or values."""
    expected = copy.deepcopy(base)
    actual = copy.deepcopy(ci)
    try:
        for name in STARTUP_PROBES:
            base_probe = expected["services"][name]["healthcheck"]
            ci_probe = actual["services"][name]["healthcheck"]
            if ci_probe.pop("start_interval", None) != "1s":
                raise ValueError("CI startup healthchecks must run every second")
            base_probe.pop("start_interval", None)
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("CI startup healthchecks are missing or malformed") from exc
    if actual != expected:
        raise ValueError("CI may only override the three startup healthcheck intervals")


def configuration(*files: str) -> dict[str, Any]:
    # Preserve the project's configured name so an overlay cannot change it
    # invisibly behind a command-line override.
    command = ["docker", "compose", "--env-file", os.devnull]
    for file in files:
        command.extend(("--file", file))
    result = subprocess.run(  # nosec B603 # Fixed Docker command and repository file paths; no shell.
        [*command, "config", "--format", "json"], cwd=ROOT,
        capture_output=True, text=True, timeout=30,
    )
    # Configuration can contain credentials; do not expose it on failure.
    if result.returncode:
        raise ValueError("Docker Compose could not render the configuration")
    parsed: Any = json.loads(result.stdout)
    if not isinstance(parsed, dict):
        raise ValueError("Docker Compose did not return a configuration object")
    return parsed


def main() -> int:
    try:
        validate_overlay(
            configuration("compose.yaml"),
            configuration("compose.yaml", ".github/compose.ci.yaml"),
        )
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        print(f"Compose CI contract failed: {exc}", file=sys.stderr)
        return 1
    print("Compose CI contract passed: only startup healthcheck intervals change")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
