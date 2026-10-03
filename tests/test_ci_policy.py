"""Exercise the actual promotion guard with GitHub pull-request contexts."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pytest


def _workflow() -> str:
    return (Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml").read_text()


def _promotion_guard(context: dict[str, Any]) -> subprocess.CompletedProcess[str]:
    # Extract the existing scalar blocks, not a second implementation of the
    # policy. Resolving its environment bindings also catches workflow wiring
    # errors (such as comparing the base repository with itself).
    step = _workflow().split("      - name: Check promotion source\n", 1)[1]
    environment, script_block = step.split("        run: |\n", 1)
    env = os.environ.copy()
    for line in environment.splitlines():
        if not line.startswith("          "):
            continue
        name, expression = line.strip().split(":", 1)
        expression = expression.strip()
        assert expression.startswith("${{ ") and expression.endswith(" }}")
        value: Any = context
        for part in expression[4:-3].split("."):
            value = value[part]
        env[name] = str(value)
    script_lines = []
    for line in script_block.splitlines():
        if line and not line.startswith("          "):
            break
        script_lines.append(line[10:])
    script = "\n".join(script_lines)
    assert script.strip()
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-c", script],
        env=env, capture_output=True, text=True, timeout=5,
    )


@pytest.mark.parametrize(
    ("base", "head", "head_repository", "accepted"),
    [
        ("master", "dev", "xormania/chess-crawl", True),
        ("master", "dev", "contributor/chess-crawl", False),
        ("master", "feature", "xormania/chess-crawl", False),
        ("dev", "dev", "xormania/chess-crawl", False),
    ],
    ids=["repository-dev", "fork-dev", "work-branch", "wrong-target"],
)
def test_promotion_checks_source_repository_and_branches(
    base: str, head: str, head_repository: str, accepted: bool,
) -> None:
    result = _promotion_guard({"github": {
        "base_ref": base,
        "head_ref": head,
        "repository": "xormania/chess-crawl",
        "event": {"pull_request": {"head": {"repo": {"full_name": head_repository}}}},
    }})
    assert (result.returncode == 0) is accepted, result.stdout + result.stderr


def test_ci_revalidates_retargeted_pull_requests() -> None:
    trigger = _workflow().split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
    activity_types = trigger.split("types: [", 1)[1].split("]", 1)[0]
    assert {"opened", "synchronize", "reopened", "edited"} <= {
        activity.strip() for activity in activity_types.split(",")
    }
