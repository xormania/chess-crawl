"""Exercise workflow guards with GitHub pull-request and dependency contexts."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest


def _workflow() -> str:
    return (Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml").read_text()


def _job(job_id: str) -> str:
    job = _workflow().split(f"\n  {job_id}:\n", 1)[1]
    return re.split(r"\n  [\w-]+:\n", job, maxsplit=1)[0]


def _run_guard(
    job_id: str,
    step_name: str,
    context: dict[str, Any],
    summary: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    # Extract the existing scalar blocks, not a second implementation of the
    # policy. Resolving its environment bindings also catches workflow wiring
    # errors (such as comparing the base repository with itself).
    step = _job(job_id).split(f"      - name: {step_name}\n", 1)[1]
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
            # GitHub resolves a missing property, including an absent step
            # output, to an empty string.
            value = value.get(part, "") if isinstance(value, dict) else ""
        env[name] = str(value)
    if summary is not None:
        env["GITHUB_STEP_SUMMARY"] = str(summary)
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


def _promotion_guard(context: dict[str, Any]) -> subprocess.CompletedProcess[str]:
    return _run_guard("promotion-source", "Check promotion source", context)


def _ci_guard(
    job_id: str,
    summary: Path,
    *,
    scope_result: str = "success",
    output: str | None = "true",
    base_ref: str = "dev",
    promotion_result: str = "skipped",
) -> subprocess.CompletedProcess[str]:
    selected_scope = "offline" if job_id == "offline-checks" else "compose"
    other_scope = "compose" if selected_scope == "offline" else "offline"
    # Different output values expose guards wired to the other job's scope.
    outputs = {other_scope: "false" if output == "true" else "true"}
    if output is not None:
        outputs[selected_scope] = output
    return _run_guard(job_id, "Validate CI prerequisites", {
        "github": {"base_ref": base_ref},
        "steps": {"scope": {"outcome": scope_result, "outputs": outputs}},
        "needs": {
            "promotion-source": {"result": promotion_result},
        },
    }, summary)


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


@pytest.mark.parametrize("job_id", ["offline-checks", "compose-smoke"])
def test_required_checks_run_even_when_promotion_dependency_is_skipped(
    job_id: str,
) -> None:
    header = _job(job_id).split("    steps:\n", 1)[0]
    properties = dict(
        line.strip().split(":", 1)
        for line in header.splitlines()
        if line.startswith("    ") and not line.startswith("     ")
        and not line.lstrip().startswith("#")
    )
    assert {
        need.strip() for need in properties["needs"].strip().strip("[]").split(",")
    } == {"promotion-source"}
    # Classify within the required jobs; a scope condition at job level could
    # skip the check before its classifier runs.
    assert properties["if"].strip() == "${{ always() && !cancelled() }}"
    expected_names = {
        "offline-checks": "Offline checks (Python ${{ matrix.python-version }})",
        "compose-smoke": "Compose API and Mercure smoke",
    }
    assert properties["name"].strip() == expected_names[job_id]
    if job_id == "offline-checks":
        versions = header.split("python-version: [", 1)[1].split("]", 1)[0]
        assert {version.strip().strip('\"') for version in versions.split(",")} == {
            "3.11", "3.13",
        }


@pytest.mark.parametrize("job_id", ["offline-checks", "compose-smoke"])
def test_required_checks_classify_before_scoping_expensive_steps(job_id: str) -> None:
    steps = _job(job_id).split("    steps:\n", 1)[1].split("      - ")[1:]
    properties = [
        dict(
            line.strip().split(":", 1)
            for line in step.splitlines()
            if line.startswith("name:") or (
                line.startswith("        ") and not line.startswith("         ")
                and not line.lstrip().startswith("#")
            )
        )
        for step in steps
    ]
    assert [step["name"].strip() for step in properties[:3]] == [
        "Checkout merge result and its parents",
        "Determine affected checks",
        "Validate CI prerequisites",
    ]
    checkout, classifier, guard = properties[:3]
    assert checkout["uses"].strip().startswith("actions/checkout@")
    assert "\n          fetch-depth: 2\n" in steps[0]
    assert classifier["id"].strip() == "scope"
    assert classifier["run"].strip() == (
        'python .github/scripts/ci_scope.py --base-ref "$BASE_REF"'
    )
    assert "\n          BASE_REF: ${{ github.base_ref }}\n" in steps[1]
    for prerequisite in (checkout, classifier, guard):
        assert "if" not in prerequisite
        assert "continue-on-error" not in prerequisite
    scope = "offline" if job_id == "offline-checks" else "compose"
    for step in properties[3:]:
        condition = step["if"].strip()
        # Every subsequent step must require this job's scope. Disjunctions
        # could otherwise let expensive work run for a documentation-only PR.
        assert "||" not in condition, step["name"]
        assert f"steps.scope.outputs.{scope} == 'true'" in {
            predicate.strip() for predicate in condition.split("&&")
        }, step["name"]


@pytest.mark.parametrize("job_id", ["offline-checks", "compose-smoke"])
@pytest.mark.parametrize("scope_result", ["failure", "cancelled", "skipped", ""])
@pytest.mark.parametrize("output", ["true", "false"])
def test_required_checks_reject_unsuccessful_classification(
    job_id: str, scope_result: str, output: str, tmp_path: Path,
) -> None:
    result = _ci_guard(
        job_id, tmp_path / "summary", scope_result=scope_result, output=output,
    )
    assert result.returncode != 0, result.stdout + result.stderr


@pytest.mark.parametrize("job_id", ["offline-checks", "compose-smoke"])
@pytest.mark.parametrize("output", [None, "", "yes", "True", "FALSE"])
def test_required_checks_reject_missing_or_invalid_scope_outputs(
    job_id: str, output: str | None, tmp_path: Path,
) -> None:
    result = _ci_guard(job_id, tmp_path / "summary", output=output)
    assert result.returncode != 0, result.stdout + result.stderr


@pytest.mark.parametrize("job_id", ["offline-checks", "compose-smoke"])
@pytest.mark.parametrize("output", ["true", "false"])
@pytest.mark.parametrize("base_ref", ["dev", "work/example", "master"])
def test_required_checks_accept_valid_scope_including_scoped_skips(
    job_id: str, output: str, base_ref: str, tmp_path: Path,
) -> None:
    summary = tmp_path / "summary"
    result = _ci_guard(
        job_id, summary, output=output, base_ref=base_ref,
        promotion_result="success" if base_ref == "master" else "skipped",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert summary.read_text().strip().endswith(f"selected: {output}")


@pytest.mark.parametrize("job_id", ["offline-checks", "compose-smoke"])
@pytest.mark.parametrize("promotion_result", ["failure", "cancelled", "skipped", ""])
@pytest.mark.parametrize("output", ["true", "false"])
def test_required_checks_reject_unsuccessful_master_promotion(
    job_id: str, promotion_result: str, output: str, tmp_path: Path,
) -> None:
    result = _ci_guard(
        job_id, tmp_path / "summary", output=output, base_ref="master",
        promotion_result=promotion_result,
    )
    assert result.returncode != 0, result.stdout + result.stderr
