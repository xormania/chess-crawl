"""Exercise workflow triggers, event isolation, and dependency guards."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest


def _workflow(name: str = "ci") -> str:
    return (Path(__file__).resolve().parents[1] / ".github" / "workflows" / f"{name}.yml").read_text()


def _context_value(context: dict[str, Any], expression: str) -> Any:
    value: Any = context
    for part in expression.strip().split("."):
        # GitHub resolves absent properties to an empty string.
        value = value.get(part, "") if isinstance(value, dict) else ""
    return value


def _job(job_id: str) -> str:
    job = _workflow().split(f"\n  {job_id}:\n", 1)[1]
    return re.split(r"\n  [\w-]+:\n", job, maxsplit=1)[0]


def _step(job_id: str, step_name: str) -> str:
    return _job(job_id).split(f"      - name: {step_name}\n", 1)[1].split(
        "\n      - ", 1,
    )[0]


def _shell_script(step: str) -> str:
    script_block = step.split("        run: |\n", 1)[1]
    script_lines = []
    for line in script_block.splitlines():
        if line and not line.startswith("          "):
            break
        script_lines.append(line[10:])
    script = "\n".join(script_lines)
    assert script.strip()
    return script


def _run_guard(
    job_id: str,
    step_name: str,
    context: dict[str, Any],
    summary: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    # Extract the existing scalar blocks, not a second implementation of the
    # policy. Resolving its environment bindings also catches workflow wiring
    # errors (such as comparing the base repository with itself).
    step = _step(job_id, step_name)
    environment = step.split("        run: |\n", 1)[0]
    env = os.environ.copy()
    for line in environment.splitlines():
        if not line.startswith("          "):
            continue
        name, expression = line.strip().split(":", 1)
        expression = expression.strip()
        assert expression.startswith("${{ ") and expression.endswith(" }}")
        env[name] = str(_context_value(context, expression[4:-3]))
    if summary is not None:
        env["GITHUB_STEP_SUMMARY"] = str(summary)
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-c", _shell_script(step)],
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
    event_name: str = "pull_request",
    database_output: str | None = "automatic",
    compose_output: str | None = None,
) -> subprocess.CompletedProcess[str]:
    selected_scope = "offline" if job_id == "offline-checks" else "compose"
    other_scope = "compose" if selected_scope == "offline" else "offline"
    # Different output values expose guards wired to the other job's scope.
    outputs = {other_scope: "false" if output == "true" else "true"}
    if output is not None:
        outputs[selected_scope] = output
    if database_output is not None:
        outputs["database"] = ("true" if output == "true" else "false") if database_output == "automatic" else database_output
    if compose_output is not None:
        outputs["compose"] = compose_output
    return _run_guard(job_id, "Validate CI prerequisites", {
        "github": {"base_ref": base_ref, "event_name": event_name},
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


@pytest.mark.parametrize("workflow", ["ci", "devbox"])
def test_validation_workflows_cover_master_pushes_and_manual_runs(workflow: str) -> None:
    trigger = _workflow(workflow).split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
    events = dict(re.findall(r"^  (\w+):([^\n]*(?:\n(?:    [^\n]*|))*)", trigger, re.MULTILINE))
    assert set(events) == {"pull_request", "push", "workflow_dispatch"}
    assert events["push"].strip() == "branches: [master]"
    assert not events["workflow_dispatch"].strip()
    branches = re.split(
        r"\n    \w+:", events["pull_request"].split("\n    branches:", 1)[1], maxsplit=1,
    )[0].strip()
    branch_values = (
        branches.strip("[]").split(",") if branches.startswith("[")
        else re.findall(r"(?:^|\n)\s*- (.+)", branches)
    )
    assert {branch.strip(" '\"") for branch in branch_values} == {"dev", "work/**", "master"}
    activity_types = events["pull_request"].split("types: [", 1)[1].split("]", 1)[0]
    assert {activity.strip() for activity in activity_types.split(",")} == {
        "opened", "synchronize", "reopened", "edited",
    }


def test_changelog_remains_a_pull_request_policy() -> None:
    trigger = _workflow("changelog").split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
    assert re.findall(r"^  (\w+):", trigger, re.MULTILINE) == ["pull_request"]


def test_promotion_source_guard_only_applies_to_master_pull_requests() -> None:
    header = _job("promotion-source").split("    steps:\n", 1)[0]
    condition = re.findall(r"^    if: (.+)$", header, re.MULTILINE)
    assert condition == ["github.event_name == 'pull_request' && github.base_ref == 'master'"]


def test_concurrency_separates_events_refs_and_workflows() -> None:
    groups = []
    for workflow in ("ci", "devbox"):
        block = _workflow(workflow).split("\nconcurrency:\n", 1)[1].split("\njobs:\n", 1)[0]
        assert re.findall(r"^  cancel-in-progress: (.+)$", block, re.MULTILINE) == ["true"]
        group = re.findall(r"^  group: (.+)$", block, re.MULTILINE)[0]
        for event_name, ref, pr_number in (
            ("pull_request", "refs/pull/23/merge", 23),
            ("pull_request", "refs/pull/24/merge", 24),
            ("push", "refs/heads/master", None),
            ("workflow_dispatch", "refs/heads/master", None),
            ("workflow_dispatch", "refs/heads/dev", None),
        ):
            context = {"github": {
                "workflow": workflow, "event_name": event_name, "ref": ref,
                "event": {"pull_request": {"number": pr_number}} if pr_number else {},
            }}

            def resolve(match: re.Match[str]) -> str:
                # Only the scalar lookups and fallback operator used by this
                # group are resolved; the workflow supplies the actual policy.
                return str(next((
                    value for operand in match[1].split("||")
                    if (value := _context_value(context, operand))
                ), ""))

            resolved = re.sub(r"\$\{\{(.*?)\}\}", resolve, group)
            assert resolved == f"{workflow}-{event_name}-{pr_number or ref}"
            groups.append(resolved)
    assert len(set(groups)) == len(groups)


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
        "Checkout tested revision and its parents",
        "Determine affected checks",
        "Validate CI prerequisites",
    ]
    checkout, classifier, guard = properties[:3]
    assert checkout["uses"].strip().startswith("actions/checkout@")
    assert "\n          fetch-depth: 2\n" in steps[0]
    assert classifier["id"].strip() == "scope"
    assert classifier["run"].strip() == (
        'python .github/scripts/ci_scope.py --event-name "$EVENT_NAME" --base-ref "$BASE_REF"'
    )
    assert "\n          EVENT_NAME: ${{ github.event_name }}\n" in steps[1]
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
    accepted = base_ref != "master" or output == "true"
    assert (result.returncode == 0) is accepted, result.stdout + result.stderr
    if accepted:
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


@pytest.mark.parametrize("job_id", ["offline-checks", "compose-smoke"])
@pytest.mark.parametrize("event_name", ["push", "workflow_dispatch"])
@pytest.mark.parametrize("output", ["true", "false", None, ""])
def test_non_pr_checks_require_full_scope_without_a_promotion(
    job_id: str, event_name: str, output: str | None, tmp_path: Path,
) -> None:
    result = _ci_guard(
        job_id, tmp_path / "summary", event_name=event_name,
        base_ref="", promotion_result="skipped", output=output,
    )
    assert (result.returncode == 0) is (output == "true"), result.stdout + result.stderr


def _compose_files() -> str:
    header = _job("compose-smoke").split("    steps:\n", 1)[0]
    values = re.findall(r"^      COMPOSE_FILE: (.+)$", header, flags=re.MULTILINE)
    assert values == ["compose.yaml:.github/compose.ci.yaml"]
    return values[0]


def test_compose_overlay_applies_to_the_whole_job() -> None:
    assert _compose_files() == "compose.yaml:.github/compose.ci.yaml"
    # A step override could make validation, startup or cleanup operate on a
    # different stack than the measured build.
    assert len(re.findall(r"^\s+COMPOSE_FILE:", _job("compose-smoke"), re.MULTILINE)) == 1
    assert "COMPOSE_FILE=" not in _job("compose-smoke")


@pytest.mark.parametrize(
    ("build_status", "pull_status"),
    [(0, 0), (7, 0), (0, 9), (7, 9)],
    ids=["both-succeed", "build-fails", "pull-fails", "both-fail"],
)
def test_parallel_image_preparation_waits_for_both_and_preserves_failure(
    build_status: int, pull_status: int, tmp_path: Path,
) -> None:
    fake_python = tmp_path / "python"
    fake_python.write_text(f"#!{sys.executable}\n" + '''
import json
import os
from pathlib import Path
import sys
import time

directory = Path(os.environ["TEST_PARALLEL_DIR"])
arguments = sys.argv[1:]
assert arguments[:2] == [".github/scripts/ci_performance.py", "measure"]
label = arguments[arguments.index("--label") + 1]
commands = {
    "image-build": ("build", ["docker", "compose", "build", "api"]),
    "mercure-pull": ("pull", ["docker", "compose", "pull", "mercure", "postgres"]),
}
role, expected = commands[label]
command = arguments[arguments.index("--") + 1:]
assert command == expected, command
assert arguments[arguments.index("--output-dir") + 1] == os.environ["CI_PERFORMANCE_DIR"]
record = {"command": command, "compose_files": os.environ.get("COMPOSE_FILE")}
(directory / f"{role}-started.json").write_text(json.dumps(record))

def wait_for(name):
    deadline = time.monotonic() + 5
    while not (directory / name).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Timed out waiting for {name}")
        time.sleep(0.01)

# Serial execution cannot satisfy this handshake: both commands must start
# before either completes.
wait_for("pull-started.json" if role == "build" else "build-started.json")
if role == "pull":
    wait_for("release-pull")
status = int(os.environ[f"TEST_{role.upper()}_STATUS"])
(directory / f"{role}-finished.json").write_text(json.dumps({"status": status}))
raise SystemExit(status)
''')
    fake_python.chmod(0o755)
    environment = os.environ | {
        "PATH": f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}",
        "TEST_PARALLEL_DIR": str(tmp_path),
        "TEST_BUILD_STATUS": str(build_status),
        "TEST_PULL_STATUS": str(pull_status),
        "CI_PERFORMANCE_DIR": str(tmp_path / "measurements"),
        "COMPOSE_FILE": _compose_files(),
    }
    script = _shell_script(_step(
        "compose-smoke", "Build application and pull infrastructure concurrently",
    ))
    log = tmp_path / "shell.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            ["bash", "--noprofile", "--norc", "-e", "-c", script],
            env=environment, stdout=output, stderr=subprocess.STDOUT, text=True,
        )
        try:
            deadline = time.monotonic() + 5
            while not (tmp_path / "build-finished.json").exists():
                assert process.poll() is None, log.read_text()
                assert time.monotonic() < deadline, log.read_text()
                time.sleep(0.01)
            # Build has exited, including in its failure cases. The shell must
            # remain waiting for the still-blocked pull. This is a bounded
            # synchronization check, not a runner-speed performance assertion.
            with pytest.raises(subprocess.TimeoutExpired):
                process.wait(timeout=0.1)
        finally:
            (tmp_path / "release-pull").touch()
            process.wait(timeout=5)
    assert (process.returncode == 0) is (build_status == pull_status == 0), log.read_text()
    for role, status in (("build", build_status), ("pull", pull_status)):
        started = json.loads((tmp_path / f"{role}-started.json").read_text())
        assert started["compose_files"] == "compose.yaml:.github/compose.ci.yaml"
        assert json.loads((tmp_path / f"{role}-finished.json").read_text()) == {"status": status}


@pytest.mark.parametrize("case", ["clean", "findings", "syntax-error", "missing-directory", "excluded-file"])
def test_bandit_step_scans_all_targets_and_preserves_failure(case: str, tmp_path: Path) -> None:
    """Execute the workflow command with the real scanner, configuration and timer."""
    root = Path(__file__).resolve().parents[1]
    project = tmp_path / "project"
    targets = ("src", "scripts", "docker", ".github/scripts")
    for target in targets:
        directory = project / target
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "probe.py").write_text('"""Harmless fixture."""\n')
    for path in ("pyproject.toml", ".github/scripts/check_bandit.py", ".github/scripts/ci_performance.py"):
        shutil.copyfile(root / path, project / path)
    smoke = project / "scripts/compose_smoke.py"
    smoke.write_text("assert True\n")
    if case == "findings":
        for target in targets:
            (project / target / "probe.py").write_text("eval(input())\n")
        # Only B101 in this exact executable test is exempt. Other rules and
        # application assertions/SQL must still be reported.
        smoke.write_text("assert True\neval(input())\n")
        (project / "src/unsafe.py").write_text(
            'assert True\ndef query(conn, value):\n'
            '    return conn.execute(f"SELECT id FROM users WHERE name = {value}")\n',
        )
    elif case == "syntax-error":
        (project / "src/probe.py").write_text("def broken(\n")
    elif case == "missing-directory":
        shutil.rmtree(project / "docker")
    elif case == "excluded-file":
        with (project / "pyproject.toml").open("a") as config:
            config.write('\n[tool.bandit]\nexclude_dirs = ["src"]\n')
    step = _step("offline-checks", "Bandit")
    assert "continue-on-error" not in step
    assert "if: steps.scope.outputs.offline == 'true' && matrix.python-version == '3.11'\n" in step
    command = step.split("        run: ", 1)[1].strip()
    evidence = tmp_path / "evidence"
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-c", command], cwd=project,
        env=os.environ | {
            "CI_PERFORMANCE_DIR": str(evidence), "UV_OFFLINE": "1",
            "UV_PROJECT_ENVIRONMENT": sys.prefix,
        },
        capture_output=True, text=True, timeout=30,
    )
    output = result.stdout + result.stderr
    assert (result.returncode == 0) is (case == "clean"), output
    sample = json.loads(next(evidence.glob("bandit-*.json")).read_text())
    assert sample["returncode"] == result.returncode
    assert sample["status"] == ("success" if case == "clean" else "failure")
    if case == "findings":
        for target in targets:
            assert f"{target}/probe.py:1: B307" in output
        assert "scripts/compose_smoke.py:2: B307" in output
        assert "scripts/compose_smoke.py:1: B101" not in output
        assert "src/unsafe.py:1: B101" in output
        assert "src/unsafe.py:3: B608" in output
    elif case == "syntax-error":
        assert "Unscanned file: src/probe.py" in output
    elif case == "missing-directory":
        assert "Missing scan directory: docker" in output
    elif case == "excluded-file":
        assert "Bandit did not scan exactly the expected Python files" in output
    else:
        assert "0 findings" in output


@pytest.mark.parametrize("optimization", ["flag", "environment"])
def test_compose_smoke_rejects_disabled_assertions(optimization: str) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts/compose_smoke.py"
    command = [sys.executable, *(["-O"] if optimization == "flag" else []), str(script)]
    result = subprocess.run(
        command, env=os.environ | {"PYTHONOPTIMIZE": "1" if optimization == "environment" else ""},
        capture_output=True, text=True, timeout=5,
    )
    assert result.returncode != 0
    assert "Compose smoke requires assertions" in result.stderr
    assert "FileNotFoundError" not in result.stderr  # Fail before reading credentials or contacting Docker.


@pytest.mark.parametrize(("step_name", "condition", "phase"), [
    ("Start disposable PostgreSQL", "steps.scope.outputs.offline == 'true' && steps.scope.outputs.database == 'true'", "start"),
    ("Remove disposable PostgreSQL", "always() && steps.scope.outputs.offline == 'true' && steps.scope.outputs.database == 'true'", "stop"),
])
def test_postgres_lifecycle_respects_scope_and_failure_cleanup(step_name: str, condition: str, phase: str) -> None:
    step = _step("offline-checks", step_name)
    conditions = re.findall(r"^        if: (.+)$", step, flags=re.MULTILINE)
    assert conditions == [condition]
    assert f"-- python .github/scripts/test_postgres.py {phase}" in step
    assert "continue-on-error" not in step


def test_postgres_starts_after_scope_validation_and_has_no_unconditional_job_service() -> None:
    job = _job("offline-checks")
    header = job.split("    steps:\n", 1)[0]
    assert "services:" not in header
    assert job.index("Validate CI prerequisites") < job.index("Start disposable PostgreSQL")
    assert job.index("Start disposable PostgreSQL") < job.index("Offline tests")
    assert job.index("Offline tests") < job.index("Remove disposable PostgreSQL")


def test_offline_step_uses_verified_merge_selection_and_retains_event_context() -> None:
    step = _step("offline-checks", "Offline tests")
    assert "EVENT_NAME: ${{ github.event_name }}" in step
    assert "BASE_REF: ${{ github.base_ref }}" in step
    assert 'ci_scope.py --event-name "$EVENT_NAME" --base-ref "$BASE_REF" --run-tests' in step
    assert "if: steps.scope.outputs.offline == 'true'" in step
    assert "continue-on-error" not in step


def test_devbox_cache_preserves_setup_and_toolchain_verification() -> None:
    workflow = _workflow("devbox")
    installer = workflow.split("      - name: Install the locked development toolchain\n", 1)[1].split("      - name:", 1)[0]
    assert "enable-cache: true" in installer
    assert "jetify-com/devbox-install-action@a0d2d53632934ae004f878c840055956d9f741b0" in installer
    for name in ("Set up the uv environment", "Verify tool and interpreter selection", "Ensure setup preserves both lock files"):
        step = workflow.split(f"      - name: {name}\n", 1)[1].split("      - name:", 1)[0]
        assert "if:" not in step and "continue-on-error" not in step


@pytest.mark.parametrize("database_output", [None, "", "yes", "True", "FALSE"])
def test_offline_guard_rejects_missing_or_invalid_database_output(tmp_path: Path, database_output: str | None) -> None:
    result = _ci_guard("offline-checks", tmp_path / "summary", database_output=database_output)
    assert result.returncode != 0


@pytest.mark.parametrize("event_name", ["push", "workflow_dispatch"])
def test_non_pr_guard_rejects_no_database_lane(tmp_path: Path, event_name: str) -> None:
    result = _ci_guard(
        "offline-checks", tmp_path / "summary", event_name=event_name,
        database_output="false", compose_output="true",
    )
    assert result.returncode != 0


def test_master_guard_rejects_no_database_lane(tmp_path: Path) -> None:
    result = _ci_guard(
        "offline-checks", tmp_path / "summary", base_ref="master", promotion_result="success",
        database_output="false", compose_output="true",
    )
    assert result.returncode != 0


@pytest.mark.parametrize(("offline", "database", "compose", "accepted"), [
    ("true", "false", "true", True),
    ("true", "false", "false", False),
    ("true", "true", "true", True),
    ("true", "true", "false", True),
    ("false", "true", "false", False),
    ("false", "false", "false", True),
])
def test_offline_guard_validates_database_scope_consistency(
    tmp_path: Path, offline: str, database: str, compose: str, accepted: bool,
) -> None:
    result = _ci_guard(
        "offline-checks", tmp_path / "summary", output=offline,
        database_output=database, compose_output=compose,
    )
    assert (result.returncode == 0) is accepted, result.stdout + result.stderr
