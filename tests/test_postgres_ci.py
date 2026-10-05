"""Exercise disposable CI database readiness, cleanup, and workflow selection."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".github/scripts/test_postgres.py"
PASSWORD = "ci-test-secret-do-not-log"
CONTAINER = "chess-crawl-test-db"


def _state(status: str = "healthy") -> str:
    return json.dumps({"Running": True, "Status": "running", "Health": {"Status": status}})


class Docker:
    """A subprocess boundary with controlled Docker observations and failures."""

    def __init__(self) -> None:
        self.states: list[str] = []
        self.default_state = _state()
        self.listing = CONTAINER + "\n"
        self.failures: set[str] = set()
        self.exceptions: dict[str, Exception] = {}
        self.calls: list[list[str]] = []

    def run(self, command: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(command)
        assert command[0] == "docker"
        assert options["capture_output"] is True and options["text"] is True
        assert options.get("shell", False) is False
        assert 0 < options["timeout"] <= 120
        operation = " ".join(command[1:3]) if command[1] == "container" else command[1]
        if operation in self.exceptions:
            raise self.exceptions[operation]
        if operation in self.failures:
            return subprocess.CompletedProcess(command, 17, stdout=PASSWORD, stderr=PASSWORD)
        if operation == "inspect":
            output = self.states.pop(0) if self.states else self.default_state
        elif operation == "container ls":
            output = self.listing
        elif operation in {"run", "rm"}:
            output = CONTAINER + "\n"
        else:
            raise AssertionError(f"Unexpected Docker operation: {operation}")
        return subprocess.CompletedProcess(command, 0, stdout=output, stderr="")


class Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, duration: float) -> None:
        assert duration > 0
        self.sleeps.append(duration)
        self.now += duration


@pytest.fixture
def helper(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    spec = importlib.util.spec_from_file_location("disposable_postgres_ci", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("POSTGRES_PASSWORD", PASSWORD)
    return module


@pytest.fixture
def docker(monkeypatch: pytest.MonkeyPatch) -> Docker:
    boundary = Docker()
    monkeypatch.setattr(subprocess, "run", boundary.run)
    return boundary


def _no_secret(capsys: pytest.CaptureFixture[str]) -> str:
    output = capsys.readouterr()
    combined = output.out + output.err
    assert PASSWORD not in combined
    return combined


def test_start_waits_for_health_and_keeps_password_out_of_process_arguments(
    helper: ModuleType, docker: Docker, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    docker.states = [_state("starting"), _state("starting"), _state()]
    clock = Clock()
    monkeypatch.setattr(helper, "time", clock)

    assert helper.main(["start"]) == 0

    assert [command[1] for command in docker.calls] == ["run", "inspect", "inspect", "inspect"]
    launch = docker.calls[0]
    assert launch[launch.index("--env") + 1] == "POSTGRES_PASSWORD"
    assert launch[launch.index("--publish") + 1] == "127.0.0.1:5432:5432"
    assert launch[-1] == "postgres:18"
    assert len(clock.sleeps) == 2
    assert PASSWORD not in json.dumps(docker.calls)
    assert "start complete" in _no_secret(capsys)


@pytest.mark.parametrize("password", [None, ""])
def test_start_rejects_absent_credentials_before_starting_container(
    helper: ModuleType, docker: Docker, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], password: str | None,
) -> None:
    if password is None:
        monkeypatch.delenv("POSTGRES_PASSWORD")
    else:
        monkeypatch.setenv("POSTGRES_PASSWORD", password)
    assert helper.main(["start"]) == 1
    assert not docker.calls
    assert "must be set" in _no_secret(capsys)


@pytest.mark.parametrize("operation", ["run", "inspect"])
def test_start_propagates_failed_docker_commands_without_echoing_diagnostics(
    helper: ModuleType, docker: Docker, capsys: pytest.CaptureFixture[str], operation: str,
) -> None:
    docker.failures.add(operation)
    assert helper.main(["start"]) == 1
    assert "start failed" in _no_secret(capsys)
    if operation == "run":
        assert len(docker.calls) == 1


@pytest.mark.parametrize("error", [
    OSError(PASSWORD),
    subprocess.TimeoutExpired(["docker", "inspect"], 5, output=PASSWORD, stderr=PASSWORD),
    UnicodeDecodeError("utf-8", b"\x80", 0, 1, PASSWORD),
])
def test_inspect_transport_failures_are_closed_and_redacted(
    helper: ModuleType, docker: Docker, capsys: pytest.CaptureFixture[str], error: Exception,
) -> None:
    docker.exceptions["inspect"] = error
    assert helper.main(["start"]) == 1
    assert "start failed" in _no_secret(capsys)


@pytest.mark.parametrize("observation", [
    PASSWORD,
    "null",
    "[]",
    "{}",
    json.dumps({"Running": 1, "Status": "running", "Health": {"Status": "healthy"}}),
    json.dumps({"Running": False, "Status": "exited", "Health": {"Status": "healthy"}}),
    json.dumps({"Running": True, "Status": "exited", "Health": {"Status": "healthy"}}),
    json.dumps({"Running": True, "Status": "running"}),
    json.dumps({"Running": True, "Status": "running", "Health": []}),
    json.dumps({"Running": True, "Status": "running", "Health": {}}),
    json.dumps({"Running": True, "Status": "running", "Health": {"Status": ["healthy"]}}),
    json.dumps({"Running": True, "Status": "running", "Health": {"Status": True}}),
    _state("unknown"),
    _state("unhealthy"),
])
def test_start_rejects_unusable_readiness_observations(
    helper: ModuleType, docker: Docker, capsys: pytest.CaptureFixture[str], observation: str,
) -> None:
    docker.default_state = observation
    assert helper.main(["start"]) == 1
    assert len(docker.calls) == 2
    assert "start failed" in _no_secret(capsys)


def test_start_has_a_finite_readiness_deadline(
    helper: ModuleType, docker: Docker, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    docker.default_state = _state("starting")
    clock = Clock()
    monkeypatch.setattr(helper, "time", clock)
    monkeypatch.setattr(helper, "READINESS_TIMEOUT", 2.5)

    assert helper.main(["start"]) == 1

    assert clock.now == 2.5
    assert len(docker.calls) < 10
    assert "deadline" in _no_secret(capsys)


def test_a_late_healthy_observation_cannot_override_the_readiness_deadline(
    helper: ModuleType, docker: Docker, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    clock = Clock()
    monkeypatch.setattr(helper, "time", clock)
    monkeypatch.setattr(helper, "READINESS_TIMEOUT", 2.5)

    def delayed_inspection(command: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        if command[1] == "inspect":
            clock.now += 3.0
        return docker.run(command, **options)

    monkeypatch.setattr(subprocess, "run", delayed_inspection)
    assert helper.main(["start"]) == 1
    assert "deadline" in _no_secret(capsys)


@pytest.mark.parametrize("present", [False, True])
def test_cleanup_distinguishes_absence_from_a_container_to_remove(
    helper: ModuleType, docker: Docker, capsys: pytest.CaptureFixture[str], present: bool,
) -> None:
    docker.listing = CONTAINER + "\n" if present else ""
    assert helper.main(["stop"]) == 0
    assert [command[1] for command in docker.calls] == (["container", "rm"] if present else ["container"])
    if present:
        assert docker.calls[-1] == ["docker", "rm", "--force", "--volumes", CONTAINER]
    assert "stop complete" in _no_secret(capsys)


@pytest.mark.parametrize("operation", ["container ls", "rm"])
def test_cleanup_fails_if_docker_daemon_or_removal_fails(
    helper: ModuleType, docker: Docker, capsys: pytest.CaptureFixture[str], operation: str,
) -> None:
    docker.failures.add(operation)
    assert helper.main(["stop"]) == 1
    assert "stop failed" in _no_secret(capsys)


@pytest.mark.parametrize("listing", ["another-container\n", CONTAINER + "\nother\n", "\n", PASSWORD + "\n"])
def test_cleanup_rejects_ambiguous_or_malformed_container_listing(
    helper: ModuleType, docker: Docker, capsys: pytest.CaptureFixture[str], listing: str,
) -> None:
    docker.listing = listing
    assert helper.main(["stop"]) == 1
    assert len(docker.calls) == 1
    assert "stop failed" in _no_secret(capsys)


@pytest.mark.parametrize("arguments", [[], ["invalid"], ["start", "--unknown"]])
def test_real_cli_rejects_invalid_operations_without_exposing_environment_password(
    arguments: list[str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", PASSWORD)
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *arguments], capture_output=True, text=True, timeout=5,
    )
    assert result.returncode == 2
    assert PASSWORD not in result.stdout + result.stderr
    assert "error:" in result.stderr


def _offline_job() -> str:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    job = workflow.split("\n  offline-checks:\n", 1)[1]
    return re.split(r"\n  [\w-]+:\n", job, maxsplit=1)[0]


def _step(name: str) -> dict[str, str]:
    block = _offline_job().split(f"      - name: {name}\n", 1)[1].split("\n      - ", 1)[0]
    properties = {}
    for line in block.splitlines():
        if line.startswith("        ") and not line.startswith("         "):
            key, value = line.strip().split(":", 1)
            properties[key] = value.strip()
    return properties


def _selected(condition: str, offline: str, database: str, previous_success: bool) -> bool:
    # Evaluate the small GitHub expression subset used by these steps. Unknown
    # predicates are rejected rather than silently interpreted as a scoped skip.
    predicates = {predicate.strip() for predicate in condition.split("&&")}
    scope_predicates = {"steps.scope.outputs.offline == 'true'", "steps.scope.outputs.database == 'true'"}
    assert scope_predicates <= predicates <= scope_predicates | {"always()"}
    assert offline in {"true", "false"} and database in {"true", "false"}
    return offline == database == "true" and (previous_success or "always()" in predicates)


def _docker_executable(tmp_path: Path) -> tuple[Path, Path]:
    directory = tmp_path / "bin"
    directory.mkdir()
    executable = directory / "docker"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "with open(os.environ['FAKE_DOCKER_EVENTS'], 'a') as stream:\n"
        "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "if sys.argv[1] == os.environ.get('FAKE_DOCKER_FAIL'):\n"
        "    print(os.environ['POSTGRES_PASSWORD'], file=sys.stderr)\n"
        "    raise SystemExit(17)\n"
        "if sys.argv[1] == 'inspect':\n"
        "    print(json.dumps({'Running': True, 'Status': 'running', 'Health': {'Status': 'healthy'}}))\n"
        "else:\n"
        "    print('chess-crawl-test-db')\n"
    )
    executable.chmod(0o700)
    return directory, tmp_path / "docker-events.jsonl"


@pytest.mark.parametrize(("offline", "database", "fail_start"), [
    ("false", "false", False), ("false", "true", False),
    ("true", "false", False), ("true", "false", True),
    ("true", "true", False), ("true", "true", True),
], ids=["docs", "offline-skip", "deployment", "deployment-no-start", "full", "failed-start-cleanup"])
def test_workflow_runs_timed_database_lifecycle_only_for_selected_offline_checks(
    tmp_path: Path, offline: str, database: str, fail_start: bool,
) -> None:
    job = _offline_job()
    assert not re.search(r"^    services:", job, re.MULTILINE)
    start = _step("Start disposable PostgreSQL")
    stop = _step("Remove disposable PostgreSQL")
    # The workflow binds a secret by environment name, never in its shell argv.
    assert "POSTGRES_PASSWORD: ${{ env.CHESS_CRAWL_TEST_DATABASE_PASSWORD }}" in job
    assert "always()" in stop["if"]
    directory, events = _docker_executable(tmp_path)
    evidence = tmp_path / "measurements"
    environment = {
        **os.environ,
        "PATH": str(directory) + os.pathsep + os.environ.get("PATH", ""),
        "POSTGRES_PASSWORD": PASSWORD,
        "FAKE_DOCKER_EVENTS": str(events),
        "FAKE_DOCKER_FAIL": "run" if fail_start else "",
        "CI_PERFORMANCE_DIR": str(evidence),
    }
    succeeded = True
    results = []
    for step in (start, stop):
        if not _selected(step["if"], offline, database, succeeded):
            continue
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-c", step["run"]],
            cwd=ROOT, env=environment, capture_output=True, text=True, timeout=10,
        )
        results.append(result)
        succeeded = succeeded and result.returncode == 0
        assert PASSWORD not in result.stdout + result.stderr

    if offline == "false" or database == "false":
        assert not results and not events.exists() and not evidence.exists()
        return
    assert len(results) == 2
    assert (results[0].returncode != 0) is fail_start
    assert results[1].returncode == 0
    samples = {sample["label"]: sample for sample in (
        json.loads(path.read_text()) for path in evidence.glob("*.json")
    )}
    assert set(samples) == {"postgres-ready", "postgres-cleanup"}
    assert samples["postgres-ready"]["status"] == ("failure" if fail_start else "success")
    assert samples["postgres-cleanup"]["status"] == "success"
    recorded = events.read_text()
    assert PASSWORD not in recorded
    assert all(PASSWORD not in path.read_text() for path in evidence.glob("*.json"))
    commands = [json.loads(line) for line in recorded.splitlines()]
    assert commands[0][0] == "run" and commands[-1][0] == "rm"


@pytest.mark.parametrize("condition", [
    "steps.scope.outputs.offline == 'true'",
    "steps.scope.outputs.database == 'true'",
    "steps.scope.outputs.offline == 'true' && steps.scope.outputs.database == 'true' && unknown()",
    "steps.scope.outputs.offline == 'true' || steps.scope.outputs.database == 'true'",
])
def test_lifecycle_condition_evaluator_rejects_unknown_or_incomplete_predicates(condition: str) -> None:
    with pytest.raises(AssertionError):
        _selected(condition, "true", "true", True)
