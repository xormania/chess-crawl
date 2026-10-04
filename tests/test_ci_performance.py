"""Exercise real command measurement and deterministic performance comparisons."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/ci_performance.py"


def run(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], env={**os.environ, **(env or {})},
        capture_output=True, text=True, timeout=15,
    )


def record(
    directory: Path, duration: float, *, label: str = "pytest", variant: str = "python-3.11",
    code: int = 0, python: str = "3.11.16", runner_os: str = "Linux", cache_state: str = "unspecified",
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"sample-{len(list(directory.glob('*.json')))}.json"
    path.write_text(json.dumps({
        "schema_version": 1, "label": label, "duration_seconds": duration,
        "returncode": code, "status": "success" if code == 0 else "failure",
        "metadata": {"variant": variant, "python": python, "runner_os": runner_os,
                     "runner_arch": "X64", "cache_state": cache_state},
    }))
    return path


def compare(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return run("compare", "--baseline", str(tmp_path / "before"), "--candidate", str(tmp_path / "after"), *args)


@pytest.mark.parametrize("code", [0, 7])
def test_measure_executes_command_and_preserves_streams_exit_and_metadata(tmp_path: Path, code: int) -> None:
    result = run(
        "measure", "--label", "pytest", "--output-dir", str(tmp_path), "--", sys.executable, "-c",
        f"import sys; print('actual stdout'); print('actual stderr', file=sys.stderr); sys.exit({code})",
        env={"CI_VARIANT": "python-3.11", "GITHUB_SHA": "abc123", "GITHUB_JOB": "offline-checks",
             "GITHUB_RUN_ATTEMPT": "2", "GITHUB_RUN_ID": "1234", "UNRELATED_SECRET": "private-value",
             "CI_CACHE_STATE": "cold", "ImageOS": "ubuntu24", "ImageVersion": "20261001.1"},
    )
    assert result.returncode == code
    assert "actual stdout" in result.stdout and "actual stderr" in result.stderr
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    sample = json.loads(files[0].read_text())
    assert sample["returncode"] == code
    assert sample["status"] == ("failure" if code else "success")
    assert sample["duration_seconds"] >= 0
    assert sample["metadata"]["variant"] == "python-3.11"
    assert sample["metadata"]["github_sha"] == "abc123"
    assert sample["metadata"]["github_job"] == "offline-checks"
    assert sample["metadata"]["github_run_attempt"] == "2"
    assert sample["metadata"]["github_run_id"] == "1234"
    assert sample["metadata"]["cache_state"] == "cold"
    assert sample["metadata"]["image_os"] == "ubuntu24"
    assert sample["metadata"]["image_version"] == "20261001.1"
    assert sample["metadata"]["python"] and sample["metadata"]["runner_os"]
    assert "private-value" not in files[0].read_text()
    assert "UNRELATED_SECRET" not in files[0].read_text()
    assert "actual stdout" not in files[0].read_text()  # No command arguments or output retained.


def test_measure_retains_repeats_and_report_appends_failures(tmp_path: Path) -> None:
    for code in [0, 9]:
        assert run("measure", "--label", "checks", "--output-dir", str(tmp_path), "--",
                   sys.executable, "-c", f"raise SystemExit({code})").returncode == code
    assert len(list(tmp_path.glob("*.json"))) == 2
    summary = tmp_path / "summary.md"
    summary.write_text("Earlier step\n")
    result = run("report", "--output-dir", str(tmp_path), env={"GITHUB_STEP_SUMMARY": str(summary)})
    assert result.returncode == 0
    assert "checks | 1 | 1 |" in result.stdout
    assert "exit 9" in result.stdout
    assert summary.read_text() == "Earlier step\n" + result.stdout


def test_missing_command_records_failure(tmp_path: Path) -> None:
    result = run("measure", "--label", "missing", "--output-dir", str(tmp_path), "--", str(tmp_path / "absent"))
    assert result.returncode == 127
    sample = json.loads(next(tmp_path.glob("*.json")).read_text())
    assert sample["returncode"] == 127 and sample["status"] == "failure"


@pytest.mark.skipif(os.name != "posix", reason="POSIX signal exit status")
def test_signalled_child_is_recorded_and_remains_failure(tmp_path: Path) -> None:
    result = run("measure", "--label", "signal", "--output-dir", str(tmp_path), "--", sys.executable,
                 "-c", "import os, signal; os.kill(os.getpid(), signal.SIGTERM)")
    assert result.returncode == 143
    assert json.loads(next(tmp_path.glob("*.json")).read_text())["returncode"] == -15


@pytest.mark.parametrize("label", ["../unsafe", "", "has space"])
def test_invalid_labels_do_not_execute_commands(tmp_path: Path, label: str) -> None:
    marker = tmp_path / "executed"
    result = run("measure", "--label", label, "--output-dir", str(tmp_path), "--", sys.executable,
                 "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()")
    assert result.returncode == 2 and not marker.exists()


def test_no_samples_does_not_claim_success(tmp_path: Path) -> None:
    result = run("report", "--output-dir", str(tmp_path), env={"GITHUB_STEP_SUMMARY": ""})
    assert result.returncode == 0 and "No timing samples recorded" in result.stdout
    result = compare(tmp_path, "--fail-on-regression")
    assert result.returncode == 2 and "no performance conclusion" in result.stdout


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [(10, 15, 1), (10, 13, 0), (3, 5, 0), (1, 1.4, 0), (10, 8, 0), (0, 3, 1), (0, 0, 0)],
)
def test_both_regression_thresholds_must_be_exceeded(tmp_path: Path, before: float, after: float, expected: int) -> None:
    record(tmp_path / "before", before)
    record(tmp_path / "after", after)
    assert compare(tmp_path).returncode == 0  # Informational by default.
    result = compare(tmp_path, "--fail-on-regression")
    assert result.returncode == expected, result.stderr


def test_custom_thresholds_and_medians_ignore_isolated_outliers(tmp_path: Path) -> None:
    for duration in [10, 11, 100]:
        record(tmp_path / "before", duration)
    for duration in [12, 13, 200]:
        record(tmp_path / "after", duration)
    result = compare(tmp_path, "--fail-on-regression", "--max-regression-percent", "15", "--min-regression-seconds", "1")
    assert result.returncode == 1
    assert "11.000s → 13.000s" in result.stdout and "n=3/3" in result.stdout


def test_variants_and_nested_downloads_remain_separate(tmp_path: Path) -> None:
    for directory in ["before", "after"]:
        record(tmp_path / directory / "py311", 10, variant="python-3.11")
        record(tmp_path / directory / "py313", 2, variant="python-3.13", python="3.13.15")
    result = compare(tmp_path, "--fail-on-regression")
    assert result.returncode == 0
    assert "python-3.11/pytest: 10.000s → 10.000s" in result.stdout
    assert "python-3.13/pytest: 2.000s → 2.000s" in result.stdout


@pytest.mark.parametrize(
    ("python", "runner_os", "cache_state"),
    [("3.13.15", "Linux", "unspecified"), ("3.11.16", "Windows", "unspecified"), ("3.11.16", "Linux", "warm")],
)
def test_environment_mismatches_are_explicitly_not_comparable(
    tmp_path: Path, python: str, runner_os: str, cache_state: str,
) -> None:
    record(tmp_path / "before", 1)
    record(tmp_path / "after", 20, python=python, runner_os=runner_os, cache_state=cache_state)
    assert compare(tmp_path).returncode == 0
    result = compare(tmp_path, "--fail-on-regression")
    assert result.returncode == 2
    assert "environment mismatch" in result.stdout and "no performance conclusion" in result.stdout


@pytest.mark.parametrize("field", ["image_os", "image_version"])
def test_runner_image_mismatch_is_not_comparable(tmp_path: Path, field: str) -> None:
    paths = [record(tmp_path / directory, 1) for directory in ["before", "after"]]
    # Local records omit both fields and remain comparable.
    assert compare(tmp_path, "--fail-on-regression").returncode == 0
    for index, path in enumerate(paths):
        payload = json.loads(path.read_text())
        payload["metadata"][field] = f"image-{index}"
        path.write_text(json.dumps(payload))
    assert compare(tmp_path).returncode == 0
    result = compare(tmp_path, "--fail-on-regression")
    assert result.returncode == 2
    assert "environment mismatch" in result.stdout and "no performance conclusion" in result.stdout


@pytest.mark.parametrize("label", ["pytest", "new-check"])
def test_failed_candidate_is_never_a_fast_success(tmp_path: Path, label: str) -> None:
    record(tmp_path / "before", 10)
    record(tmp_path / "after", 0.01, code=3, label=label)
    result = compare(tmp_path)
    assert result.returncode == 1
    assert "failed measurement" in result.stdout and "Candidate contains failed commands" in result.stdout
    assert "→" not in result.stdout


def test_missing_samples_and_failed_baseline_do_not_imply_improvement(tmp_path: Path) -> None:
    record(tmp_path / "before", 10, label="removed")
    record(tmp_path / "before", 0.1, label="broken", code=4)
    record(tmp_path / "after", 1, label="broken")
    record(tmp_path / "after", 1, label="added")
    result = compare(tmp_path, "--fail-on-regression")
    assert result.returncode == 2
    assert "no candidate samples" in result.stdout and "new label; no baseline" in result.stdout
    assert "failed measurement" in result.stdout and "no performance conclusion" in result.stdout


def test_strict_comparison_requires_every_baseline_label_but_allows_new_labels(tmp_path: Path) -> None:
    record(tmp_path / "before", 10)
    record(tmp_path / "after", 10)
    record(tmp_path / "after", 1, label="new")
    assert compare(tmp_path, "--fail-on-regression").returncode == 0
    record(tmp_path / "before", 1, label="missing")
    result = compare(tmp_path, "--fail-on-regression")
    assert result.returncode == 2 and "lacks complete comparable evidence" in result.stdout


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), -1, True, "fast"])
def test_invalid_duration_records_fail_explicitly(tmp_path: Path, bad_value: Any) -> None:
    path = record(tmp_path, 1)
    payload = json.loads(path.read_text())
    payload["duration_seconds"] = bad_value
    path.write_text(json.dumps(payload))
    result = run("report", "--output-dir", str(tmp_path))
    assert result.returncode == 2 and "Invalid timing record" in result.stderr
