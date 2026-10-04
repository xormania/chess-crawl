"""Exercise scope decisions and actual Git merge/rename behavior."""

from __future__ import annotations

import os
import runpy
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

from ci_support import commit as _commit
from ci_support import git as _git
from ci_support import merge_feature as _merge
from ci_support import repository
from ci_support import write as _write


SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/ci_scope.py"


@pytest.mark.parametrize(
    ("paths", "expected"),
    [
        (["AGENTS.md", "PROJECT.md", "CONTRIBUTING.md", "CHANGELOG.md"], (False, False)),
        (["docs/cli.md", "docs/nested/guide.md", ".github/pull_request_template.md"], (False, False)),
        (["tests/test_api.py", "tests/fixtures/game.json", "docs/cli.md"], (True, False)),
        (["README.md"], (True, True)),
        (["LICENSE"], (True, True)),
        (["pyproject.toml"], (True, True)),
        (["uv.lock"], (True, True)),
        (["src/chess_crawl/storage/schema.sql"], (True, True)),
        (["Dockerfile", ".dockerignore", "compose.yaml"], (False, True)),
        (["docker/mercure-entrypoint.sh"], (False, True)),
        ([".env.example"], (False, True)),
        ([".github/compose.ci.yaml"], (False, True)),
        (["docker/healthcheck.py"], (True, True)),
        (["docker/new-entrypoint.sh"], (True, True)),
        (["scripts/bootstrap_dev.py"], (True, True)),
        (["scripts/compose_smoke.py"], (True, True)),
        ([".github/workflows/ci.yml"], (True, True)),
        ([".github/workflows/changelog.yml"], (True, False)),
        ([".github/scripts/check_changelog.py"], (True, False)),
        ([".github/scripts/ci_scope.py"], (True, True)),
        (["tests/test_api.py", "compose.yaml"], (True, True)),
        ([".github/workflows/changelog.yml", "Dockerfile"], (True, True)),
        (["docs/cli.md", "compose.yaml"], (False, True)),
        (["docs/example.py"], (True, True)),
        (["new-file.md"], (True, True)),
        ([], (True, True)),
    ],
)
def test_scope_is_conservative(paths: list[str], expected: tuple[bool, bool]) -> None:
    classify = runpy.run_path(str(SCRIPT))["classify"]
    assert cast(tuple[bool, bool], classify(paths, "dev")) == expected
    assert classify(paths, "master") == (True, True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return repository(tmp_path, {"src/original.py": "content\n", "docs/original.md": "content\n"})


def _run(repo: Path, base_ref: str = "dev") -> tuple[subprocess.CompletedProcess[str], str, str]:
    output = repo.parent / "outputs"
    summary = repo.parent / "summary"
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--base-ref", base_ref],
        cwd=repo,
        env={**os.environ, "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(summary)},
        capture_output=True, text=True, timeout=10,
    )
    return result, output.read_text() if output.exists() else "", summary.read_text() if summary.exists() else ""


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        ("src/original.py", "docs/moved.md", "offline=true\ncompose=true\n"),
        ("docs/original.md", "src/moved.py", "offline=true\ncompose=true\n"),
        ("docs/original.md", "tests/moved.md", "offline=true\ncompose=false\n"),
        ("docs/original.md", "docs/moved.md", "offline=false\ncompose=false\n"),
        ("docs/original.md", "compose.yaml", "offline=false\ncompose=true\n"),
        ("src/original.py", "compose.yaml", "offline=true\ncompose=true\n"),
    ],
)
def test_renames_consider_removed_and_added_paths(repo: Path, old: str, new: str, expected: str) -> None:
    (repo / new).parent.mkdir(parents=True, exist_ok=True)
    (repo / old).rename(repo / new)
    _merge(repo)
    result, output, summary = _run(repo)
    assert result.returncode == 0, result.stderr
    assert output == expected
    assert "2 changed paths" in summary


@pytest.mark.parametrize(
    ("path", "expected"),
    [("src/original.py", "offline=true\ncompose=true\n"),
     ("docs/original.md", "offline=false\ncompose=false\n")],
)
def test_deleted_paths_are_classified(repo: Path, path: str, expected: str) -> None:
    (repo / path).unlink()
    _merge(repo)
    result, output, _ = _run(repo)
    assert result.returncode == 0, result.stderr
    assert output == expected


@pytest.mark.parametrize(
    ("feature_path", "base_path", "expected"),
    [("docs/feature.md", "src/base.py", "offline=false\ncompose=false\n"),
     ("src/feature.py", "docs/base.md", "offline=true\ncompose=true\n")],
)
def test_scope_compares_merge_result_with_first_parent(
    repo: Path, feature_path: str, base_path: str, expected: str,
) -> None:
    _write(repo, feature_path)
    _commit(repo)
    _git(repo, "checkout", "dev")
    _write(repo, base_path)
    _commit(repo)
    _git(repo, "merge", "--no-ff", "feature", "-m", "Pull request merge")
    result, output, _ = _run(repo)
    assert result.returncode == 0, result.stderr
    assert output == expected


def test_newline_filename_is_one_complete_path(repo: Path) -> None:
    _write(repo, "docs/guide.md\n")
    _merge(repo)
    result, output, summary = _run(repo)
    assert result.returncode == 0, result.stderr
    assert output == "offline=true\ncompose=true\n"
    assert "1 changed paths" in summary


def test_empty_merge_runs_all_checks(repo: Path) -> None:
    _merge(repo)
    result, output, _ = _run(repo)
    assert result.returncode == 0, result.stderr
    assert output == "offline=true\ncompose=true\n"


def test_promotion_runs_all_checks_for_documentation(repo: Path) -> None:
    _write(repo, "docs/new.md")
    _merge(repo)
    result, output, _ = _run(repo, "master")
    assert result.returncode == 0, result.stderr
    assert output == "offline=true\ncompose=true\n"


def test_non_merge_head_fails_without_outputs(repo: Path) -> None:
    result, output, summary = _run(repo)
    assert result.returncode != 0
    assert "two-parent" in result.stderr
    assert output == summary == ""


def test_missing_git_repository_fails_without_outputs(tmp_path: Path) -> None:
    repo = tmp_path / "no-repository"
    repo.mkdir()
    result, output, summary = _run(repo)
    assert result.returncode != 0
    assert "Cannot determine CI scope" in result.stderr
    assert output == summary == ""


def test_shallow_checkout_without_parents_fails_without_outputs(repo: Path, tmp_path: Path) -> None:
    _write(repo, "docs/new.md")
    _merge(repo)
    shallow = tmp_path / "shallow"
    _git(tmp_path, "clone", "--depth=1", repo.as_uri(), str(shallow))
    result, output, summary = _run(shallow)
    assert result.returncode != 0
    assert "two-parent" in result.stderr
    assert output == summary == ""


def test_depth_two_checkout_contains_enough_merge_history(repo: Path, tmp_path: Path) -> None:
    _write(repo, "docs/new.md")
    _merge(repo)
    shallow = tmp_path / "shallow"
    _git(tmp_path, "clone", "--depth=2", repo.as_uri(), str(shallow))
    assert _git(shallow, "rev-parse", "--is-shallow-repository") == "true"
    result, output, summary = _run(shallow)
    assert result.returncode == 0, result.stderr
    assert output == "offline=false\ncompose=false\n"
    assert "1 changed paths" in summary


@pytest.mark.parametrize("base_ref", ["dev", "work/example", "master"])
def test_mixed_scope_is_order_independent(base_ref: str) -> None:
    classify = runpy.run_path(str(SCRIPT))["classify"]
    assert classify(["compose.yaml", "tests/test_api.py"], base_ref) == (True, True)
    assert classify(["tests/test_api.py", "compose.yaml"], base_ref) == (True, True)
