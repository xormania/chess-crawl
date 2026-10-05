"""Exercise scope decisions and actual Git merge/rename behavior."""

from __future__ import annotations

import json
import os
import runpy
import shutil
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
        (["docs/postgresql-operations.md"], (True, False)),
        (["README.md"], (True, True)),
        (["LICENSE"], (True, True)),
        (["pyproject.toml"], (True, True)),
        (["uv.lock"], (True, True)),
        (["src/chess_crawl/storage/schema.sql"], (True, True)),
        (["Dockerfile", ".dockerignore", "compose.yaml"], (True, True)),
        (["docker/mercure-entrypoint.sh"], (True, True)),
        ([".env.example"], (True, True)),
        ([".github/compose.ci.yaml"], (True, True)),
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
        (["docs/cli.md", "compose.yaml"], (True, True)),
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


def _run(
    repo: Path, base_ref: str | None = "dev", *, event_name: str | None = "pull_request",
) -> tuple[subprocess.CompletedProcess[str], str, str]:
    output = repo.parent / "outputs"
    summary = repo.parent / "summary"
    command = [sys.executable, str(SCRIPT)]
    if event_name is not None:
        command.extend(["--event-name", event_name])
    if base_ref is not None:
        command.extend(["--base-ref", base_ref])
    result = subprocess.run(
        command,
        cwd=repo,
        env={**os.environ, "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(summary)},
        capture_output=True, text=True, timeout=10,
    )
    return result, output.read_text() if output.exists() else "", summary.read_text() if summary.exists() else ""


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        ("src/original.py", "docs/moved.md", "offline=true\ncompose=true\ndatabase=true\n"),
        ("docs/original.md", "src/moved.py", "offline=true\ncompose=true\ndatabase=true\n"),
        ("docs/original.md", "tests/moved.md", "offline=true\ncompose=false\ndatabase=true\n"),
        ("docs/original.md", "docs/moved.md", "offline=false\ncompose=false\ndatabase=false\n"),
        ("docs/original.md", "compose.yaml", "offline=true\ncompose=true\ndatabase=false\n"),
        ("src/original.py", "compose.yaml", "offline=true\ncompose=true\ndatabase=true\n"),
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
    [("src/original.py", "offline=true\ncompose=true\ndatabase=true\n"),
     ("docs/original.md", "offline=false\ncompose=false\ndatabase=false\n")],
)
def test_deleted_paths_are_classified(repo: Path, path: str, expected: str) -> None:
    (repo / path).unlink()
    _merge(repo)
    result, output, _ = _run(repo)
    assert result.returncode == 0, result.stderr
    assert output == expected


@pytest.mark.parametrize(
    ("feature_path", "base_path", "expected"),
    [("docs/feature.md", "src/base.py", "offline=false\ncompose=false\ndatabase=false\n"),
     ("src/feature.py", "docs/base.md", "offline=true\ncompose=true\ndatabase=true\n")],
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
    assert output == "offline=true\ncompose=true\ndatabase=true\n"
    assert "1 changed paths" in summary


def test_empty_merge_runs_all_checks(repo: Path) -> None:
    _merge(repo)
    result, output, _ = _run(repo)
    assert result.returncode == 0, result.stderr
    assert output == "offline=true\ncompose=true\ndatabase=true\n"


def test_promotion_runs_all_checks_for_documentation(repo: Path) -> None:
    _write(repo, "docs/new.md")
    _merge(repo)
    result, output, _ = _run(repo, "master")
    assert result.returncode == 0, result.stderr
    assert output == "offline=true\ncompose=true\ndatabase=true\n"


@pytest.mark.parametrize("history", ["root", "squash", "merge", "shallow"])
@pytest.mark.parametrize("event_name", ["push", "workflow_dispatch"])
def test_non_pr_events_always_run_all_checks(
    repo: Path, tmp_path: Path, history: str, event_name: str,
) -> None:
    if history == "squash":
        _write(repo, "docs/new.md")
        _commit(repo)
        _git(repo, "checkout", "dev")
        _git(repo, "merge", "--squash", "feature")
        _commit(repo)
        assert len(_git(repo, "rev-list", "--parents", "-n", "1", "HEAD").split()) == 2
    elif history in {"merge", "shallow"}:
        _write(repo, "docs/new.md")
        _merge(repo)
        if history == "shallow":
            shallow = tmp_path / "shallow"
            _git(tmp_path, "clone", "--depth=1", repo.as_uri(), str(shallow))
            repo = shallow
    result, output, summary = _run(repo, None, event_name=event_name)
    assert result.returncode == 0, result.stderr
    assert output == "offline=true\ncompose=true\ndatabase=true\n"
    assert event_name in summary


@pytest.mark.parametrize("event_name", ["push", "workflow_dispatch"])
def test_non_pr_scope_does_not_consult_git(tmp_path: Path, event_name: str) -> None:
    # A directory without Git metadata makes any Git-dependent classification
    # fail, even if the implementation were to accidentally accept linear HEADs.
    directory = tmp_path / "no-repository"
    directory.mkdir()
    result, output, _ = _run(directory, "", event_name=event_name)
    assert result.returncode == 0, result.stderr
    assert output == "offline=true\ncompose=true\ndatabase=true\n"


@pytest.mark.parametrize("event_name", [None, "", "pull_request_target", "repository_dispatch"])
def test_missing_or_unsupported_event_fails_without_outputs(
    repo: Path, event_name: str | None,
) -> None:
    _write(repo, "docs/new.md")
    _merge(repo)
    result, output, summary = _run(repo, event_name=event_name)
    assert result.returncode != 0
    assert output == summary == ""


@pytest.mark.parametrize("base_ref", [None, ""])
def test_pr_requires_a_base_ref(repo: Path, base_ref: str | None) -> None:
    _write(repo, "docs/new.md")
    _merge(repo)
    result, output, summary = _run(repo, base_ref)
    assert result.returncode != 0
    assert output == summary == ""


@pytest.mark.parametrize("history", ["root", "linear"])
def test_non_merge_pr_head_fails_without_outputs(repo: Path, history: str) -> None:
    if history == "linear":
        _write(repo, "docs/new.md")
        _commit(repo)
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
    assert output == "offline=false\ncompose=false\ndatabase=false\n"
    assert "1 changed paths" in summary


@pytest.mark.parametrize("base_ref", ["dev", "work/example", "master"])
def test_mixed_scope_is_order_independent(base_ref: str) -> None:
    classify = runpy.run_path(str(SCRIPT))["classify"]
    assert classify(["compose.yaml", "tests/test_api.py"], base_ref) == (True, True)
    assert classify(["tests/test_api.py", "compose.yaml"], base_ref) == (True, True)


@pytest.mark.parametrize(
    ("changes", "files", "scope", "expected"),
    [
        (["tests/test_leaf.py"], {}, "leaf", ["tests/test_leaf.py"]),
        (["tests/test_leaf.py", "docs/readme.md"], {}, "leaf", ["tests/test_leaf.py"]),
        (["tests/test_leaf.py", "tests/support.py"], {}, "full", []),
        (["tests/test_leaf.py", "tests/conftest.py"], {}, "full", []),
        (["tests/test_leaf.py", "tests/fixtures/body.json"], {}, "full", []),
        (["tests/test_leaf.py", "src/change.py"], {}, "full", []),
        (["tests/test_leaf.py", "pyproject.toml"], {}, "full", []),
        (["docs/postgresql-operations.md"], {}, "full", []),
        (["tests/test_leaf.py", ".github/workflows/changelog.yml"], {}, "full", []),
        (["tests/test_leaf.py", "tests/test_deleted.py"], {}, "full", []),
        (["tests/test_leaf.py"], {"tests/test_other.py": "from test_leaf import helper"}, "full", []),
        (["tests/test_leaf.py"], {"tests/test_other.py": "import tests.test_leaf"}, "full", []),
        (["tests/test_leaf.py"], {"tests/support.py": "from tests import test_leaf"}, "full", []),
        (["tests/test_leaf.py"], {"tests/conftest.py": 'pytest_plugins = ["test_leaf"]'}, "full", []),
        (["tests/test_leaf.py"], {"tests/test_other.py": 'importlib.import_module("test_leaf")'}, "full", []),
        (["tests/test_leaf.py"], {"tests/test_other.py": 'importlib.import_module(module_name)'}, "full", []),
        (["tests/test_leaf.py"], {"tests/test_other.py": 'from importlib import import_module as load\nload(module_name)'}, "full", []),
        (["tests/test_leaf.py"], {"tests/test_other.py": 'importlib.import_module("yaml")'}, "leaf", ["tests/test_leaf.py"]),
        (["tests/test_leaf.py"], {"tests/test_leaf.py": 'pytest_plugins = ["custom"]'}, "full", []),
        (["tests/test_leaf.py"], {"tests/test_leaf.py": 'def pytest_collection_modifyitems(items): pass'}, "full", []),
        (["tests/test_leaf.py"], {"tests/test_other.py": 'def broken('}, "full", []),
    ],
)
def test_select_only_leaf_modules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changes: list[str], files: dict[str, str],
    scope: str, expected: list[str],
) -> None:
    _write(tmp_path, "tests/test_leaf.py", "def test_leaf(): pass\n")
    for name, content in files.items():
        _write(tmp_path, name, content)
    monkeypatch.chdir(tmp_path)
    select = runpy.run_path(str(SCRIPT))["selected_tests"]
    assert select(changes, "dev") == (expected, scope)
    assert select(changes, "master") == ([], "full")


@pytest.mark.parametrize("changed", ["Dockerfile", "compose.yaml", ".env.example", ".github/compose.ci.yaml"])
def test_container_edits_select_deployment_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: str,
) -> None:
    module = runpy.run_path(str(SCRIPT))
    contracts = module["DEPLOYMENT_TESTS"]
    for path in contracts:
        _write(tmp_path, path, "def test_contract(): pass\n")
    _write(tmp_path, "tests/test_leaf.py", "def test_leaf(): pass\n")
    monkeypatch.chdir(tmp_path)
    select = module["selected_tests"]
    assert select([changed, "docs/a.md"], "dev") == (sorted(contracts), "deployment")
    assert select([changed, "tests/test_leaf.py"], "dev") == (
        sorted([*contracts, "tests/test_leaf.py"]), "deployment",
    )
    assert select([changed, "tests/support.py"], "dev") == ([], "full")


def test_missing_deployment_contract_is_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    select = runpy.run_path(str(SCRIPT))["selected_tests"]
    with pytest.raises(ValueError, match="missing"):
        select(["Dockerfile"], "dev")


def test_renamed_test_cannot_be_selected_as_a_leaf(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = repository(tmp_path, {"tests/test_old.py": "def test_case(): pass\n"})
    (repo / "tests/test_old.py").rename(repo / "tests/test_new.py")
    _merge(repo)
    monkeypatch.chdir(repo)
    module = runpy.run_path(str(SCRIPT))
    assert module["selected_tests"](module["changed_paths"](), "dev") == ([], "full")


@pytest.mark.parametrize("case", ["passing", "failing", "uncollected"])
@pytest.mark.parametrize("event_name", ["pull_request", "workflow_dispatch"])
def test_scoped_runner_preserves_pytest_result(tmp_path: Path, case: str, event_name: str) -> None:
    repo = repository(tmp_path, {"tests/test_other.py": "def test_unselected(): assert False\n"})
    source = {
        "passing": "def test_selected(): assert True\n",
        "failing": "def test_selected(): assert False\n",
        "uncollected": "def helper(): pass\n",
    }[case]
    _write(repo, "tests/test_selected.py", source)
    _merge(repo)
    scripts = repo / ".github/scripts"
    scripts.mkdir(parents=True)
    for name in ("ci_scope.py", "ci_performance.py"):
        shutil.copyfile(SCRIPT.with_name(name), scripts / name)
    result = subprocess.run(
        [sys.executable, str(scripts / "ci_scope.py"), "--event-name", event_name,
         "--base-ref", "dev", "--run-tests"],
        cwd=repo, env={**os.environ, "CI_PERFORMANCE_DIR": str(tmp_path / "performance")},
        capture_output=True, text=True, timeout=30,
    )
    if event_name == "pull_request":
        assert result.returncode == {"passing": 0, "failing": 1, "uncollected": 5}[case], result.stdout + result.stderr
        assert "selection: leaf; tests/test_selected.py" in result.stdout
        assert "test_unselected" not in result.stdout
        label = "offline-tests-leaf"
    else:
        assert result.returncode == 1, result.stdout + result.stderr
        assert "selection: full; complete suite" in result.stdout
        assert "test_unselected" in result.stdout
        label = "offline-tests"
    sample = json.loads(next((tmp_path / "performance").glob(f"{label}-*.json")).read_text())
    assert sample["label"] == label
    assert sample["returncode"] == result.returncode


@pytest.mark.parametrize("paths", [
    ["Dockerfile"], ["compose.yaml", "docs/a.md"], [".env.example", "CHANGELOG.md"],
    ["Dockerfile", "tests/test_leaf.py"], ["Dockerfile", "tests/test_cloud_deployment_contract.py"],
    ["tests/test_leaf.py"], ["Dockerfile", "tests/support.py"], ["src/change.py"],
    ["docs/a.md"], [],
])
def test_only_pure_deployment_prs_can_omit_offline_postgres(paths: list[str]) -> None:
    deployment_only = runpy.run_path(str(SCRIPT))["deployment_only"]
    assert deployment_only(paths, "dev", "pull_request") is (
        paths in [["Dockerfile"], ["compose.yaml", "docs/a.md"], [".env.example", "CHANGELOG.md"]]
    )
    for event in ("push", "workflow_dispatch"):
        assert not deployment_only(paths, "", event)
    assert not deployment_only(paths, "master", "pull_request")


@pytest.mark.parametrize("extra_test", [None, "tests/test_leaf.py", "tests/test_cloud_deployment_contract.py"])
def test_database_output_for_deployment_merge(repo: Path, extra_test: str | None) -> None:
    _write(repo, "Dockerfile", "FROM scratch\n")
    if extra_test:
        _write(repo, extra_test, "def test_case(): pass\n")
    _merge(repo)
    result, output, _ = _run(repo)
    assert result.returncode == 0, result.stderr
    assert output == f"offline=true\ncompose=true\ndatabase={'true' if extra_test else 'false'}\n"
