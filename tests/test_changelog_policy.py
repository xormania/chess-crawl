"""Run the changelog workflow's checker against actual Git merge results."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

from ci_support import commit, git, merge_feature, repository, write


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/changelog.yml"
ENTRY = "# Changelog\n\n## Unreleased\n\n- Previous behavior.\n"


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return repository(tmp_path, {
        "CHANGELOG.md": ENTRY,
        "src/app.py": "application source\n",
        "tests/existing.py": "test source\n",
    })


def _run(repo: Path) -> subprocess.CompletedProcess[str]:
    # Use the actual workflow command so a stale/miswired checker path fails.
    command = re.search(r"^        run: python (\S+)$", WORKFLOW.read_text(), re.MULTILINE)
    assert command is not None
    return subprocess.run(
        [sys.executable, str(ROOT / command.group(1))],
        cwd=repo, capture_output=True, text=True, timeout=15,
    )


@pytest.mark.parametrize("path", [
    "tests/test_new.py",
    ".github/workflows/example.yml",
    ".github/actions/example/action.yml",
    "scripts/compose_smoke.py",
])
def test_ci_and_test_paths_are_exempt(repo: Path, path: str) -> None:
    write(repo, path)
    merge_feature(repo)
    result = _run(repo)
    assert result.returncode == 0, result.stderr
    assert "no changelog entry is required" in result.stdout


@pytest.mark.parametrize("path", [
    "src/app.py", "docs/guide.md", "README.md", "uv.lock", "Dockerfile",
    ".github/scripts/check_changelog.py", ".github/compose.ci.yaml",
    "tests-outside.py", "tests\n/source.py",
])
def test_nonexempt_paths_require_an_entry_even_mixed_with_tests(repo: Path, path: str) -> None:
    write(repo, path, "new behavior\n")
    write(repo, "tests/test_new.py")
    merge_feature(repo)
    result = _run(repo)
    assert result.returncode != 0
    assert "Add a non-empty entry" in result.stderr


@pytest.mark.parametrize(("old", "new", "accepted"), [
    ("tests/existing.py", "tests/moved.py", True),
    ("src/app.py", "tests/moved.py", False),
    ("tests/existing.py", "src/moved.py", False),
    ("CHANGELOG.md", "tests/old_changelog.md", False),
])
def test_exemption_considers_both_sides_of_a_rename(repo: Path, old: str, new: str, accepted: bool) -> None:
    (repo / new).parent.mkdir(parents=True, exist_ok=True)
    (repo / old).rename(repo / new)
    merge_feature(repo)
    result = _run(repo)
    assert (result.returncode == 0) is accepted, result.stdout + result.stderr


@pytest.mark.parametrize(("addition", "accepted"), [
    ("- Preserve acquired data.\n", True),
    ("+++ Added literal content.\n", True),
    ("\n\t \n", False),
    ("\u00a0\u2003\n", False),
    ("", False),
])
def test_only_nonblank_added_content_satisfies_policy(repo: Path, addition: str, accepted: bool) -> None:
    write(repo, "src/app.py", "changed implementation\n")
    write(repo, "CHANGELOG.md", ENTRY + addition)
    merge_feature(repo)
    result = _run(repo)
    assert (result.returncode == 0) is accepted, result.stdout + result.stderr


@pytest.mark.parametrize("operation", ["delete", "delete-content", "mode-only", "binary"])
def test_nonentry_changelog_changes_fail(repo: Path, operation: str) -> None:
    changelog = repo / "CHANGELOG.md"
    if operation == "delete":
        changelog.unlink()
    elif operation == "delete-content":
        changelog.write_text("# Changelog\n")
    elif operation == "mode-only":
        changelog.chmod(0o755)
    else:
        changelog.write_bytes(b"\0binary\n")
    merge_feature(repo)
    result = _run(repo)
    assert result.returncode != 0
    assert "Add a non-empty entry" in result.stderr


@pytest.mark.parametrize(("addition", "accepted"), [
    ("", False), ("\n \t\n", False), ("- Newly documented behavior.\n", True),
])
def test_renaming_to_changelog_requires_added_content(tmp_path: Path, addition: str, accepted: bool) -> None:
    old_content = "# Notes\n" + "".join(f"- Previous entry {index}.\n" for index in range(30))
    repo = repository(tmp_path, {"NOTES.md": old_content})
    (repo / "NOTES.md").rename(repo / "CHANGELOG.md")
    write(repo, "CHANGELOG.md", old_content + addition)
    merge_feature(repo)
    result = _run(repo)
    assert (result.returncode == 0) is accepted, result.stdout + result.stderr


def test_new_changelog_content_is_accepted(tmp_path: Path) -> None:
    repo = repository(tmp_path, {"src/app.py": "original\n"})
    write(repo, "CHANGELOG.md", "- Introduce the changelog policy.\n")
    merge_feature(repo)
    result = _run(repo)
    assert result.returncode == 0, result.stderr


def test_base_only_entry_cannot_satisfy_pull_request(repo: Path) -> None:
    write(repo, "src/app.py", "undocumented feature\n")
    commit(repo)
    git(repo, "checkout", "dev")
    write(repo, "CHANGELOG.md", ENTRY + "- Independent base branch change.\n")
    commit(repo)
    git(repo, "merge", "--no-ff", "feature", "-m", "Merge result")
    result = _run(repo)
    assert result.returncode != 0
    assert "Add a non-empty entry" in result.stderr


def test_later_branch_updates_cannot_change_a_checked_out_result(repo: Path) -> None:
    write(repo, "src/app.py", "undocumented feature\n")
    merge_feature(repo)
    checked_merge = git(repo, "rev-parse", "HEAD")
    git(repo, "checkout", "feature")
    write(repo, "CHANGELOG.md", ENTRY + "- A later revision adds documentation.\n")
    commit(repo)
    git(repo, "checkout", "--detach", checked_merge)
    result = _run(repo)
    assert result.returncode != 0
    assert "Add a non-empty entry" in result.stderr


@pytest.mark.parametrize("nonexempt", [True, False])
def test_large_change_lists_are_classified_completely(repo: Path, nonexempt: bool) -> None:
    for index in range(3001):
        write(repo, f"tests/file-{index:04}.txt")
    if nonexempt:
        write(repo, "z-undocumented.md")
    merge_feature(repo)
    result = _run(repo)
    assert (result.returncode == 0) is (not nonexempt), result.stdout + result.stderr


def test_nonmerge_checkout_fails_closed(repo: Path) -> None:
    result = _run(repo)
    assert result.returncode != 0
    assert "two-parent" in result.stderr


@pytest.mark.parametrize("depth", [1, 2])
def test_checkout_must_include_merge_parents(repo: Path, tmp_path: Path, depth: int) -> None:
    write(repo, "CHANGELOG.md", ENTRY + "- A documented change.\n")
    merge_feature(repo)
    shallow = tmp_path / "shallow"
    git(tmp_path, "clone", f"--depth={depth}", repo.as_uri(), str(shallow))
    result = _run(shallow)
    assert (result.returncode == 0) is (depth == 2), result.stdout + result.stderr


def test_workflow_preserves_required_name_and_retarget_events() -> None:
    workflow = WORKFLOW.read_text()
    assert "    name: Changelog policy\n" in workflow
    assert "types: [opened, synchronize, reopened, edited]" in workflow
    assert "          fetch-depth: 2\n" in workflow
    assert "  contents: read\n" in workflow
    assert "    if:" not in workflow
    assert "          ref:" not in workflow
    assert "pull_request_target:" not in workflow
