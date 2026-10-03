"""Real local Git repositories for CI policy behavior tests."""

from __future__ import annotations

import subprocess
from pathlib import Path


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=CI Test", "-c", "user.email=ci@example.invalid",
         "-c", "commit.gpgsign=false", *args],
        cwd=repo, check=True, capture_output=True, text=True, timeout=10,
    ).stdout.strip()


def write(repo: Path, path: str, text: str = "content\n") -> None:
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)


def commit(repo: Path) -> None:
    git(repo, "add", "--all")
    git(repo, "commit", "--allow-empty", "-m", "Test changes")


def repository(tmp_path: Path, files: dict[str, str]) -> Path:
    path = tmp_path / "repo"
    path.mkdir()
    git(path, "init", "--initial-branch=dev")
    for name, content in files.items():
        write(path, name, content)
    commit(path)
    git(path, "checkout", "-b", "feature")
    return path


def merge_feature(repo: Path) -> None:
    commit(repo)
    git(repo, "checkout", "dev")
    git(repo, "merge", "--no-ff", "feature", "-m", "Pull request merge")
