"""Check changelog policy against the immutable checked-out PR merge result."""

from __future__ import annotations

import os
import subprocess  # nosec B404 # Runs only Git diff with repository-controlled arguments.
import sys

from ci_scope import changed_paths


def _git_diff(*args: str) -> bytes:
    return subprocess.run(  # nosec B603, B607 # Trusted runner Git; argv list, no shell.
        ["git", "diff", "--no-color", "--no-ext-diff", "--no-textconv", "--output-indicator-new=+", *args],
        check=True, capture_output=True, timeout=30,
    ).stdout


def is_ci_or_test(path: str) -> bool:
    return path.startswith(("tests/", ".github/workflows/", ".github/actions/")) or path == "scripts/compose_smoke.py"


def has_added_entry() -> bool:
    # Scope/exemption uses both sides of renames. Content validation separately
    # keeps rename detection: moving a file to CHANGELOG.md is not a new entry.
    records = iter(_git_diff("--find-renames", "--name-status", "-z", "HEAD^1", "HEAD").split(b"\0")[:-1])
    previous = None
    found = False
    for status in records:
        source = os.fsdecode(next(records))
        target = os.fsdecode(next(records)) if status.startswith(b"R") else source
        if target == "CHANGELOG.md" and status != b"D":
            previous = source if status != b"A" else None
            found = True
            break
    if not found:
        return False

    if previous is None:
        patch = _git_diff("--unified=0", "HEAD^1", "HEAD", "--", "CHANGELOG.md")
    else:
        patch = _git_diff("--unified=0", f"HEAD^1:{previous}", "HEAD:CHANGELOG.md")
    in_hunk = False
    for line in patch.splitlines():
        if line.startswith(b"@@ "):
            in_hunk = True
        elif in_hunk and line.startswith(b"+") and line[1:].decode("utf-8", errors="replace").strip():
            return True
    return False


def main() -> int:
    try:
        paths = changed_paths()
        if all(is_ci_or_test(path) for path in paths):
            print("Only CI or test files changed; no changelog entry is required.")
            return 0
        if not has_added_entry():
            print(
                "This PR changes files beyond CI/tests. Add a non-empty entry to CHANGELOG.md. "
                "Deletion, rename-only, mode-only and blank-line-only changes do not satisfy this policy.",
                file=sys.stderr,
            )
            return 1
        print("CHANGELOG.md has an added entry. Reviewers must assess its accuracy and the PR evidence.")
    except (OSError, ValueError, StopIteration, subprocess.SubprocessError) as error:
        print(f"Cannot determine changelog compliance: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
