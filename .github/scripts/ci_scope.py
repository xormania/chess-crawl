"""Select CI work from the checked-out PR merge result, conservatively."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


DOCUMENTATION_PATHS = {
    "AGENTS.md",
    "PROJECT.md",
    "CONTRIBUTING.md",
    "CHANGELOG.md",
    ".github/pull_request_template.md",
}


def classify(paths: list[str], base_ref: str) -> tuple[bool, bool]:
    """Return whether offline and Compose checks are needed."""
    if base_ref == "master" or not paths:
        return True, True
    offline = False
    for path in paths:
        if path in DOCUMENTATION_PATHS or (path.startswith("docs/") and path.endswith(".md")):
            continue
        if path.startswith("tests/"):
            offline = True
            continue
        # Unknown files, build metadata, application code and CI changes all
        # need full validation. README and LICENSE are packaging inputs.
        return True, True
    return offline, False


def changed_paths() -> list[str]:
    parents = subprocess.run(
        ["git", "rev-list", "--parents", "-n", "1", "HEAD"],
        check=True, capture_output=True, text=True, timeout=30,
    ).stdout.split()
    if len(parents) != 3:
        raise ValueError("CI scope requires a two-parent pull-request merge commit.")
    diff = subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", "-z", "HEAD^1", "HEAD"],
        check=True, capture_output=True, timeout=30,
    ).stdout
    # Disabling rename detection exposes both the removed and added paths.
    # NUL separation also handles filenames containing whitespace/newlines.
    return [os.fsdecode(path) for path in diff.split(b"\0") if path]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", required=True)
    args = parser.parse_args()
    try:
        paths = changed_paths()
        offline, compose = classify(paths, args.base_ref)
        outputs = f"offline={str(offline).lower()}\ncompose={str(compose).lower()}\n"
        summary = (
            f"CI scope: {len(paths)} changed paths in the pull-request merge result.\n\n"
            f"Offline checks: {'run' if offline else 'skip (documentation only)'}.\n\n"
            f"Compose smoke: {'run' if compose else 'skip (documentation/tests only)'}.\n"
        )
        if output_path := os.environ.get("GITHUB_OUTPUT"):
            with Path(output_path).open("a", encoding="utf-8") as output:
                output.write(outputs)
        if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(summary_path).open("a", encoding="utf-8") as output:
                output.write(summary)
        print(summary)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Cannot determine CI scope: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
