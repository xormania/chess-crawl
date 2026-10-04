"""Scope pull requests; fully validate push and manually selected revisions."""

from __future__ import annotations

import argparse
import os
import subprocess  # nosec B404 # Inspects the checked-out Git merge using fixed commands.
import sys
from pathlib import Path


DOCUMENTATION_PATHS = {
    "AGENTS.md",
    "PROJECT.md",
    "CONTRIBUTING.md",
    "CHANGELOG.md",
    ".github/pull_request_template.md",
}

OFFLINE_PATHS = {
    ".github/workflows/changelog.yml",
    ".github/scripts/check_changelog.py",
}

COMPOSE_PATHS = {
    "Dockerfile",
    "compose.yaml",
    ".dockerignore",
    ".env.example",
    "docker/mercure-entrypoint.sh",
    ".github/compose.ci.yaml",
}


def classify(paths: list[str], base_ref: str) -> tuple[bool, bool]:
    """Return whether offline and Compose checks are needed."""
    if base_ref == "master" or not paths:
        return True, True
    offline = compose = False
    for path in paths:
        if path in DOCUMENTATION_PATHS or (path.startswith("docs/") and path.endswith(".md")):
            continue
        if path.startswith("tests/") or path in OFFLINE_PATHS:
            offline = True
            continue
        if path in COMPOSE_PATHS:
            compose = True
            continue
        # Unknown files, build metadata, application code and CI changes all
        # need full validation. README and LICENSE are packaging inputs.
        return True, True
    return offline, compose


def changed_paths() -> list[str]:
    parents = subprocess.run(  # nosec B603, B607 # Fixed argv and trusted runner Git; no shell.
        ["git", "rev-list", "--parents", "-n", "1", "HEAD"],
        check=True, capture_output=True, text=True, timeout=30,
    ).stdout.split()
    if len(parents) != 3:
        raise ValueError("CI scope requires a two-parent pull-request merge commit.")
    diff = subprocess.run(  # nosec B603, B607 # Fixed argv and trusted runner Git; no shell.
        ["git", "diff", "--name-only", "--no-renames", "-z", "HEAD^1", "HEAD"],
        check=True, capture_output=True, timeout=30,
    ).stdout
    # Disabling rename detection exposes both the removed and added paths.
    # NUL separation also handles filenames containing whitespace/newlines.
    return [os.fsdecode(path) for path in diff.split(b"\0") if path]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--event-name", required=True,
        choices=("pull_request", "push", "workflow_dispatch"),
    )
    parser.add_argument("--base-ref", default="")
    args = parser.parse_args()
    try:
        if args.event_name == "pull_request":
            if not args.base_ref:
                raise ValueError("Pull-request CI scope requires a base branch.")
            paths = changed_paths()
            offline, compose = classify(paths, args.base_ref)
            reason = f"{len(paths)} changed paths in the pull-request merge result."
        else:
            # Pushes and manual runs validate the selected commit itself. Its
            # history may be a squash, fast-forward, root, or merge commit;
            # neither a PR base nor a changed-files comparison is needed.
            offline = compose = True
            reason = f"Full validation for {args.event_name} at the checked-out commit."
        outputs = f"offline={str(offline).lower()}\ncompose={str(compose).lower()}\n"
        summary = (
            f"CI scope: {reason}\n\n"
            f"Offline checks: {'run' if offline else 'skip (no offline inputs)'}.\n\n"
            f"Compose smoke: {'run' if compose else 'skip (no container inputs)'}.\n"
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
