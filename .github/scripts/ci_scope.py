"""Scope pull requests; fully validate push and manually selected revisions."""

from __future__ import annotations

import argparse
import ast
import os
import re
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
    "docs/postgresql-operations.md",  # Contains the executable restore verifier.
}

COMPOSE_PATHS = {
    "Dockerfile",
    "compose.yaml",
    ".dockerignore",
    ".env.example",
    "docker/mercure-entrypoint.sh",
    ".github/compose.ci.yaml",
}

# Container inputs must also exercise the Python contracts that inspect them.
# These tests are offline and do not need an initialized application database.
DEPLOYMENT_TESTS = (
    "tests/test_cloud_deployment_contract.py",
    "tests/test_compose_ci.py",
    "tests/test_compose_credentials.py",
    "tests/test_compose_external.py",
)


def documentation(path: str) -> bool:
    return path not in OFFLINE_PATHS and (
        path in DOCUMENTATION_PATHS or (path.startswith("docs/") and path.endswith(".md"))
    )


def deployment_only(paths: list[str], base_ref: str, event_name: str) -> bool:
    """Only the maintained deployment contracts are known to need no database.

    Even edits to those contract tests themselves can add database coverage, so
    mixed or test-only changes retain PostgreSQL provisioning.
    """
    return (
        event_name == "pull_request" and base_ref != "master"
        and any(path in COMPOSE_PATHS for path in paths)
        and all(documentation(path) or path in COMPOSE_PATHS for path in paths)
    )


def classify(paths: list[str], base_ref: str) -> tuple[bool, bool]:
    """Return whether offline and Compose checks are needed."""
    if base_ref == "master" or not paths:
        return True, True
    offline = compose = False
    for path in paths:
        if documentation(path):
            continue
        if path.startswith("tests/") or path in OFFLINE_PATHS:
            offline = True
            continue
        if path in COMPOSE_PATHS:
            offline = compose = True
            continue
        # Unknown files, build metadata, application code and CI changes all
        # need full validation. README and LICENSE are packaging inputs.
        return True, True
    return offline, compose


def selected_tests(paths: list[str], base_ref: str) -> tuple[list[str], str]:
    """Select only independent test modules; an empty selection means all tests.

    Application edits never use inferred test dependencies. Test-only edits can
    select leaf modules, while any shared input or uncertain import falls back
    to the full suite. Deleted/renamed tests have a missing old path and therefore
    also fall back. Both sides of the merge diff are supplied by changed_paths.
    """
    if base_ref == "master" or not paths:
        return [], "full"
    selected: set[str] = set()
    container = False
    for path in paths:
        if documentation(path):
            continue
        if path in COMPOSE_PATHS:
            container = True
            continue
        # Keep helper packages, shared fixtures/data, custom collection, unusual
        # names and symlinks out of the selective path.
        if (
            not re.fullmatch(r"tests/test_[A-Za-z0-9_]+\.py", path)
            or not Path(path).is_file() or Path(path).is_symlink()
        ):
            return [], "full"
        selected.add(path)
    if selected and not leaf_tests(selected):
        return [], "full"
    if container:
        selected.update(DEPLOYMENT_TESTS)
    if not selected:
        return [], "full"
    # Missing maintained contracts must not silently become a smaller selection.
    if any(not Path(path).is_file() for path in selected):
        raise ValueError("Selected deployment/test contract is missing.")
    return sorted(selected), "deployment" if container else "leaf"


def leaf_tests(selected: set[str]) -> bool:
    names = {Path(path).stem for path in selected}
    try:
        for path in Path("tests").rglob("*.py"):
            if path.is_symlink():
                return False
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            dynamic_names = {"import_module", "__import__", "run_module"}
            for imported in ast.walk(tree):
                if isinstance(imported, ast.ImportFrom):
                    dynamic_names.update(
                        alias.asname or alias.name for alias in imported.names
                        if alias.name in dynamic_names
                    )
            for node in ast.walk(tree):
                # Module-level pytest hooks/plugins can affect collection even
                # when the module is not imported by another test.
                if str(path) in selected and (
                    isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name.startswith("pytest_")
                    or isinstance(node, ast.Name) and node.id == "pytest_plugins"
                ):
                    return False
                if str(path) in selected:
                    continue
                if isinstance(node, ast.Call):
                    function = node.func.attr if isinstance(node.func, ast.Attribute) else (
                        node.func.id if isinstance(node.func, ast.Name) else ""
                    )
                    if function in dynamic_names and (
                        not node.args or not isinstance(node.args[0], ast.Constant)
                        or not isinstance(node.args[0].value, str)
                    ):
                        return False
                imports: list[str] = []
                if isinstance(node, ast.Import):
                    imports = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    imports = [node.module or "", *(alias.name for alias in node.names)]
                elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                    # Also catches literal dynamic imports / plugin names.
                    imports = [node.value]
                if any(names.intersection(module.replace("/", ".").split(".")) for module in imports):
                    return False
    except (OSError, SyntaxError, UnicodeError):
        return False
    return True


def run_tests(paths: list[str], base_ref: str, event_name: str) -> int:
    tests, selection = selected_tests(paths, base_ref) if event_name == "pull_request" else ([], "full")
    output_dir = Path(os.environ.get("CI_PERFORMANCE_DIR", ".ci-performance"))
    print(f"Offline test selection: {selection}; {', '.join(tests) if tests else 'complete suite'}", flush=True)
    label = "offline-tests" if selection == "full" else f"offline-tests-{selection}"
    command = [
        sys.executable, str(Path(__file__).with_name("ci_performance.py")),
        "measure", "--label", label, "--output-dir", str(output_dir), "--",
        sys.executable, "-m", "pytest", "-q", "-m", "not live and not slow", "--durations=15",
        f"--junitxml={output_dir / 'pytest.xml'}", *tests,
    ]
    # Pytest preserves collection errors, zero collected cases and test failures.
    return subprocess.run(command, check=False).returncode  # nosec B603 # Fixed argv; selected paths are validated Python test files.


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
    parser.add_argument("--run-tests", action="store_true")
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
            paths = []
            reason = f"Full validation for {args.event_name} at the checked-out commit."
        if args.run_tests:
            if not offline:
                raise ValueError("Offline tests requested for a revision without offline inputs.")
            return run_tests(paths, args.base_ref, args.event_name)
        database = offline and not deployment_only(paths, args.base_ref, args.event_name)
        outputs = (
            f"offline={str(offline).lower()}\ncompose={str(compose).lower()}\n"
            f"database={str(database).lower()}\n"
        )
        summary = (
            f"CI scope: {reason}\n\n"
            f"Offline checks: {'run' if offline else 'skip (no offline inputs)'}.\n\n"
            f"Compose smoke: {'run' if compose else 'skip (no container inputs)'}.\n"
            f"Offline PostgreSQL: {'start' if database else 'skip (no database tests)'}.\n"
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
