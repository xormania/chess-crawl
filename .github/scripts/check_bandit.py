"""Run the pinned security scanner and reject incomplete scans as well as findings."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess  # nosec B404 # Executes only the installed Bandit module below.
import sys
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[2]
TARGETS = ("src", "scripts", "docker", ".github/scripts")


def main() -> int:
    try:
        for target in TARGETS:
            if not (ROOT / target).is_dir():
                raise ValueError(f"Missing scan directory: {target}")
        with TemporaryDirectory(prefix="chess-crawl-bandit-") as temporary:
            report = Path(temporary) / "report.json"
            result = subprocess.run(  # nosec B603 # Fixed scanner arguments; no shell.
                [sys.executable, "-m", "bandit", "-c", "pyproject.toml", "-r", *TARGETS,
                 # Default .git exclusion also matches .github without a .git directory.
                 "--exclude", "",
                 "--severity-level", "all", "--confidence-level", "all",
                 "--format", "json", "--output", str(report)],
                cwd=ROOT, check=False,
            )
            if result.returncode not in {0, 1}:
                return result.returncode if result.returncode > 0 else 128 - result.returncode
            findings = json.loads(report.read_text(encoding="utf-8"))
        for issue in findings["results"]:
            print(
                f"{issue['filename']}:{issue['line_number']}: {issue['test_id']} "
                f"[{issue['issue_severity']}/{issue['issue_confidence']}] {issue['issue_text']}",
            )
        for error in findings["errors"]:
            print(f"Unscanned file: {error['filename']}: {error['reason']}", file=sys.stderr)
        scanned = set(findings["metrics"]) - {"_totals"}
        expected = {str(path.relative_to(ROOT)) for target in TARGETS for path in (ROOT / target).rglob("*.py")}
        if not expected or scanned != expected:
            raise ValueError("Bandit did not scan exactly the expected Python files")
        print(f"Bandit: {len(scanned)} Python files, {len(findings['results'])} findings.")
        return 1 if findings["errors"] or findings["results"] else result.returncode
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Security scan failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
