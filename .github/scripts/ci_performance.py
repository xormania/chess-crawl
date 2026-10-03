"""Measure real CI commands and compare repeated samples without hiding failures."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import time


@dataclass
class Sample:
    label: str
    duration_seconds: float
    returncode: int
    metadata: dict[str, str]

    @property
    def group(self) -> tuple[str, str]:
        return self.metadata.get("variant", ""), self.label

    @property
    def environment(self) -> tuple[str, ...]:
        return (
            *(self.metadata.get(key, "") for key in ("runner_os", "runner_arch", "python")),
            self.metadata.get("cache_state", "unspecified"),
            *(self.metadata.get(key, "") for key in ("image_os", "image_version")),
        )


def measure(label: str, output_dir: Path, command: list[str]) -> int:
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        raise ValueError("measure requires a command after --")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", label):
        raise ValueError("label must be 1–80 letters, digits, dots, underscores or hyphens")
    # Fail before running the command if its evidence cannot be written.
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "variant": os.getenv("CI_VARIANT", ""),
        "github_sha": os.getenv("GITHUB_SHA", ""),
        "github_job": os.getenv("GITHUB_JOB", ""),
        "github_run_id": os.getenv("GITHUB_RUN_ID", ""),
        "github_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", ""),
        "runner_os": os.getenv("RUNNER_OS", platform.system()),
        "runner_arch": os.getenv("RUNNER_ARCH", platform.machine()),
        "python": platform.python_version(),
        "cache_state": os.getenv("CI_CACHE_STATE", "unspecified"),
        "image_os": os.getenv("ImageOS", ""),
        "image_version": os.getenv("ImageVersion", ""),
    }
    started = time.perf_counter()
    try:
        returncode = subprocess.run(command, check=False).returncode
    except OSError as error:
        returncode = 127 if isinstance(error, FileNotFoundError) else 126
        print(f"Cannot execute measured command ({type(error).__name__}).", file=sys.stderr)
    except KeyboardInterrupt:
        returncode = 130
    sample = Sample(label, time.perf_counter() - started, returncode, metadata)
    payload = {"schema_version": 1, **asdict(sample), "status": "success" if returncode == 0 else "failure"}
    # Retain repeated measurements. Do not record command arguments or arbitrary environment variables.
    target = output_dir / f"{label}-{time.time_ns()}-{os.getpid()}.json"
    with target.open("x", encoding="utf-8") as output:
        json.dump(payload, output, indent=2, allow_nan=False)
        output.write("\n")
    return returncode if returncode >= 0 else 128 - returncode


def read_samples(directory: Path) -> list[Sample]:
    samples = []
    for path in sorted(directory.rglob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise ValueError(f"Unsupported timing record: {path.name}")
        label, duration, code, metadata = (
            payload.get("label"), payload.get("duration_seconds"),
            payload.get("returncode"), payload.get("metadata"),
        )
        if (
            not isinstance(label, str) or not label
            or isinstance(duration, bool) or not isinstance(duration, (float, int))
            or not math.isfinite(duration) or duration < 0
            or type(code) is not int or not isinstance(metadata, dict)
            or not all(isinstance(key, str) and isinstance(value, str) for key, value in metadata.items())
            or payload.get("status") != ("success" if code == 0 else "failure")
        ):
            raise ValueError(f"Invalid timing record: {path.name}")
        samples.append(Sample(label, float(duration), code, metadata))
    return samples


def grouped(samples: list[Sample]) -> dict[tuple[str, str], list[Sample]]:
    result: dict[tuple[str, str], list[Sample]] = {}
    for sample in samples:
        result.setdefault(sample.group, []).append(sample)
    return result


def cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def report(samples: list[Sample]) -> str:
    if not samples:
        return "No timing samples recorded; checks may have been omitted.\n"
    lines = [
        "| Variant | Command label | Successful samples | Failed samples | Successful median |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    failures = []
    for (variant, label), group in sorted(grouped(samples).items()):
        successful = [sample.duration_seconds for sample in group if sample.returncode == 0]
        failed = [sample for sample in group if sample.returncode != 0]
        median = f"{statistics.median(successful):.3f}s" if successful else "—"
        lines.append(f"| {cell(variant) or 'unspecified'} | {cell(label)} | {len(successful)} | {len(failed)} | {median} |")
        for sample in failed:
            failures.append(f"Failed: {cell(variant)}/{cell(label)} (exit {sample.returncode}, {sample.duration_seconds:.3f}s).")
    return "\n".join(lines) + "\n" + "".join(f"\n{failure}\n" for failure in failures)


def compare(
    baseline: list[Sample], candidate: list[Sample], *, max_regression_percent: float,
    min_regression_seconds: float, fail_on_regression: bool,
) -> tuple[str, int]:
    before, after = grouped(baseline), grouped(candidate)
    lines = ["Timing comparison uses medians of successful samples with matching variant, label, environment and cache state."]
    regressions = False
    incomplete = False
    failed_candidate = any(sample.returncode != 0 for sample in candidate)
    comparable = 0
    for variant, label in sorted(before.keys() | after.keys()):
        name = f"{cell(variant) or 'unspecified'}/{cell(label)}"
        old, new = before.get((variant, label), []), after.get((variant, label), [])
        if any(sample.returncode != 0 for sample in old + new):
            incomplete = True
            lines.append(f"- {name}: failed measurement; not compared as a performance result.")
            continue
        if not old or not new:
            incomplete |= bool(old)
            lines.append(f"- {name}: {'new label; no baseline' if new else 'no candidate samples'}; not compared.")
            continue
        environments = {sample.environment for sample in old + new}
        # Runner-image fields must match when present; both may be empty locally.
        if len(environments) != 1 or not all(next(iter(environments))[:4]):
            incomplete = True
            lines.append(f"- {name}: environment mismatch, cache mismatch or missing metadata; not comparable.")
            continue
        comparable += 1
        old_median = statistics.median(sample.duration_seconds for sample in old)
        new_median = statistics.median(sample.duration_seconds for sample in new)
        delta = new_median - old_median
        percent = 100 * delta / old_median if old_median else (math.inf if delta > 0 else 0.0)
        regressed = delta > min_regression_seconds and percent > max_regression_percent
        regressions |= regressed
        lines.append(
            f"- {name}: {old_median:.3f}s → {new_median:.3f}s "
            f"({delta:+.3f}s, {percent:+.1f}%; n={len(old)}/{len(new)})"
            f"{' — regression' if regressed else ''}."
        )
    if not comparable:
        lines.append("No comparable successful measurements; no performance conclusion.")
    if failed_candidate:
        lines.append("Candidate contains failed commands; timing cannot establish success.")
    code = 0
    if failed_candidate:
        code = 1
    elif fail_on_regression and (incomplete or not comparable):
        lines.append("Strict comparison lacks complete comparable evidence for the baseline.")
        code = 2
    elif fail_on_regression and regressions:
        code = 1
    return "\n".join(lines) + "\n", code


def nonnegative(value: str) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise argparse.ArgumentTypeError("threshold must be finite and nonnegative")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    measuring = commands.add_parser("measure", help="run a command and retain its exit status")
    measuring.add_argument("--label", required=True)
    measuring.add_argument("--output-dir", required=True, type=Path)
    measuring.add_argument("command", nargs=argparse.REMAINDER)
    reporting = commands.add_parser("report", help="summarize available measurements")
    reporting.add_argument("--output-dir", required=True, type=Path)
    comparing = commands.add_parser("compare", help="compare repeated measurements; informational by default")
    comparing.add_argument("--baseline", required=True, type=Path)
    comparing.add_argument("--candidate", required=True, type=Path)
    comparing.add_argument("--max-regression-percent", type=nonnegative, default=30)
    comparing.add_argument("--min-regression-seconds", type=nonnegative, default=2)
    comparing.add_argument("--fail-on-regression", action="store_true")
    args = parser.parse_args()
    try:
        if args.action == "measure":
            return measure(args.label, args.output_dir, args.command)
        if args.action == "compare":
            summary, code = compare(
                read_samples(args.baseline), read_samples(args.candidate),
                max_regression_percent=args.max_regression_percent,
                min_regression_seconds=args.min_regression_seconds,
                fail_on_regression=args.fail_on_regression,
            )
            print(summary, end="")
            return code
        summary = report(read_samples(args.output_dir))
        print(summary, end="")
        if summary_path := os.getenv("GITHUB_STEP_SUMMARY"):
            with Path(summary_path).open("a", encoding="utf-8") as output:
                output.write(summary)
        return 0
    except (OSError, ValueError) as error:
        print(f"Cannot process CI timings: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
