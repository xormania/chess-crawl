"""Own the disposable PostgreSQL server for a selected offline CI job."""

from __future__ import annotations

import argparse
import json
import os
import subprocess  # nosec B404 # Starts and removes the job's fixed disposable Docker service.
import sys
import time
from collections.abc import Sequence
from typing import Any


CONTAINER_NAME = "chess-crawl-test-db"
READINESS_TIMEOUT = 60.0
POLL_INTERVAL = 1.0


def _docker(*arguments: str, timeout: float = 30) -> str:
    try:
        result = subprocess.run(  # nosec B603, B607 # Fixed Docker argv and trusted runner PATH; no shell or password values.
            ["docker", *arguments], check=False, capture_output=True,
            text=True, timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise RuntimeError("Docker invocation did not complete") from None
    if result.returncode:
        # Docker diagnostics can include environment values. Never echo them.
        raise RuntimeError("Docker command failed")
    return result.stdout


def _health(*, timeout: float = 5) -> str:
    try:
        state: Any = json.loads(_docker(
            "inspect", "--format", "{{json .State}}", CONTAINER_NAME, timeout=timeout,
        ))
    except (ValueError, TypeError):
        raise RuntimeError("PostgreSQL container state is malformed") from None
    if not isinstance(state, dict):
        raise RuntimeError("PostgreSQL container state is missing")
    if state.get("Running") is not True or state.get("Status") != "running":
        raise RuntimeError("PostgreSQL container is not running")
    health = state.get("Health")
    status = health.get("Status") if isinstance(health, dict) else None
    if not isinstance(status, str) or status not in {"starting", "healthy", "unhealthy"}:
        raise RuntimeError("PostgreSQL container health is missing or malformed")
    return status


def start() -> None:
    if not os.getenv("POSTGRES_PASSWORD"):
        raise RuntimeError("POSTGRES_PASSWORD must be set for the disposable database")
    _docker(
        "run", "--detach", "--name", CONTAINER_NAME,
        "--publish", "127.0.0.1:5432:5432", "--env", "POSTGRES_PASSWORD",
        "--health-cmd", "pg_isready -U postgres -d postgres",
        "--health-interval", "1s", "--health-timeout", "3s",
        "--health-retries", "30", "--health-start-period", "5s",
        "postgres:18", timeout=120,
    )
    deadline = time.monotonic() + READINESS_TIMEOUT
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("PostgreSQL container did not become ready before the deadline")
        health = _health(timeout=min(5, remaining))
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("PostgreSQL container did not become ready before the deadline")
        if health == "healthy":
            return
        if health == "unhealthy":
            raise RuntimeError("PostgreSQL container failed its readiness healthcheck")
        time.sleep(min(POLL_INTERVAL, remaining))


def stop() -> None:
    # Listing distinguishes an absent container from an unavailable Docker
    # daemon. Treating every failed rm/inspect as absence would hide failures.
    output = _docker(
        "container", "ls", "--all", "--filter", f"name=^/{CONTAINER_NAME}$",
        "--format", "{{.Names}}",
    )
    names = output.splitlines()
    if not names:
        return
    if names != [CONTAINER_NAME]:
        raise RuntimeError("Docker returned an unexpected disposable database container")
    _docker("rm", "--force", "--volumes", CONTAINER_NAME)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("start", "stop"))
    args = parser.parse_args(argv)
    try:
        if args.operation == "start":
            start()
        else:
            stop()
    except RuntimeError as exc:
        print(f"Disposable PostgreSQL {args.operation} failed: {exc}", file=sys.stderr)
        return 1
    print(f"Disposable PostgreSQL {args.operation} complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
