"""Bounded child-process handshakes with cleanup even when startup fails."""
from __future__ import annotations

import os
import selectors
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


@contextmanager
def child_process(args: list[str], **kwargs: Any) -> Iterator[subprocess.Popen[str]]:
    child = subprocess.Popen(
        args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, **kwargs,
    )
    try:
        yield child
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)


def expect_output(child: subprocess.Popen[str], expected: str, *, timeout: float = 20) -> None:
    """Read a complete first line, including partial writes, within one deadline."""
    assert child.stdout is not None
    deadline = time.monotonic() + timeout
    received = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(child.stdout, selectors.EVENT_READ)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise AssertionError(f"Child did not emit {expected!r} before startup timeout; received {bytes(received)!r}")
            # readline() can block indefinitely after select() if a child writes
            # only a partial line. Read one ready byte without consuming later output.
            chunk = os.read(child.stdout.fileno(), 1)
            if not chunk:
                raise AssertionError(f"Child exited before emitting {expected!r}; received {bytes(received)!r}")
            if chunk == b"\n":
                break
            received.extend(chunk)
            if len(received) > 4096:
                raise AssertionError("Child startup line exceeded 4096 bytes")
    assert received.decode().strip() == expected
