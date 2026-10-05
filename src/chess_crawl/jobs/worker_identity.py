"""Container-local process binding for probing exactly one worker incarnation."""
from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

from chess_crawl.jobs.state import worker_alive
from chess_crawl.storage.db import Connection

_MAX_IDENTITY_BYTES = 4096


@dataclass(frozen=True)
class WorkerIdentity:
    worker_id: str
    pid: int
    start_ticks: int


def _process_details(proc_id: str) -> tuple[int, int] | None:
    try:
        with Path(f"/proc/{proc_id}/stat").open("rb") as source:
            body = source.read(_MAX_IDENTITY_BYTES + 1)
        if len(body) > _MAX_IDENTITY_BYTES:
            return None
        # Linux's comm field may contain spaces and parentheses. Fields after
        # its final ')' start with state (field 3); starttime is field 22.
        fields = body.rsplit(b")", 1)[1].split()
        if fields[0] in {b"Z", b"X"}:
            return None
        return int(body.split(b" ", 1)[0]), int(fields[19])
    except (OSError, ValueError, IndexError):
        return None


def write_worker_identity(path: str | Path, worker_id: str) -> WorkerIdentity:
    """Atomically replace one nonsecret, private local binding after startup."""
    if re.fullmatch(r"[0-9a-f]{32}", worker_id) is None:
        raise ValueError("Worker identity must be a UUID hex incarnation")
    process = _process_details("self")
    if process is None:
        raise RuntimeError("Worker process identity is unavailable")
    # Use the PID exposed by this procfs mount, which can belong to an ancestor
    # PID namespace. The container probe uses the same mounted process view.
    pid, start_ticks = process
    identity = WorkerIdentity(worker_id, pid, start_ticks)
    target = Path(path)
    temporary: Path | None = None
    try:
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=target.parent, delete=False) as output:
            temporary = Path(output.name)
            json.dump({"schema_version":1, "worker_id":worker_id, "pid":pid, "start_ticks":start_ticks}, output)
        os.replace(temporary, target)
        temporary = None
    except OSError:
        raise RuntimeError("Worker process identity could not be stored") from None
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
    return identity


def read_worker_identity(path: str | Path) -> WorkerIdentity | None:
    """Reject missing, malformed, oversized, dead or reused-PID bindings."""
    try:
        with Path(path).open("rb") as source:
            body = source.read(_MAX_IDENTITY_BYTES + 1)
        if len(body) > _MAX_IDENTITY_BYTES:
            return None
        data = json.loads(body)
        if not isinstance(data, dict) or set(data) != {"schema_version","worker_id","pid","start_ticks"}:
            return None
        if type(data["schema_version"]) is not int or data["schema_version"] != 1:
            return None
        worker_id, pid, start_ticks = data["worker_id"], data["pid"], data["start_ticks"]
        if not isinstance(worker_id, str) or re.fullmatch(r"[0-9a-f]{32}", worker_id) is None:
            return None
        if type(pid) is not int or pid <= 0 or type(start_ticks) is not int or start_ticks < 0:
            return None
        if _process_details(str(pid)) != (pid, start_ticks):
            return None
        return WorkerIdentity(worker_id, pid, start_ticks)
    except (OSError, ValueError, UnicodeError):
        return None


def local_worker_alive(conn: Connection, path: str | Path, *, now: float | None = None) -> bool:
    """Require both this local process and its own live database heartbeat."""
    identity = read_worker_identity(path)
    return identity is not None and worker_alive(conn, identity.worker_id, now=now)
