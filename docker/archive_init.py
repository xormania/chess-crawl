"""Prepare only the mounted archive root for the unprivileged local services."""
from __future__ import annotations

import os
from pathlib import Path


ARCHIVE_DIRECTORY = Path("/var/lib/chess-crawl/archive")


def prepare_archive(path: Path = ARCHIVE_DIRECTORY, *, uid: int = 10001, gid: int = 10001) -> None:
    if not path.is_dir() or path.is_symlink():
        raise ValueError("A real archive directory mount is required")
    # Never recursively change ownership of unrelated or existing evidence.
    path.chmod(0o700)
    os.chown(path, uid, gid)


if __name__ == "__main__":
    prepare_archive()
