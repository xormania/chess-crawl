"""Prepare only a mounted storage root for the unprivileged local services."""
from __future__ import annotations

import argparse
import os
from pathlib import Path


ARCHIVE_DIRECTORY = Path("/var/lib/chess-crawl/archive")


def prepare_archive(path: Path = ARCHIVE_DIRECTORY, *, uid: int = 10001, gid: int = 10001) -> None:
    if not path.is_dir() or path.is_symlink():
        raise ValueError("A real storage directory mount is required")
    # Never recursively change ownership of unrelated or existing evidence.
    path.chmod(0o700)
    os.chown(path, uid, gid)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, nargs="?", default=ARCHIVE_DIRECTORY)
    args = parser.parse_args(argv)
    prepare_archive(args.directory)


if __name__ == "__main__":
    main()
