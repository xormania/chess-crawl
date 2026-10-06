"""Trusted retention for optional event notifications."""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from chess_crawl.jobs.locking import ExecutorBusy, executor_lock
from chess_crawl.storage.db import DatabaseError, connection, database_url
from chess_crawl.storage.events import prune_events
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prune event notifications with trusted operator access")
    parser.add_argument("--database-url")
    parser.add_argument("--delivered-before", type=float, required=True,
                        help="Exclusive delivery-time cutoff, Unix seconds")
    parser.add_argument("--discard-pending-before", type=float,
                        help="Explicitly discard undelivered notifications older than this occurrence-time cutoff")
    parser.add_argument("--batch-size", type=int, default=256)
    args = parser.parse_args(argv)
    try:
        with connection(database_url(args.database_url), mode="rw") as conn:
            if current_version(conn) != SCHEMA_VERSION:
                raise ValueError("Run chess-crawl-admin migrate before event retention")
            with executor_lock(conn, purpose="events"):
                result = prune_events(conn, delivered_before=args.delivered_before,
                                      discard_pending_before=args.discard_pending_before, limit=args.batch_size)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (ValueError, RuntimeError, ExecutorBusy) as error:
        print(str(error), file=sys.stderr)
        return 2
    except DatabaseError:
        print("Event retention: the archive database is unavailable", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
