"""Run the optional, independently retrying Mercure outbox publisher."""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
import time
from collections.abc import Callable, Sequence

from chess_crawl.events.mercure import MercurePublisher, MercureSettings
from chess_crawl.storage.db import open_database
from chess_crawl.storage.events import acknowledge_event, defer_event, next_pending_event


def publish_pending(
    conn: sqlite3.Connection,
    publisher: MercurePublisher,
    *,
    limit: int = 100,
    clock: Callable[[], float] = time.time,
) -> int:
    """Publish a bounded batch in order; caller must hold the publisher lock.

    HTTP happens outside a transaction. A crash after acceptance and before
    acknowledgment can redeliver the same event ID; consumers must deduplicate.
    """
    if limit < 1:
        raise ValueError("Event batch limit must be positive")
    delivered = 0
    for _ in range(limit):
        event = next_pending_event(conn)
        now = clock()
        if event is None or event.next_attempt_at > now:
            break
        result = publisher.publish(event, now=now)
        if result.succeeded:
            acknowledge_event(conn, event.outbox_id, now=clock())
            delivered += 1
        else:
            delay = publisher.retry_delay(event.attempts, result.retry_after_s)
            defer_event(
                conn, event.outbox_id,
                next_attempt_at=clock() + delay,
                error=result.error or "delivery_error",
            )
            break
    return delivered


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish committed chess-crawl events to Mercure")
    parser.add_argument("--db", required=True, help="Path to the archive shared with the crawl worker")
    parser.add_argument("--once", action="store_true", help="Attempt one bounded batch, then exit")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Seconds between batches")
    args = parser.parse_args(argv)
    if not math.isfinite(args.poll_interval) or args.poll_interval <= 0:
        parser.error("--poll-interval must be finite and positive")
    try:
        settings = MercureSettings.from_env()
        # Separate from the crawl executor lock: hub outages cannot stop crawls.
        from chess_crawl.jobs.locking import archive_lock

        with open_database(args.db, writable=True) as conn:
            with archive_lock(args.db, purpose="events"), MercurePublisher(settings) as publisher:
                while True:
                    publish_pending(conn, publisher)
                    if args.once:
                        return 0
                    time.sleep(args.poll_interval)
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Event publisher: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
