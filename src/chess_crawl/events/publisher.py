"""Run the optional, independently retrying Mercure outbox publisher."""

from __future__ import annotations

import argparse
import math
import sys
import time
from collections.abc import Callable, Sequence

from chess_crawl.events.mercure import MercurePublisher, MercureSettings
from chess_crawl.events.settings import EventSettings
from chess_crawl.jobs.locking import ExecutorLease, executor_lock
from chess_crawl.storage.db import Connection, DatabaseError, connection, database_url
from chess_crawl.storage.events import acknowledge_event, defer_event, next_pending_event, prune_events
from chess_crawl.storage.migrations import initialize


def publish_pending(
    conn: Connection,
    publisher: MercurePublisher,
    *,
    limit: int = 100,
    clock: Callable[[], float] = time.time,
    lease: ExecutorLease | None = None,
) -> int:
    """Publish a bounded batch in order; caller must hold the publisher lock.

    HTTP happens outside a transaction. A crash after acceptance and before
    acknowledgment can redeliver the same event ID; consumers must deduplicate.
    """
    if limit < 1:
        raise ValueError("Event batch limit must be positive")
    delivered = 0
    for _ in range(limit):
        if lease is not None:
            lease.require(conn, purpose="events")
        event = next_pending_event(conn)
        now = clock()
        if event is None or event.next_attempt_at > now:
            break
        result = publisher.publish(event, now=now)
        if lease is not None:
            lease.require(conn, purpose="events")
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
    parser.add_argument(
        "--database-url", help="PostgreSQL connection URL; defaults to CHESS_CRAWL_DATABASE_URL",
    )
    parser.add_argument("--once", action="store_true", help="Attempt one bounded batch, then exit")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Seconds between batches")
    args = parser.parse_args(argv)
    if not math.isfinite(args.poll_interval) or args.poll_interval <= 0:
        parser.error("--poll-interval must be finite and positive")
    try:
        target = database_url(args.database_url)
        events = EventSettings.from_env()
        if not events.enabled:
            raise ValueError("Event delivery is disabled by CHESS_CRAWL_EVENTS_ENABLED")
        settings = MercureSettings.from_env()
        # Separate from the crawl executor lock: hub outages cannot stop crawls.
        with connection(target, mode="rw") as conn, executor_lock(conn, purpose="events") as lease:
            initialize(conn)
            with MercurePublisher(settings) as publisher:
                while True:
                    publish_pending(conn, publisher, lease=lease)
                    prune_events(conn, delivered_before=max(0.0, time.time()-events.retention_seconds),
                                 limit=events.cleanup_batch_size)
                    if args.once:
                        return 0
                    time.sleep(args.poll_interval)
    except DatabaseError:
        print("Event publisher: the PostgreSQL archive is unavailable", file=sys.stderr)
        return 1
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Event publisher: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
