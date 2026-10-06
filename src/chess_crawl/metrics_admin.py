"""Trusted operator JSON metrics command; no public HTTP endpoint."""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from chess_crawl.storage.db import DatabaseError, connection, database_url
from chess_crawl.storage.operations_metrics import operational_snapshot, validate_statement_timeout


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read anonymous PostgreSQL workload metrics")
    parser.add_argument("--database-url", help="PostgreSQL connection settings; prefer environment password sources")
    parser.add_argument("--statement-timeout-ms", type=int, default=10000, help="Per-query deadline in milliseconds (default: 10000)")
    args = parser.parse_args(argv)
    try:
        validate_statement_timeout(args.statement_timeout_ms)
        with connection(database_url(args.database_url)) as conn:
            output = operational_snapshot(conn, statement_timeout_ms=args.statement_timeout_ms)
        print(json.dumps(output, sort_keys=True, allow_nan=False))
        return 0
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    except DatabaseError:
        print("Operational metrics are unavailable. Check PostgreSQL availability, schema, and credentials.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
