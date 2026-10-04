"""Bounded operational relocation, separate from SQL schema migrations."""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from chess_crawl.storage.archives import relocate_raw_payloads
from chess_crawl.storage.db import database_url, open_database
from chess_crawl.storage.object_store import configured_store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Relocate a bounded batch of archived bodies")
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args(argv)
    store = configured_store()
    if store is None:
        parser.error("Select a local or s3 CHESS_CRAWL_ARCHIVE_BACKEND")
    with open_database(database_url(), writable=True) as conn:
        result = relocate_raw_payloads(conn, store=store, batch_size=args.batch_size)
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
