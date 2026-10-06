"""Operator commands for versioned acquisition settings shared by replicas."""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from chess_crawl.config import Config
from chess_crawl.providers.registry import known_keys
from chess_crawl.storage.db import DatabaseError, connection, database_url
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version
from chess_crawl.storage.operating_policy import (
    effective_provider_settings, provider_policy, provider_policy_history, set_provider_policy,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect or update shared provider policy using trusted operator access")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("show", "set", "history"):
        command = commands.add_parser(name)
        command.add_argument("--provider", required=True, choices=known_keys())
        command.add_argument("--database-url")
        if name == "set":
            command.add_argument("--expected-version", required=True, type=int, help="Observed version, or 0 before first install")
            command.add_argument("--min-delay-s", type=float)
            command.add_argument("--max-retries", type=int)
        if name == "history":
            command.add_argument("--after-version", type=int, default=0)
            command.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    try:
        output: dict[str, Any]
        fallback = Config.from_env().provider(args.provider)
        with connection(database_url(args.database_url), mode="rw" if args.command == "set" else "ro") as conn:
            if current_version(conn) != SCHEMA_VERSION:
                raise ValueError("Migrate the archive before provider policy administration")
            if args.command == "set":
                if args.min_delay_s is None and args.max_retries is None:
                    raise ValueError("Set at least one provider policy value")
                current = effective_provider_settings(conn, args.provider, fallback)
                changes = {key: value for key, value in {"min_delay_s": args.min_delay_s, "max_retries": args.max_retries}.items()
                           if value is not None}
                output = {"policy": set_provider_policy(conn, replace(current, **changes), expected_version=args.expected_version)}
            elif args.command == "history":
                page = provider_policy_history(conn, args.provider, after_version=args.after_version, limit=args.limit)
                output = {"items": page, "next_version": page[-1]["version"] if len(page) == args.limit else None}
            else:
                stored = provider_policy(conn, args.provider)
                effective = effective_provider_settings(conn, args.provider, fallback)
                output = {"provider": args.provider, "version": stored["version"] if stored is not None else 0,
                          "source": "database" if stored is not None else "deployment",
                          "effective": {"min_delay_s": effective.min_delay_s, "max_retries": effective.max_retries}}
        print(json.dumps(output, sort_keys=True))
        return 0
    except DatabaseError:
        print("Provider policy administration failed; check operator database access", file=sys.stderr)
        return 1
    except (ValueError, KeyError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
