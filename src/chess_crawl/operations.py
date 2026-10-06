"""Operational commands; product collection and archive access use the API."""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict

from chess_crawl.storage.archives import relocate_raw_payloads
from chess_crawl.storage.db import DatabaseError, connection, database_label, database_url
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version, initialize
from chess_crawl.storage.object_store import configured_store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chess-crawl-admin",
        description="Maintain a Chess-Crawl archive. Product features are available through the authenticated API.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    policy = commands.add_parser("operating-policy", help="Administer versioned acquisition policy across replicas")
    policy.add_argument("arguments", nargs=argparse.REMAINDER)
    configuration = commands.add_parser("config", help="Validate or inspect effective settings without service access")
    configuration.add_argument("arguments", nargs=argparse.REMAINDER)
    workspaces = commands.add_parser("workspaces", help="Provision workspace access and quotas using trusted operator access")
    workspaces.add_argument("arguments", nargs=argparse.REMAINDER)
    migrate = commands.add_parser("migrate", help="Apply packaged database migrations without provider requests")
    info = commands.add_parser("info", help="Inspect schema readiness without changing the archive")
    relocate = commands.add_parser("relocate", help="Move one resumable batch of inline backup bodies to configured storage")
    budgets = commands.add_parser("budgets", help="Inspect or resume run budgets using trusted operator access")
    budgets.add_argument("arguments", nargs=argparse.REMAINDER)
    events = commands.add_parser("prune-events", help="Prune notifications with trusted operator access")
    events.add_argument("arguments", nargs=argparse.REMAINDER)
    metrics = commands.add_parser("metrics", help="Read anonymous workload and delivery metrics with trusted access")
    metrics.add_argument("arguments", nargs=argparse.REMAINDER)
    prune = commands.add_parser("prune-results", help="Delete one workspace's old analysis results in a bounded batch")
    prune.add_argument("--workspace-id", required=True)
    prune.add_argument("--before", required=True, type=int, help="Exclusive creation-time cutoff, as Unix seconds")
    prune.add_argument("--batch-size", type=int, default=256)
    artifacts = commands.add_parser("prune-artifacts", help="Delete expired private export artifacts in a bounded resumable batch")
    artifacts.add_argument("--workspace-id", required=True)
    artifacts.add_argument("--before", required=True, type=int, help="Inclusive expiration-time cutoff, as Unix seconds")
    artifacts.add_argument("--batch-size", type=int, default=10)
    for command in (migrate, info, relocate, prune, artifacts):
        command.add_argument("--database-url", help="PostgreSQL connection settings; prefer environment password sources")
    relocate.add_argument("--batch-size", type=int, default=100)
    relocate.add_argument("--count-remaining", action="store_true", help="Compute an optional exact pending count")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if values and values[0] == "operating-policy":
        from chess_crawl.operating_policy import main as operating_policy_main
        return operating_policy_main(values[1:])
    if values and values[0] == "config":
        from chess_crawl.configuration import main as configuration_main
        return configuration_main(values[1:])
    if values and values[0] == "workspaces":
        from chess_crawl.workspace_admin import main as workspace_main
        return workspace_main(values[1:])
    if values and values[0] == "budgets":
        from chess_crawl.jobs.budget import main as budget_main
        return budget_main(values[1:])
    if values and values[0] == "prune-events":
        from chess_crawl.events.maintenance import main as events_main
        return events_main(values[1:])
    if values and values[0] == "metrics":
        from chess_crawl.metrics_admin import main as metrics_main
        return metrics_main(values[1:])
    args = build_parser().parse_args(values)
    try:
        target = database_url(args.database_url)
        if args.command == "migrate":
            with connection(target, mode="rw") as conn:
                result = initialize(conn)
            output = asdict(result)
        elif args.command == "info":
            with connection(target) as conn:
                version = current_version(conn)
            output = {"database": database_label(target), "schema_version": version,
                      "application_schema_version": SCHEMA_VERSION, "ready": version == SCHEMA_VERSION}
        elif args.command == "prune-artifacts":
            from chess_crawl.storage.artifacts import prune_artifacts
            with connection(target, mode="rw") as conn:
                if current_version(conn) != SCHEMA_VERSION:
                    raise ValueError("Run chess-crawl-admin migrate before artifact retention")
                output = {"workspace_id": args.workspace_id, **prune_artifacts(
                    conn, args.workspace_id, before=args.before, limit=args.batch_size,
                )}
        elif args.command == "prune-results":
            from chess_crawl.storage.working_sets import prune_results
            with connection(target, mode="rw") as conn:
                if current_version(conn) != SCHEMA_VERSION:
                    raise ValueError("Run chess-crawl-admin migrate before result retention")
                output = {"workspace_id": args.workspace_id, **prune_results(
                    conn, args.workspace_id, before=args.before, limit=args.batch_size,
                )}
        else:
            store = configured_store()
            if store is None:
                raise ValueError("Select a local or s3 CHESS_CRAWL_ARCHIVE_BACKEND before relocation")
            with connection(target, mode="rw") as conn:
                if current_version(conn) != SCHEMA_VERSION:
                    raise ValueError("Run chess-crawl-admin migrate before archive relocation")
                output = asdict(relocate_raw_payloads(conn, store=store, batch_size=args.batch_size,
                                                     count_remaining=args.count_remaining))
        print(json.dumps(output, sort_keys=True))
        return 0
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    except DatabaseError:
        print("The archive operation could not be completed. Check PostgreSQL availability and credentials.", file=sys.stderr)
        return 1
    except (OSError, RuntimeError):
        print("The archive storage operation could not be completed. Check storage configuration and access.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
