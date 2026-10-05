"""Operator-only budget inspection and checkpoint resumption through direct DB access."""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict

from psycopg.errors import UndefinedTable

from chess_crawl.jobs import state
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.storage.db import DatabaseError, connection, database_url, transaction
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version


def _run_id(value: str) -> int:
    try:
        run_id = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("run ID must be a positive PostgreSQL bigint") from None
    if not 1 <= run_id < 2**63:
        raise argparse.ArgumentTypeError("run ID must be a positive PostgreSQL bigint")
    return run_id


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inspect or resume work budgets using trusted operator database access",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("show", "Show stored lifetime usage, monthly workspace usage, and configured policy"),
        ("resume", "Extend ceilings from operator environment and resume budget-blocked checkpoints"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--run-id", required=True, type=_run_id)
        command.add_argument("--database-url", help="PostgreSQL URL; defaults to CHESS_CRAWL_DATABASE_URL")
    args = parser.parse_args(argv)
    try:
        # Configuration comes from the operator environment. Neither callers
        # of the HTTP service nor CLI flags can supply policy/owner overrides.
        policy = BudgetPolicy.from_env()
        from chess_crawl.storage.work_budgets import get_run_budget, resume_run_budget
        with connection(database_url(args.database_url), mode="ro" if args.command == "show" else "rw") as conn:
            with transaction(conn, write=args.command == "resume"):
                try:
                    version = current_version(conn)
                except UndefinedTable:
                    raise ValueError("Migrate the archive with this application version before budget administration") from None
                if version != SCHEMA_VERSION:
                    raise ValueError("Migrate the archive with this application version before budget administration")
                run = state.get_run(conn, args.run_id)
                if run is None:
                    raise ValueError("Crawl run not found")
                owner = str(run["workspace_id"])
                budget = (
                    get_run_budget(conn, args.run_id, owner) if args.command == "show" else
                    resume_run_budget(conn, args.run_id, owner, policy)
                )
                if budget is None:
                    raise ValueError("Crawl run has no work budget")
            print(json.dumps({"run_id": args.run_id, "workspace_id": owner,
                              "configured_policy": asdict(policy), "budget": budget}, sort_keys=True))
    except DatabaseError:
        print("Budget administration: the PostgreSQL archive is unavailable", file=sys.stderr)
        return 1
    except (ValueError, RuntimeError) as exc:
        print(f"Budget administration: {exc}", file=sys.stderr)
        return 1
    return 0
