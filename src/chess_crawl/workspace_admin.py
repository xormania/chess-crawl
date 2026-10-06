"""Operator-only workspace lifecycle through direct database access."""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from chess_crawl.application.errors import ValidationError
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.storage import workspace_access
from chess_crawl.storage.db import DatabaseError, connection, database_url
from chess_crawl.storage.migrations import SCHEMA_VERSION, current_version
from chess_crawl.storage.work_budgets import set_workspace_policy


def _version(value: str) -> int:
    try:
        version = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("version must be a nonnegative PostgreSQL bigint") from None
    if not 0 <= version < 2**63:
        raise argparse.ArgumentTypeError("version must be a nonnegative PostgreSQL bigint")
    return version


def _policy(path: str) -> BudgetPolicy:
    values = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(values, dict):
        raise ValueError("Workspace policy must be a JSON object of BudgetPolicy fields")
    try:
        return BudgetPolicy(**values)
    except TypeError:
        raise ValueError("Workspace policy contains unknown fields") from None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Provision workspace service access and quotas using trusted database access")
    commands = parser.add_subparsers(dest="command", required=True)
    for name, description in (
        ("provision", "Create a workspace with managed policy and its first credential"),
        ("show", "Inspect current policy, usage, and credential metadata without secrets"),
        ("issue", "Issue an additional independently revocable credential"),
        ("rotate", "Atomically revoke all existing credentials and issue one replacement"),
        ("revoke", "Revoke one credential without changing workspace ownership or usage"),
        ("set-policy", "Replace workspace policy with an expected-version check"),
    ):
        command = commands.add_parser(name, help=description)
        command.add_argument("--workspace-id", required=True)
        command.add_argument("--database-url", help="Defaults to CHESS_CRAWL_DATABASE_URL")
        if name in {"provision", "set-policy"}:
            command.add_argument("--policy-file", required=True, help="JSON BudgetPolicy fields; omitted fields use finite application defaults")
        if name == "set-policy":
            command.add_argument("--expected-version", required=True, type=_version, help="Current revision; 0 explicitly initializes policy on an existing workspace")
        if name == "revoke":
            command.add_argument("--credential-id", required=True)
    args = parser.parse_args(argv)
    try:
        policy = _policy(args.policy_file) if args.command in {"provision", "set-policy"} else None
        with connection(database_url(args.database_url), mode="ro" if args.command == "show" else "rw") as conn:
            if current_version(conn) != SCHEMA_VERSION:
                raise ValueError("Run chess-crawl-admin migrate before workspace administration")
            if args.command == "provision":
                if policy is None:
                    raise ValueError("Workspace policy is required")
                result = workspace_access.provision_workspace(conn, args.workspace_id, policy)
            elif args.command == "show":
                result = workspace_access.workspace_snapshot(conn, args.workspace_id)
            elif args.command == "issue":
                result = workspace_access.issue_credential(conn, args.workspace_id)
            elif args.command == "rotate":
                result = workspace_access.rotate_credentials(conn, args.workspace_id)
            elif args.command == "revoke":
                result = workspace_access.revoke_credential(conn, args.workspace_id, args.credential_id)
            else:
                if policy is None:
                    raise ValueError("Workspace policy is required")
                version = set_workspace_policy(conn, args.workspace_id, policy, expected_version=args.expected_version)
                result = {"workspace_id": args.workspace_id, "policy_version": version}
        # Only issue/provision/rotate print a secret, once, after commit. Store it
        # directly in the SaaS secret manager rather than shell history or logs.
        print(json.dumps(result, sort_keys=True))
    except DatabaseError:
        print("Workspace administration: the PostgreSQL archive is unavailable", file=sys.stderr)
        return 1
    except (ValueError, ValidationError, OSError) as error:
        print(f"Workspace administration: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
