"""Command line interface for chess-crawl."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence
from time import time

from chess_crawl import __version__
from chess_crawl import application
from chess_crawl.export.writers import export_games_jsonl, export_graph_csv, export_users_jsonl
from chess_crawl.ingest import (
    IngestResult,
    fetch_chesscom_archives,
    fetch_chesscom_month,
    fetch_chesscom_stats,
    fetch_lichess_games,
    fetch_user_profile,
)
from chess_crawl.jobs import state as job_state
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs.locking import ExecutorBusy, ExecutorLease, ExecutorLeaseLost, executor_lock
from chess_crawl.providers.registry import list_provider_infos
from chess_crawl.storage.queries import (
    games_by_month,
    opponent_report,
    query_game,
    query_raw,
    query_user,
    summary_report,
    user_game_summary,
)
from chess_crawl.storage.db import (
    Connection,
    DatabaseError,
    connection,
    database_label,
    database_url,
    open_database,
)
from chess_crawl.storage.migrations import initialize
from chess_crawl.storage.repository import database_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chess-crawl",
        description="Provider-neutral, raw-first PostgreSQL chess archive.",
    )
    parser.add_argument("--version", action="version", version=f"chess-crawl {__version__}")

    subcommands = parser.add_subparsers(dest="command", required=True)

    init_parser = subcommands.add_parser("init", help="Initialize or migrate the PostgreSQL archive schema")
    _add_database_url(init_parser)
    init_parser.set_defaults(handler=_cmd_init)

    provider_parser = subcommands.add_parser("provider", help="Inspect configured providers")
    provider_subcommands = provider_parser.add_subparsers(dest="provider_command", required=True)
    provider_list_parser = provider_subcommands.add_parser("list", help="List supported providers")
    provider_list_parser.set_defaults(handler=_cmd_provider_list)

    db_parser = subcommands.add_parser("db", help="Inspect PostgreSQL archive state")
    db_subcommands = db_parser.add_subparsers(dest="db_command", required=True)
    db_info_parser = db_subcommands.add_parser("info", help="Print schema and provider summary")
    _add_database_url(db_info_parser)
    db_info_parser.set_defaults(handler=_cmd_db_info)

    fetch_parser = subcommands.add_parser("fetch", help="Fetch bounded public provider data")
    fetch_subcommands = fetch_parser.add_subparsers(dest="fetch_command", required=True)

    fetch_user_parser = fetch_subcommands.add_parser("user", help="Fetch and normalize a public user profile")
    fetch_user_parser.add_argument("provider", choices=("chess.com", "lichess"))
    fetch_user_parser.add_argument("username")
    _add_database_url(fetch_user_parser)
    fetch_user_parser.set_defaults(handler=_cmd_fetch_user)

    fetch_stats_parser = fetch_subcommands.add_parser("stats", help="Fetch and normalize Chess.com public stats")
    fetch_stats_parser.add_argument("provider", choices=("chess.com",))
    fetch_stats_parser.add_argument("username")
    _add_database_url(fetch_stats_parser)
    fetch_stats_parser.set_defaults(handler=_cmd_fetch_stats)

    fetch_archives_parser = fetch_subcommands.add_parser("archives", help="Fetch Chess.com monthly archive index")
    fetch_archives_parser.add_argument("provider", choices=("chess.com",))
    fetch_archives_parser.add_argument("username")
    _add_database_url(fetch_archives_parser)
    fetch_archives_parser.set_defaults(handler=_cmd_fetch_archives)

    fetch_games_parser = fetch_subcommands.add_parser("games", help="Fetch bounded public games")
    fetch_games_parser.add_argument("provider", choices=("chess.com", "lichess"))
    fetch_games_parser.add_argument("username")
    _add_database_url(fetch_games_parser)
    fetch_games_parser.add_argument("--month", help="Chess.com month bound in YYYY-MM form")
    fetch_games_parser.add_argument("--since", help="Lichess inclusive lower date bound, YYYY-MM-DD")
    fetch_games_parser.add_argument("--until", help="Lichess exclusive upper date bound, YYYY-MM-DD")
    fetch_games_parser.add_argument("--limit", type=int, help="Lichess max games to fetch")
    fetch_games_parser.set_defaults(handler=_cmd_fetch_games)

    submit_parser = subcommands.add_parser("submit", help="Queue durable acquisition for a separate worker")
    submit_commands = submit_parser.add_subparsers(dest="submit_command", required=True)
    import_parser = submit_commands.add_parser("import", help="Queue a bounded player profile and game import")
    import_parser.add_argument("provider", choices=("chess.com", "lichess"))
    import_parser.add_argument("username")
    import_parser.add_argument("--since", required=True, help="Inclusive YYYY-MM or YYYY-MM-DD")
    import_parser.add_argument("--until", required=True, help="Exclusive YYYY-MM or YYYY-MM-DD")
    import_parser.add_argument("--max-games", required=True, type=int)
    import_parser.add_argument("--idempotency-key", required=True)
    _add_database_url(import_parser)
    import_parser.set_defaults(handler=_cmd_submit_import)

    query_parser = subcommands.add_parser("query", help="Query the archive")
    query_subcommands = query_parser.add_subparsers(dest="query_command", required=True)

    query_user_parser = query_subcommands.add_parser("user", help="Query a provider-scoped user")
    query_user_parser.add_argument("provider", choices=("chess.com", "lichess"))
    query_user_parser.add_argument("username")
    _add_database_url(query_user_parser)
    query_user_parser.set_defaults(handler=_cmd_query_user)

    query_game_parser = query_subcommands.add_parser("game", help="Query a provider-scoped game")
    query_game_parser.add_argument("provider", choices=("chess.com", "lichess"))
    query_game_parser.add_argument("game_id")
    _add_database_url(query_game_parser)
    query_game_parser.set_defaults(handler=_cmd_query_game)

    query_raw_parser = query_subcommands.add_parser("raw", help="List stored raw payloads")
    query_raw_parser.add_argument("--provider", choices=("chess.com", "lichess"), required=True)
    query_raw_parser.add_argument("--limit", type=int, default=10)
    _add_database_url(query_raw_parser)
    query_raw_parser.set_defaults(handler=_cmd_query_raw)

    crawl_parser = subcommands.add_parser("crawl", help="Run bounded discovery strategies")
    crawl_subcommands = crawl_parser.add_subparsers(dest="crawl_command", required=True)
    crawl_opp_parser = crawl_subcommands.add_parser("opponents", help="Bounded provider-scoped opponent crawl")
    crawl_opp_parser.add_argument("provider", choices=("chess.com", "lichess"))
    crawl_opp_parser.add_argument("username")
    crawl_opp_parser.add_argument("--depth", type=int, required=True)
    crawl_opp_parser.add_argument("--max-users", type=int, required=True)
    crawl_opp_parser.add_argument("--max-games", type=int, required=True)
    crawl_opp_parser.add_argument("--max-jobs", type=int, required=True)
    crawl_opp_parser.add_argument("--since", required=True, help="YYYY-MM or YYYY-MM-DD inclusive lower bound")
    crawl_opp_parser.add_argument("--until", required=True, help="YYYY-MM or YYYY-MM-DD exclusive upper bound")
    _add_database_url(crawl_opp_parser)
    crawl_opp_parser.add_argument("--enqueue-only", action="store_true", help="Leave execution to the archive worker")
    crawl_opp_parser.add_argument("--idempotency-key", help="Reuse a previous submission with these exact parameters")
    crawl_opp_parser.set_defaults(handler=_cmd_crawl_opponents)

    jobs_parser = subcommands.add_parser("jobs", help="Inspect and resume durable jobs")
    jobs_subcommands = jobs_parser.add_subparsers(dest="jobs_command", required=True)
    jobs_status_parser = jobs_subcommands.add_parser("status", help="Summarize job states and crawl runs")
    _add_database_url(jobs_status_parser)
    jobs_status_parser.add_argument("--run", type=int, help="Limit to one crawl_run id")
    jobs_status_parser.set_defaults(handler=_cmd_jobs_status)
    jobs_list_parser = jobs_subcommands.add_parser("list", help="List recent jobs")
    _add_database_url(jobs_list_parser)
    jobs_list_parser.add_argument("--limit", type=int, default=100)
    jobs_list_parser.set_defaults(handler=_cmd_jobs_list)
    jobs_show_parser = jobs_subcommands.add_parser("show", help="Show one job")
    jobs_show_parser.add_argument("job_id", type=int)
    _add_database_url(jobs_show_parser)
    jobs_show_parser.set_defaults(handler=_cmd_jobs_show)
    jobs_resume_parser = jobs_subcommands.add_parser("resume", help="Resume stale and pending jobs")
    _add_database_url(jobs_resume_parser)
    jobs_resume_parser.add_argument("--run", type=int, help="Limit to one crawl_run id")
    jobs_resume_parser.add_argument("--max-jobs", type=int, help="Maximum jobs to execute in this invocation")
    jobs_resume_parser.set_defaults(handler=_cmd_jobs_resume)

    report_parser = subcommands.add_parser("report", help="Read-only archive reports")
    report_subcommands = report_parser.add_subparsers(dest="report_command", required=True)
    report_summary_parser = report_subcommands.add_parser("summary", help="Archive summary")
    _add_database_url(report_summary_parser)
    report_summary_parser.set_defaults(handler=_cmd_report_summary)
    report_user_parser = report_subcommands.add_parser("user", help="Provider-scoped user summary")
    report_user_parser.add_argument("provider", choices=("chess.com", "lichess"))
    report_user_parser.add_argument("username")
    _add_database_url(report_user_parser)
    report_user_parser.set_defaults(handler=_cmd_report_user)
    report_opponents_parser = report_subcommands.add_parser("opponents", help="Provider-scoped opponent summary")
    report_opponents_parser.add_argument("provider", choices=("chess.com", "lichess"))
    report_opponents_parser.add_argument("username")
    _add_database_url(report_opponents_parser)
    report_opponents_parser.set_defaults(handler=_cmd_report_opponents)
    report_month_parser = report_subcommands.add_parser("games-by-month", help="Provider game counts by month")
    report_month_parser.add_argument("--provider", choices=("chess.com", "lichess"), required=True)
    _add_database_url(report_month_parser)
    report_month_parser.set_defaults(handler=_cmd_report_games_by_month)

    export_parser = subcommands.add_parser("export", help="Export normalized local data")
    export_subcommands = export_parser.add_subparsers(dest="export_command", required=True)
    export_games_parser = export_subcommands.add_parser("games", help="Export normalized games")
    export_games_parser.add_argument("--format", choices=("jsonl",), required=True)
    export_games_parser.add_argument("--output", type=Path, help="Output file; stdout when omitted")
    export_games_parser.add_argument("--provider", choices=("chess.com", "lichess"))
    _add_database_url(export_games_parser)
    export_games_parser.set_defaults(handler=_cmd_export_games)
    export_users_parser = export_subcommands.add_parser("users", help="Export normalized users")
    export_users_parser.add_argument("--format", choices=("jsonl",), required=True)
    export_users_parser.add_argument("--output", type=Path, help="Output file; stdout when omitted")
    export_users_parser.add_argument("--provider", choices=("chess.com", "lichess"))
    _add_database_url(export_users_parser)
    export_users_parser.set_defaults(handler=_cmd_export_users)
    export_graph_parser = export_subcommands.add_parser("graph", help="Export discovery graph edges")
    export_graph_parser.add_argument("--format", choices=("csv",), required=True)
    export_graph_parser.add_argument("--output", type=Path, help="Output file; stdout when omitted")
    export_graph_parser.add_argument("--provider", choices=("chess.com", "lichess"))
    _add_database_url(export_graph_parser)
    export_graph_parser.set_defaults(handler=_cmd_export_graph)

    return parser


def _add_database_url(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--database-url",
        help="PostgreSQL connection URL; defaults to CHESS_CRAWL_DATABASE_URL",
    )


@contextmanager
def _writable_database(target: str, *, executor: bool = True) -> Iterator[tuple[Connection, ExecutorLease | None]]:
    # Acquire executor ownership before migrations or acquisition mutations.
    # A losing contender may connect, but must leave the owner's state intact.
    with connection(target, mode="rw") as conn:
        with executor_lock(conn) if executor else nullcontext() as lease:
            initialize(conn)
            yield conn, lease


def _cmd_init(args: argparse.Namespace) -> int:
    with connection(args.database_url, mode="rw") as conn:
        result = initialize(conn)
    applied = ", ".join(result.applied) if result.applied else "none"

    print(f"Database: {database_label(args.database_url)}")
    print(f"Schema version: {result.version}")
    print(f"Applied migrations: {applied}")
    print(f"Providers: {', '.join(result.providers)}")
    return 0


def _cmd_provider_list(args: argparse.Namespace) -> int:
    del args
    rows = list_provider_infos()
    headers = ("PROVIDER", "ID MODEL", "ARCHIVE UNIT", "CACHING", "429 POLICY", "AUTH")
    table = [
        (
            row.key,
            row.id_model,
            row.archive_unit,
            row.caching,
            row.rate_limit,
            row.auth,
        )
        for row in rows
    ]
    _print_table(headers, table)
    return 0


def _cmd_db_info(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        summary = database_summary(conn)

    print(f"Database: {database_label(args.database_url)}")
    print(f"Schema version: {summary.schema_version}")
    print(f"Migrations: {summary.migration_count}")
    print(f"Tables: {summary.table_count}")
    print(f"Providers: {', '.join(summary.providers)}")
    return 0


def _require_provider_ready(conn, provider: str) -> None:
    deadline = job_state.provider_ready_at(conn, provider)
    if deadline is not None and deadline > time():
        ready_at = datetime.fromtimestamp(deadline, UTC).isoformat()
        raise ValueError(f"{provider} is cooling down until {ready_at}; retry this fetch after that deadline.")


def _cmd_fetch_user(args: argparse.Namespace) -> int:
    with _writable_database(args.database_url) as (conn, _lease):
        _require_provider_ready(conn, args.provider)
        result = fetch_user_profile(conn, args.provider, args.username)
    return _print_ingest_result(result)


def _cmd_fetch_stats(args: argparse.Namespace) -> int:
    with _writable_database(args.database_url) as (conn, _lease):
        _require_provider_ready(conn, args.provider)
        result = fetch_chesscom_stats(conn, args.username)
    return _print_ingest_result(result)


def _cmd_fetch_archives(args: argparse.Namespace) -> int:
    with _writable_database(args.database_url) as (conn, _lease):
        _require_provider_ready(conn, args.provider)
        result = fetch_chesscom_archives(conn, args.username)
    return _print_ingest_result(result)


def _cmd_fetch_games(args: argparse.Namespace) -> int:
    with _writable_database(args.database_url) as (conn, _lease):
        _require_provider_ready(conn, args.provider)
        if args.provider == "chess.com":
            if not args.month:
                print("Chess.com game fetch requires --month YYYY-MM.", file=sys.stderr)
                return 2
            year, month = _parse_month(args.month)
            result = fetch_chesscom_month(conn, args.username, year, month)
        else:
            if args.limit is None or args.limit <= 0:
                print("Lichess game fetch requires --limit N with N > 0.", file=sys.stderr)
                return 2
            if not args.since or not args.until:
                print("Lichess game fetch requires --since YYYY-MM-DD and --until YYYY-MM-DD.", file=sys.stderr)
                return 2
            result = fetch_lichess_games(
                conn,
                args.username,
                since=_parse_date(args.since),
                until=_parse_date(args.until),
                limit=args.limit,
            )
    return _print_ingest_result(result)


def _cmd_query_user(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        row = query_user(conn, args.provider, args.username)
    if row is None:
        print("User not found.", file=sys.stderr)
        return 1
    print(f"Provider: {row.provider}")
    print(f"Username: {row.display_username} ({row.username})")
    print(f"Provider user id: {row.provider_user_id or '-'}")
    print(f"Title: {row.title or '-'}")
    print(f"Status: {row.account_status or '-'}")
    print(f"Snapshots: {row.snapshots}")
    print(f"Games: {row.games}")
    return 0


def _cmd_query_game(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        row = query_game(conn, args.provider, args.game_id)
    if row is None:
        print("Game not found.", file=sys.stderr)
        return 1
    print(f"Provider: {row.provider}")
    print(f"Game id: {row.provider_game_id or '-'}")
    print(f"URL: {row.canonical_url or '-'}")
    print(f"Players: {row.white or '-'} vs {row.black or '-'}")
    print(f"Outcome: {row.outcome or '-'}")
    print(f"Live: {int(row.is_live)}")
    print(f"Status: {row.status_raw or '-'}")
    print(f"Ended at: {row.ended_at or '-'}")
    print(f"Variant: {row.variant}")
    print(f"Time class: {row.time_class}")
    return 0


def _cmd_query_raw(args: argparse.Namespace) -> int:
    if args.limit <= 0:
        print("--limit must be greater than zero.", file=sys.stderr)
        return 2
    with open_database(args.database_url) as conn:
        rows = query_raw(conn, args.provider, args.limit)
    headers = ("ID", "PROVIDER", "ENDPOINT", "STATUS", "BYTES", "NORM", "SOURCE")
    table = [
        (
            str(row["id"]),
            row["provider"],
            row["endpoint_type"],
            str(row["response_status"]),
            str(row["body_bytes"]),
            row["normalization_status"],
            row["canonical_source_key"],
        )
        for row in rows
    ]
    _print_table(headers, table)
    return 0


def _cmd_submit_import(args: argparse.Namespace) -> int:
    limits = application.Limits.from_env()
    request = application.validate_import(application.ImportRequest(
        provider=args.provider, username=args.username,
        since=_parse_date_or_month(args.since), until=_parse_date_or_month(args.until),
        max_games=args.max_games,
    ), limits=limits)
    application.validate_idempotency_key(args.idempotency_key)
    with open_database(args.database_url, writable=True) as conn:
        result = application.submit_import(conn, request, idempotency_key=args.idempotency_key, limits=limits)
    print(json.dumps(result, sort_keys=True))
    return 0


def _cmd_crawl_opponents(args: argparse.Namespace) -> int:
    limits = application.Limits.from_env()
    request = application.validate_crawl(application.CrawlRequest(
        provider=args.provider,
        username=args.username,
        since=_parse_date_or_month(args.since),
        until=_parse_date_or_month(args.until),
        max_depth=args.depth,
        max_users=args.max_users,
        max_games=args.max_games,
        max_jobs=args.max_jobs,
    ), limits=limits)
    key = application.validate_idempotency_key(args.idempotency_key or uuid.uuid4().hex)
    with _writable_database(args.database_url, executor=not args.enqueue_only) as (conn, lease):
        submission = application.submit_crawl(conn, request, idempotency_key=key, limits=limits)
        if args.enqueue_only:
            print(json.dumps(submission, sort_keys=True))
            return 0
        run_id, root_job_id = submission["run_id"], submission["job_ids"][0]
        result = JobRunner(conn, lease=lease).run(crawl_run_id=run_id)
    print(f"crawl_run #{run_id} started ({args.provider}, seed={args.username.strip().lower()}, depth={args.depth})")
    print(f"root job: {root_job_id}")
    print(
        "Jobs: "
        f"{result.done} done, {result.skipped} skipped, {result.blocked} blocked, {result.errors} failed"
    )
    if result.errors:
        return 5
    if result.blocked:
        return 4
    return 0


def _cmd_jobs_status(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        runs = job_state.crawl_runs(conn)
        states = job_state.job_state_counts(conn, crawl_run_id=args.run)
        by_kind = job_state.job_kind_state_counts(conn, crawl_run_id=args.run)

    print("Crawl runs")
    run_rows = [
        (
            str(row["id"]),
            row["provider"],
            row["status"],
            row["seed_spec"],
            str(row["started_at"]),
            str(row["finished_at"] or "-"),
        )
        for row in runs
        if args.run is None or int(row["id"]) == args.run
    ]
    _print_table(("ID", "PROVIDER", "STATUS", "SEED", "STARTED", "FINISHED"), run_rows)
    print()
    print("Job states")
    _print_table(("STATE", "COUNT"), [(row["state"], str(row["count"])) for row in states])
    print()
    print("By kind/depth")
    _print_table(
        ("DEPTH", "KIND", "STATE", "COUNT"),
        [(str(row["depth"]), row["kind"], row["state"], str(row["count"])) for row in by_kind],
    )
    return 0


def _cmd_jobs_list(args: argparse.Namespace) -> int:
    if args.limit <= 0:
        print("--limit must be greater than zero.", file=sys.stderr)
        return 2
    with open_database(args.database_url) as conn:
        rows = job_state.list_jobs(conn, limit=args.limit)
    _print_table(
        ("ID", "RUN", "PROVIDER", "KIND", "TARGET", "STATE", "DEPTH", "ATTEMPTS"),
        [
            (
                str(row["id"]),
                str(row["crawl_run_id"] or "-"),
                row["provider"],
                row["kind"],
                row["target"],
                row["state"],
                str(row["depth"]),
                str(row["attempts"]),
            )
            for row in rows
        ],
    )
    return 0


def _cmd_jobs_show(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        job = job_state.get_job(conn, args.job_id)
    if job is None:
        print(f"Job not found: {args.job_id}", file=sys.stderr)
        return 1
    print(f"ID: {job.id}")
    print(f"Run: {job.crawl_run_id or '-'}")
    print(f"Parent: {job.parent_job_id or '-'}")
    print(f"Provider: {job.provider}")
    print(f"Kind: {job.kind}")
    print(f"Target: {job.target}")
    print(f"State: {job.state}")
    print(f"Depth: {job.depth}")
    print(f"Priority: {job.priority}")
    print(f"Attempts: {job.attempts}")
    print(f"Dedup key: {job.dedup_key}")
    print(f"Reason: {job.reason or '-'}")
    print("Params:")
    print(json.dumps(job_state.load_params(job.params_json), indent=2, sort_keys=True))
    return 0


def _cmd_jobs_resume(args: argparse.Namespace) -> int:
    if args.max_jobs is not None and args.max_jobs <= 0:
        print("--max-jobs must be greater than zero.", file=sys.stderr)
        return 2
    with _writable_database(args.database_url) as (conn, lease):
        result = JobRunner(conn, lease=lease).run(
            crawl_run_id=args.run,
            max_jobs=args.max_jobs,
            resume_stale=True,
            unblock=True,
        )
    print(f"Stale in_progress -> pending: {result.stale_resumed}")
    print(f"Blocked -> pending: {result.unblocked}")
    print(
        "Jobs: "
        f"{result.done} done, {result.skipped} skipped, {result.blocked} blocked, {result.errors} failed"
    )
    if result.errors:
        return 5
    if result.blocked:
        return 4
    return 0


def _cmd_report_summary(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        report = summary_report(conn)
    print("Providers")
    _print_table(
        ("PROVIDER", "USERS", "GAMES"),
        [(row["provider"], str(row["users"]), str(row["games"])) for row in report["providers"]],
    )
    print(f"Raw payloads: {report['raw_payloads']}")
    print()
    print("Crawl runs")
    _print_table(("STATUS", "COUNT"), [(row["status"], str(row["count"])) for row in report["runs"]])
    print()
    print("Jobs")
    _print_table(("STATE", "COUNT"), [(row["state"], str(row["count"])) for row in report["jobs"]])
    return 0


def _cmd_report_user(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        row = user_game_summary(conn, args.provider, args.username)
    if row is None:
        print("User not found.", file=sys.stderr)
        return 1
    print(f"Provider: {row['provider']}")
    print(f"Username: {row['display_username']} ({row['username']})")
    print(f"Provider-supplied account status: {row['account_status'] or '-'}")
    print(f"Games: {row['games']} ({row['rated_games']} rated, {row['unrated_games']} unrated)")
    print(f"W/D/L/no result: {row['wins']}/{row['draws']}/{row['losses']}/{row['no_result']}")
    print(f"In progress: {row['in_progress']}")
    print(f"Distinct opponents: {row['distinct_opponents']}")
    print(f"First game: {row['first_game_ts'] or '-'}")
    print(f"Last game: {row['last_game_ts'] or '-'}")
    return 0


def _cmd_report_opponents(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        rows = opponent_report(conn, args.provider, args.username)
    if rows is None:
        print("User not found.", file=sys.stderr)
        return 1
    _print_table(
        ("PROVIDER", "OPPONENT", "GAMES", "MY_WINS", "DRAWS", "MY_LOSSES", "NO_RESULT", "IN_PROGRESS"),
        [
            (
                row["provider"],
                row["opponent_display"],
                str(row["games"]),
                str(row["my_wins"]),
                str(row["draws"]),
                str(row["my_losses"]),
                str(row["no_result"]),
                str(row["in_progress"]),
            )
            for row in rows
        ],
    )
    return 0


def _cmd_report_games_by_month(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        rows = games_by_month(conn, provider=args.provider)
    _print_table(
        ("MONTH", "GAMES", "WHITE_WINS", "BLACK_WINS", "DRAWS", "NO_RESULT", "IN_PROGRESS"),
        [
            (
                row["month"],
                str(row["games"]),
                str(row["white_wins"]),
                str(row["black_wins"]),
                str(row["draws"]),
                str(row["no_result"]),
                str(row["in_progress"]),
            )
            for row in rows
        ],
    )
    return 0


def _cmd_export_games(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        count = export_games_jsonl(conn, output=args.output, provider=args.provider)
    _print_export_result("games", count, args.output)
    return 0


def _cmd_export_users(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        count = export_users_jsonl(conn, output=args.output, provider=args.provider)
    _print_export_result("users", count, args.output)
    return 0


def _cmd_export_graph(args: argparse.Namespace) -> int:
    with open_database(args.database_url) as conn:
        count = export_graph_csv(conn, output=args.output, provider=args.provider)
    _print_export_result("graph edges", count, args.output)
    return 0


def _print_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))

    fmt = "  ".join(f"{{:<{width}}}" for width in widths)
    print(fmt.format(*headers))
    for row in rows:
        print(fmt.format(*row))


def _print_ingest_result(result: IngestResult) -> int:
    print(f"Provider: {result.provider}")
    print(f"Endpoint: {result.endpoint_type}")
    print(f"HTTP status: {result.status_code}")
    print(f"Raw payload: {result.raw_payload_id or '-'}")
    print(f"Normalized rows: {len(result.normalized_ids)}")
    print(result.message)
    return 0 if result.status_code in {200, 304} else 1


def _print_export_result(label: str, count: int, output: Path | None) -> None:
    destination = str(output) if output is not None else "stdout"
    stream = sys.stdout if output is not None else sys.stderr
    print(f"Exported {count} {label} to {destination}.", file=stream)


def _parse_month(value: str) -> tuple[int, int]:
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise SystemExit("--month must be in YYYY-MM form.") from exc
    return parsed.year, parsed.month


def _parse_date(value: str) -> int:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError as exc:
        raise SystemExit("Dates must be in YYYY-MM-DD form.") from exc
    return int(parsed.timestamp())


def _parse_date_or_month(value: str) -> int:
    if len(value) == 7:
        year, month = _parse_month(value)
        parsed = datetime(year, month, 1, tzinfo=UTC)
        return int(parsed.timestamp())
    return _parse_date(value)


def run(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if hasattr(args, "database_url"):
            args.database_url = database_url(args.database_url)
        return args.handler(args)
    except application.ValidationError as exc:
        print(exc.message, file=sys.stderr)
        return 2
    except application.ApplicationError as exc:
        print(exc.message, file=sys.stderr)
        return 1
    except DatabaseError:
        print("The PostgreSQL archive is unavailable or its schema requires initialization.", file=sys.stderr)
        return 1
    except (FileNotFoundError, ValueError, ExecutorBusy, ExecutorLeaseLost) as exc:
        print(str(exc), file=sys.stderr)
        return 1


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


if __name__ == "__main__":
    main()
