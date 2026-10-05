from __future__ import annotations

import json

import chess_crawl.jobs.state as state
from chess_crawl.jobs.budget import BudgetPolicy
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.db import require_row, transaction
from chess_crawl.storage.raw import latest_job_payload, store_raw_payload
from chess_crawl.storage.work_budgets import admit_run_budget
from chess_crawl.storage.workspaces import submission_context


def _indexes(node: dict) -> set[str]:
    found = {node["Index Name"]} if "Index Name" in node else set()
    for child in node.get("Plans", []):
        found.update(_indexes(child))
    return found


def test_budget_lookups_use_indexes_despite_large_terminal_history(initialized_conn) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, RawRecord(
        provider="lichess", endpoint_type="user_profile", request_url="https://lichess.org/api/user/target",
        canonical_source_key="lichess/user/target/profile", body=b'{"id":"target","username":"Target"}',
        media_type="application/json",
    ))
    with transaction(conn):
        submission_context(conn, "alpha")
        run_id = state.create_crawl_run(conn, provider="lichess", seed_spec="target", params={})
        job_id = state.enqueue_job(conn, provider="lichess", kind="normalize_payload", target=str(raw_id), crawl_run_id=run_id).job_id
        budget_id = admit_run_budget(conn, run_id, "alpha", BudgetPolicy())["id"]
        conn.execute("""INSERT INTO discovery_jobs(provider,kind,target,state,dedup_key,enqueued_at)
            SELECT 'lichess','normalize_payload','finished-'||n,'done','finished-'||n,0
            FROM generate_series(1,10000) n""")
        conn.execute("""INSERT INTO fetch_logs(provider,job_id,url,endpoint_type,status_code,attempted_at,raw_payload_id)
            SELECT 'lichess',id,'https://lichess.org/api/user/target','user_profile',200,0,%s
            FROM discovery_jobs WHERE target LIKE 'finished-%%'""", (raw_id,))
        conn.execute("""INSERT INTO fetch_logs(provider,job_id,url,endpoint_type,status_code,attempted_at,raw_payload_id)
            VALUES('lichess',%s,'https://lichess.org/api/user/target','user_profile',200,1,%s)""", (job_id,raw_id))
    conn.execute("ANALYZE discovery_jobs")
    conn.execute("ANALYZE fetch_logs")
    cases = [
        ("SELECT COUNT(*) FROM discovery_jobs WHERE work_budget_id=%s", (budget_id,), "ix_jobs_budget"),
        ("SELECT COUNT(*) FROM discovery_jobs WHERE workspace_id=%s AND state IN ('pending','blocked','in_progress')", ("alpha",), "ix_jobs_workspace_unfinished"),
        ("""SELECT f.raw_payload_id,f.id FROM fetch_logs f
            JOIN raw_payloads r ON r.id=f.raw_payload_id JOIN discovery_jobs j ON j.id=f.job_id
            WHERE f.job_id=%s AND f.status_code IN (200,304) AND r.endpoint_type=%s
              AND f.raw_payload_id IS NOT NULL AND r.provider=j.provider
              AND r.owner_scope IN ('public',j.workspace_id) AND r.canonical_source_key=%s
            ORDER BY f.attempted_at DESC,f.id DESC LIMIT 1""",
         (job_id,"user_profile","lichess/user/target/profile"), "ix_fetchlog_job_success"),
    ]
    for query, params, expected in cases:
        plan = require_row(conn.execute("EXPLAIN (ANALYZE,FORMAT JSON) " + query, params))[0]
        plan = json.loads(plan) if isinstance(plan, str) else plan
        assert expected in _indexes(plan[0]["Plan"])
    assert require_row(conn.execute(cases[0][0], cases[0][1]))[0] == 1
    assert require_row(conn.execute(cases[1][0], cases[1][1]))[0] == 1
    captured = latest_job_payload(conn, job_id, endpoint_type="user_profile", source_key="lichess/user/target/profile")
    assert captured is not None and captured[0] == raw_id
