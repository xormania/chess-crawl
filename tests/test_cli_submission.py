from __future__ import annotations

import json

from chess_crawl import cli
from chess_crawl.jobs import state
from chess_crawl.storage.db import open_database


def test_cli_queues_the_same_durable_import_as_the_application(database_url, capsys) -> None:
    args = [
        "submit", "import", "lichess", "Alice", "--since", "2024-01", "--until", "2024-02",
        "--max-games", "5", "--idempotency-key", "cli-import", "--database-url", str(database_url),
    ]
    assert cli.run(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert cli.run(args) == 0
    again = json.loads(capsys.readouterr().out)
    assert first["run_id"] == again["run_id"]
    assert again["replayed"] is True
    with open_database(database_url) as conn:
        jobs = [state.get_job(conn, job_id) for job_id in first["job_ids"]]
        assert [job.state for job in jobs if job] == ["pending", "pending"]
        assert [job.attempts for job in jobs if job] == [0, 0]
        assert jobs[1] is not None
        assert state.load_params(jobs[1].params_json)["until"] == 1706745600


def test_invalid_submission_does_not_connect(monkeypatch) -> None:
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid submission must fail before connecting")
    monkeypatch.setattr(cli, "open_database", forbidden)
    assert cli.run([
        "submit", "import", "lichess", "Alice", "--since", "2024-02", "--until", "2024-01",
        "--max-games", "5", "--idempotency-key", "cli-invalid", "--database-url",
        "postgresql://test@127.0.0.1:1/unavailable",
    ]) == 2
