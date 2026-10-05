from __future__ import annotations

import json
import threading
import copy
import time
from typing import cast

import pytest

from chess_crawl.jobs import state
from chess_crawl.jobs.dispatch import SqsConsumer, SqsDispatcher
from chess_crawl.jobs.runner import JobRunner
from chess_crawl.jobs.worker import Worker
from chess_crawl.ingest import installed_parser_target
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.raw import store_raw_payload, insert_fetch_log
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.normalize import users as user_normalization
from chess_crawl.normalize import games as normalization
from chess_crawl.storage.repository import upsert_provider_user
from chess_crawl.storage.db import (
    Connection, ExecutorLeaseLost, connection, lock_key, release_lock_key, require_row, transaction,
)


def test_independent_writes_do_not_take_archive_exclusive_lock(database_url: str) -> None:
    finished = threading.Event()
    failures: list[BaseException] = []

    def write_other() -> None:
        try:
            with connection(database_url, mode="rw") as conn:
                state.enqueue_job(conn, provider="lichess", kind="fetch_user_profile", target="second")
        except BaseException as exc:
            failures.append(exc)
        finally:
            finished.set()

    with connection(database_url, mode="rw") as conn, transaction(conn):
        state.enqueue_job(conn, provider="chess.com", kind="fetch_user_profile", target="first")
        thread = threading.Thread(target=write_other)
        thread.start()
        assert finished.wait(3), "Independent transaction waited on the entire archive"
    thread.join(3)
    assert failures == []


def test_claims_serialize_provider_acquisition_but_not_processing(database_url: str) -> None:
    with connection(database_url, mode="rw") as setup:
        first = state.enqueue_job(setup, provider="lichess", kind="fetch_user_profile", target="first", priority=1).job_id
        second = state.enqueue_job(setup, provider="lichess", kind="fetch_user_profile", target="second", priority=2).job_id
        local = state.enqueue_job(setup, provider="lichess", kind="normalize_payload", target="123", priority=3).job_id
    with connection(database_url, mode="rw") as owner, connection(database_url, mode="rw") as other:
        first_claim = state.claim_next_job(owner, worker_id="owner")
        local_claim = state.claim_next_job(other, worker_id="other")
        assert first_claim is not None and first_claim.id == first
        assert local_claim is not None and local_claim.id == local
        state.finish_attempt(other, local, "done", reason="local")
        state.release_job_ownership(other)
        assert state.claim_next_job(other, worker_id="other") is None
        state.finish_attempt(owner, first, "done", reason="provider")
        state.release_job_ownership(owner)
        second_claim = state.claim_next_job(other, worker_id="other")
        assert second_claim is not None and second_claim.id == second
        state.release_job_ownership(other)


@pytest.mark.parametrize("first_priority", [20, 10])
def test_checkpointed_job_returns_behind_equal_priority_work(
    initialized_conn: Connection, first_priority: int,
) -> None:
    first = state.enqueue_job(
        initialized_conn, provider="lichess", kind="normalize_payload", target="1", priority=first_priority,
    ).job_id
    claimed = state.claim_next_job(initialized_conn, now=100)
    assert claimed is not None and claimed.id == first
    second = state.enqueue_job(
        initialized_conn, provider="lichess", kind="normalize_payload", target="2", priority=20,
    ).job_id
    initialized_conn.execute(
        "UPDATE discovery_jobs SET enqueued_at=CASE id WHEN %s THEN 100 WHEN %s THEN 150 END WHERE id IN (%s,%s)",
        (first, second, first, second),
    )
    state.finish_attempt(initialized_conn, first, "pending", reason="checkpointed", now=200)
    state.release_job_ownership(initialized_conn)
    next_job = state.claim_next_job(initialized_conn, now=201)
    assert next_job is not None and next_job.id == (second if first_priority == 20 else first)
    assert next_job.priority == first_priority
    state.release_job_ownership(initialized_conn)


def test_heartbeat_age_cannot_steal_live_job_and_manual_release_fences_stale_owner(database_url: str) -> None:
    with connection(database_url, mode="rw") as owner, connection(database_url, mode="rw") as successor:
        job_id = state.enqueue_job(owner, provider="lichess", kind="normalize_payload", target="123").job_id
        first = state.claim_next_job(owner, worker_id="old", now=1)
        assert first is not None
        assert state.resume_stale_in_progress(successor, now=10**10) == 0
        release_lock_key(owner, lock_key("executor-job", job_id))
        assert state.resume_stale_in_progress(successor, now=10**10) == 1
        replacement = state.claim_next_job(successor, worker_id="new")
        assert replacement is not None and replacement.ownership_token != first.ownership_token
        with pytest.raises(ExecutorLeaseLost):
            state.mark_done(owner, job_id, reason="stale completion")
        with pytest.raises(ExecutorLeaseLost):
            state.enqueue_job(owner, provider="lichess", kind="normalize_payload", target="456")
        state.finish_attempt(successor, job_id, "done", reason="new owner")
        state.release_job_ownership(successor)
        state.release_job_ownership(owner)


def test_crashed_session_recovery_and_duplicate_delivery(database_url: str) -> None:
    with connection(database_url, mode="rw") as owner:
        job_id = state.enqueue_job(owner, provider="lichess", kind="normalize_payload", target="123").job_id
        first = state.claim_next_job(owner, worker_id="crashed")
        assert first is not None
        generation = first.ownership_generation
    with connection(database_url, mode="rw") as successor:
        assert state.resume_stale_in_progress(successor) == 1
        next_job = state.claim_next_job(successor, worker_id="new", job_id=job_id)
        assert next_job is not None and next_job.ownership_generation == generation + 1
        state.finish_attempt(successor, job_id, "done", reason="complete")
        state.release_job_ownership(successor)
        assert state.claim_next_job(successor, worker_id="duplicate", job_id=job_id) is None
        assert require_row(successor.execute("SELECT attempts FROM discovery_jobs WHERE id=%s", (job_id,)))[0] == 2


class FakeSqs:
    def __init__(self) -> None:
        self.messages: list[dict[str, str]] = []
        self.deleted: list[str] = []
        self.fail = False
        self.queues: list[str] = []

    def send_message(self, **kwargs):
        if self.fail:
            raise RuntimeError("fail")
        self.messages.append({"Body": kwargs["MessageBody"], "ReceiptHandle": str(len(self.messages))})
        self.queues.append(kwargs["QueueUrl"])
        return {}

    def receive_message(self, **kwargs):
        return {"Messages": self.messages[:1]}

    def delete_message(self, **kwargs):
        self.deleted.append(kwargs["ReceiptHandle"])
        self.messages = self.messages[1:]


@pytest.mark.parametrize("queue_visibility", [120, 3600])
def test_consumer_preserves_configured_queue_visibility(queue_visibility) -> None:
    class ConfiguredQueue(FakeSqs):
        effective_visibility = None

        def receive_message(self, **kwargs):
            self.effective_visibility = kwargs.get("VisibilityTimeout", queue_visibility)
            assert kwargs["QueueUrl"] == "configured-queue"
            assert kwargs["WaitTimeSeconds"] == 7 and kwargs["MaxNumberOfMessages"] == 1
            return {"Messages": []}

    sqs = ConfiguredQueue()
    assert SqsConsumer(sqs, "configured-queue", wait_seconds=7).run_once(cast(Connection, object()), lambda identity: 1) == 0
    assert sqs.effective_visibility == queue_visibility


@pytest.mark.parametrize("job_id", [2**63, 10**100])
def test_overflow_queue_job_ids_stay_malformed_without_database_execution(job_id) -> None:
    sqs = FakeSqs()
    sqs.messages = [{"Body": json.dumps({"job_id": job_id}), "ReceiptHandle": "malformed"}]
    calls: list[int] = []

    def execute(identity: int) -> int:
        calls.append(identity)
        return 0

    assert SqsConsumer(sqs, "queue", wait_seconds=0).run_once(cast(Connection, object()), execute) == 0
    assert calls == [] and sqs.deleted == []


def test_dispatch_failure_preserves_outbox_and_duplicate_terminal_delivery_is_acked(initialized_conn) -> None:
    sqs = FakeSqs()
    job_id = state.enqueue_job(initialized_conn, provider="lichess", kind="normalize_payload", target="123").job_id
    dispatcher = SqsDispatcher(sqs, "queue", clock=lambda: 100)
    sqs.fail = True
    assert not dispatcher.publish_one(initialized_conn)
    assert require_row(initialized_conn.execute("SELECT delivered_at FROM dispatch_outbox"))[0] is None
    sqs.fail = False
    dispatcher.clock = lambda: 130
    assert dispatcher.publish_one(initialized_conn)
    assert json.loads(sqs.messages[0]["Body"]) == {"job_id": job_id}
    state.mark_done(initialized_conn, job_id)
    calls: list[int] = []
    def execute(identity: int) -> int:
        calls.append(identity)
        return 0
    assert SqsConsumer(sqs, "queue", wait_seconds=0).run_once(
        initialized_conn, execute,
    ) == 0
    assert calls == [job_id] and sqs.deleted == ["0"]


@pytest.mark.parametrize("hint", ["expired", "busy-provider"])
def test_sqs_worker_polls_database_after_missing_or_unclaimable_hint(database_url: str, hint: str) -> None:
    sqs = FakeSqs()
    with connection(database_url, mode="rw") as owner:
        raw_id = store_raw_payload(owner, RawRecord(
            provider="lichess", endpoint_type="user_profile", request_url="https://lichess.org/api/user/stored",
            canonical_source_key="lichess/user/stored/profile",
            body=json.dumps({"id": "stored", "username": "Stored"}).encode(), media_type="application/json",
        ))
        local = state.enqueue_job(
            owner, provider="lichess", kind="normalize_payload", target=str(raw_id), priority=20,
        ).job_id
        blocked = None
        if hint == "expired":
            assert SqsDispatcher(sqs, "queue").publish_one(owner)
            assert require_row(owner.execute("SELECT delivered_at FROM dispatch_outbox WHERE job_id=%s", (local,)))[0] is not None
            sqs.messages.clear()  # Published hint expired or moved to the DLQ.
        else:
            active = state.enqueue_job(
                owner, provider="lichess", kind="fetch_user_profile", target="active", priority=1,
            ).job_id
            blocked = state.enqueue_job(
                owner, provider="lichess", kind="fetch_user_profile", target="blocked", priority=2,
            ).job_id
            assert state.claim_next_job(owner, worker_id="other-acquisition", job_id=active) is not None
            sqs.messages = [{"Body": json.dumps({"job_id": blocked}), "ReceiptHandle": "busy"}]
        try:
            assert Worker(database_url, queue_consumer=SqsConsumer(sqs, "queue", wait_seconds=0)).run(once=True) == 1
        finally:
            state.release_job_ownership(owner)
        completed = state.get_job(owner, local)
        assert completed is not None and completed.state == "done"
        assert require_row(owner.execute("SELECT normalization_status FROM raw_payloads WHERE id=%s", (raw_id,)))[0] == "parsed"
        assert require_row(owner.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 0
        if blocked is not None:
            pending = state.get_job(owner, blocked)
            assert pending is not None and pending.state == "pending"
            assert sqs.deleted == []


def test_processing_jobs_ignore_provider_backoff(initialized_conn) -> None:
    state.defer_provider(initialized_conn, "lichess", not_before=1000, reason="provider unavailable", now=1)
    state.enqueue_job(initialized_conn, provider="lichess", kind="fetch_user_profile", target="remote", priority=1)
    local = state.enqueue_job(initialized_conn, provider="lichess", kind="normalize_payload", target="123", priority=2)
    claimed = state.claim_next_job(initialized_conn, worker_id="offline", stage="processing", now=2)
    assert claimed is not None and claimed.id == local.job_id
    state.release_job_ownership(initialized_conn)


def test_busy_provider_frontier_cannot_starve_other_provider_or_local_jobs(database_url: str) -> None:
    with connection(database_url, mode="rw") as setup:
        for index in range(105):
            state.enqueue_job(setup, provider="lichess", kind="fetch_user_profile", target=str(index), priority=1)
        local = state.enqueue_job(setup, provider="lichess", kind="normalize_payload", target="123", priority=2)
    with connection(database_url, mode="rw") as owner, connection(database_url, mode="rw") as other:
        assert state.claim_next_job(owner, worker_id="provider-owner") is not None
        selected = state.claim_next_job(other, worker_id="local-owner")
        assert selected is not None and selected.id == local.job_id
        state.release_job_ownership(other)
        state.release_job_ownership(owner)


def test_offline_upgrade_resumes_fixed_source_selection_and_excludes_other_workspace(initialized_conn) -> None:
    def source(name: str, scope: str = "public") -> int:
        return store_raw_payload(initialized_conn, RawRecord(
            provider="lichess", endpoint_type="user_profile",
            request_url=f"https://lichess.org/api/user/{name}",
            canonical_source_key=f"lichess/user/{name}/scope/{scope}/profile",
            request_params={"owner_scope": scope},
            owner_scope=scope,
            body=json.dumps({"id": name, "username": name}).encode(), media_type="application/json",
        ))

    first, second = source("first"), source("second")
    private = source("private", "another-workspace")
    state.enqueue_job(initialized_conn, provider="lichess", kind="reprocess_archive", target="upgrade-one",
                      params={"batch_size": 1, "owner_scope": "workspace-one"})
    runner = JobRunner(initialized_conn, stage="processing")
    assert runner.run(max_jobs=1).claimed == 1
    progress = require_row(initialized_conn.execute("SELECT * FROM data_upgrades WHERE id='upgrade-one'"))
    assert progress["parser_version"] == installed_parser_target()
    assert progress["last_raw_id"] == first and progress["high_water_raw_id"] == second
    later = source("later")
    assert runner.run(max_jobs=1).claimed == 1
    assert runner.run(max_jobs=1).done == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM provider_users"))[0] == 2
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM fetch_logs"))[0] == 0
    assert all(require_row(initialized_conn.execute(
        "SELECT normalization_status FROM raw_payloads WHERE id=%s", (raw_id,),
    ))[0] == "pending" for raw_id in (private, later))


def test_offline_upgrade_rejects_unavailable_parser_before_source_changes(initialized_conn) -> None:
    raw_id = store_raw_payload(initialized_conn, RawRecord(
        provider="lichess", endpoint_type="user_profile", request_url="https://lichess.org/api/user/example",
        canonical_source_key="lichess/user/example/profile",
        body=json.dumps({"id": "example", "username": "example"}).encode(), media_type="application/json",
    ))
    job = state.enqueue_job(initialized_conn, provider="lichess", kind="reprocess_archive", target="bad-parser",
                            params={"parser_version": "never-installed-v999"}).job_id
    result = JobRunner(initialized_conn, stage="processing").run(max_jobs=1)
    assert result.errors == 1 and result.done == 0
    stored_job = state.get_job(initialized_conn, job)
    assert stored_job is not None and stored_job.state == "error"
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM data_upgrades"))[0] == 0
    assert require_row(initialized_conn.execute("SELECT normalization_status FROM raw_payloads WHERE id=%s", (raw_id,)))[0] == "pending"


def test_offline_upgrade_resume_rejects_changed_installed_manifest(initialized_conn, monkeypatch) -> None:
    for name in ("first", "second"):
        store_raw_payload(initialized_conn, RawRecord(
            provider="lichess", endpoint_type="user_profile", request_url=f"https://lichess.org/api/user/{name}",
            canonical_source_key=f"lichess/user/{name}/profile",
            body=json.dumps({"id": name, "username": name}).encode(), media_type="application/json",
        ))
    state.enqueue_job(initialized_conn, provider="lichess", kind="reprocess_archive", target="pinned-parser",
                      params={"batch_size": 1})
    runner = JobRunner(initialized_conn, stage="processing")
    assert runner.run(max_jobs=1).claimed == 1
    monkeypatch.setattr("chess_crawl.ingest.USERS_PARSER_VERSION", "users-normalizer-future")
    assert runner.run(max_jobs=1).errors == 1
    progress = require_row(initialized_conn.execute("SELECT * FROM data_upgrades WHERE id='pinned-parser'"))
    assert progress["processed"] == 1 and progress["state"] == "error"


@pytest.mark.parametrize("second_scope", ["workspace-alpha", "workspace-beta"])
def test_offline_upgrade_identity_rejection_preserves_existing_progress(initialized_conn, second_scope) -> None:
    first = state.enqueue_job(
        initialized_conn, provider="lichess", kind="reprocess_archive", target="upgrade-shared",
        params={"owner_scope": "workspace-alpha"},
    ).job_id
    runner = JobRunner(initialized_conn, stage="processing")
    assert runner.run(max_jobs=1).done == 1
    before = dict(require_row(initialized_conn.execute("SELECT * FROM data_upgrades WHERE id='upgrade-shared'")))
    assert before["job_id"] == first and before["state"] == "done"

    second = state.enqueue_job(
        initialized_conn, provider="lichess", kind="reprocess_archive", target="upgrade-shared",
        params={"owner_scope": second_scope},
    ).job_id
    assert second != first
    assert runner.run(max_jobs=1).errors == 1
    assert dict(require_row(initialized_conn.execute("SELECT * FROM data_upgrades WHERE id='upgrade-shared'"))) == before
    rejected = state.get_job(initialized_conn, second)
    assert rejected is not None and rejected.state == "error"


def test_offline_upgrade_owned_replay_failure_records_its_error(initialized_conn) -> None:
    store_raw_payload(initialized_conn, RawRecord(
        provider="lichess", endpoint_type="user_profile", request_url="https://lichess.org/api/user/broken",
        canonical_source_key="lichess/user/broken/profile", body=b"not-json", media_type="application/json",
    ))
    job_id = state.enqueue_job(
        initialized_conn, provider="lichess", kind="reprocess_archive", target="upgrade-broken",
        params={"owner_scope": "workspace-alpha"},
    ).job_id
    assert JobRunner(initialized_conn, stage="processing").run(max_jobs=1).errors == 1
    progress = require_row(initialized_conn.execute("SELECT * FROM data_upgrades WHERE id='upgrade-broken'"))
    assert progress["job_id"] == job_id and progress["owner_scope"] == "workspace-alpha"
    assert progress["state"] == "error" and progress["error"]
    assert progress["processed"] == 0


def test_dispatch_ignores_stale_revisions_and_routes_stages(initialized_conn) -> None:
    remote = state.enqueue_job(initialized_conn, provider="lichess", kind="fetch_user_profile", target="remote").job_id
    local = state.enqueue_job(initialized_conn, provider="lichess", kind="normalize_payload", target="123").job_id
    state.claim_next_job(initialized_conn, job_id=remote, now=100)
    state.finish_attempt(initialized_conn, remote, "blocked", reason="retry", transient=True,
                         retry_after=100, now=100)
    sqs = FakeSqs()
    dispatcher = SqsDispatcher(sqs, "shared", acquisition_queue_url="acquisition",
                               processing_queue_url="processing", clock=lambda: 110)
    assert dispatcher.publish_one(initialized_conn)
    assert sqs.queues == ["processing"]
    assert json.loads(sqs.messages[0]["Body"])["job_id"] == local
    assert not dispatcher.publish_one(initialized_conn)
    dispatcher.clock = lambda: 200
    assert dispatcher.publish_one(initialized_conn)
    assert sqs.queues[-1] == "acquisition"
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM dispatch_outbox WHERE superseded_at IS NOT NULL"))[0] == 1


def _stored_games(conn, fixtures_dir, refs, players):
    archive = json.loads((fixtures_dir / "chesscom/archive_2024_01.json").read_bytes())
    games = []
    for ref, (white, black) in zip(refs, players):
        game = copy.deepcopy(archive["games"][0])
        game.update(uuid=ref, url=f"https://www.chess.com/game/live/{ref}")
        game["white"]["username"], game["black"]["username"] = white, black
        games.append(game)
    return store_raw_payload(conn, RawRecord(
        provider="chess.com", endpoint_type="monthly_archive", request_url="https://api.chess.com/pub/player/example/games/2024/01",
        canonical_source_key=f"chess.com/player/{refs[0]}/games/2024/01", fetched_at=150,
        body=json.dumps({"games": games}).encode(), media_type="application/json",
    ))


def test_same_provider_disjoint_games_normalize_in_parallel_with_parsing_outside_writes(
    database_url, fixtures_dir, monkeypatch,
) -> None:
    with connection(database_url, mode="rw") as setup:
        first = _stored_games(setup, fixtures_dir, ["parallel-first"], [("first-white", "first-black")])
        second = _stored_games(setup, fixtures_dir, ["parallel-second"], [("second-white", "second-black")])
    barrier = threading.Barrier(2)
    failures: list[BaseException] = []
    original = normalization._normalize_game
    parse = normalization.parse_game_evidence
    parsing_in_transaction: list[bool] = []
    local = threading.local()

    def prepare(game):
        parsing_in_transaction.append(local.conn.in_transaction)
        return parse(game)

    def persist(conn, game, **kwargs):
        assert conn.in_transaction
        barrier.wait(8)
        return original(conn, game, **kwargs)

    def normalize(raw_id):
        try:
            with connection(database_url, mode="rw") as conn:
                local.conn = conn
                normalization.normalize_games_payload(conn, raw_id)
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(normalization, "_normalize_game", persist)
    monkeypatch.setattr(normalization, "parse_game_evidence", prepare)
    threads = [threading.Thread(target=normalize, args=(raw_id,)) for raw_id in (first, second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(12)
    assert not any(thread.is_alive() for thread in threads)
    assert failures == []
    assert parsing_in_transaction == [False, False]
    with connection(database_url) as observer:
        assert require_row(observer.execute("SELECT COUNT(*) FROM games"))[0] == 2


def test_profile_rename_waits_for_game_identity_transaction(database_url, fixtures_dir, monkeypatch) -> None:
    with connection(database_url, mode="rw") as setup:
        survivor = upsert_provider_user(setup, provider="chess.com", username="oldname", provider_user_id="123", now=100)
        upsert_provider_user(setup, provider="chess.com", username="newname", now=110)
        raw_id = _stored_games(setup, fixtures_dir, ["rename-game"], [("oldname", "opponent")])
    entered, release, connected = threading.Event(), threading.Event(), threading.Event()
    pids: list[int] = []
    failures: list[BaseException] = []
    original = normalization._normalize_game

    def persist(conn, game, **kwargs):
        entered.set()
        assert release.wait(8)
        return original(conn, game, **kwargs)

    def normalize():
        try:
            with connection(database_url, mode="rw") as conn:
                normalization.normalize_games_payload(conn, raw_id)
        except BaseException as exc:
            failures.append(exc)

    def rename():
        try:
            with connection(database_url, mode="rw") as conn:
                pids.append(conn.info.backend_pid)
                connected.set()
                assert upsert_provider_user(conn, provider="chess.com", username="newname", provider_user_id="123", now=200) == survivor
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(normalization, "_normalize_game", persist)
    worker = threading.Thread(target=normalize)
    renamer = threading.Thread(target=rename)
    worker.start()
    assert entered.wait(8)
    renamer.start()
    try:
        assert connected.wait(4)
        with connection(database_url) as observer:
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                if require_row(observer.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=%s AND locktype='advisory' AND NOT granted)",
                    (pids[0],),
                ))[0]:
                    break
            else:
                pytest.fail("Certified rename did not wait for the shared reconciliation gate")
    finally:
        release.set()
        worker.join(10)
        renamer.join(10)
    assert failures == []
    assert not worker.is_alive() and not renamer.is_alive()
    with connection(database_url) as observer:
        assert require_row(observer.execute("SELECT id FROM provider_users WHERE username_normalized='newname'"))[0] == survivor
        assert require_row(observer.execute("SELECT provider_user_id FROM game_participants WHERE color='white'"))[0] == survivor


def test_partial_game_normalization_is_checkpointed_without_certifying_source(initialized_conn, fixtures_dir, monkeypatch) -> None:
    raw_id = _stored_games(initialized_conn, fixtures_dir, ["checkpoint-first", "checkpoint-second"],
                           [("white", "black"), ("white", "black")])
    original = normalization._normalize_game
    calls: list[str] = []
    fail = [True]

    def persist(conn, game, **kwargs):
        calls.append(game.provider_game_id)
        if game.provider_game_id == "checkpoint-second" and fail[0]:
            fail[0] = False
            raise RuntimeError("interrupted second game")
        return original(conn, game, **kwargs)

    monkeypatch.setattr(normalization, "_normalize_game", persist)
    state.enqueue_job(initialized_conn, provider="chess.com", kind="normalize_payload", target=str(raw_id))
    runner = JobRunner(initialized_conn, stage="processing")
    assert runner.run(max_jobs=1).errors == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM games"))[0] == 1
    assert require_row(initialized_conn.execute("SELECT normalization_status FROM raw_payloads WHERE id=%s", (raw_id,)))[0] == "pending"
    state.enqueue_job(initialized_conn, provider="chess.com", kind="normalize_payload", target=str(raw_id))
    assert runner.run(max_jobs=1).done == 1
    assert calls == ["checkpoint-first", "checkpoint-second", "checkpoint-second"]
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM games"))[0] == 2
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM game_versions"))[0] == 2
    assert require_row(initialized_conn.execute("SELECT normalization_status FROM raw_payloads WHERE id=%s", (raw_id,)))[0] == "parsed"


def test_independent_same_provider_profile_transactions_overlap(database_url, monkeypatch) -> None:
    with connection(database_url, mode="rw") as setup:
        sources = [store_raw_payload(setup, RawRecord(
            provider="chess.com", endpoint_type="user_profile",
            request_url=f"https://api.chess.com/pub/player/{name}",
            canonical_source_key=f"chess.com/player/{name}/profile",
            body=json.dumps({"username": name, "player_id": stable_id}).encode(), media_type="application/json",
        )) for name, stable_id in (("parallel-profile-a", 111), ("parallel-profile-b", 222))]
    barrier = threading.Barrier(2)
    original = user_normalization.upsert_user_snapshot
    failures: list[BaseException] = []

    def snapshot(conn, **kwargs):
        barrier.wait(8)
        return original(conn, **kwargs)

    def normalize(raw_id):
        try:
            with connection(database_url, mode="rw") as conn:
                normalize_user_payload(conn, raw_id)
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(user_normalization, "upsert_user_snapshot", snapshot)
    threads = [threading.Thread(target=normalize, args=(raw_id,)) for raw_id in sources]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(12)
    assert failures == []
    assert not any(thread.is_alive() for thread in threads)
    with connection(database_url) as observer:
        assert require_row(observer.execute("SELECT COUNT(*) FROM user_snapshots"))[0] == 2


def test_stale_source_finisher_preserves_newer_completed_observation(initialized_conn, database_url, fixtures_dir, monkeypatch) -> None:
    raw_id = _stored_games(initialized_conn, fixtures_dir, ["stale-finisher"], [("white", "black")])
    entered, release = threading.Event(), threading.Event()
    original = normalization.parse_game_evidence
    failures: list[BaseException] = []

    def prepare(game):
        if threading.current_thread().name == "older-normalizer":
            entered.set()
            assert release.wait(10)
        return original(game)

    def older():
        try:
            with connection(database_url, mode="rw") as conn:
                normalization.normalize_games_payload(conn, raw_id)
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(normalization, "parse_game_evidence", prepare)
    thread = threading.Thread(target=older, name="older-normalizer")
    thread.start()
    try:
        assert entered.wait(10)
        insert_fetch_log(initialized_conn, provider="chess.com", url="https://example.test/stale-finisher",
                         endpoint_type="monthly_archive", attempted_at=300, status_code=200, raw_payload_id=raw_id)
        normalization.normalize_games_payload(initialized_conn, raw_id)
        assert require_row(initialized_conn.execute("SELECT normalization_status FROM raw_payloads WHERE id=%s", (raw_id,)))[0] == "parsed"
    finally:
        release.set()
        thread.join(12)
    assert failures == [] and not thread.is_alive()
    assert require_row(initialized_conn.execute("SELECT normalization_status FROM raw_payloads WHERE id=%s", (raw_id,)))[0] == "parsed"
    assert require_row(initialized_conn.execute("SELECT state FROM payload_normalization_runs WHERE raw_payload_id=%s", (raw_id,)))[0] == "complete"
