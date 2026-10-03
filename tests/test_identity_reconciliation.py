"""Stable provider identities survive renames and sparse game observations."""

from __future__ import annotations

import json
import sqlite3

import pytest

from support import seed_game
from chess_crawl.jobs import state
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.storage.discovery import OpponentEdge, record_discovery_edges
from chess_crawl.storage.raw import insert_source_record, store_raw_payload
from chess_crawl.storage.repository import upsert_provider_user, upsert_user_snapshot


def profile(conn: sqlite3.Connection, username: str, player_id: int | None, *, at: int) -> int:
    body: dict[str, object] = {"username": username, "status": "closed:fair_play_violations", "title": "GM"}
    if player_id is not None:
        body["player_id"] = player_id
    return store_raw_payload(conn, RawRecord(
        provider="chess.com", endpoint_type="user_profile",
        canonical_source_key=f"chess.com/player/{username.lower()}/profile",
        request_url=f"https://example.test/{username.lower()}",
        fetched_at=at, body=json.dumps(body).encode(),
    ))


@pytest.mark.parametrize("placeholder_first", [False, True])
def test_rename_reconciles_sparse_user_and_every_dependent_record(initialized_conn, monkeypatch, placeholder_first) -> None:
    conn = initialized_conn
    old_raw = profile(conn, "OldName", 123, at=100)
    user = normalize_user_payload(conn, old_raw)
    assert user is not None
    old_game, _, opponent = seed_game(conn, provider="chess.com", game_key="old", white="OldName", black="Opponent")
    new_game, placeholder, _ = seed_game(conn, provider="chess.com", game_key="new", white="NewName", black="Opponent")
    sparse_raw = profile(conn, "NewName", None, at=200)
    assert normalize_user_payload(conn, sparse_raw) == placeholder
    foreign_user = upsert_provider_user(conn, provider="lichess", username="NewName", provider_user_id="123")

    # Equal snapshot projections may have arrived through either user row.
    survivor = upsert_user_snapshot(
        conn, provider_user_id=user, captured_at=100, observed_username="NewName",
        content_hash="same-snapshot", raw_payload_id=old_raw, count_win=1,
    )
    duplicate = upsert_user_snapshot(
        conn, provider_user_id=placeholder, captured_at=200, observed_username="NewName",
        content_hash="same-snapshot", raw_payload_id=sparse_raw, count_win=2,
    )
    for snapshot, raw in ((survivor, old_raw), (duplicate, sparse_raw)):
        insert_source_record(conn, entity_type="user_snapshot", entity_id=snapshot,
                             provider="chess.com", endpoint_type="user_profile", raw_payload_id=raw)
    insert_source_record(conn, entity_type="user", entity_id=placeholder,
                         provider="chess.com", endpoint_type="user_profile", raw_payload_id=old_raw)

    runs = [state.create_crawl_run(conn, provider="chess.com", seed_spec="rename", params={}) for _ in range(2)]
    observations = [(user, old_game), (placeholder, new_game)]
    if placeholder_first:
        observations.reverse()
    for owner, game in observations:
        for run in runs:
            record_discovery_edges(conn, crawl_run_id=run, provider="chess.com", from_user_id=owner,
                                   depth=1, edges=[OpponentEdge(opponent, "opponent", game, 1)])
            record_discovery_edges(conn, crawl_run_id=run, provider="chess.com", from_user_id=opponent,
                                   depth=2, edges=[OpponentEdge(owner, "oldname" if owner == user else "newname", game, 1)])
    for run in runs:
        state.update_crawl_run(conn, run, counters=state.run_counters(conn, run), status="cancelled", finished=True)
        assert state.run_counters(conn, run)["edges"] == 4
    participants_before = [tuple(row) for row in conn.execute("SELECT id, username_normalized FROM game_participants")]
    raw_count = conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0]

    renamed_raw = profile(conn, "NewName", 123, at=300)
    before = list(conn.iterdump())
    update_run = state.update_crawl_run

    def fail_after_run_update(*args, **kwargs):
        update_run(*args, **kwargs)
        raise RuntimeError("interrupted reconciliation")

    with monkeypatch.context() as failure:
        failure.setattr(state, "update_crawl_run", fail_after_run_update)
        with pytest.raises(RuntimeError, match="interrupted reconciliation"):
            normalize_user_payload(conn, renamed_raw)
    assert list(conn.iterdump()) == before
    assert normalize_user_payload(conn, renamed_raw) == user

    assert conn.execute("SELECT 1 FROM provider_users WHERE id=?", (placeholder,)).fetchone() is None
    current = conn.execute("SELECT username_normalized, provider_user_id, account_status, title FROM provider_users WHERE id=?", (user,)).fetchone()
    assert tuple(current) == ("newname", "123", "closed:fair_play_violations", "GM")
    assert conn.execute("SELECT provider FROM provider_users WHERE id=?", (foreign_user,)).fetchone()[0] == "lichess"
    assert [tuple(row) for row in conn.execute("SELECT id, username_normalized FROM game_participants")] == participants_before
    assert conn.execute("SELECT provider_user_id FROM game_participants WHERE game_id=? AND color='white'", (new_game,)).fetchone()[0] == user
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == raw_count + 1
    assert conn.execute("SELECT COUNT(*) FROM source_records WHERE entity_type='user' AND entity_id=?", (user,)).fetchone()[0] == 3
    assert conn.execute("SELECT COUNT(*) FROM source_records WHERE entity_type='user' AND entity_id=?", (placeholder,)).fetchone()[0] == 0
    snapshot = conn.execute("SELECT captured_at, raw_payload_id, count_win FROM user_snapshots WHERE id=?", (survivor,)).fetchone()
    assert tuple(snapshot) == (200, sparse_raw, 2)
    assert conn.execute("SELECT 1 FROM user_snapshots WHERE id=?", (duplicate,)).fetchone() is None
    assert conn.execute("SELECT COUNT(*) FROM source_records WHERE entity_type='user_snapshot' AND entity_id=?", (survivor,)).fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM discovery_edges").fetchone()[0] == 2
    assert {row[0] for row in conn.execute("SELECT game_count FROM discovery_edges")} == {2}
    assert {tuple(row) for row in conn.execute("SELECT crawl_run_id, via_game_id FROM discovery_edges")} == {
        (runs[0], new_game if placeholder_first else old_game),
    }
    assert conn.execute("SELECT COUNT(*) FROM run_edges").fetchone()[0] == 4
    for run in runs:
        updated = state.get_run(conn, run)
        assert updated is not None and updated["status"] == "cancelled"
        assert json.loads(updated["counters"])["edges"] == 2
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize("known_identity", [False, True])
@pytest.mark.parametrize("conflict_at", [50, 200], ids=["historical", "current"])
def test_distinct_stable_ids_do_not_overwrite_or_merge(initialized_conn, known_identity, conflict_at) -> None:
    conn = initialized_conn
    first = profile(conn, "TakenName", 111, at=100)
    normalize_user_payload(conn, first)
    if known_identity:
        normalize_user_payload(conn, profile(conn, "OtherName", 222, at=100))
    conflicting = profile(conn, "TakenName", 222, at=conflict_at)
    before = list(conn.iterdump())
    if known_identity and conflict_at < 100:
        # The stable ID identifies a known historical account independently
        # of who currently holds the observed username.
        assert normalize_user_payload(conn, conflicting) == 2
        assert conn.execute("SELECT username_normalized FROM provider_users WHERE provider_user_id='222'").fetchone()[0] == "othername"
        return
    with pytest.raises(ValueError, match="conflicting provider user IDs"):
        normalize_user_payload(conn, conflicting)
    assert [tuple(row) for row in conn.execute("SELECT provider_user_id, username_normalized FROM provider_users ORDER BY id")] == (
        [("111", "takenname"), ("222", "othername")] if known_identity else [("111", "takenname")]
    )
    assert conn.execute("SELECT normalization_status FROM raw_payloads WHERE id=?", (conflicting,)).fetchone()[0] == "pending"
    assert not conn.in_transaction
    assert list(conn.iterdump()) == before


def test_old_profile_replay_does_not_rename_current_identity_or_merge_reused_name(initialized_conn) -> None:
    conn = initialized_conn
    old_raw = profile(conn, "OldName", 123, at=100)
    user = normalize_user_payload(conn, old_raw)
    assert normalize_user_payload(conn, profile(conn, "NewName", 123, at=200)) == user
    other = normalize_user_payload(conn, profile(conn, "OldName", 456, at=300))
    assert normalize_user_payload(conn, old_raw) == user
    assert [tuple(row) for row in conn.execute("SELECT id, provider_user_id, username_normalized FROM provider_users ORDER BY id")] == [
        (user, "123", "newname"), (other, "456", "oldname"),
    ]


def test_stale_sparse_observation_cannot_rename_stable_identity(initialized_conn) -> None:
    conn = initialized_conn
    user = upsert_provider_user(conn, provider="lichess", username="NewName", provider_user_id="stable", now=200)
    assert upsert_provider_user(conn, provider="lichess", username="OldName", provider_user_id="stable", now=100) == user
    assert conn.execute("SELECT username_normalized FROM provider_users WHERE id=?", (user,)).fetchone()[0] == "newname"


def test_sparse_observation_can_supply_missing_stable_id_to_existing_placeholder(initialized_conn) -> None:
    conn = initialized_conn
    user = upsert_provider_user(conn, provider="lichess", username="Alice", now=200)
    assert upsert_provider_user(conn, provider="lichess", username="Alice", provider_user_id="alice", now=100) == user
    assert tuple(conn.execute("SELECT provider_user_id, updated_at FROM provider_users WHERE id=?", (user,)).fetchone()) == ("alice", 200)
