"""Full profile observations can remove metadata; sparse observations cannot."""

from __future__ import annotations

import json
import sqlite3

import httpx
import pytest

from support import seed_game
from chess_crawl.application import list_users
from chess_crawl.config import Config
from chess_crawl.ingest import fetch_user_profile, replay_raw_payload
from chess_crawl.normalize.codes import canonical_hash
from chess_crawl.normalize.users import PARSER_VERSION, normalize_user_payload
from chess_crawl.providers.base import EndpointType, RawRecord
from chess_crawl.providers.lichess.parser import parse_user_profile
from chess_crawl.storage.db import transaction
from chess_crawl.storage.raw import insert_fetch_log, store_raw_payload, update_raw_payload_status
from chess_crawl.storage.repository import upsert_provider_user


PROFILES = [
    ("lichess", {"id": "alice", "username": "Alice"}, {"tosViolation": True}),
    ("chess.com", {"player_id": 42, "username": "Alice"}, {"status": "closed"}),
    ("chess.com", {"username": "Alice"}, {"status": "closed"}),
]


def normalize_observation(
    conn: sqlite3.Connection,
    provider: str,
    body: dict,
    *,
    fetched_at: int,
    endpoint: EndpointType = "user_profile",
) -> int | None:
    raw_id = store_raw_payload(conn, RawRecord(
        provider=provider,
        endpoint_type=endpoint,
        canonical_source_key=f"{provider}/player/alice/{endpoint}",
        request_url="https://example.test/alice",
        http_status=200,
        fetched_at=fetched_at,
        body=json.dumps(body).encode(),
    ))
    insert_fetch_log(
        conn, provider=provider, url="https://example.test/alice", endpoint_type=endpoint,
        attempted_at=fetched_at, status_code=200, raw_payload_id=raw_id,
    )
    return normalize_user_payload(conn, raw_id)


@pytest.mark.parametrize(("provider", "identity", "status"), PROFILES)
def test_profile_refresh_clears_removed_metadata(
    initialized_conn: sqlite3.Connection, provider: str, identity: dict, status: dict,
) -> None:
    conn = initialized_conn
    original_id = normalize_observation(conn, provider, {**identity, **status, "title": "FM"}, fetched_at=100)
    refreshed_id = normalize_observation(conn, provider, identity, fetched_at=200)

    assert refreshed_id == original_id
    user = list_users(conn, provider=provider)["items"][0]
    assert user["account_status"] is None
    assert user["title"] is None
    assert user["updated_at"] == 200
    snapshots = conn.execute("SELECT status, title FROM user_snapshots ORDER BY captured_at").fetchall()
    assert snapshots[0]["status"] is not None
    assert tuple(snapshots[1]) == (None, None)


@pytest.mark.parametrize(("provider", "identity", "status"), PROFILES)
def test_sparse_game_observation_preserves_profile_metadata(
    initialized_conn: sqlite3.Connection, provider: str, identity: dict, status: dict,
) -> None:
    conn = initialized_conn
    normalize_observation(conn, provider, {**identity, **status, "title": "FM"}, fetched_at=100)
    before = list_users(conn, provider=provider)["items"][0]

    seed_game(conn, provider=provider, game_key="sparse", white="Alice", black="Bob")

    after = next(user for user in list_users(conn, provider=provider)["items"] if user["id"] == before["id"])
    assert after["account_status"] == before["account_status"]
    assert after["title"] == "FM"


def test_stats_observation_preserves_profile_metadata(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    normalize_observation(
        conn, "chess.com", {"player_id": 42, "username": "Alice", "status": "closed", "title": "FM"},
        fetched_at=100,
    )
    normalize_observation(
        conn, "chess.com", {"chess_blitz": {"record": {"win": 1}}},
        fetched_at=200, endpoint="user_stats",
    )

    user = list_users(conn, provider="chess.com")["items"][0]
    assert user["account_status"] == "closed"
    assert user["title"] == "FM"


@pytest.mark.parametrize(("provider", "identity", "status"), PROFILES)
def test_old_profile_replay_preserves_newer_profile(
    initialized_conn: sqlite3.Connection, provider: str, identity: dict, status: dict,
) -> None:
    conn = initialized_conn
    normalize_observation(conn, provider, {**identity, **status, "title": "FM"}, fetched_at=100)
    old_raw_id = conn.execute("SELECT id FROM raw_payloads").fetchone()[0]
    normalize_observation(conn, provider, {**identity, "title": "IM"}, fetched_at=200)

    normalize_user_payload(conn, old_raw_id)

    user = list_users(conn, provider=provider)["items"][0]
    assert user["account_status"] is None
    assert user["title"] == "IM"
    assert user["updated_at"] == 200


@pytest.mark.parametrize("timestamps", [(100, 200, 300), (100, 100, 100)])
def test_repeated_profile_body_is_a_new_observation(initialized_conn: sqlite3.Connection, timestamps: tuple) -> None:
    conn = initialized_conn
    original = {"id": "alice", "username": "Alice", "title": "FM"}
    changed = {"id": "alice", "username": "Alice", "title": "IM"}
    for payload, timestamp in zip((original, changed, original), timestamps, strict=True):
        normalize_observation(conn, "lichess", payload, fetched_at=timestamp)
    changed_raw_id = conn.execute("SELECT id FROM raw_payloads ORDER BY id DESC LIMIT 1").fetchone()[0]

    normalize_user_payload(conn, changed_raw_id)

    user = list_users(conn)["items"][0]
    assert user["title"] == "FM"
    assert user["updated_at"] == timestamps[-1]
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 2
    assert conn.execute("SELECT fetched_at FROM raw_payloads ORDER BY id LIMIT 1").fetchone()[0] == timestamps[0]
    assert conn.execute("SELECT captured_at FROM user_snapshots WHERE title = 'FM'").fetchone()[0] == timestamps[-1]


def test_profile_refresh_is_independent_of_sparse_observation_time(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    original = {"player_id": 42, "username": "Alice", "status": "closed", "title": "FM"}
    normalize_observation(conn, "chess.com", original, fetched_at=100)
    normalize_observation(conn, "chess.com", {}, fetched_at=300, endpoint="user_stats")

    normalize_observation(conn, "chess.com", {"player_id": 42, "username": "Alice"}, fetched_at=200)

    user = list_users(conn)["items"][0]
    assert user["account_status"] is None
    assert user["title"] is None
    assert user["updated_at"] == 300


def test_equivalent_snapshot_replay_keeps_latest_capture(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    identity = {"id": "alice", "username": "Alice", "title": "FM"}
    normalize_observation(conn, "lichess", {**identity, "seenAt": 200000}, fetched_at=200)
    latest_raw_id = conn.execute("SELECT id FROM raw_payloads").fetchone()[0]

    # seenAt is raw profile evidence but not part of the normalized snapshot.
    normalize_observation(conn, "lichess", {**identity, "seenAt": 100000}, fetched_at=100)

    snapshot = conn.execute("SELECT captured_at, raw_payload_id FROM user_snapshots").fetchone()
    assert tuple(snapshot) == (200, latest_raw_id)
    assert conn.execute("SELECT COUNT(*) FROM user_snapshots").fetchone()[0] == 1


def test_304_replays_profiles_normalized_before_metadata_fix(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    config = Config(chesscom_delay_s=0, max_retries=0)
    first = fetch_user_profile(
        conn, "chess.com", "Alice", config=config,
        transport=httpx.MockTransport(lambda request: httpx.Response(
            200, headers={"etag": '"profile"'}, json={"player_id": 42, "username": "Alice"},
        )),
    )
    assert first.raw_payload_id is not None
    # Simulate an existing archive parsed by v1, which retained removed metadata.
    upsert_provider_user(conn, provider="chess.com", username="Alice", account_status="closed", title="FM")
    update_raw_payload_status(conn, first.raw_payload_id, status="parsed", parser_version="users-normalizer-v1")

    def unchanged(request: httpx.Request) -> httpx.Response:
        assert request.headers["If-None-Match"] == '"profile"'
        return httpx.Response(304, headers={"etag": '"profile"'})

    refreshed = fetch_user_profile(
        conn, "chess.com", "Alice", config=config, transport=httpx.MockTransport(unchanged),
    )

    assert refreshed.status_code == 304
    assert refreshed.normalized_ids == first.normalized_ids
    user = list_users(conn)["items"][0]
    assert user["account_status"] is None
    assert user["title"] is None
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 1
    assert conn.execute("SELECT parser_version FROM raw_payloads").fetchone()[0] == PARSER_VERSION


@pytest.mark.parametrize(("first", "second"), [
    ({"profile": {"flag": "GR"}}, {"profile": {"flag": "pirate"}}),
    ({"disabled": False}, {"disabled": True}),
    ({"verified": False}, {"verified": True}),
    ({"disabled": False}, {}),
    ({"verified": False}, {}),
    ({"tosViolation": False}, {}),
])
def test_lichess_native_profile_changes_are_preserved_in_snapshots(
    initialized_conn: sqlite3.Connection, first: dict, second: dict,
) -> None:
    conn = initialized_conn
    identity = {"id": "alice", "username": "Alice", "perfs": {"blitz": {"rating": 1500}}}
    for metadata, at in ((first, 100), (second, 200), (first, 300)):
        normalize_observation(conn, "lichess", {**identity, **metadata}, fetched_at=at)

    snapshots = conn.execute("SELECT country, perfs_or_stats, captured_at FROM user_snapshots ORDER BY captured_at").fetchall()
    assert len(snapshots) == 2
    assert [json.loads(row["perfs_or_stats"]) for row in snapshots] == [
        {"perfs": identity["perfs"], **second}, {"perfs": identity["perfs"], **first},
    ]
    assert [row["captured_at"] for row in snapshots] == [200, 300]
    # A Lichess flag may be symbolic; it is not an inferred country.
    assert all(row["country"] is None for row in snapshots)


@pytest.mark.parametrize("verified", [True, False, None])
def test_lichess_parser_preserves_verified_and_explicit_country(verified: bool | None) -> None:
    body: dict[str, object] = {"id": "alice", "username": "Alice", "profile": {"country": "GR", "flag": "pirate"}}
    if verified is not None:
        body["verified"] = verified
    user = parse_user_profile(json.dumps(body).encode())
    assert user.is_verified is verified
    assert user.country == "GR"


def test_v2_lichess_profile_replay_recovers_native_metadata(initialized_conn: sqlite3.Connection) -> None:
    conn = initialized_conn
    body = {
        "id": "alice", "username": "Alice", "profile": {"flag": "pirate", "bio": "Chess player"},
        "disabled": True, "verified": False, "tosViolation": False,
    }
    normalize_observation(conn, "lichess", body, fetched_at=100)
    raw_id = conn.execute("SELECT id FROM raw_payloads").fetchone()[0]
    # Persist the old parser's content key and reduced JSON as an existing
    # archive would contain before upgrading; the raw response is unchanged.
    old_hash = canonical_hash({
        "kind": "profile", "username": "Alice", "status": None, "title": None,
        "country": None, "patron": None, "count": {}, "perfs": {},
    })
    with transaction(conn):
        conn.execute("UPDATE user_snapshots SET perfs_or_stats = ?, content_hash = ?", ('{"perfs":{}}', old_hash))
        update_raw_payload_status(conn, raw_id, status="parsed", parser_version="users-normalizer-v2")

    replay_raw_payload(conn, raw_id)

    snapshot = conn.execute("SELECT perfs_or_stats FROM user_snapshots ORDER BY captured_at DESC, id DESC").fetchone()
    assert json.loads(snapshot["perfs_or_stats"]) == {
        "perfs": {}, "profile": body["profile"], "disabled": True, "verified": False, "tosViolation": False,
    }
    assert conn.execute("SELECT parser_version FROM raw_payloads").fetchone()[0] == PARSER_VERSION
    assert PARSER_VERSION != "users-normalizer-v2"
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 1
