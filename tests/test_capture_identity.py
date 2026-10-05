"""Capture identity, reconciliation and private timestamps retain their evidence."""

import json

import httpx
import pytest

from chess_crawl.ingest import (
    _persist_response,
    fetch_chesscom_stats,
    fetch_user_resource,
    replay_raw_payload,
)
from chess_crawl.api.compat import _export_chunks
from chess_crawl.normalize.resources import normalize_resource_payload
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.providers.resources import resource_source_key
from chess_crawl.storage.player_profiles import (
    player_profile,
    profile_history,
    resource_history,
)
from chess_crawl.storage.raw import insert_fetch_log, store_raw_payload
from chess_crawl.storage.queries import iter_users, query_user, summary_report, user_page
from chess_crawl.storage.player_profiles import resolve_capture_account
from support import seed_game
from test_player_resources import _config, _profile, _resource


def _history(conn, user_id, kind):
    if kind == "resource":
        return resource_history(conn, user_id)
    return [
        row
        for row in profile_history(conn, user_id)
        if row["endpoint_type"] == "user_stats"
    ]


def _record(username, kind, at):
    if kind == "resource":
        key = resource_source_key("chess.com", username, "clubs")
        endpoint = "user_resource"
        params = {
            "resource_key": "clubs",
            "parameters": {},
            "authenticated": False,
            "owner_scope": "public",
        }
        body = b'{"clubs":[]}'
        url = f"https://api.chess.com/pub/player/{username.lower()}/clubs"
    else:
        key = f"chess.com/player/{username.lower()}/stats"
        endpoint = "user_stats"
        params = {}
        body = b'{"chess_blitz":{"last":{"rating":1500}}}'
        url = f"https://api.chess.com/pub/player/{username.lower()}/stats"
    return RawRecord(
        provider="chess.com",
        endpoint_type=endpoint,
        canonical_source_key=key,
        request_url=url,
        target_username=username,
        request_params=params,
        body=body,
        fetched_at=at,
    )


def _assert_public_users(conn, provider, expected_names, database_url):
    for selected_provider in (None, provider):
        streamed = list(iter_users(conn, provider=selected_provider))
        rows, total = user_page(conn, provider=selected_provider, after=0, limit=100)
        text = "".join(_export_chunks(database_url, "users", selected_provider, "alpha"))
        jsonl = [json.loads(line) for line in text.splitlines()]
        exported = len(jsonl)
        actual = {
            "iterator": {row["username_normalized"] for row in streamed},
            "page": {row["username_normalized"] for row in rows},
            "page_total": total,
            "export": {row["username_normalized"] for row in jsonl},
            "export_count": exported,
        }
        assert actual == {
            "iterator": expected_names,
            "page": expected_names,
            "page_total": len(expected_names),
            "export": expected_names,
            "export_count": len(expected_names),
        }
    summary = summary_report(conn)
    assert next(row["users"] for row in summary["providers"] if row["provider"] == provider) == len(expected_names)


def test_private_acquisition_keeps_public_envelope_unchanged(initialized_conn):
    conn = initialized_conn
    user, _ = _profile(conn, {"id": "privateuser", "username": "PrivateUser"}, at=100)
    before = player_profile(conn, "lichess", "PrivateUser")
    history_before = profile_history(conn, user)
    fetch_user_resource(
        conn,
        "lichess",
        "PrivateUser",
        "teams",
        owner_scope="alpha",
        config=_config(token="test-token", owner_scope="alpha"),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=[{"id": "private-team"}])
        ),
    )
    assert player_profile(conn, "lichess", "PrivateUser") == before
    assert profile_history(conn, user) == history_before


@pytest.mark.parametrize("status", [404, 429, 503])
@pytest.mark.parametrize("existing", [False, True])
def test_failed_stats_request_cannot_create_or_refresh_account(initialized_conn, status, existing):
    conn = initialized_conn
    if existing:
        user, _ = _profile(
            conn, {"username": "Alice", "player_id": 42, "title": "FM"},
            provider="chess.com", at=100,
        )
        before = player_profile(conn, "chess.com", "Alice")
        history_before = profile_history(conn, user)
        raw_count = conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0]
    result = fetch_chesscom_stats(
        conn, "Alice", config=_config(),
        transport=httpx.MockTransport(lambda request: httpx.Response(status)),
    )
    assert result.status_code == status and result.raw_payload_id is None
    assert result.normalized_ids == ()
    if existing:
        assert player_profile(conn, "chess.com", "Alice") == before
        assert profile_history(conn, user) == history_before
        assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == raw_count
    else:
        assert conn.execute("SELECT COUNT(*) FROM provider_users").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 0
    evidence = conn.execute(
        "SELECT status_code,raw_payload_id,provider_user_id FROM fetch_logs WHERE endpoint_type='user_stats'"
    ).fetchall()
    assert [(row["status_code"], row["raw_payload_id"], row["provider_user_id"]) for row in evidence] == [
        (status, None, None),
    ]


def test_private_only_identity_does_not_expose_collection_times(initialized_conn, database_url):
    conn = initialized_conn
    fetch_user_resource(
        conn,
        "lichess",
        "PrivateOnly",
        "teams",
        owner_scope="alpha",
        config=_config(token="test-token", owner_scope="alpha"),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=[{"id": "private-team"}])
        ),
    )
    visible = player_profile(conn, "lichess", "PrivateOnly", owner_scope="alpha")
    assert visible is not None and len(visible["resources"]) == 1
    for scope in ("public", "beta"):
        public = player_profile(conn, "lichess", "PrivateOnly", owner_scope=scope)
        assert public is None or (
            public["first_seen_at"] is None and public["updated_at"] is None
        )
    _assert_public_users(conn, "lichess", set(), database_url)
    assert query_user(conn, "lichess", "PrivateOnly") is None


@pytest.mark.parametrize("kind", ["resource", "stats"])
def test_acquired_placeholder_merges_into_stable_identity(initialized_conn, kind):
    conn = initialized_conn
    stable, _ = _profile(
        conn, {"username": "OldDave", "player_id": 7}, provider="chess.com", at=100
    )
    if kind == "resource":
        fetch_user_resource(
            conn,
            "chess.com",
            "Dave",
            "clubs",
            config=_config(),
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"clubs": []})
            ),
        )
    else:
        fetch_chesscom_stats(
            conn,
            "Dave",
            config=_config(),
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"chess_blitz": {"last": {"rating": 1500}}}
                )
            ),
        )
    merged, _ = _profile(
        conn, {"username": "Dave", "player_id": 7}, provider="chess.com", at=300
    )
    assert merged == stable
    assert len(_history(conn, stable, kind)) == 1


@pytest.mark.parametrize("kind", ["resource", "stats"])
def test_normalization_honors_acquisition_binding_after_rename(initialized_conn, kind):
    conn = initialized_conn
    original, _ = _profile(
        conn, {"username": "Eve", "player_id": 8}, provider="chess.com", at=100
    )
    raw_id, fetch_id = _persist_response(
        conn, _record("Eve", kind, 110), job_id=None, crawl_run_id=None
    )
    _profile(
        conn, {"username": "FormerEve", "player_id": 8}, provider="chess.com", at=200
    )
    new, _ = _profile(
        conn, {"username": "Eve", "player_id": 9}, provider="chess.com", at=300
    )
    normalizer = (
        normalize_resource_payload if kind == "resource" else normalize_user_payload
    )
    normalizer(conn, raw_id, prefer_observed_identity=False, fetch_log_id=fetch_id)
    assert [row["observed_at"] for row in _history(conn, original, kind)] == [110]
    assert _history(conn, new, kind) == []
    assert (
        player_profile(conn, "chess.com", "Eve")["aliases"][0]["first_seen_at"] == 300
    )


def test_reused_body_keeps_both_owners_through_network_fetch_and_replay(
    initialized_conn,
):
    conn = initialized_conn
    former, _ = _profile(
        conn, {"username": "Alice", "player_id": 1}, provider="chess.com", at=100
    )
    _, raw_id = _resource(conn, "chess.com", "clubs", {"clubs": []}, at=110)
    _profile(
        conn, {"username": "FormerAlice", "player_id": 1}, provider="chess.com", at=200
    )
    current, _ = _profile(
        conn, {"username": "Alice", "player_id": 2}, provider="chess.com", at=300
    )
    result = fetch_user_resource(
        conn,
        "chess.com",
        "Alice",
        "clubs",
        config=_config(),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, content=json.dumps({"clubs": []}).encode()
            )
        ),
    )
    assert result.raw_payload_id == raw_id
    before_former = resource_history(conn, former)
    before_current = resource_history(conn, current)
    aliases_former = player_profile(conn, "chess.com", "FormerAlice")["aliases"]
    aliases_current = player_profile(conn, "chess.com", "Alice")["aliases"]
    assert [row["observed_at"] for row in before_former] == [110]
    assert len(before_current) == 1 and before_current[0]["observed_at"] > 300
    replay_raw_payload(conn, raw_id)
    assert resource_history(conn, former) == before_former
    assert resource_history(conn, current) == before_current
    assert player_profile(conn, "chess.com", "FormerAlice")["aliases"] == aliases_former
    assert player_profile(conn, "chess.com", "Alice")["aliases"] == aliases_current


def test_conditional_response_binds_current_username_holder(initialized_conn):
    conn = initialized_conn
    original, _ = _profile(
        conn, {"username": "Carol", "player_id": 5}, provider="chess.com", at=100
    )
    record = _record("Carol", "resource", 110)
    from dataclasses import replace

    raw_id = store_raw_payload(conn, replace(record, etag='"clubs-v1"'))
    insert_fetch_log(
        conn,
        provider="chess.com",
        endpoint_type="user_resource",
        url=record.request_url,
        raw_payload_id=raw_id,
        status_code=200,
        attempted_at=110,
        etag='"clubs-v1"',
    )
    normalize_resource_payload(conn, raw_id)
    _profile(
        conn, {"username": "FormerCarol", "player_id": 5}, provider="chess.com", at=200
    )
    new, _ = _profile(
        conn, {"username": "Carol", "player_id": 6}, provider="chess.com", at=300
    )

    def respond(request):
        assert request.headers["if-none-match"] == '"clubs-v1"'
        return httpx.Response(304, headers={"etag": '"clubs-v1"'})

    fetch_user_resource(
        conn,
        "chess.com",
        "Carol",
        "clubs",
        config=_config(),
        transport=httpx.MockTransport(respond),
    )
    assert [row["observed_at"] for row in resource_history(conn, original)] == [110]
    assert len(resource_history(conn, new)) == 1


@pytest.mark.parametrize("kind", ["profile", "resource", "game", "stats"])
def test_public_observations_populate_private_placeholder_dates(initialized_conn, kind, database_url):
    conn = initialized_conn
    provider = "chess.com" if kind == "stats" else "lichess"
    user_id = resolve_capture_account(
        conn,
        provider=provider,
        username="PrivateOnly",
        observed_at=100,
        owner_scope="alpha",
    )
    assert player_profile(conn, provider, "PrivateOnly") is None
    before = conn.execute(
        "SELECT first_seen_at,updated_at FROM provider_users WHERE id=%s", (user_id,)
    ).fetchone()
    assert before["first_seen_at"] is None and before["updated_at"] is None
    _assert_public_users(conn, provider, set(), database_url)
    assert query_user(conn, provider, "PrivateOnly") is None
    if kind == "profile":
        _profile(conn, {"id": "privateonly", "username": "PrivateOnly"}, at=200)
    elif kind == "resource":
        _resource(conn, "lichess", "activity", [], username="PrivateOnly", at=200)
    elif kind == "game":
        _, participant, _ = seed_game(
            conn,
            provider="lichess",
            game_key="public-game",
            white="PrivateOnly",
            black="Opponent",
        )
        assert participant == user_id
    else:
        raw_id, fetch_id = _persist_response(
            conn, _record("PrivateOnly", "stats", 200), job_id=None, crawl_run_id=None
        )
        normalize_user_payload(conn, raw_id, fetch_log_id=fetch_id)
    public = player_profile(conn, provider, "PrivateOnly")
    assert public["id"] == user_id
    assert public["first_seen_at"] == public["updated_at"]
    assert (
        public["first_seen_at"] == 200
        if kind != "game"
        else public["first_seen_at"] > 100
    )
    expected_names = {"privateonly", "opponent"} if kind == "game" else {"privateonly"}
    _assert_public_users(conn, provider, expected_names, database_url)
    assert query_user(conn, provider, "PrivateOnly").id == user_id


def test_zero_public_identity_date_survives_private_capture(initialized_conn, database_url):
    conn = initialized_conn
    user_id = resolve_capture_account(
        conn, provider="lichess", username="Alice", observed_at=0
    )
    before = player_profile(conn, "lichess", "Alice")
    assert before["first_seen_at"] == before["updated_at"] == 0
    fetch_user_resource(
        conn,
        "lichess",
        "Alice",
        "teams",
        owner_scope="alpha",
        config=_config(token="test-token", owner_scope="alpha"),
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[])),
    )
    assert player_profile(conn, "lichess", "Alice") == before
    assert len(resource_history(conn, user_id, owner_scope="alpha")) == 1
    _assert_public_users(conn, "lichess", {"alice"}, database_url)
    assert query_user(conn, "lichess", "Alice").id == user_id


def test_statistics_replay_uses_each_accounts_own_capture_clock(initialized_conn):
    conn = initialized_conn
    former, _ = _profile(
        conn, {"username": "Bob", "player_id": 3}, provider="chess.com", at=100
    )
    raw_id, first_fetch = _persist_response(
        conn, _record("Bob", "stats", 110), job_id=None, crawl_run_id=None
    )
    normalize_user_payload(conn, raw_id, fetch_log_id=first_fetch)
    _profile(
        conn, {"username": "FormerBob", "player_id": 3}, provider="chess.com", at=200
    )
    new, _ = _profile(
        conn, {"username": "Bob", "player_id": 4}, provider="chess.com", at=300
    )
    duplicate, second_fetch = _persist_response(
        conn, _record("Bob", "stats", 400), job_id=None, crawl_run_id=None
    )
    assert duplicate == raw_id
    normalize_user_payload(conn, raw_id, fetch_log_id=second_fetch)
    replay_raw_payload(conn, raw_id)
    old_observations = _history(conn, former, "stats")
    new_observations = _history(conn, new, "stats")
    assert [(row["observed_at"], row["captured_at"]) for row in old_observations] == [
        (110, 110)
    ]
    assert [(row["observed_at"], row["captured_at"]) for row in new_observations] == [
        (400, 400)
    ]
    assert player_profile(conn, "chess.com", "FormerBob")["updated_at"] == 200
    assert player_profile(conn, "chess.com", "Bob")["updated_at"] == 400
