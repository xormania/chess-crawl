"""Complete player facts and resource observations survive refreshes and replay."""

from __future__ import annotations

import json
from datetime import date

import httpx
import pytest

from chess_crawl.config import Config
from chess_crawl.ingest import fetch_user_profile, fetch_user_resource, replay_raw_payload
from chess_crawl.normalize.resources import normalize_resource_payload
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.providers.base import RawRecord
from chess_crawl.providers.chesscom.client import ChessComClient
from chess_crawl.providers.lichess.client import LichessClient
from chess_crawl.providers.resources import get_resource, list_resources, resource_source_key
from chess_crawl.storage.db import Connection, require_row
from chess_crawl.storage.db import connection
from chess_crawl.storage import migrations
from chess_crawl.storage.player_profiles import player_profile, profile_history, resource_attempts, resource_current, resource_history
from chess_crawl.storage.raw import insert_fetch_log, read_raw_payload, store_raw_payload
from chess_crawl.storage.raw import compute_body_hash
from chess_crawl.storage.repository import upsert_provider_user, upsert_user_snapshot


def _config(*, token: str | None = None, owner_scope: str = "local") -> Config:
    return Config(chesscom_delay_s=0, lichess_delay_s=0, max_retries=0, lichess_token=token,
                  lichess_token_owner_scope=owner_scope)


def _profile(conn: Connection, body: dict, *, provider: str = "lichess", at: int = 100) -> tuple[int, int]:
    username = body.get("username", "Alice")
    raw_id = store_raw_payload(conn, RawRecord(
        provider=provider, endpoint_type="user_profile", request_url="https://example.test/profile",
        canonical_source_key=f"{provider}/player/{username.lower()}/profile", fetched_at=at,
        body=json.dumps(body).encode(),
    ))
    insert_fetch_log(conn, provider=provider, endpoint_type="user_profile", url="https://example.test/profile",
                     raw_payload_id=raw_id, status_code=200, attempted_at=at)
    user_id = normalize_user_payload(conn, raw_id)
    assert user_id is not None
    return user_id, raw_id


def _resource(
    conn: Connection, provider: str, key: str, data: object, *, at: int = 100, authenticated: bool = False,
    owner_scope: str = "public", parameters: dict | None = None, username: str = "Alice",
) -> tuple[int, int]:
    raw_id = store_raw_payload(conn, RawRecord(
        provider=provider, endpoint_type="user_resource", request_url=get_resource(provider, key).url(username, parameters),
        canonical_source_key=resource_source_key(provider, username, key, parameters,
                                                 owner_scope=owner_scope, authenticated=authenticated),
        request_params={"resource_key": key, "parameters": parameters or {}, "authenticated": authenticated,
                        "owner_scope": owner_scope}, owner_scope=owner_scope, fetched_at=at,
        body=json.dumps(data).encode(),
    ))
    insert_fetch_log(conn, provider=provider, endpoint_type="user_resource", url="https://example.test/resource",
                     raw_payload_id=raw_id, status_code=200, attempted_at=at)
    return normalize_resource_payload(conn, raw_id), raw_id


def test_complete_public_profile_facts_and_unknown_fields_are_queryable(initialized_conn: Connection) -> None:
    body = {
        "username": "Alice", "player_id": 42, "joined": 0, "last_online": 123,
        "name": "Example Player", "location": "Port-au-Prince", "avatar": "https://example.test/avatar",
        "url": "https://example.test/alice", "followers": 0, "is_streamer": False, "is_verified": False,
        "fide": 1800, "futureField": {"absent": None, "false": False, "zero": 0},
    }
    user_id, raw_id = _profile(initialized_conn, body, provider="chess.com")
    profile = player_profile(initialized_conn, "chess.com", "Alice")
    assert profile is not None and profile["id"] == user_id
    facts = profile["profile"]
    assert facts["native_data"] == body
    assert (facts["created_at"], facts["last_seen_at"], facts["followers"]) == (0, 123, 0)
    assert (facts["is_verified"], facts["is_streamer"]) == (False, False)
    assert facts["fide_rating"] == 1800 and "fide_id" not in facts
    assert facts["raw_payload_id"] == raw_id
    assert require_row(initialized_conn.execute(
        "SELECT native_data -> 'futureField' ->> 'zero' FROM user_snapshots WHERE id = %s", (facts["id"],),
    ))[0] == "0"


@pytest.mark.parametrize("times", [(100, 200, 300), (100, 100, 100)])
def test_every_profile_occurrence_is_preserved_when_values_recur(initialized_conn: Connection, times: tuple) -> None:
    conn = initialized_conn
    bodies = [{"id": "alice", "username": "Alice", "title": title} for title in ("FM", "IM", "FM")]
    raw_ids = []
    for body, at in zip(bodies, times, strict=True):
        user_id, raw_id = _profile(conn, body, at=at)
        raw_ids.append(raw_id)
    assert raw_ids[0] == raw_ids[2]
    history = profile_history(conn, user_id)
    assert [row["native_data"]["title"] for row in history] == ["FM", "IM", "FM"]
    assert [row["observed_at"] for row in history] == list(times)
    replay_raw_payload(conn, raw_ids[0])
    assert len(profile_history(conn, user_id)) == 3
    profile = player_profile(conn, "lichess", "alice")
    assert profile is not None and profile["profile"]["title"] == "FM"


def test_conditional_profile_refresh_adds_observation_without_new_body(initialized_conn: Connection) -> None:
    responses = iter([
        httpx.Response(200, headers={"etag": '"v1"'}, json={"username": "Alice", "player_id": 42}),
        httpx.Response(304, headers={"etag": '"v1"'}),
    ])
    transport = httpx.MockTransport(lambda request: next(responses))
    fetch_user_profile(initialized_conn, "chess.com", "Alice", config=_config(), transport=transport)
    result = fetch_user_profile(initialized_conn, "chess.com", "Alice", config=_config(), transport=transport)
    assert result.status_code == 304
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM user_observations"))[0] == 2


def test_ratings_keep_zero_and_false_without_inventing_unknown_counts(initialized_conn: Connection) -> None:
    conn = initialized_conn
    user_id, _ = _profile(conn, {
        "id": "alice", "username": "Alice", "createdAt": 123456, "seenAt": 234567,
        "profile": {"flag": "pirate", "fideRating": 1700}, "verified": False,
        "perfs": {"blitz": {"rating": 1500, "rd": 0, "games": 0, "prov": False, "prog": 0},
                  "chess960": {"rating": 1600}},
    })
    profile = player_profile(conn, "lichess", "alice")
    assert profile is not None
    assert profile["profile"]["country"] is None
    assert profile["profile"]["native_data"]["createdAt"] == 123456
    records = {item["performance"]: item for item in profile["ratings"]}
    assert (records["blitz"]["rating_deviation"], records["blitz"]["games"], records["blitz"]["provisional"]) == (0, 0, False)
    assert records["chess960"]["games"] is None
    assert records["blitz"]["wins"] is None
    assert len(profile_history(conn, user_id)) == 1


def test_stable_identity_rename_retains_alias_lookup(initialized_conn: Connection) -> None:
    conn = initialized_conn
    user_id, _ = _profile(conn, {"username": "OldName", "player_id": 42}, provider="chess.com", at=100)
    renamed_id, _ = _profile(conn, {"username": "NewName", "player_id": 42}, provider="chess.com", at=200)
    assert renamed_id == user_id
    profile = player_profile(conn, "chess.com", "OldName")
    assert profile is not None
    assert profile["username_normalized"] == "newname"
    assert {alias["username_normalized"] for alias in profile["aliases"]} == {"oldname", "newname"}
    other_id, _ = _profile(conn, {"username": "OldName", "player_id": 43}, provider="chess.com", at=300)
    profile = player_profile(conn, "chess.com", "OldName")
    assert profile is not None and profile["id"] == other_id


@pytest.mark.parametrize(("provider", "key", "path", "parameters", "body"), [
    ("chess.com", "clubs", "/pub/player/alice/clubs", None, {"clubs": []}),
    ("chess.com", "matches", "/pub/player/alice/matches", None, {"finished": [], "in_progress": [], "registered": []}),
    ("chess.com", "tournaments", "/pub/player/alice/tournaments", None, {"finished": [], "in_progress": [], "registered": []}),
    ("chess.com", "online", "/pub/player/alice/is-online", None, {"online": False}),
    ("lichess", "rating-history", "/api/user/alice/rating-history", None, []),
    ("lichess", "activity", "/api/user/alice/activity", None, []),
    ("lichess", "performance", "/api/user/alice/perf/chess960", {"perf": "chess960"}, {"stat": {"count": {"all": 0}}}),
])
def test_resource_requests_preserve_complete_json_and_registered_metadata(
    initialized_conn: Connection, provider: str, key: str, path: str, parameters: dict | None, body: object,
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path == path
        assert request.headers["accept"] == "application/json"
        return httpx.Response(200, json=body)

    result = fetch_user_resource(initialized_conn, provider, "Alice", key, parameters=parameters,
                                 config=_config(), transport=httpx.MockTransport(respond))
    assert result.raw_payload_id is not None
    raw = read_raw_payload(initialized_conn, result.raw_payload_id)
    assert json.loads(raw.body) == body
    assert raw.request_params is not None and json.loads(raw.request_params)["resource_key"] == key
    snapshot = require_row(initialized_conn.execute("SELECT * FROM user_resource_snapshots"))
    assert snapshot["native_data"] == body
    assert snapshot["parameters"] == (parameters or {})
    assert snapshot["owner_scope"] == "public"


def test_rating_history_zero_based_months_and_uninterpreted_values(initialized_conn: Connection) -> None:
    conn = initialized_conn
    body = [{"name": "Blitz", "points": [[2024, 0, 1, 1500], [2024, 1, 29, 1501], [2024, 12, 1, 1502],
                                           [2024, 2, 1, False], {"future": "format"}]}]
    snapshot_id, raw_id = _resource(conn, "lichess", "rating-history", body, authenticated=True)
    points = conn.execute("SELECT rating_date, rating FROM rating_history_points ORDER BY point_index").fetchall()
    assert [tuple(point.values()) for point in points] == [(date(2024, 1, 1), 1500), (date(2024, 2, 29), 1501)]
    snapshot = require_row(conn.execute("SELECT * FROM user_resource_snapshots WHERE id = %s", (snapshot_id,)))
    assert snapshot["native_data"] == body and snapshot["coverage_status"] == "partial"
    replay_raw_payload(conn, raw_id)
    assert require_row(conn.execute("SELECT COUNT(*) FROM rating_history_points"))[0] == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM user_resource_observations"))[0] == 1


def test_empty_unauthenticated_history_is_not_claimed_complete(initialized_conn: Connection) -> None:
    conn = initialized_conn
    first, first_raw = _resource(conn, "lichess", "rating-history", [], at=100)
    second, second_raw = _resource(conn, "lichess", "rating-history", [], authenticated=True, at=200)
    assert first_raw != second_raw and first != second
    statuses = [row[0] for row in conn.execute("SELECT coverage_status FROM user_resource_snapshots ORDER BY id")]
    assert statuses == ["unknown", "empty"]
    user_id = require_row(conn.execute("SELECT id FROM provider_users"))[0]
    assert resource_current(conn, user_id)[0]["coverage_status"] == "empty"


def test_resource_history_preserves_recurring_memberships(initialized_conn: Connection) -> None:
    conn = initialized_conn
    for at, clubs in ((100, [{"@id": "club-a", "joined": 50}]), (200, []), (300, [{"@id": "club-a", "joined": 50}])):
        _resource(conn, "chess.com", "clubs", {"clubs": clubs}, at=at)
    user_id = require_row(conn.execute("SELECT id FROM provider_users"))[0]
    history = resource_history(conn, user_id, resource_key="clubs")
    assert [row["coverage_status"] for row in history] == ["observed", "empty", "observed"]
    assert len(resource_history(conn, user_id, after_id=history[0]["observation_id"])) == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 2
    assert require_row(conn.execute("SELECT COUNT(*) FROM user_resource_snapshots"))[0] == 2


def test_resource_replay_follows_stable_account_after_rename(initialized_conn: Connection) -> None:
    conn = initialized_conn
    user_id, _ = _profile(conn, {"username": "OldName", "player_id": 42}, provider="chess.com", at=100)
    _, resource_raw = _resource(conn, "chess.com", "clubs", {"clubs": []}, username="OldName", at=110)
    _profile(conn, {"username": "NewName", "player_id": 42}, provider="chess.com", at=200)
    replay_raw_payload(conn, resource_raw)
    assert require_row(conn.execute("SELECT COUNT(*) FROM provider_users"))[0] == 1
    assert require_row(conn.execute("SELECT provider_user_id FROM user_resource_snapshots"))[0] == user_id
    assert require_row(conn.execute("SELECT username_normalized FROM provider_users"))[0] == "newname"


def test_teams_require_token_and_owner_before_request() -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        raise AssertionError("validation must precede network access")

    for token, scope, match in ((None, "workspace:a", "OAuth"), ("secret", "public", "workspace"),
                                ("secret", "workspace:other", "different workspace")):
        client = LichessClient(_config(token=token).provider("lichess"), transport=httpx.MockTransport(forbidden))
        try:
            with pytest.raises(ValueError, match=match):
                client.get_user_resource("Alice", "teams", owner_scope=scope)
        finally:
            client.close()


def test_authenticated_teams_are_isolated_in_normalized_and_raw_storage(initialized_conn: Connection) -> None:
    conn = initialized_conn
    secret_team = [{"id": "hidden-team", "name": "Private membership"}]

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer secret-token"
        return httpx.Response(200, json=secret_team)

    results = [fetch_user_resource(conn, "lichess", "Alice", "teams", owner_scope=scope,
                                   config=_config(token="secret-token", owner_scope=scope), transport=httpx.MockTransport(respond))
               for scope in ("workspace:a", "workspace:b")]
    assert results[0].raw_payload_id != results[1].raw_payload_id
    user_id = require_row(conn.execute("SELECT id FROM provider_users"))[0]
    assert resource_current(conn, user_id) == []
    assert resource_history(conn, user_id) == []
    assert len(resource_current(conn, user_id, owner_scope="workspace:a")) == 1
    assert len(resource_history(conn, user_id, owner_scope="workspace:b")) == 1
    assert resource_current(conn, user_id, owner_scope="workspace:a")[0]["owner_scope"] == "workspace:a"
    for result in results:
        assert result.raw_payload_id is not None
        raw = read_raw_payload(conn, result.raw_payload_id)
        assert raw.owner_scope.startswith("workspace:")
        assert "secret-token" not in (raw.request_params or "")
    assert all("Authorization" not in json.loads(row[0]) for row in conn.execute("SELECT response_headers FROM raw_payloads"))


def test_invalid_resource_requests_do_not_make_network_calls() -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        raise AssertionError("validation must precede network access")

    client = ChessComClient(_config().provider("chess.com"), transport=httpx.MockTransport(forbidden))
    try:
        with pytest.raises(ValueError, match="unsupported player resource"):
            client.get_user_resource("Alice", "https://example.test/arbitrary")
        with pytest.raises(ValueError, match="does not accept"):
            client.get_user_resource("Alice", "clubs", parameters={"next": "https://example.test"})
        with pytest.raises(ValueError, match="empty"):
            client.get_user_resource("", "clubs")
    finally:
        client.close()
    with pytest.raises(ValueError, match="unsupported perf"):
        get_resource("lichess", "performance").parameters({"perf": "../admin"})
    assert len(list_resources()) == 8


def test_public_lichess_profile_requests_all_public_extensions_without_oauth() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        assert dict(request.url.params) == {"trophies": "true", "profile": "true", "rank": "true", "fideId": "true"}
        return httpx.Response(200, json={"id": "alice", "username": "Alice"})

    client = LichessClient(_config(token="private-token").provider("lichess"), transport=httpx.MockTransport(respond))
    try:
        raw = client.get_user_profile("Alice")
        assert raw.owner_scope == "public" and raw.request_params["fideId"] == "true"
    finally:
        client.close()


def test_legacy_oauth_profile_is_preserved_and_quarantined(initialized_conn: Connection) -> None:
    conn = initialized_conn
    raw_id = store_raw_payload(conn, RawRecord(
        provider="lichess", endpoint_type="user_profile", request_url="https://example.test/profile",
        canonical_source_key="lichess/player/alice/profile", fetched_at=100,
        body=b'{"id":"alice","username":"Alice","following":false}',
    ))
    with pytest.raises(ValueError, match="scoped ownership"):
        normalize_user_payload(conn, raw_id)
    assert read_raw_payload(conn, raw_id).body.endswith(b'"following":false}')
    assert read_raw_payload(conn, raw_id).owner_scope == "unassigned:legacy-profile"
    assert require_row(conn.execute("SELECT COUNT(*) FROM user_snapshots"))[0] == 0


def test_unavailable_resource_is_not_normalized_as_empty(initialized_conn: Connection) -> None:
    result = fetch_user_resource(
        initialized_conn, "chess.com", "Alice", "clubs", config=_config(),
        transport=httpx.MockTransport(lambda request: httpx.Response(404)),
    )
    assert result.status_code == 404 and result.raw_payload_id is None
    evidence = resource_attempts(initialized_conn, "chess.com", "Alice")
    assert len(evidence) == 1 and evidence[0]["status_code"] == 404
    assert evidence[0]["raw_payload_id"] is None
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM user_resource_snapshots"))[0] == 0


def test_conditional_resource_refresh_preserves_observation_history(initialized_conn: Connection) -> None:
    responses = iter([
        httpx.Response(200, headers={"etag": '"clubs-v1"'}, json={"clubs": []}),
        httpx.Response(304, headers={"etag": '"clubs-v1"'}),
    ])

    def respond(request: httpx.Request) -> httpx.Response:
        response = next(responses)
        if response.status_code == 304:
            assert request.headers["if-none-match"] == '"clubs-v1"'
        return response

    transport = httpx.MockTransport(respond)
    for _ in range(2):
        fetch_user_resource(initialized_conn, "chess.com", "Alice", "clubs", config=_config(), transport=transport)
    user_id = require_row(initialized_conn.execute("SELECT id FROM provider_users"))[0]
    assert len(resource_history(initialized_conn, user_id)) == 2
    assert require_row(initialized_conn.execute("SELECT COUNT(*) FROM raw_payloads"))[0] == 1


def test_provider_token_owner_is_loaded_explicitly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_TOKEN", "private-token")
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_TOKEN_OWNER_SCOPE", "workspace:a")
    settings = Config.from_env().provider("lichess")
    assert settings.oauth_owner_scope == "workspace:a" and settings.oauth_token == "private-token"


def test_populated_archive_upgrade_recovers_recurring_observation_history(
    uninitialized_database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = tuple(item for item in migrations.migration_resources() if item[0] <= 5)
    with connection(uninitialized_database_url, mode="rwc") as conn:
        with monkeypatch.context() as old:
            old.setattr(migrations, "migration_resources", lambda: baseline)
            old.setattr(migrations, "SCHEMA_VERSION", 5)
            migrations.initialize(conn)
        user_id = upsert_provider_user(conn, provider="lichess", username="Alice", now=100)
        raw_ids = []
        for title, at in (("FM", 100), ("IM", 200)):
            body = json.dumps({"id": "alice", "username": "Alice", "title": title}).encode()
            raw_id = require_row(conn.execute(
                """INSERT INTO raw_payloads(provider, endpoint_type, canonical_source_key, response_status, fetched_at,
                   body_hash, body_compression, raw_body, body_bytes)
                   VALUES('lichess', 'user_profile', 'lichess/player/alice/profile', 200, %s, %s, 'none', %s, %s) RETURNING id""",
                (at, compute_body_hash(body), body, len(body)),
            ))[0]
            raw_ids.append(raw_id)
            upsert_user_snapshot(conn, provider_user_id=user_id, captured_at=at, observed_username="Alice",
                                 content_hash=title, raw_payload_id=raw_id, title=title)
        for raw_id, at in ((raw_ids[0], 100), (raw_ids[1], 200), (raw_ids[0], 300)):
            insert_fetch_log(conn, provider="lichess", endpoint_type="user_profile", url="https://example.test/profile",
                             raw_payload_id=raw_id, status_code=200, attempted_at=at)
        upsert_user_snapshot(conn, provider_user_id=user_id, captured_at=300, observed_username="Alice",
                             content_hash="FM", raw_payload_id=raw_ids[0], title="FM")
        migrations.initialize(conn)
        history = profile_history(conn, user_id)
        assert [item["title"] for item in history] == ["FM", "IM", "FM"]
        assert [item["observed_at"] for item in history] == [100, 200, 300]
        assert all(item["native_data"] is None for item in history)
        for raw_id in raw_ids:
            replay_raw_payload(conn, raw_id)
        history = profile_history(conn, user_id)
        assert [item["native_data"]["title"] for item in history] == ["FM", "IM", "FM"]
        assert len(history) == 3
        profile = player_profile(conn, "lichess", "Alice")
        assert profile is not None and profile["profile"]["native_data"]["title"] == "FM"
