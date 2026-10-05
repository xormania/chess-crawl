"""Legacy Lichess names repair locally while retaining the source profile."""

import json

import pytest

from chess_crawl.ingest import replay_raw_payload
from chess_crawl.normalize.users import PARSER_VERSION
from chess_crawl.storage.db import transaction
from chess_crawl.storage.player_profiles import player_profile, profile_history
from chess_crawl.storage.raw import read_raw_payload, update_raw_payload_status
from helpers.players import _profile


@pytest.mark.parametrize(("names", "expected"), [
    ({"firstName": " Ada ", "lastName": " Lovelace "}, "Ada Lovelace"),
    ({"firstName": "Ada"}, "Ada"),
    ({"lastName": "Lovelace"}, "Lovelace"),
    ({"firstName": "  ", "lastName": ""}, None),
    ({"firstName": False, "lastName": 123}, None),
    ({"firstName": "Old", "lastName": "Name", "realName": "Current Name"}, "Current Name"),
    ({"firstName": "Old", "lastName": "Name", "realName": ""}, ""),
    ({"firstName": "Old", "lastName": "Name", "realName": None}, None),
    ({"firstName": "Old", "lastName": "Name", "realName": 123}, None),
])
def test_lichess_names_preserve_optional_fields_and_native_json(initialized_conn, names, expected):
    conn = initialized_conn
    body = {"id": "alice", "username": "Alice", "profile": {**names, "futureField": {"zero": 0}}}
    user_id, raw_id = _profile(conn, body)
    rich = player_profile(conn, "lichess", "Alice")
    assert rich is not None and rich["id"] == user_id
    assert rich["profile"]["real_name"] == expected
    assert rich["profile"]["native_data"] == body
    assert json.loads(read_raw_payload(conn, raw_id).body) == body
    assert profile_history(conn, user_id)[0]["native_data"] == body


def test_legacy_name_replay_repairs_v6_without_new_acquisition(initialized_conn):
    conn = initialized_conn
    body = {"id": "alice", "username": "Alice", "profile": {"firstName": "Ada", "lastName": "Lovelace"}}
    user_id, raw_id = _profile(conn, body)
    before = player_profile(conn, "lichess", "Alice")
    assert before is not None
    with transaction(conn):
        conn.execute("UPDATE user_snapshots SET real_name=NULL WHERE raw_payload_id=%s", (raw_id,))
        update_raw_payload_status(conn, raw_id, status="parsed", parser_version="users-normalizer-v6")
    replay_raw_payload(conn, raw_id)
    after = player_profile(conn, "lichess", "Alice")
    assert after is not None and after["profile"]["real_name"] == "Ada Lovelace"
    assert after["profile"]["id"] == before["profile"]["id"]
    assert after["profile"]["native_data"] == body
    assert len(profile_history(conn, user_id)) == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_payloads").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM fetch_logs").fetchone()[0] == 1
    assert read_raw_payload(conn, raw_id).parser_version == PARSER_VERSION
    assert PARSER_VERSION != "users-normalizer-v6"
