"""Provider configuration and archived player data builders."""
from __future__ import annotations

import json
from chess_crawl.config import Config
from chess_crawl.normalize.resources import normalize_resource_payload
from chess_crawl.normalize.users import normalize_user_payload
from chess_crawl.providers.base import EndpointType, RawRecord
from chess_crawl.providers.resources import get_resource, resource_source_key
from chess_crawl.storage.db import Connection
from chess_crawl.storage.raw import insert_fetch_log, store_raw_payload


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


def _record(username: str, kind: str, at: int) -> RawRecord:
    endpoint: EndpointType
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
        url = get_resource("chess.com", "clubs").url(username, None)
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
