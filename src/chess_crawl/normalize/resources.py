"""Normalize complete supplementary player JSON and source rating history."""

from __future__ import annotations

import json
import time
from datetime import date

from chess_crawl.providers.resources import get_resource, resource_owner_scope
from chess_crawl.storage.db import Connection, transaction
from chess_crawl.storage.player_profiles import record_alias, resource_account, store_resource_snapshot
from chess_crawl.storage.raw import insert_source_record, payload_observed_at, read_raw_payload, update_raw_payload_status
from chess_crawl.storage.repository import upsert_provider_user


PARSER_VERSION = "player-resources-normalizer-v1"


def normalize_resource_payload(conn: Connection, raw_payload_id: int) -> int:
    raw = read_raw_payload(conn, raw_payload_id)
    if raw.endpoint_type != "user_resource":
        raise ValueError("supplementary player normalizer requires user_resource")
    params = json.loads(raw.request_params or "{}")
    resource_key = params.get("resource_key")
    if not isinstance(resource_key, str):
        raise ValueError("player resource is missing its registered resource key")
    resource = get_resource(raw.provider, resource_key)
    parameters = resource.parameters(params.get("parameters"))
    owner_scope = resource_owner_scope(resource, raw.owner_scope)
    if params.get("owner_scope", "public") != owner_scope:
        raise ValueError("resource capture ownership disagrees with its raw payload")
    username = _username(raw.canonical_source_key)
    data = json.loads(raw.body)
    if not isinstance(data, (dict, list)):
        raise ValueError("player resource must contain a JSON object or array")
    status, note = _coverage(raw.provider, resource_key, data, params.get("authenticated") is True)
    points: list[tuple[int, int, str, date, int, str]] = []
    if resource_key == "rating-history":
        points, issues = _rating_points(data)
        if issues:
            status = "partial"
            note = f"{len(issues)} uninterpreted rating history element(s): " + "; ".join(issues[:5])
    with transaction(conn):
        observed_at = payload_observed_at(conn, raw_payload_id)
        user_id = resource_account(conn, raw.provider, username, raw_payload_id)
        if user_id is None:
            user_id = upsert_provider_user(conn, provider=raw.provider, username=username, now=observed_at)
        record_alias(conn, user_id, username, observed_at=observed_at,
                     raw_payload_id=raw_payload_id if owner_scope == "public" else None, first_observed_at=raw.fetched_at)
        snapshot_id = store_resource_snapshot(
            conn, user_id=user_id, resource_key=resource_key, parameters=parameters, native_data=data,
            coverage_status=status, coverage_note=note, parser_version=PARSER_VERSION, raw_payload_id=raw_payload_id,
            rating_points=points, owner_scope=owner_scope,
        )
        insert_source_record(
            conn, entity_type="user_resource", entity_id=snapshot_id, provider=raw.provider,
            endpoint_type=raw.endpoint_type, raw_payload_id=raw_payload_id, source_key=raw.canonical_source_key,
        )
        update_raw_payload_status(
            conn, raw_payload_id, status="parsed", parser_version=PARSER_VERSION, normalized_at=int(time.time()),
        )
    return snapshot_id


def _username(source_key: str) -> str:
    from urllib.parse import unquote

    parts = source_key.split("/")
    if len(parts) < 5 or parts[1] != "player" or parts[3] != "resources" or not parts[2]:
        raise ValueError("player resource has an invalid source identity")
    return unquote(parts[2])


def _coverage(provider: str, key: str, data: object, authenticated: bool) -> tuple[str, str | None]:
    if provider == "lichess":
        if key in {"rating-history", "activity", "teams"} and not isinstance(data, list):
            return "unknown", "Expected array; full supplied JSON retained for interpretation."
        if key == "rating-history" and data == [] and not authenticated:
            return "unknown", "Unauthenticated empty response may mean rating history has not been cached."
        if key == "activity":
            return "partial", "Provider activity window; not complete lifetime activity coverage."
        if key == "teams":
            return "partial", "Hidden teams are visible only when the authenticated caller also belongs."
    if provider == "chess.com" and isinstance(data, dict):
        fields = ("clubs",) if key == "clubs" else ("finished", "in_progress", "registered")
        if key in {"clubs", "matches", "tournaments"}:
            if not all(isinstance(data.get(field), list) for field in fields):
                return "unknown", "Missing or malformed listing fields; full supplied JSON retained."
            return ("empty" if not any(data[field] for field in fields) else "observed"), None
    return ("empty" if data in ({}, []) else "observed"), None


def _rating_points(data: object) -> tuple[list[tuple[int, int, str, date, int, str]], list[str]]:
    points: list[tuple[int, int, str, date, int, str]] = []
    issues: list[str] = []
    if not isinstance(data, list):
        return points, ["root is not an array"]
    for perf_index, perf in enumerate(data):
        if not isinstance(perf, dict) or not isinstance(perf.get("name"), str) or not isinstance(perf.get("points"), list):
            issues.append(f"/{perf_index}: expected named performance and points array")
            continue
        for point_index, point in enumerate(perf["points"]):
            pointer = f"/{perf_index}/points/{point_index}"
            if not isinstance(point, list) or len(point) != 4 or any(type(value) is not int for value in point):
                issues.append(f"{pointer}: expected [year, zero-based month, day, rating]")
                continue
            year, month, day, rating = point
            try:
                rating_date = date(year, month + 1, day)
            except ValueError:
                issues.append(f"{pointer}: invalid calendar date")
                continue
            points.append((perf_index, point_index, perf["name"], rating_date, rating, pointer))
    return points, issues
