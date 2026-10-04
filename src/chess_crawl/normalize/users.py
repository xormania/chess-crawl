"""Normalize stored raw user/profile/stat payloads."""

from __future__ import annotations

import json
import time
from typing import Any

from chess_crawl.normalize.codes import canonical_hash
from chess_crawl.providers.chesscom import parser as chesscom_parser
from chess_crawl.providers.lichess import parser as lichess_parser
from chess_crawl.providers.base import NormalizedUser
from chess_crawl.storage.db import Connection, transaction
from chess_crawl.storage.raw import insert_source_record, payload_observed_at, read_raw_payload, update_raw_payload_status
from chess_crawl.storage.repository import upsert_provider_user, upsert_user_snapshot
from chess_crawl.storage.player_profiles import (
    publish_verified_legacy_profile, quarantine_unowned_profile, record_alias, record_profile_observations,
    stats_account, store_profile_facts, store_rating_records,
)


PARSER_VERSION = "users-normalizer-v5"


def normalize_user_payload(
    conn: Connection, raw_payload_id: int, *, prefer_observed_identity: bool = True,
) -> int | None:
    raw = read_raw_payload(conn, raw_payload_id)
    if raw.provider == "lichess" and raw.endpoint_type == "user_profile":
        data = json.loads(raw.body)
        if isinstance(data, dict) and any(key in data for key in ("following", "blocking", "followable")):
            quarantine_unowned_profile(conn, raw_payload_id)
            raise ValueError("account-relative profile relationships require scoped ownership before normalization")
    if raw.endpoint_type == "user_profile":
        if raw.provider == "chess.com":
            user = chesscom_parser.parse_user_profile(raw.body)
            snapshot = _chesscom_profile_snapshot(raw.body, user)
        elif raw.provider == "lichess":
            user = lichess_parser.parse_user_profile(raw.body)
            snapshot = _lichess_profile_snapshot(raw.body, user)
        else:
            raise ValueError(f"unsupported provider: {raw.provider}")
    elif raw.endpoint_type == "user_stats" and raw.provider == "chess.com":
        user = _user_from_stats_key(raw.canonical_source_key)
        snapshot = _chesscom_stats_snapshot(raw.body, user)
    else:
        return None

    with transaction(conn):
        native_data = json.loads(raw.body)
        if not isinstance(native_data, dict):
            raise ValueError("user profiles and statistics must be JSON objects")
        observed_at = payload_observed_at(conn, raw_payload_id)
        account = (
            stats_account(
                conn, user.provider, user.display_username, raw_payload_id,
                prefer_observed_identity=prefer_observed_identity,
            )
            if raw.endpoint_type == "user_stats" else None
        )
        provider_user_id = upsert_provider_user(
            conn,
            provider=user.provider,
            username=user.display_username if account is None else account["username_normalized"],
            provider_user_id=user.provider_user_id if account is None else account["provider_user_id"],
            display_username=user.display_username if account is None else account["display_username"],
            account_status=user.account_status_raw,
            title=user.title,
            now=observed_at,
            profile_raw_payload_id=raw_payload_id if raw.endpoint_type == "user_profile" else None,
        )
        snapshot_id = upsert_user_snapshot(
            conn,
            provider_user_id=provider_user_id,
            captured_at=observed_at,
            observed_username=user.display_username,
            status=user.account_status_raw,
            title=user.title,
            country=user.country,
            followers=snapshot.get("followers"),
            patron=snapshot.get("patron"),
            count_all=snapshot.get("count_all"),
            count_rated=snapshot.get("count_rated"),
            count_win=snapshot.get("count_win"),
            count_loss=snapshot.get("count_loss"),
            count_draw=snapshot.get("count_draw"),
            perfs_or_stats=snapshot.get("perfs_or_stats"),
            content_hash=snapshot["content_hash"],
            raw_payload_id=raw_payload_id,
        )
        store_profile_facts(conn, snapshot_id, native_data=native_data, facts=_profile_facts(native_data, user))
        store_rating_records(conn, snapshot_id, _rating_records(native_data, user.provider))
        record_profile_observations(conn, provider_user_id, snapshot_id, raw_payload_id)
        if raw.endpoint_type == "user_profile":
            record_alias(conn, provider_user_id, user.display_username, observed_at=observed_at, raw_payload_id=raw_payload_id,
                         first_observed_at=raw.fetched_at)
        insert_source_record(
            conn,
            entity_type="user",
            entity_id=provider_user_id,
            provider=raw.provider,
            endpoint_type=raw.endpoint_type,
            raw_payload_id=raw_payload_id,
            source_key=raw.canonical_source_key,
        )
        insert_source_record(
            conn,
            entity_type="user_snapshot",
            entity_id=snapshot_id,
            provider=raw.provider,
            endpoint_type=raw.endpoint_type,
            raw_payload_id=raw_payload_id,
            source_key=raw.canonical_source_key,
        )
        update_raw_payload_status(
            conn,
            raw_payload_id,
            status="parsed",
            parser_version=PARSER_VERSION,
            normalized_at=int(time.time()),
        )
        if raw.provider == "lichess" and raw.endpoint_type == "user_profile":
            publish_verified_legacy_profile(conn, raw_payload_id)
    return provider_user_id


def _chesscom_profile_snapshot(body: bytes, user: NormalizedUser) -> dict[str, Any]:
    data = json.loads(body)
    payload = {
        "kind": "profile",
        "username": user.display_username,
        "status": user.account_status_raw,
        "title": user.title,
        "country": user.country,
        "followers": data.get("followers"),
        "created_at": user.created_at,
    }
    return {
        "followers": _int_or_none(data.get("followers")),
        "perfs_or_stats": payload,
        "content_hash": canonical_hash({"kind": "profile", "native_data": data}),
    }


def _lichess_profile_snapshot(body: bytes, user: NormalizedUser) -> dict[str, Any]:
    data = json.loads(body)
    count = data.get("count") or {}
    perfs = data.get("perfs") or {}
    # Keep provider-native profile/account facts, including explicit false and
    # absent values. In particular, profile.flag need not represent a country.
    profile_facts = {"perfs": perfs}
    for key in ("profile", "disabled", "verified", "tosViolation"):
        if key in data:
            profile_facts[key] = data[key]
    return {
        "patron": bool(data.get("patron")) if data.get("patron") is not None else None,
        "count_all": _int_or_none(count.get("all")),
        "count_rated": _int_or_none(count.get("rated")),
        "count_win": _int_or_none(count.get("win")),
        "count_loss": _int_or_none(count.get("loss")),
        "count_draw": _int_or_none(count.get("draw")),
        "perfs_or_stats": profile_facts,
        "content_hash": canonical_hash({"kind": "profile", "native_data": data}),
    }


def _chesscom_stats_snapshot(body: bytes, user: NormalizedUser) -> dict[str, Any]:
    stats = json.loads(body)
    aggregate = _aggregate_chesscom_stats(stats)
    payload = {
        "kind": "stats",
        "username": user.display_username,
        "stats": stats,
    }
    return {
        **aggregate,
        "perfs_or_stats": stats,
        "content_hash": canonical_hash(payload),
    }


def _aggregate_chesscom_stats(stats: dict[str, Any]) -> dict[str, int | None]:
    # The stats endpoint describes each record as all games played, without a
    # rated-only total. A present game type with incomplete data must not be
    # silently counted as zero; absent game types have no reported play.
    records = []
    for name, value in stats.items():
        if isinstance(value, dict) and (
            "record" in value or name.rsplit("_", 1)[-1] in {"daily", "bullet", "blitz", "rapid"}
        ):
            record = value.get("record")
            records.append(record if isinstance(record, dict) else {})
    wins = _sum_record_counts(records, "win")
    losses = _sum_record_counts(records, "loss")
    draws = _sum_record_counts(records, "draw")
    total = None if wins is None or losses is None or draws is None else wins + losses + draws
    return {
        "count_all": total,
        "count_rated": None,
        "count_win": wins,
        "count_loss": losses,
        "count_draw": draws,
    }


def _sum_record_counts(records: list[dict[str, Any]], field: str) -> int | None:
    if not records:
        return None
    total = 0
    for record in records:
        value = record.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            return None
        total += value
    return total


def _user_from_stats_key(source_key: str) -> NormalizedUser:
    parts = source_key.split("/")
    username = parts[2] if len(parts) >= 3 else "unknown"
    return NormalizedUser(
        provider="chess.com",
        provider_user_id=None,
        username_normalized=username.lower(),
        display_username=username,
    )


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _profile_facts(data: dict[str, Any], user: NormalizedUser) -> dict[str, Any]:
    profile_data = data.get("profile")
    profile = profile_data if isinstance(profile_data, dict) else {}
    return {
        "created_at": user.created_at,
        "last_seen_at": user.last_seen_at,
        "real_name": _string_or_none(data.get("name") if user.provider == "chess.com" else profile.get("realName")),
        "location": _string_or_none(data.get("location") if user.provider == "chess.com" else profile.get("location")),
        "avatar_url": _string_or_none(data.get("avatar")),
        "profile_url": _string_or_none(data.get("url")),
        "is_verified": user.is_verified,
        "is_streamer": _boolean_or_none(data.get("is_streamer") if user.provider == "chess.com" else data.get("streaming")),
        # Chess.com's fide is a self-reported rating, never a federation ID.
        "fide_rating": _strict_integer(data.get("fide") if user.provider == "chess.com" else profile.get("fideRating")),
    }


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _strict_integer(value: object) -> int | None:
    return value if type(value) is int else None


def _boolean_or_none(value: object) -> bool | None:
    return value if type(value) is bool else None


def _rating_records(data: dict[str, Any], provider: str) -> list[dict[str, Any]]:
    perfs = data.get("perfs") if provider == "lichess" else data
    if not isinstance(perfs, dict):
        return []
    records = []
    for performance, value in perfs.items():
        if not isinstance(value, dict):
            continue
        if provider == "chess.com":
            if not any(key in value for key in ("last", "best", "record", "highest", "lowest")):
                continue
            last = _dictionary(value.get("last"))
            best = _dictionary(value.get("best") if "best" in value else value.get("highest"))
            lowest = _dictionary(value.get("lowest"))
            score = _dictionary(value.get("record"))
            wins, losses, draws = (_strict_integer(score.get(field)) for field in ("win", "loss", "draw"))
            games = None if wins is None or losses is None or draws is None else wins + losses + draws
            current, deviation, provisional, progress = last.get("rating"), last.get("rd"), None, None
        else:
            best, lowest, wins, losses, draws = {}, {}, None, None, None
            current, deviation, provisional, progress = value.get("rating"), value.get("rd"), value.get("prov"), value.get("prog")
            games = _strict_integer(value.get("games"))
        records.append({
            "performance": performance, "rating": _strict_integer(current),
            "best_rating": _strict_integer(best.get("rating")), "best_at": _strict_integer(best.get("date")),
            "lowest_rating": _strict_integer(lowest.get("rating")), "lowest_at": _strict_integer(lowest.get("date")),
            "rating_deviation": deviation if type(deviation) in {int, float} else None,
            "provisional": provisional if type(provisional) is bool else None,
            "games": games, "wins": wins, "losses": losses, "draws": draws,
            "progress": _strict_integer(progress), "native_data": value,
        })
    return records


def _dictionary(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
