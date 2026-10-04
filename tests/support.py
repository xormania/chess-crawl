"""Shared deterministic data builders for offline tests."""

from __future__ import annotations

from chess_crawl.storage.db import Connection, transaction

from chess_crawl.storage.repository import (
    get_or_create_time_control,
    get_or_create_variant,
    upsert_game,
    upsert_game_participant,
    upsert_provider_user,
)


class Clock:
    """An explicit clock whose sleeps advance time without delaying tests."""

    def __init__(self, now: float = 1_700_000_000) -> None:
        self.now = now
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def seed_game(
    conn: Connection,
    *,
    provider: str,
    game_key: str,
    white: str,
    black: str,
    outcome: str | None = "white_win",
    ended_at: int = 1704067200,
) -> tuple[int, int, int]:
    with transaction(conn):
        white_id = upsert_provider_user(conn, provider=provider, username=white)
        black_id = upsert_provider_user(conn, provider=provider, username=black)
        variant_id = get_or_create_variant(
            conn,
            provider=provider,
            provider_native_name="standard",
            canonical_name="standard",
        )
        time_control_id = get_or_create_time_control(
            conn,
            kind="clock",
            initial_seconds=300,
            increment_seconds=0,
            days=None,
            time_class="blitz",
            raw_label="300",
        )
        game_id = upsert_game(
            conn,
            provider=provider,
            provider_game_id=game_key,
            canonical_url=f"https://example.test/{provider}/{game_key}",
            content_hash=f"sha256:{provider}:{game_key}",
            variant_id=variant_id,
            time_control_id=time_control_id,
            rated=True,
            outcome=outcome,
            ended_at=ended_at,
        )
        upsert_game_participant(
            conn,
            game_id=game_id,
            color="white",
            provider_user_id=white_id,
            username_normalized=white.lower(),
        )
        upsert_game_participant(
            conn,
            game_id=game_id,
            color="black",
            provider_user_id=black_id,
            username_normalized=black.lower(),
        )
        return game_id, white_id, black_id
