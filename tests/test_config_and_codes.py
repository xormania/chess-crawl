from __future__ import annotations

import pytest

from chess_crawl.config import Config, build_user_agent
from chess_crawl.normalize.codes import (
    canonical_hash,
    chesscom_outcome,
    lichess_outcome,
    map_variant,
    normalize_time_class,
    normalize_username,
)


def test_config_from_env_and_provider_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHESS_CRAWL_CONTACT", "ops@example.test")
    monkeypatch.setenv("CHESS_CRAWL_USER_AGENT", "custom-agent")
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_TOKEN", "lip_test")

    config = Config.from_env()

    assert config.contact == "ops@example.test"
    assert config.provider("chess.com").oauth_token is None
    assert config.provider("lichess").oauth_token == "lip_test"
    assert config.provider("lichess").user_agent == "custom-agent"
    with pytest.raises(KeyError):
        config.provider("unknown")


def test_default_user_agent_contains_contact() -> None:
    assert "chess-crawl/" in build_user_agent("ops@example.test")
    assert "ops@example.test" in Config(contact="ops@example.test").provider("chess.com").user_agent


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        (" SameName ", "samename"),
    ],
)
def test_normalize_username(value: str | None, expected: str | None) -> None:
    assert normalize_username(value) == expected


def test_canonical_hash_is_order_independent() -> None:
    assert canonical_hash({"b": 2, "a": 1}) == canonical_hash({"a": 1, "b": 2})


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("daily", "correspondence"),
        ("ultraBullet", "bullet"),
        ("rapid", "rapid"),
        ("custom", "custom"),
    ],
)
def test_normalize_time_class(value: str, expected: str) -> None:
    assert normalize_time_class(value) == expected


@pytest.mark.parametrize(
    ("provider", "native", "expected", "mapped"),
    [
        ("chess.com", "chess", "standard", True),
        ("lichess", "kingOfTheHill", "kingofthehill", True),
        ("lichess", "unknownVariant", "unknownvariant", False),
        ("chess.com", None, "standard", False),
    ],
)
def test_map_variant(provider: str, native: str | None, expected: str, mapped: bool) -> None:
    assert map_variant(provider, native) == (expected, mapped)


@pytest.mark.parametrize(
    ("white", "black", "outcome", "is_live"),
    [
        ("win", "checkmated", "white_win", False),
        ("timeout", "win", "black_win", False),
        ("agreed", "agreed", "draw", False),
        ("none", "", None, True),
        ("resigned", "checkmated", None, False),
    ],
)
def test_chesscom_outcome(white: str, black: str, outcome: str | None, is_live: bool) -> None:
    assert chesscom_outcome(white, black) == (outcome, is_live)


@pytest.mark.parametrize(
    ("winner", "status", "outcome", "is_live"),
    [
        ("white", "resign", "white_win", False),
        ("black", "mate", "black_win", False),
        (None, "draw", "draw", False),
        (None, "started", None, True),
        (None, "aborted", None, False),
        (None, "unknown", None, False),
    ],
)
def test_lichess_outcome(winner: str | None, status: str, outcome: str | None, is_live: bool) -> None:
    assert lichess_outcome(winner, status) == (outcome, is_live)
