from __future__ import annotations

import pytest

from chess_crawl.config import Config, ProviderSettings, build_user_agent
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


def test_lichess_capture_defaults_and_independent_environment_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    defaults = Config.from_env().provider("lichess")
    assert (defaults.include_clocks, defaults.include_evals, defaults.include_accuracy) == (True, True, True)
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_CLOCKS", "false")
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_EVALS", "true")
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_ACCURACY", "0")
    configured = Config.from_env().provider("lichess")
    assert (configured.include_clocks, configured.include_evals, configured.include_accuracy) == (False, True, False)


@pytest.mark.parametrize(("value", "expected"), [
    ("1", True), (" TRUE ", True), ("yes", True), ("on", True),
    ("0", False), ("False", False), ("NO", False), ("off", False),
])
def test_lichess_capture_environment_boolean_values(monkeypatch: pytest.MonkeyPatch, value: str, expected: bool) -> None:
    monkeypatch.setenv("CHESS_CRAWL_LICHESS_CLOCKS", value)
    assert Config.from_env().provider("lichess").include_clocks is expected


@pytest.mark.parametrize("field", ["clocks", "evals", "accuracy"])
def test_invalid_capture_configuration_is_rejected_before_client_creation(monkeypatch: pytest.MonkeyPatch, field: str) -> None:
    name = f"CHESS_CRAWL_LICHESS_{field.upper()}"
    monkeypatch.setenv(name, "maybe")
    with pytest.raises(ValueError, match=name):
        Config.from_env()
    with pytest.raises(ValueError, match=f"lichess_{field}"):
        Config(**{f"lichess_{field}": "false"})  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=f"include_{field}"):
        ProviderSettings("lichess", 0, "test", **{f"include_{field}": "false"})  # type: ignore[arg-type]


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
        ("none", "", None, False),
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
