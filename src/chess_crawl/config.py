"""Configuration value objects for provider clients and CLI runtime settings."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from chess_crawl import __version__
from chess_crawl.settings import SettingsSource, dataclass_settings


DEFAULT_CONTACT = "set-me@example.invalid"


@dataclass(frozen=True)
class ProviderSettings:
    key: str
    min_delay_s: float
    user_agent: str
    oauth_token: str | None = field(default=None, repr=False)
    max_retries: int = 3
    include_clocks: bool = True
    include_evals: bool = True
    include_accuracy: bool = True
    oauth_owner_scope: str = "local"

    def __post_init__(self) -> None:
        _validate_delay(self.min_delay_s, "min_delay_s")
        _validate_retries(self.max_retries)
        for name in ("include_clocks", "include_evals", "include_accuracy"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a boolean")


@dataclass(frozen=True)
class Config:
    contact: str = DEFAULT_CONTACT
    user_agent: str | None = None
    lichess_token: str | None = field(default=None, repr=False)
    chesscom_delay_s: float = 1.0
    lichess_delay_s: float = 1.5
    max_retries: int = 3
    lichess_clocks: bool = True
    lichess_evals: bool = True
    lichess_accuracy: bool = True
    lichess_token_owner_scope: str = "local"

    def __post_init__(self) -> None:
        _validate_delay(self.chesscom_delay_s, "chesscom_delay_s")
        _validate_delay(self.lichess_delay_s, "lichess_delay_s")
        _validate_retries(self.max_retries)
        for name in ("lichess_clocks", "lichess_evals", "lichess_accuracy"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a boolean")

    @classmethod
    def from_env(cls, *, source: SettingsSource | None = None) -> "Config":
        return dataclass_settings(cls, source=source, aliases={"max_retries": "PROVIDER_MAX_RETRIES"})

    def provider(self, key: str) -> ProviderSettings:
        if key == "chess.com":
            delay = self.chesscom_delay_s
            token = None
        elif key == "lichess":
            delay = self.lichess_delay_s
            token = self.lichess_token
        else:
            raise KeyError(f"Unknown provider: {key}")

        return ProviderSettings(
            key=key,
            min_delay_s=delay,
            user_agent=self.user_agent or build_user_agent(self.contact),
            oauth_token=token,
            max_retries=self.max_retries,
            include_clocks=self.lichess_clocks,
            include_evals=self.lichess_evals,
            include_accuracy=self.lichess_accuracy,
            oauth_owner_scope=self.lichess_token_owner_scope,
        )


def build_user_agent(contact: str = DEFAULT_CONTACT) -> str:
    return f"chess-crawl/{__version__} (+contact: {contact})"


def _validate_delay(value: float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite and nonnegative")


def _validate_retries(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("max_retries must be a nonnegative integer")
