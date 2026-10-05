"""Configuration value objects for provider clients and CLI runtime settings."""

from __future__ import annotations

import os
from dataclasses import dataclass

from chess_crawl import __version__


DEFAULT_CONTACT = "set-me@example.invalid"


@dataclass(frozen=True)
class ProviderSettings:
    key: str
    min_delay_s: float
    user_agent: str
    oauth_token: str | None = None
    max_retries: int = 3
    include_clocks: bool = True
    include_evals: bool = True
    include_accuracy: bool = True
    oauth_owner_scope: str = "local"

    def __post_init__(self) -> None:
        for name in ("include_clocks", "include_evals", "include_accuracy"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a boolean")


@dataclass(frozen=True)
class Config:
    contact: str = DEFAULT_CONTACT
    user_agent: str | None = None
    lichess_token: str | None = None
    chesscom_delay_s: float = 1.0
    lichess_delay_s: float = 1.5
    max_retries: int = 3
    lichess_clocks: bool = True
    lichess_evals: bool = True
    lichess_accuracy: bool = True
    lichess_token_owner_scope: str = "local"

    def __post_init__(self) -> None:
        for name in ("lichess_clocks", "lichess_evals", "lichess_accuracy"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a boolean")

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            contact=os.getenv("CHESS_CRAWL_CONTACT", DEFAULT_CONTACT),
            user_agent=os.getenv("CHESS_CRAWL_USER_AGENT"),
            lichess_token=os.getenv("CHESS_CRAWL_LICHESS_TOKEN"),
            lichess_token_owner_scope=os.getenv("CHESS_CRAWL_LICHESS_TOKEN_OWNER_SCOPE", "local"),
            lichess_clocks=_boolean_from_env("CHESS_CRAWL_LICHESS_CLOCKS", default=True),
            lichess_evals=_boolean_from_env("CHESS_CRAWL_LICHESS_EVALS", default=True),
            lichess_accuracy=_boolean_from_env("CHESS_CRAWL_LICHESS_ACCURACY", default=True),
        )

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


def _boolean_from_env(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true/false, 1/0, yes/no, or on/off")
