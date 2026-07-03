"""Configuration value objects for provider clients.

Phase 1 does not load config files or perform HTTP requests. These dataclasses
define the provider-neutral settings seam used by later phases.
"""

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
    default_page_max: int | None = None


@dataclass(frozen=True)
class Config:
    contact: str = DEFAULT_CONTACT
    user_agent: str | None = None
    lichess_token: str | None = None
    chesscom_delay_s: float = 1.0
    lichess_delay_s: float = 1.5
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            contact=os.getenv("CHESS_CRAWL_CONTACT", DEFAULT_CONTACT),
            user_agent=os.getenv("CHESS_CRAWL_USER_AGENT"),
            lichess_token=os.getenv("CHESS_CRAWL_LICHESS_TOKEN"),
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
        )


def build_user_agent(contact: str = DEFAULT_CONTACT) -> str:
    return f"chess-crawl/{__version__} (+contact: {contact})"
