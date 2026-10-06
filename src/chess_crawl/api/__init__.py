"""Optional HTTP interface to the shared archive application services."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from chess_crawl.api.app import create_app

__all__ = ["create_app"]


def __getattr__(name: str) -> Any:
    if name == "create_app":
        from chess_crawl.api.app import create_app
        return create_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
