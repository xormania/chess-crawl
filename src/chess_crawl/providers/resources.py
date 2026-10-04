"""Catalog of supported player resources and their acquisition contracts.

Collectors accept registered keys rather than arbitrary URLs. New adapters can
register provider-specific resources without changing this dispatch contract.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal
from urllib.parse import quote

from chess_crawl.normalize.codes import canonical_hash


Authentication = Literal["none", "optional", "required"]


@dataclass(frozen=True)
class PlayerResource:
    provider: str
    key: str
    endpoint_template: str
    authentication: Authentication
    pagination: str
    freshness: str
    coverage: str
    docs_url: str
    parameter: str | None = None
    allowed_values: tuple[str, ...] = ()
    access_scope: Literal["public", "workspace"] = "public"
    oauth_scopes: tuple[str, ...] = ()

    def parameters(self, values: dict[str, Any] | None = None) -> dict[str, str]:
        values = values or {}
        if self.parameter is None:
            if values:
                raise ValueError(f"resource {self.key} does not accept parameters")
            return {}
        if set(values) != {self.parameter}:
            raise ValueError(f"resource {self.key} requires only {self.parameter}")
        value = values[self.parameter]
        if not isinstance(value, str) or value not in self.allowed_values:
            raise ValueError(f"unsupported {self.parameter} for resource {self.key}")
        return {self.parameter: value}

    def url(self, username: str, values: dict[str, Any] | None = None) -> str:
        normalized = username.strip().lower()
        if not normalized:
            raise ValueError("resource username cannot be empty")
        return self.endpoint_template.format(username=quote(normalized, safe=""), **self.parameters(values))


_CHESSCOM_DOCS = "https://www.chess.com/news/view/published-data-api"
_LICHESS_DOCS = "https://github.com/lichess-org/api/blob/master/doc/specs/tags"
_PERFS = ("ultraBullet", "bullet", "blitz", "rapid", "classical", "correspondence", "chess960", "crazyhouse",
          "antichess", "atomic", "horde", "kingOfTheHill", "racingKings", "threeCheck")

_RESOURCES: dict[tuple[str, str], PlayerResource] = {}


def register_resource(resource: PlayerResource) -> None:
    """Adapter registration; duplicate keys fail instead of silently replacing contracts."""
    key = (resource.provider, resource.key)
    if key in _RESOURCES:
        raise ValueError(f"resource already registered: {resource.provider}/{resource.key}")
    _RESOURCES[key] = resource


for _key, _suffix, _coverage in (
    ("clubs", "clubs", "Current memberships with supplied join/activity dates; no removed memberships."),
    ("matches", "matches", "Provider-listed registered, in-progress, and finished team matches."),
    ("tournaments", "tournaments", "Provider-listed registered, in-progress, and finished tournaments."),
    ("online", "is-online", "Transient online observation; no historical activity reconstruction."),
):
    register_resource(PlayerResource(
        provider="chess.com", key=_key,
        endpoint_template=f"https://api.chess.com/pub/player/{{username}}/{_suffix}",
        authentication="none", pagination="none",
        freshness="Respect response Cache-Control and conditional ETag/Last-Modified validators.",
        coverage=_coverage, docs_url=_CHESSCOM_DOCS,
    ))

for _key, _suffix, _authentication, _coverage, _spec in (
    ("rating-history", "user/{username}/rating-history", "optional",
     "At most one point/day/performance; empty unauthenticated response may mean no cached history.",
     "users/api-user-username-rating-history.yaml"),
    ("activity", "user/{username}/activity", "none",
     "Provider activity feed window; not a guaranteed complete lifetime activity history.",
     "users/api-user-username-activity.yaml"),
    ("teams", "team/of/{username}", "required",
     "Hidden member lists appear only when the authenticated caller belongs to that team.",
     "teams/api-team-of-username.yaml"),
):
    register_resource(PlayerResource(
        provider="lichess", key=_key, endpoint_template=f"https://lichess.org/api/{_suffix}",
        authentication=_authentication,  # type: ignore[arg-type]
        pagination="none", freshness="Refresh is explicit; respect serial access and wait 60 seconds after HTTP 429.",
        coverage=_coverage, docs_url=f"{_LICHESS_DOCS}/{_spec}",
        access_scope="workspace" if _key == "teams" else "public",
        oauth_scopes=("team:read",) if _key == "teams" else (),
    ))

register_resource(PlayerResource(
    provider="lichess", key="performance", endpoint_template="https://lichess.org/api/user/{username}/perf/{perf}",
    authentication="none", pagination="none",
    freshness="Refresh is explicit; respect serial access and wait 60 seconds after HTTP 429.",
    coverage="Statistics for one performance; missing records remain unknown.",
    docs_url=f"{_LICHESS_DOCS}/users/api-user-username-perf-perf.yaml", parameter="perf", allowed_values=_PERFS,
))


def get_resource(provider: str, key: str) -> PlayerResource:
    try:
        return _RESOURCES[(provider, key)]
    except KeyError as exc:
        raise ValueError(f"unsupported player resource: {provider}/{key}") from exc


def list_resources(provider: str | None = None) -> list[dict[str, Any]]:
    return [asdict(resource) for key, resource in sorted(_RESOURCES.items()) if provider is None or key[0] == provider]


def resource_owner_scope(resource: PlayerResource, owner_scope: str) -> str:
    if not isinstance(owner_scope, str) or not owner_scope or len(owner_scope) > 200:
        raise ValueError("resource owner scope must contain 1..200 characters")
    if resource.access_scope == "workspace" and owner_scope == "public":
        raise ValueError(f"resource {resource.key} requires a workspace owner scope")
    return owner_scope if resource.access_scope == "workspace" else "public"


def resource_source_key(
    provider: str, username: str, key: str, parameters: dict[str, Any] | None = None, *, owner_scope: str = "public",
    authenticated: bool = False,
) -> str:
    resource = get_resource(provider, key)
    values = resource.parameters(parameters)
    scope = resource_owner_scope(resource, owner_scope)
    normalized = username.strip().lower()
    if not normalized:
        raise ValueError("resource username cannot be empty")
    suffix = "" if not values else f"/{canonical_hash(values)}"
    # Username encoding prevents source-key separators from changing identity.
    scope_suffix = "" if scope == "public" else f"/scope/{quote(scope, safe='')}"
    # Optional OAuth can generate rating history rather than return an empty
    # cache miss. Keep those observations separate even if their bodies match.
    auth_suffix = "/authenticated" if resource.authentication == "optional" and authenticated else ""
    return f"{provider}/player/{quote(normalized, safe='')}/resources/{key}{suffix}{scope_suffix}{auth_suffix}"
