"""Service authentication adapters: local static secrets or shared credentials."""
from __future__ import annotations

import json
import secrets
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from chess_crawl.settings import setting
from chess_crawl.storage import workspaces
from chess_crawl.storage.db import connection
from chess_crawl.storage.workspace_access import authenticate_token, valid_credential_token


@dataclass(frozen=True)
class WorkspaceAuthenticator:
    archive: str = field(repr=False)
    credentials: Mapping[str, str] | None = field(repr=False)

    def resolve(self, supplied: str) -> str | None:
        if self.credentials is None:
            if not valid_credential_token(supplied):
                return None
            with connection(self.archive) as conn:
                return authenticate_token(conn, supplied)
        workspace_id = None
        for candidate, credential in self.credentials.items():
            if secrets.compare_digest(supplied.encode("utf-8"), credential.encode("utf-8")):
                workspace_id = candidate
        return workspace_id


def configured_authenticator(
    archive: str, api_token: str | None, workspace_tokens: Mapping[str, str] | None,
    auth_mode: str | None,
) -> WorkspaceAuthenticator:
    mode = auth_mode if auth_mode is not None else setting("CHESS_CRAWL_API_AUTH_MODE", "static")
    if mode not in {"static", "database"}:
        raise ValueError("CHESS_CRAWL_API_AUTH_MODE must be static or database")
    token = api_token if api_token is not None else setting("CHESS_CRAWL_API_TOKEN", "")
    token_file = setting("CHESS_CRAWL_API_TOKEN_FILE")
    workspace_file = setting("CHESS_CRAWL_API_WORKSPACE_TOKENS_FILE")
    if mode == "database":
        if token or token_file or workspace_file or workspace_tokens is not None:
            raise ValueError("Database authentication cannot be combined with static API credentials")
        return WorkspaceAuthenticator(archive, None)
    if token and token_file:
        raise ValueError("Set either CHESS_CRAWL_API_TOKEN or CHESS_CRAWL_API_TOKEN_FILE, not both")
    if token_file:
        token = Path(token_file).read_text(encoding="utf-8").strip()
    if workspace_tokens is not None and workspace_file:
        raise ValueError("Set workspace_tokens or CHESS_CRAWL_API_WORKSPACE_TOKENS_FILE, not both")
    if workspace_file:
        loaded = json.loads(Path(workspace_file).read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("Workspace credentials must be a JSON object")
        workspace_tokens = loaded
    if workspace_tokens is not None and (token or token_file):
        raise ValueError("Set a single API token or workspace tokens, not both")
    if workspace_tokens is None and (not token or not token.strip()):
        raise ValueError("CHESS_CRAWL_API_TOKEN must be set before starting the HTTP API")
    if any(character.isspace() for character in token):
        raise ValueError("The HTTP API bearer token must not contain whitespace")
    credentials = dict(workspace_tokens) if workspace_tokens is not None else {"local": token}
    if not credentials:
        raise ValueError("At least one API workspace credential is required")
    for workspace_id, credential in credentials.items():
        workspaces.validate_workspace(workspace_id)
        if not isinstance(credential, str) or not credential or any(char.isspace() for char in credential):
            raise ValueError("Workspace bearer tokens must be nonempty strings without whitespace")
    if len(set(credentials.values())) != len(credentials):
        raise ValueError("Workspace bearer tokens must be unique")
    return WorkspaceAuthenticator(archive, credentials)
