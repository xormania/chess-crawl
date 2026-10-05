"""Workspace-aware API clients and collection request builders."""
from __future__ import annotations

from fastapi.testclient import TestClient
from chess_crawl.api import create_app
from chess_crawl.jobs.budget import BudgetPolicy


TOKENS = {"alpha": "alpha-secret", "beta": "beta-secret"}


def client(database_url: str, workspace: str = "alpha") -> TestClient:
    return TestClient(create_app(database_url, workspace_tokens=TOKENS), headers={"Authorization":f"Bearer {TOKENS[workspace]}"})


FULL = {"provider":"lichess", "username":"alice", "max_games":2, "collection_mode":"full"}


def budget_client(archive: str, policy: BudgetPolicy, owner: str = "alpha") -> TestClient:
    return TestClient(create_app(archive, workspace_tokens=TOKENS, budget_policy=policy),
                      headers={"Authorization":f"Bearer {TOKENS[owner]}"})
