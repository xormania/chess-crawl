"""Small container probes using only local API or shared storage contracts."""

from __future__ import annotations

import http.client
import sys
from pathlib import Path

from chess_crawl.jobs.worker_identity import local_worker_alive
from chess_crawl.settings import setting
from chess_crawl.storage.db import connection, database_url


def api_probe_token() -> str:
    mode = setting("CHESS_CRAWL_API_AUTH_MODE", "static")
    if mode not in {"static", "database"}:
        return ""
    token = setting("CHESS_CRAWL_HEALTHCHECK_TOKEN")
    token_file = setting("CHESS_CRAWL_HEALTHCHECK_TOKEN_FILE")
    if token is None and token_file is None:
        if mode != "static":
            return ""
        token = setting("CHESS_CRAWL_API_TOKEN", "")
        token_file = setting("CHESS_CRAWL_API_TOKEN_FILE")
    if token_file is not None:
        if token or not token_file:
            return ""
        token = Path(token_file).read_text(encoding="utf-8").strip()
    token = token or ""
    return "" if any(character.isspace() for character in token) else token


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    try:
        if mode == "api":
            token = api_probe_token()
            if not token:
                return 1
            client = http.client.HTTPConnection("127.0.0.1", 8000, timeout=3)
            try:
                client.request("GET", "/health/ready", headers={"Authorization": f"Bearer {token}"})
                return 0 if client.getresponse().status == 200 else 1
            finally:
                client.close()
        if mode == "worker":
            with connection(database_url()) as conn:
                identity = setting("CHESS_CRAWL_WORKER_IDENTITY_FILE")
                return 0 if identity and local_worker_alive(conn, identity) else 1
        return 1
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
