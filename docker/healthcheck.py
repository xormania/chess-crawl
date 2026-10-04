"""Small container probes using only local API or shared storage contracts."""

from __future__ import annotations

import http.client
import os
import sys
from pathlib import Path

from chess_crawl.jobs.state import worker_status
from chess_crawl.storage.db import connection


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    try:
        if mode == "api":
            token = os.getenv("CHESS_CRAWL_API_TOKEN", "")
            token_file = os.getenv("CHESS_CRAWL_API_TOKEN_FILE")
            if token_file:
                if token:
                    return 1
                token = Path(token_file).read_text().strip()
            if not token:
                return 1
            client = http.client.HTTPConnection("127.0.0.1", 8000, timeout=3)
            try:
                client.request("GET", "/health/ready", headers={"Authorization": f"Bearer {token}"})
                return 0 if client.getresponse().status == 200 else 1
            finally:
                client.close()
        if mode == "worker":
            with connection(os.environ["CHESS_CRAWL_DATABASE_URL"]) as conn:
                return 0 if worker_status(conn)["alive"] else 1
        return 1
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
