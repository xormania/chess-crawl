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
            token = Path(os.environ["CHESS_CRAWL_API_TOKEN_FILE"]).read_text().strip()
            client = http.client.HTTPConnection("127.0.0.1", 8000, timeout=3)
            try:
                client.request("GET", "/health/ready", headers={"Authorization": f"Bearer {token}"})
                return 0 if client.getresponse().status == 200 else 1
            finally:
                client.close()
        if mode == "worker":
            with connection(os.environ["CHESS_CRAWL_DB"]) as conn:
                return 0 if worker_status(conn)["alive"] else 1
        return 1
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
