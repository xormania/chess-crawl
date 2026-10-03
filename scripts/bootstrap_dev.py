"""Create persistent local development credentials without printing their values.

The scoped JWTs deliberately have no expiry for local development. Deployment
credentials and end-user authorization are managed by the deploying application.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import secrets
import tempfile
from pathlib import Path
from urllib.parse import urlsplit


def _encoded(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _jwt(signing_key: str, permission: str, topics: list[str]) -> str:
    header = _encoded(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _encoded(json.dumps({"mercure": {permission: topics}}, separators=(",", ":")).encode())
    message = f"{header}.{payload}"
    signature = hmac.new(signing_key.encode(), message.encode("ascii"), hashlib.sha256).digest()
    return f"{message}.{_encoded(signature)}"


def _write_secret(path: Path, value: str) -> None:
    # Directory access is restricted on the host. Files must be readable by
    # container UID 10001 because local Compose file secrets use bind mounts.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            output.write(value + "\n")
            output.flush()
            temporary.chmod(0o444)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def _persistent_secret(path: Path) -> str:
    if path.exists():
        value = path.read_text(encoding="utf-8").strip()
        if len(value) < 32 or not value.isascii() or any(character.isspace() for character in value):
            raise ValueError(f"Existing secret is invalid: {path.name}")
        path.chmod(0o444)
        return value
    value = secrets.token_urlsafe(48)
    _write_secret(path, value)
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare credentials for the local Compose stack")
    parser.add_argument("--directory", type=Path, default=Path(os.getenv("CHESS_CRAWL_SECRETS_DIR", "data/dev-secrets")))
    parser.add_argument("--topic-prefix", default=os.getenv("CHESS_CRAWL_MERCURE_TOPIC_PREFIX", "https://chess-crawl.local"))
    args = parser.parse_args()
    prefix = args.topic_prefix.rstrip("/")
    url = urlsplit(prefix)
    if (
        url.scheme not in {"http", "https"} or not url.netloc or url.username is not None
        or url.password is not None or url.query or url.fragment or any(character.isspace() for character in prefix)
    ):
        parser.error("--topic-prefix must be an HTTP(S) URL without credentials, query, or fragment")
    args.directory.mkdir(parents=True, exist_ok=True)
    args.directory.chmod(0o700)
    _persistent_secret(args.directory / "api_token")
    key = _persistent_secret(args.directory / "mercure_signing_key")
    topics = [f"{prefix}/jobs/{{id}}", f"{prefix}/runs/{{id}}"]
    _write_secret(args.directory / "mercure_publisher_jwt", _jwt(key, "publish", topics))
    _write_secret(args.directory / "mercure_subscriber_jwt", _jwt(key, "subscribe", topics))
    print(f"Development credentials ready in {args.directory}; existing API and signing keys were retained.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
