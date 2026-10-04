"""Immutable compressed object adapters; no database or credentials in references."""
from __future__ import annotations

import base64
import hashlib
import importlib
import os
import re
import tempfile
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol


class ObjectStore(Protocol):
    @property
    def backend(self) -> str: ...

    @property
    def location(self) -> str: ...

    def put(self, key: str, body: bytes) -> None: ...

    def read(self, key: str, *, expected_size: int) -> bytes: ...


def digest(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def object_key(body_hash: str) -> str:
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", body_hash):
        raise ValueError("Invalid archive body hash")
    value = body_hash.removeprefix("sha256:")
    return f"sha256/{value[:2]}/{value}.gz"


def _validate_key(key: str) -> None:
    if not re.fullmatch(r"(?:[A-Za-z0-9_-]+/)*sha256/[0-9a-f]{2}/[0-9a-f]{64}\.gz", key):
        raise ValueError("Invalid archive object key")


@dataclass(frozen=True)
class LocalObjectStore:
    location: str
    backend: str = "local"

    def __post_init__(self) -> None:
        object.__setattr__(self, "location", str(Path(self.location).expanduser().resolve()))

    def _path(self, key: str) -> Path:
        _validate_key(key)
        root = Path(self.location)
        path = root / key
        if not path.resolve().is_relative_to(root):
            raise ValueError("Archive object path escapes its configured directory")
        return path

    def put(self, key: str, body: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Hard-link publication is atomic and refuses to replace an existing object.
        fd, temporary = tempfile.mkstemp(prefix=".archive-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if self.read(key, expected_size=len(body)) != body:
                    raise ValueError("Existing archive object differs from supplied bytes") from None
            if os.name == "posix":
                # Sync even a reused object: another writer may have published
                # its link but not yet synced the directory before we commit.
                root_parent = Path(self.location).parent
                for directory in (path.parent, *path.parent.parents):
                    directory_fd = os.open(directory, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                    if directory == root_parent:
                        break
        finally:
            Path(temporary).unlink(missing_ok=True)

    def read(self, key: str, *, expected_size: int) -> bytes:
        path = self._path(key)
        if path.is_symlink():
            raise ValueError("Archive objects must not be symbolic links")
        with path.open("rb") as stream:
            body = stream.read(expected_size + 1)
        if len(body) != expected_size:
            raise ValueError("Archive object size mismatch")
        return body


class S3ObjectStore:
    backend = "s3"

    def __init__(self, location: str, *, client: Any = None) -> None:
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", location):
            raise ValueError("Archive S3 location must be a bucket name")
        self.location = location
        if client is None:
            try:
                boto3 = importlib.import_module("boto3")
            except ModuleNotFoundError:
                raise RuntimeError("S3 archives require the chess-crawl[s3] extra") from None
            client = boto3.client("s3")
        self.client = client

    def put(self, key: str, body: bytes) -> None:
        _validate_key(key)
        # Boto3 uses its credential provider chain (including ECS/EC2 roles).
        # Conditional publication makes retries safe and never overwrites evidence.
        try:
            self.client.put_object(
                Bucket=self.location, Key=key, Body=body, IfNoneMatch="*",
                ContentType="application/gzip",
                ChecksumSHA256=base64.b64encode(hashlib.sha256(body).digest()).decode("ascii"),
            )
        except Exception as exc:
            response = getattr(exc, "response", {})
            code = response.get("Error", {}).get("Code")
            if code not in {"PreconditionFailed", "412"}:
                # A 409 remains a retryable failure; no DB reference is published.
                raise
            if self.read(key, expected_size=len(body)) != body:
                raise ValueError("Existing archive object differs from supplied bytes") from None

    def read(self, key: str, *, expected_size: int) -> bytes:
        _validate_key(key)
        response = self.client.get_object(Bucket=self.location, Key=key)
        stream = response["Body"]
        try:
            body = stream.read(expected_size + 1)
        finally:
            stream.close()
        if len(body) != expected_size:
            raise ValueError("Archive object size mismatch")
        return bytes(body)


def configured_store() -> ObjectStore | None:
    backend = os.getenv("CHESS_CRAWL_ARCHIVE_BACKEND", "database")
    if backend == "database":
        return None
    if backend == "local":
        directory = str(Path(os.getenv("CHESS_CRAWL_ARCHIVE_DIRECTORY", "data/archive")).expanduser().resolve())
        return store_for_reference("local", directory)
    if backend == "s3":
        return store_for_reference("s3", os.getenv("CHESS_CRAWL_ARCHIVE_S3_BUCKET", ""))
    raise ValueError("CHESS_CRAWL_ARCHIVE_BACKEND must be database, local, or s3")


@lru_cache(maxsize=16)
def store_for_reference(backend: str, location: str) -> ObjectStore:
    if backend == "local":
        return LocalObjectStore(location)
    if backend == "s3":
        return S3ObjectStore(location)
    raise ValueError("Unsupported archive object backend")
