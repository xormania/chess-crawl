"""Exercise transport policy before any PostgreSQL connection is attempted."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from chess_crawl.storage import db


PASSWORD = "transport-test:password@with spaces"


class Session:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[Any, ...] | None]] = []
        self.closed = False

    def execute(self, query: str, parameters: tuple[Any, ...] | None = None) -> None:
        self.statements.append((query, parameters))

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        permit = getattr(self, "_session_permit", None)
        if permit is not None:
            permit.release()


class Backend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.session = Session()

    def connect(self, target: str, **options: Any) -> Session:
        self.calls.append((target, options))
        return self.session


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> Iterator[Backend]:
    # libpq inherits these independently of the application's settings. Each
    # case explicitly selects its environment rather than using the host's.
    for name in (
        "PGHOST", "PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGSSLMODE",
        "PGGSSENCMODE", "PGSSLROOTCERT", "PGPASSWORD",
        "CHESS_CRAWL_DATABASE_TRANSPORT", "CHESS_CRAWL_DATABASE_TRUSTED_HOST",
        "CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE", "CHESS_CRAWL_DATABASE_PASSWORD",
        "CHESS_CRAWL_DATABASE_PASSWORD_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    boundary = Backend()
    monkeypatch.setattr(db.Connection, "connect", staticmethod(boundary.connect))
    try:
        yield boundary
    finally:
        boundary.session.close()


@pytest.mark.parametrize("mode", ["ro", "rw", "rwc"])
@pytest.mark.parametrize("target", [
    "postgresql://postgres@database.example/archive?sslmode=disable&gssencmode=prefer",
    "dbname=archive host=database.example sslmode=allow gssencmode=require",
    "dbname=archive service=weak-service sslmode=prefer gssencmode=prefer",
])
def test_verified_default_overrides_weaker_url_environment_and_service_options_for_every_access_mode(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, mode: db.AccessMode, target: str,
) -> None:
    monkeypatch.setenv("PGSSLMODE", "disable")
    monkeypatch.setenv("PGGSSENCMODE", "require")
    monkeypatch.setenv("PGSERVICE", "weak-environment-service")

    connection = db.connect(target, mode=mode)

    assert connection is backend.session
    assert len(backend.calls) == 1
    passed_target, options = backend.calls[0]
    assert passed_target == target
    assert options["sslmode"] == "verify-full"
    assert options["gssencmode"] == "disable"
    assert options["autocommit"] is True
    assert 0 < options["connect_timeout"] <= 5
    assert backend.session.statements[0] == (
        "SELECT set_config('chess_crawl.events_enabled',%s,false)", ("true",),
    )
    assert (("SET default_transaction_read_only = on", None) in backend.session.statements) is (mode == "ro")


@pytest.mark.parametrize(("configured", "canonical"), [("off", "false"), ("yes", "true")])
def test_connection_applies_canonical_event_setting_to_its_session(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, configured: str, canonical: str,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_EVENTS_ENABLED", configured)
    db.connect("dbname=archive host=database.example")
    assert backend.session.statements[0] == (
        "SELECT set_config('chess_crawl.events_enabled',%s,false)", (canonical,),
    )


def test_verified_root_certificate_setting_overrides_other_ca_selections(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    certificate = tmp_path / "selected-ca.pem"
    certificate.write_text("test CA contents are interpreted by libpq")
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE", str(certificate))
    monkeypatch.setenv("PGSSLROOTCERT", "/unselected-environment-ca")

    db.connect("dbname=archive host=database.example sslrootcert=/unselected-url-ca")

    assert backend.calls[0][1]["sslrootcert"] == str(certificate)
    assert backend.calls[0][1]["sslmode"] == "verify-full"


@pytest.mark.parametrize("transport", ["", "disable", "prefer", "Verified", "local "])
def test_invalid_transport_policy_fails_before_backend_connection(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, transport: str,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", transport)
    with pytest.raises(ValueError, match="verified or local"):
        db.connect("dbname=archive")
    assert not backend.calls


@pytest.mark.parametrize("target", [
    "dbname=archive",
    "dbname=archive host=/var/run/postgresql",
    "dbname=archive host=@postgres-socket",
    "postgresql://postgres@127.0.0.1/archive",
    "dbname=archive host=127.0.0.2",
    "postgresql://postgres@[::1]/archive",
    "dbname=archive host=::ffff:127.0.0.1",
    "dbname=archive hostaddr=127.0.0.1",
    "dbname=archive host=untrusted.example hostaddr=127.0.0.1",
])
def test_explicit_local_transport_accepts_only_effective_unix_or_loopback_routes(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, target: str,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    db.connect(target)
    assert len(backend.calls) == 1
    assert backend.session.statements[:3] == [
        ("SELECT set_config('chess_crawl.events_enabled',%s,false)", ("true",)),
        ("SET TIME ZONE 'UTC'", None),
        ("SET lock_timeout = '5s'", None),
    ]


@pytest.mark.parametrize("host", ["localhost", "LOCALHOST"])
def test_localhost_exception_is_pinned_to_loopback_instead_of_dns(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, host: str,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    db.connect(f"dbname=archive host={host}")
    assert backend.calls[0][1]["hostaddr"] == "127.0.0.1"


@pytest.mark.parametrize("target", [
    "dbname=archive host=postgres",
    "dbname=archive host=database.example",
    "dbname=archive host=192.0.2.1",
    "dbname=archive host=::ffff:192.0.2.1",
    "dbname=archive hostaddr=192.0.2.1",
    "dbname=archive host=127.0.0.1 hostaddr=192.0.2.1",
    "dbname=archive host=localhost hostaddr=192.0.2.1",
    "dbname=archive host=/var/run/postgresql hostaddr=192.0.2.1",
    "dbname=archive host=127.0.0.1,database.example",
    "dbname=archive host=127.0.0.1,127.0.0.2",
    "dbname=archive hostaddr=127.0.0.1,127.0.0.2",
    "dbname=archive service=opaque",
])
def test_local_transport_rejects_external_or_opaque_routes_before_backend_connection(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, target: str,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    with pytest.raises(ValueError):
        db.connect(target)
    assert not backend.calls


@pytest.mark.parametrize("target", [
    "dbname=archive",
    "dbname=archive host='' hostaddr='' service=''",
    "postgresql:///archive?host=&hostaddr=&service=",
])
@pytest.mark.parametrize(("variable", "value"), [
    ("PGHOST", "database.example"),
    ("PGHOST", "127.0.0.1,127.0.0.2"),
    ("PGHOSTADDR", "192.0.2.1"),
    ("PGHOSTADDR", "127.0.0.1,127.0.0.2"),
    ("PGSERVICE", "opaque-environment-service"),
])
def test_empty_url_routing_options_cannot_hide_nonlocal_environment_fallbacks(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, target: str, variable: str, value: str,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    monkeypatch.setenv(variable, value)
    with pytest.raises(ValueError):
        db.connect(target)
    assert not backend.calls


def test_local_environment_loopback_is_resolved_and_pinned_before_backend_connection(
    backend: Backend, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    monkeypatch.setenv("PGHOST", "localhost")
    db.connect("dbname=archive host='' hostaddr=''")
    assert backend.calls[0][1]["host"] == "localhost"
    assert backend.calls[0][1]["hostaddr"] == "127.0.0.1"


def test_windows_implicit_localhost_uses_a_pinned_loopback_route(
    backend: Backend, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    # Restore os.name before pytest/pathlib inspect the host filesystem.
    with monkeypatch.context() as platform:
        platform.setattr(db.os, "name", "nt")
        db.connect("dbname=archive")
    assert backend.calls[0][1]["hostaddr"] == "127.0.0.1"


def test_explicit_loopback_route_overrides_environment_without_using_its_host_address(
    backend: Backend, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    monkeypatch.setenv("PGHOST", "database.example")
    monkeypatch.setenv("PGHOSTADDR", "192.0.2.1")
    db.connect("dbname=archive host=localhost hostaddr=127.0.0.1")
    assert backend.calls[0][1]["hostaddr"] == "127.0.0.1"


@pytest.mark.parametrize("host", ["postgres", "Postgres", "postgres.example"])
def test_bundled_host_trust_is_an_exact_explicit_operator_selection(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, host: str,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRUSTED_HOST", "postgres")
    if host == "postgres":
        db.connect(f"dbname=archive host={host}")
        assert backend.calls[0][1]["host"] == host
    else:
        with pytest.raises(ValueError):
            db.connect(f"dbname=archive host={host}")
        assert not backend.calls


@pytest.mark.parametrize("source", ["url", "environment"])
def test_trusted_host_does_not_allow_an_external_effective_host_address(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, source: str,
) -> None:
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRANSPORT", "local")
    monkeypatch.setenv("CHESS_CRAWL_DATABASE_TRUSTED_HOST", "postgres")
    target = "dbname=archive host=postgres"
    if source == "url":
        target += " hostaddr=192.0.2.1"
    else:
        monkeypatch.setenv("PGHOSTADDR", "192.0.2.1")
    with pytest.raises(ValueError):
        db.connect(target)
    assert not backend.calls


@pytest.mark.parametrize("source", ["environment", "file"])
def test_explicit_password_source_is_preserved_with_verified_transport(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, source: str,
) -> None:
    if source == "environment":
        monkeypatch.setenv("CHESS_CRAWL_DATABASE_PASSWORD", PASSWORD)
    else:
        password_file = tmp_path / "password"
        password_file.write_text(PASSWORD + "\r\n")
        monkeypatch.setenv("CHESS_CRAWL_DATABASE_PASSWORD_FILE", str(password_file))
    db.connect("dbname=archive host=database.example")
    options = backend.calls[0][1]
    assert options["password"] == PASSWORD
    assert options["sslmode"] == "verify-full"
    assert options["gssencmode"] == "disable"


def test_password_in_connection_settings_remains_available_to_libpq(
    backend: Backend,
) -> None:
    target = "dbname=archive host=database.example password='connection-password'"
    db.connect(target)
    assert backend.calls[0][0] == target
    assert "password" not in backend.calls[0][1]
    assert backend.calls[0][1]["sslmode"] == "verify-full"


@pytest.mark.parametrize("conflict", ["two-settings", "url-and-environment", "url-and-file"])
def test_conflicting_password_sources_fail_before_connection_without_echoing_values(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, conflict: str,
) -> None:
    password_file = tmp_path / "password"
    password_file.write_text(PASSWORD)
    target = "dbname=archive host=database.example"
    if conflict != "two-settings":
        target += " password='connection-password'"
    if conflict != "url-and-file":
        monkeypatch.setenv("CHESS_CRAWL_DATABASE_PASSWORD", PASSWORD)
    if conflict != "url-and-environment":
        monkeypatch.setenv("CHESS_CRAWL_DATABASE_PASSWORD_FILE", str(password_file))
    with pytest.raises(ValueError) as error:
        db.connect(target)
    assert PASSWORD not in str(error.value)
    assert "connection-password" not in str(error.value)
    assert not backend.calls
