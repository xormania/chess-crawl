"""Reviewable deployment boundaries: private transport, retained data, zero startup."""
from __future__ import annotations

import hashlib
import importlib.util
from importlib import import_module
import json
import ssl
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
yaml = import_module("yaml")


def template():
    return json.loads((ROOT / "deploy/aws/template.json").read_text())


def test_task_size_rule_rejects_unsupported_fargate_pairs() -> None:
    value = template()
    assertions = value["Rules"]["ValidTaskSize"]["Assertions"]
    # AWS Fargate sizing, restricted to the template's exposed parameter values:
    # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html
    supported = {
        "512": {"1024", "2048", "4096"},
        "1024": {"2048", "4096", "8192"},
        "2048": {"4096", "8192"},
        "4096": {"8192"},
    }

    def evaluate(expression, parameters):
        if not isinstance(expression, dict):
            return expression
        operator, operands = next(iter(expression.items()))
        if operator == "Ref":
            return parameters[operands]
        if operator == "Fn::Equals":
            return evaluate(operands[0], parameters) == evaluate(operands[1], parameters)
        if operator == "Fn::Contains":
            return evaluate(operands[1], parameters) in evaluate(operands[0], parameters)
        if operator == "Fn::And":
            return all(evaluate(operand, parameters) for operand in operands)
        if operator == "Fn::Or":
            return any(evaluate(operand, parameters) for operand in operands)
        raise AssertionError(f"Unsupported rule operator: {operator}")

    for cpu in value["Parameters"]["TaskCpu"]["AllowedValues"]:
        for memory in value["Parameters"]["TaskMemory"]["AllowedValues"]:
            parameters = {"TaskCpu": cpu, "TaskMemory": memory}
            accepted = all(evaluate(assertion["Assert"], parameters) for assertion in assertions)
            assert accepted is (memory in supported[cpu]), parameters


def test_cloud_starts_without_runtime_tasks_and_keeps_data_protected() -> None:
    value = template()
    resources = value["Resources"]
    for name in ("ApiDesiredCount", "WorkerDesiredCount", "DispatcherDesiredCount"):
        assert value["Parameters"][name]["Default"] == 0
    db = resources["Database"]
    assert db["DeletionPolicy"] == db["UpdateReplacePolicy"] == "Snapshot"
    assert db["Properties"]["StorageEncrypted"] is True
    assert db["Properties"]["PubliclyAccessible"] is False
    assert db["Properties"]["ManageMasterUserPassword"] is True
    assert "MasterUserPassword" not in db["Properties"]
    assert resources["ArchiveBucket"]["DeletionPolicy"] == "Retain"
    assert all(resources["ArchiveBucket"]["Properties"]["PublicAccessBlockConfiguration"].values())
    assert resources["ArchiveBucket"]["Properties"]["VersioningConfiguration"]["Status"] == "Enabled"
    assert resources["WorkQueue"]["Properties"]["RedrivePolicy"]["deadLetterTargetArn"] == {
        "Fn::GetAtt": ["DeadLetterQueue", "Arn"],
    }


def test_runtime_has_no_master_secret_or_plaintext_database_password() -> None:
    resources = template()["Resources"]
    for name in ("ApiTask", "WorkerTask", "DispatcherTask"):
        container = resources[name]["Properties"]["ContainerDefinitions"][0]
        assert container["User"] == "10001:10001" and container["ReadonlyRootFilesystem"] is True
        environment = {item["Name"]: item["Value"] for item in container["Environment"]}
        assert environment["CHESS_CRAWL_DATABASE_TRANSPORT"] == "verified"
        assert environment["CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE"] == "/app/docker/rds-ca-bundle.pem"
        assert "CHESS_CRAWL_DATABASE_PASSWORD" not in environment
        assert "archive_admin" not in json.dumps(container)
        assert "MasterUserSecret" not in json.dumps(container)
    assert resources["LoadBalancer"]["Properties"]["Scheme"] == "internal"
    ingress = resources["LoadBalancerSecurityGroup"]["Properties"]["SecurityGroupIngress"]
    assert len(ingress) == 1 and ingress[0]["FromPort"] == ingress[0]["ToPort"] == 443
    assert "CidrIp" not in ingress[0]
    assert resources["ApiListener"]["Properties"]["Protocol"] == "HTTPS"


def test_local_archive_shared_volume_gates_workers_and_is_readonly_for_api() -> None:
    config = yaml.safe_load((ROOT / "compose.yaml").read_text())
    services = config["services"]
    initializer = services["archive-init"]
    assert initializer["user"] == "0:0" and initializer["cap_add"] == ["CHOWN", "FOWNER"]
    assert initializer["read_only"] is True
    for name in ("api", "worker"):
        service = services[name]
        assert service["environment"]["CHESS_CRAWL_ARCHIVE_BACKEND"] == "local"
        assert service["depends_on"]["archive-init"]["condition"] == "service_completed_successfully"
    assert services["api"]["volumes"] == ["archive_data:/var/lib/chess-crawl/archive:ro"]
    assert services["worker"]["volumes"] == ["archive_data:/var/lib/chess-crawl/archive"]


def test_archive_initializer_changes_only_mount_root(tmp_path, monkeypatch) -> None:
    spec = importlib.util.spec_from_file_location("archive_init", ROOT / "docker/archive_init.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    archived = tmp_path / "body.gz"
    archived.write_bytes(b"unchanged evidence")
    observed = []
    monkeypatch.setattr(module.os, "chown", lambda path, uid, gid: observed.append((path, uid, gid)))
    module.prepare_archive(tmp_path)
    assert observed == [(tmp_path, 10001, 10001)]
    assert archived.read_bytes() == b"unchanged evidence"
    assert tmp_path.stat().st_mode & 0o777 == 0o700


def test_rds_ca_bundle_is_pinned_and_loads_as_trust_roots() -> None:
    metadata = json.loads((ROOT / "docker/rds-ca-bundle.json").read_text())
    bundle = ROOT / "docker/rds-ca-bundle.pem"
    assert hashlib.sha256(bundle.read_bytes()).hexdigest() == metadata["sha256"]
    assert metadata["source_url"] == "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
    context = ssl.create_default_context(cafile=str(bundle))
    assert context.cert_store_stats()["x509_ca"] > 0


def test_container_image_contains_s3_sdk_and_archive_bootstrap() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert dockerfile.count("--extra s3") == 2
    assert "COPY docker/archive_init.py /app/docker/archive_init.py" in dockerfile
    assert "COPY docker/rds-ca-bundle.pem /app/docker/rds-ca-bundle.pem" in dockerfile
