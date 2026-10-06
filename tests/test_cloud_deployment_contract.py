"""Reviewable deployment boundaries: private transport, retained data, zero startup."""
from __future__ import annotations

import hashlib
import importlib.util
from importlib import import_module
import json
import ssl
from pathlib import Path

import pytest

from chess_crawl.api.exports import ExportLimits

ROOT = Path(__file__).resolve().parents[1]
yaml = import_module("yaml")


def template():
    return json.loads((ROOT / "deploy/aws/template.json").read_text())


@pytest.mark.parametrize("mode", ["static", "database"])
def test_cloud_auth_mode_injects_the_correct_service_credential(mode, monkeypatch) -> None:
    from chess_crawl.api.auth import configured_authenticator
    value = template()
    parameter = value["Parameters"]["ApiAuthMode"]
    assert parameter["Default"] == "static" and mode in parameter["AllowedValues"]
    equals = value["Conditions"]["DatabaseApiAuth"]["Fn::Equals"]
    parameters = {"ApiAuthMode": mode}
    database_mode = parameters[equals[0]["Ref"]] == equals[1]
    api = value["Resources"]["ApiTask"]["Properties"]["ContainerDefinitions"][0]
    environment = {item["Name"]: item["Value"] for item in api["Environment"]}
    assert parameters[environment["CHESS_CRAWL_API_AUTH_MODE"]["Ref"]] == mode
    assert not any(name.startswith("CHESS_CRAWL_API_TOKEN") for name in environment)
    secret = next(item for item in api["Secrets"] if item["ValueFrom"] == {"Ref": "ApiTokenSecretArn"})
    condition, enabled_name, disabled_name = secret["Name"]["Fn::If"]
    assert condition == "DatabaseApiAuth"
    supplied_name = enabled_name if database_mode else disabled_name
    assert supplied_name == ("CHESS_CRAWL_HEALTHCHECK_TOKEN" if mode == "database" else "CHESS_CRAWL_API_TOKEN")
    monkeypatch.setenv("CHESS_CRAWL_API_AUTH_MODE", mode)
    monkeypatch.setenv(supplied_name, "ccw_" + "a" * 43)
    auth = configured_authenticator("postgresql://postgres@localhost/chess_crawl", None, None, None)
    assert auth.credentials == (None if mode == "database" else {"local": "ccw_" + "a" * 43})


def test_task_size_rule_rejects_unsupported_fargate_pairs() -> None:
    value = template()
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

    for role in ("Api", "Acquisition", "Processing", "Dispatcher", "Migration"):
        assertions = value["Rules"][f"Valid{role}TaskSize"]["Assertions"]
        for cpu in value["Parameters"][f"{role}Cpu"]["AllowedValues"]:
            for memory in value["Parameters"][f"{role}Memory"]["AllowedValues"]:
                parameters = {f"{role}Cpu": cpu, f"{role}Memory": memory}
                accepted = all(evaluate(assertion["Assert"], parameters) for assertion in assertions)
                assert accepted is (memory in supported[cpu]), parameters


def test_cloud_starts_without_runtime_tasks_and_keeps_data_protected() -> None:
    value = template()
    resources = value["Resources"]
    for name in ("ApiDesiredCount", "AcquisitionDesiredCount", "ProcessingDesiredCount", "DispatcherDesiredCount"):
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
    for stage in ("Acquisition", "Processing"):
        assert resources[f"{stage}Queue"]["Properties"]["RedrivePolicy"]["deadLetterTargetArn"] == {
            "Fn::GetAtt": [f"{stage}DeadLetterQueue", "Arn"],
        }
    policy = resources["QueuePolicy"]["Properties"]
    queues = {stage + tail for stage in ("Acquisition", "Processing") for tail in ("Queue", "DeadLetterQueue")}
    assert {item["Ref"] for item in policy["Queues"]} == queues
    statement = policy["PolicyDocument"]["Statement"][0]
    assert statement["Effect"] == "Deny"
    assert statement["Condition"] == {"Bool": {"aws:SecureTransport": "false"}}
    assert {item["Fn::GetAtt"][0] for item in statement["Resource"]} == queues


def test_runtime_has_no_master_secret_or_plaintext_database_password() -> None:
    resources = template()["Resources"]
    for name in ("ApiTask", "AcquisitionTask", "ProcessingTask", "DispatcherTask"):
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


@pytest.mark.parametrize("name", ["ApiTask", "AcquisitionTask", "ProcessingTask", "DispatcherTask", "MigrationTask"])
def test_fargate_scratch_uses_supported_ephemeral_volume_with_application_permissions(name) -> None:
    props = template()["Resources"][name]["Properties"]
    assert props["RequiresCompatibilities"] == ["FARGATE"]
    container = props["ContainerDefinitions"][0]
    assert container["ReadonlyRootFilesystem"] is True
    assert not {"Tmpfs", "Devices", "SharedMemorySize"} & container["LinuxParameters"].keys()
    mounted = [item for item in container["MountPoints"] if item["ContainerPath"] == "/tmp"]
    assert len(mounted) == 1 and mounted[0]["ReadOnly"] is False
    volume = next(item for item in props["Volumes"] if item["Name"] == mounted[0]["SourceVolume"])
    # No host source, Docker-managed persistence, or EFS: scratch dies with the task.
    assert set(volume) == {"Name"}
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert 'VOLUME ["/tmp"]' in dockerfile
    assert "chown 10001:10001 /tmp" in dockerfile and "chmod 0700 /tmp" in dockerfile


@pytest.mark.parametrize("stage", ["Acquisition", "Processing"])
def test_cloud_worker_probes_its_own_process_and_database_heartbeat(stage) -> None:
    worker = template()["Resources"][f"{stage}Task"]["Properties"]["ContainerDefinitions"][0]
    probe = worker["HealthCheck"]
    assert probe["Command"] == ["CMD", "python", "/app/docker/healthcheck.py", "worker"]
    assert probe["StartPeriod"] >= 30 and probe["Timeout"] == 5 and probe["Retries"] >= 3
    assert "CHESS_CRAWL_WORKER_IDENTITY_FILE=/tmp/chess-crawl-worker.json" in (ROOT / "Dockerfile").read_text()


def test_api_idle_timeout_allows_export_preparation_before_headers() -> None:
    attributes = template()["Resources"]["LoadBalancer"]["Properties"]["LoadBalancerAttributes"]
    values = {item["Key"]: item["Value"] for item in attributes}
    assert int(values["idle_timeout.timeout_seconds"]) > ExportLimits().prepare_seconds


def test_local_archive_shared_volume_gates_workers_and_is_readonly_for_api() -> None:
    config = yaml.safe_load((ROOT / "compose.yaml").read_text())
    services = config["services"]
    initializer = services["archive-init"]
    assert initializer["user"] == "0:0" and initializer["cap_add"] == ["CHOWN", "FOWNER"]
    assert initializer["read_only"] is True
    for name in ("api", "worker"):
        service = services[name]
        assert service["environment"]["CHESS_CRAWL_ARCHIVE_BACKEND"] == "${CHESS_CRAWL_ARCHIVE_BACKEND:-local}"
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


def test_stage_tasks_route_hints_and_cannot_consume_each_others_queues() -> None:
    resources = template()["Resources"]
    for stage in ("Acquisition", "Processing"):
        task = resources[f"{stage}Task"]["Properties"]
        container = task["ContainerDefinitions"][0]
        assert container["Command"] == ["python", "-m", "chess_crawl.jobs.worker", "--stage", stage.lower()]
        environment = {item["Name"]: item["Value"] for item in container["Environment"]}
        assert environment[f"CHESS_CRAWL_SQS_{stage.upper()}_QUEUE_URL"] == {"Ref": f"{stage}Queue"}
        assert "CHESS_CRAWL_SQS_QUEUE_URL" not in environment
        statements = resources[f"{stage}Role"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        queue_permissions = [item for item in statements if "sqs:ReceiveMessage" in item["Action"]]
        assert len(queue_permissions) == 1
        assert queue_permissions[0]["Resource"] == {"Fn::GetAtt": [f"{stage}Queue", "Arn"]}
        assert container["StopTimeout"] == 90
    dispatcher = resources["DispatcherTask"]["Properties"]["ContainerDefinitions"][0]
    environment = {item["Name"]: item["Value"] for item in dispatcher["Environment"]}
    for stage in ("Acquisition", "Processing"):
        assert environment[f"CHESS_CRAWL_SQS_{stage.upper()}_QUEUE_URL"] == {"Ref": f"{stage}Queue"}
    processing = resources["ProcessingRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    assert not any("s3:PutObject" in item["Action"] for item in processing)
    permissions = resources["DispatcherRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    assert permissions[0]["Action"] == ["sqs:SendMessage"]
    assert permissions[0]["Resource"] == [{"Fn::GetAtt": [f"{stage}Queue", "Arn"]} for stage in ("Acquisition", "Processing")]


def test_runtime_roles_share_budget_parameters_but_have_independent_sizes() -> None:
    value = template()
    environments = {}
    for role in ("Api", "Acquisition", "Processing", "Dispatcher", "Migration"):
        task = value["Resources"][f"{role}Task"]["Properties"]
        assert task["Cpu"] == {"Ref": f"{role}Cpu"}
        assert task["Memory"] == {"Ref": f"{role}Memory"}
        environments[role] = {item["Name"]: item["Value"] for item in task["ContainerDefinitions"][0]["Environment"]}
    budget_names = {name for name, parameter in value["Parameters"].items()
                    if parameter.get("Description", "").startswith(("CHESS_CRAWL_JOB_MAX_", "CHESS_CRAWL_WORKSPACE_MAX_"))}
    budget_names.discard("JobMaxRetries")
    assert budget_names
    for name in budget_names:
        key = value["Parameters"][name]["Description"]
        for role in ("Api", "Acquisition", "Processing"):
            assert environments[role][key] == {"Ref": name}
    for role in ("Api", "Acquisition", "Processing", "Dispatcher"):
        assert environments[role]["CHESS_CRAWL_EVENTS_ENABLED"] == {"Ref": "EventsEnabled"}
    assert value["Parameters"]["EventsEnabled"]["Default"] == "false"


def test_database_auth_uses_provisioned_readiness_credential_without_static_bypass() -> None:
    value = template()
    assert value["Parameters"]["ApiAuthMode"]["AllowedValues"] == ["static", "database"]
    assert value["Conditions"]["DatabaseApiAuth"] == {"Fn::Equals": [{"Ref": "ApiAuthMode"}, "database"]}
    api = value["Resources"]["ApiTask"]["Properties"]["ContainerDefinitions"][0]
    environment = {item["Name"]: item["Value"] for item in api["Environment"]}
    assert environment["CHESS_CRAWL_API_AUTH_MODE"] == {"Ref": "ApiAuthMode"}
    bearer = [item for item in api["Secrets"] if item["ValueFrom"] == {"Ref": "ApiTokenSecretArn"}]
    assert len(bearer) == 1
    assert bearer[0]["Name"] == {"Fn::If": ["DatabaseApiAuth", "CHESS_CRAWL_HEALTHCHECK_TOKEN", "CHESS_CRAWL_API_TOKEN"]}
    assert "CHESS_CRAWL_API_TOKEN" not in environment
    assert "CHESS_CRAWL_API_TOKEN_FILE" not in environment


def test_cloud_float_settings_do_not_reject_valid_fractional_runtime_values() -> None:
    parameters = template()["Parameters"]
    for name in ("ChesscomDelaySeconds", "LichessDelaySeconds", "DispatchRetentionSeconds",
                 "WorkerPollInterval", "WorkerHeartbeatInterval", "WorkerHeartbeatMaxAge",
                 "JobRetryBase", "JobRetryMax", "DispatchCleanupIntervalSeconds"):
        assert parameters[name]["Type"] == "Number"
        assert parameters[name]["MinValue"] == 0
