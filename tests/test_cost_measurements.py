"""Measured workload evidence and explicit-price estimates remain distinguishable."""
from __future__ import annotations

import json

import pytest

from chess_crawl import costs


def test_workload_log_tracks_success_and_failure_without_exception_content(monkeypatch, capsys) -> None:
    monkeypatch.setenv("CHESS_CRAWL_USAGE_LOG", "true")
    with costs.measure_workload("archive_write") as counters:
        counters.add(source_bytes=1000, stored_bytes=100, objects_written=1)
    with pytest.raises(RuntimeError, match="private-secret"):
        with costs.measure_workload("archive_read"):
            raise RuntimeError("private-secret: provider token")
    raw = capsys.readouterr().out
    assert "private-secret" not in raw and "provider token" not in raw
    records = [json.loads(line) for line in raw.splitlines()]
    assert [row["outcome"] for row in records] == ["success", "failure"]
    assert records[0]["source_bytes"] == 1000 and records[0]["stored_bytes"] == 100
    assert records[0]["games_imported"] is None
    assert all(row["wall_seconds"] >= 0 and row["cpu_seconds"] >= 0 for row in records)


def test_measurement_default_is_silent_and_rejects_invalid_counters(capsys) -> None:
    with costs.measure_workload("normalization") as counters:
        with pytest.raises(ValueError):
            counters.add(source_bytes=-1)
        with pytest.raises(ValueError):
            counters.add(private_user_id=1)
    assert capsys.readouterr().out == ""
    with pytest.raises(ValueError):
        costs.UsageSample("archive_write", float("nan"), 0)
    with pytest.raises(ValueError):
        costs.UsageSample("archive_write", 0, 0, source_bytes=True)


def test_unknown_game_counts_remain_distinguishable_from_zero() -> None:
    report = costs.summarize([
        costs.UsageSample("archive_write", 1, 0.1, source_bytes=100, stored_bytes=10),
        costs.UsageSample("normalization", 2, 0.5, games_imported=0),
        costs.UsageSample("normalization", 3, 0.5, games_imported=2),
    ])
    assert report["totals"]["games_imported"] == 2
    assert report["reported_counter_samples"]["games_imported"] == 2
    assert report["totals"]["samples"] == 3
    unknown = costs.summarize([costs.UsageSample("archive_write", 1, 0)])
    assert unknown["totals"]["games_imported"] is None
    assert unknown["reported_counter_samples"]["games_imported"] == 0


def test_decimal_costs_use_operator_rates_and_eligible_credits() -> None:
    report = costs.estimate_cost(
        {"fargate_vcpu_hours": "10", "fargate_gib_hours": "20"},
        rates={"fargate_vcpu_hours": "0.05", "fargate_gib_hours": "0.01"},
        applicable_credits="0.30", pricing_as_of="synthetic test inputs", currency="USD",
    )
    assert report["gross_known_cost"] == "0.70"
    assert report["net_known_cost"] == "0.40"
    assert report["complete_supplied_units"] is True


def test_missing_rates_do_not_become_free_services() -> None:
    report = costs.estimate_cost(
        {"s3_gib_months": 100, "rds_instance_hours": 730}, rates={"s3_gib_months": "0.02"},
        pricing_as_of="synthetic test inputs",
    )
    assert report["missing_rates"] == ["rds_instance_hours"]
    assert report["complete_supplied_units"] is False
    assert report["gross_known_cost"] == "2.00"
    assert "unprovided" in report["scope"]


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", True])
def test_invalid_cost_inputs_fail(value) -> None:
    with pytest.raises(ValueError):
        costs.estimate_cost({"sqs_requests": value}, rates={"sqs_requests": "1"}, pricing_as_of="test")


def test_cost_command_roundtrips_clean_jsonl(tmp_path, capsys) -> None:
    usage = tmp_path / "usage.jsonl"
    usage.write_text(json.dumps({
        "stage": "source_dedupe", "wall_seconds": 0, "cpu_seconds": 0, "deduplicated": 2,
    }) + "\n")
    assert costs.main([str(usage)]) == 0
    assert json.loads(capsys.readouterr().out)["measurements"]["totals"]["deduplicated"] == 2
