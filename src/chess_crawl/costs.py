"""Bounded workload measurements and reproducible estimates with operator rates.

Measurements describe application work. They are not AWS billing records, and
the estimator never assumes CPU utilization equals allocated Fargate capacity.
"""
from __future__ import annotations

from chess_crawl.settings import setting, boolean

import argparse
import json
import math
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


STAGES = {"archive_write", "archive_read", "source_dedupe", "acquisition", "normalization", "analysis"}
COUNTERS = {"source_bytes", "stored_bytes", "objects_written", "objects_read", "deduplicated", "games_imported"}
RATE_UNITS = {"fargate_vcpu_hours", "fargate_gib_hours", "s3_gib_months", "s3_put_requests",
              "s3_get_requests", "sqs_requests", "rds_instance_hours", "rds_storage_gib_months",
              "alb_hours", "alb_lcu_hours", "logs_ingested_gib", "nat_gateway_hours",
              "nat_processed_gib", "vpc_endpoint_hours", "data_transfer_gib"}


@dataclass(frozen=True)
class UsageSample:
    stage: str
    wall_seconds: float
    cpu_seconds: float
    outcome: str = "success"
    source_bytes: int = 0
    stored_bytes: int = 0
    objects_written: int = 0
    objects_read: int = 0
    deduplicated: int = 0
    games_imported: int | None = None
    kind: str = "chess_crawl_usage"
    version: int = 1

    def __post_init__(self) -> None:
        if self.stage not in STAGES or self.outcome not in {"success", "failure"}:
            raise ValueError("Invalid workload stage or outcome")
        if self.kind != "chess_crawl_usage" or self.version != 1:
            raise ValueError("Unsupported usage measurement format")
        for name in ("wall_seconds", "cpu_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError("Workload duration must be finite and nonnegative")
        for name in COUNTERS:
            value = getattr(self, name)
            if name == "games_imported" and value is None:
                continue
            if type(value) is not int or value < 0:
                raise ValueError("Workload counters must be nonnegative integers")


class UsageCounters:
    def __init__(self) -> None:
        self.values: dict[str, int | None] = {name: 0 for name in COUNTERS}
        self.values["games_imported"] = None

    def add(self, **values: int) -> None:
        for name, value in values.items():
            if name not in COUNTERS or type(value) is not int or value < 0:
                raise ValueError("Invalid workload counter")
            self.values[name] = (self.values[name] or 0) + value


def emit_sample(sample: UsageSample) -> None:
    if boolean(setting("CHESS_CRAWL_USAGE_LOG", "false"), "CHESS_CRAWL_USAGE_LOG"):
        # No source body, identity, credential, URL, key, or exception text.
        print(json.dumps(asdict(sample), sort_keys=True, separators=(",", ":")), flush=True)


@contextmanager
def measure_workload(stage: str) -> Iterator[UsageCounters]:
    if stage not in STAGES:
        raise ValueError("Unknown workload stage")
    counters = UsageCounters()
    started, cpu_started = time.monotonic(), time.process_time()
    outcome = "success"
    try:
        yield counters
    except BaseException:
        outcome = "failure"
        raise
    finally:
        measured: dict[str, Any] = dict(counters.values)
        emit_sample(UsageSample(
            stage, max(0.0, time.monotonic() - started), max(0.0, time.process_time() - cpu_started),
            outcome=outcome, **measured,
        ))


def summarize(samples: list[UsageSample]) -> dict[str, Any]:
    totals: dict[str, Any] = {name: 0 for name in COUNTERS}
    totals.update(samples=len(samples), successes=0, failures=0, wall_seconds=0.0, cpu_seconds=0.0)
    stages: dict[str, dict[str, Any]] = {}
    reported = {name: 0 for name in COUNTERS}
    for sample in samples:
        bucket = stages.setdefault(sample.stage, {name: 0 for name in (*COUNTERS, "samples", "wall_seconds", "cpu_seconds")})
        totals["successes" if sample.outcome == "success" else "failures"] += 1
        bucket["samples"] += 1
        for name in (*COUNTERS, "wall_seconds", "cpu_seconds"):
            value = getattr(sample, name)
            if value is None:
                continue
            if name in COUNTERS:
                reported[name] += 1
            totals[name] += value
            bucket[name] += value
    if reported["games_imported"] == 0:
        totals["games_imported"] = None
    for stage, bucket in stages.items():
        if not any(sample.stage == stage and sample.games_imported is not None for sample in samples):
            bucket["games_imported"] = None
    return {"totals": totals, "stages": stages, "reported_counter_samples": reported}


def estimate_cost(
    units: Mapping[str, str | int | float], *, rates: Mapping[str, str | int | float],
    pricing_as_of: str, currency: str = "USD", applicable_credits: str | int | float = "0",
) -> dict[str, Any]:
    if not pricing_as_of or not currency:
        raise ValueError("Pricing date/source and currency must be supplied")
    if set(units) - RATE_UNITS or set(rates) - RATE_UNITS:
        raise ValueError("Unknown billing unit")

    def amount(value: str | int | float) -> Decimal:
        if isinstance(value, bool):
            raise ValueError("Billing amounts cannot be booleans")
        result = Decimal(str(value))
        if not result.is_finite() or result < 0:
            raise ValueError("Billing units, rates, and credits must be finite and nonnegative")
        return result

    missing = sorted(name for name, value in units.items() if amount(value) > 0 and name not in rates)
    lines = {name: str(amount(value) * amount(rates[name])) for name, value in units.items() if name in rates}
    gross = sum((Decimal(value) for value in lines.values()), Decimal(0))
    credits = amount(applicable_credits)
    return {
        "currency": currency, "pricing_as_of": pricing_as_of, "cost_by_unit": lines,
        "gross_known_cost": str(gross), "missing_rates": missing, "complete_supplied_units": not missing,
        "scope": "Only supplied billing units; unprovided services are excluded",
        # Credits are manually selected eligible amounts, never all account credits.
        "applicable_credits": str(credits),
        "net_known_cost": str(max(Decimal(0), gross - credits)),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize workload evidence and supplied billing projections")
    parser.add_argument("usage", type=Path, help="JSONL containing only chess_crawl_usage records")
    parser.add_argument("--billing-input", type=Path, help="JSON with units, rates, pricing_as_of and optional credits")
    args = parser.parse_args(argv)
    samples = [UsageSample(**json.loads(line)) for line in args.usage.read_text().splitlines() if line.strip()]
    output: dict[str, Any] = {"measurements": summarize(samples)}
    if args.billing_input:
        billing = json.loads(args.billing_input.read_text())
        output["estimate"] = estimate_cost(**billing)
    print(json.dumps(output, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
