"""Trusted operator policy and explicit resumable work-budget failures."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields

from chess_crawl.settings import dataclass_settings


@dataclass(frozen=True)
class BudgetPolicy:
    job_max_games: int = 100000
    job_max_normalization_units: int = 100000
    job_max_remote_bytes: int = 268435456
    job_max_remote_requests: int = 2000
    max_response_bytes: int = 16777216
    workspace_max_games: int = 1000000
    workspace_max_normalization_units: int = 1000000
    workspace_max_remote_bytes: int = 2147483648
    workspace_max_remote_requests: int = 10000
    workspace_max_active_jobs: int = 2
    workspace_max_queued_jobs: int = 32

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if type(value) is not int or not 1 <= value < 2**63:
                raise ValueError(f"{field.name} must be a positive PostgreSQL bigint")

    @classmethod
    def from_env(cls) -> BudgetPolicy:
        return dataclass_settings(cls)


class BudgetExceeded(RuntimeError):
    code = "budget_exhausted"

    def __init__(
        self, dimension: str, *, remaining: int = 0,
        reset_at: int | None = None, budget_id: int | None = None,
    ) -> None:
        self.dimension = dimension
        self.remaining = remaining
        self.reset_at = reset_at
        self.budget_id = budget_id
        super().__init__(f"{self.code}: {dimension}; requested work remains incomplete")


class QuotaExceeded(BudgetExceeded):
    code = "workspace_quota_exceeded"


def main(argv: Sequence[str] | None = None) -> int:
    from chess_crawl.jobs.budget_admin import main as administer
    return administer(argv)


if __name__ == "__main__":
    raise SystemExit(main())
