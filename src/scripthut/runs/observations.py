"""Immutable, process-local scheduler evidence; never persisted with run state."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field, fields
from datetime import datetime
from enum import Enum
from typing import Any

from scripthut.backends.base import JobStats
from scripthut.models import JobState


class AccountingOutcome(Enum):
    NOT_REQUESTED = "not_requested"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class QueueObservation:
    jobs: tuple[tuple[str, JobState, str | None], ...]
    observed_at: datetime
    succeeded: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "jobs", tuple(tuple(entry) for entry in self.jobs))
        if self.observed_at.utcoffset() is None:
            raise ValueError("Observation time must be timezone-aware")


@dataclass(frozen=True)
class ItemIdentity:
    run_id: str
    task_id: str
    job_id: str
    attempt_id: str | None
    submitted_at: datetime | None
    run_instance: int
    item_instance: int
    lifecycle_revision: int


@dataclass(frozen=True)
class PollPlan:
    backend_name: str
    sequence: int
    queue: QueueObservation
    items: tuple[ItemIdentity, ...]
    accounting_ids: tuple[str, ...]


@dataclass(frozen=True, init=False)
class BackendObservation:
    plan: PollPlan
    accounting_outcome: AccountingOutcome
    # Copy fields without flattening nested ResourceUsage dataclasses.
    # Readers receive independent values, including nested measurements.
    _rows: tuple[tuple[str, tuple[Any, ...]], ...] = field(repr=False)

    def __init__(
        self,
        plan: PollPlan,
        accounting_outcome: AccountingOutcome,
        accounting: Mapping[str, JobStats] | None = None,
    ) -> None:
        object.__setattr__(self, "plan", plan)
        object.__setattr__(self, "accounting_outcome", accounting_outcome)
        object.__setattr__(
            self, "_rows", tuple((key, tuple(deepcopy(getattr(value, f.name)) for f in fields(value)))
                                for key, value in (accounting or {}).items())
        )
        if accounting_outcome != AccountingOutcome.SUCCEEDED and self._rows:
            raise ValueError("Only successful accounting can contain rows")

    @property
    def accounting(self) -> dict[str, JobStats]:
        return {key: JobStats(*deepcopy(values)) for key, values in self._rows}

    @property
    def fresh(self) -> bool:
        return self.plan.queue.succeeded and (
            self.accounting_outcome == AccountingOutcome.SUCCEEDED
            or (
                not self.plan.accounting_ids
                and self.accounting_outcome == AccountingOutcome.NOT_REQUESTED
            )
        )
