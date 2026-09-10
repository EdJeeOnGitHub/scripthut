"""Durable scheduler submission evidence, shared by drivers and run management."""

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any


class SubmissionRejected(RuntimeError):  # noqa: N818
    """The scheduler explicitly rejected a submission without returning a job ID."""


class SubmissionConflict(ValueError):  # noqa: N818
    """An action would discard or contradict unresolved submission evidence."""


@dataclass
class SubmissionAttempt:
    id: str
    created_at: datetime
    scheduler_name: str
    destination: str
    user: str
    resolution: str = "unknown"
    job_id: str | None = None
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SubmissionAttempt":
        stamp = datetime.fromisoformat(data["created_at"])
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=UTC)
        return cls(**{**data, "created_at": stamp})
