"""Serialize submission and preserve evidence across transport or controller loss."""

from __future__ import annotations

import asyncio
import re
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from scripthut.backends.slurm import SlurmBackend
from scripthut.runs.models import Run, RunItem, RunItemStatus
from scripthut.submission import SubmissionAttempt, SubmissionConflict, SubmissionRejected

if TYPE_CHECKING:
    from scripthut.runs.manager import RunManager


class SubmissionManager:
    # Retry delays (seconds) for a scheduler-match query that comes back
    # empty. A job that both queues AND finishes faster than this round
    # trip can vanish from squeue before sacct's accounting has caught
    # up, so a single-shot "0 matches" is evidence of "not observed
    # yet," not "never happened." Backing off here is what actually
    # removes most SUBMISSION_UNKNOWN verdicts rather than just making
    # them easier to clean up after the fact.
    _FIND_ATTEMPT_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0, 4.0)

    # Cap on how many times a single task will be (re)submitted through
    # this path. Each SUBMISSION_UNKNOWN → resolve(retry) → resubmit
    # cycle is a fresh sbatch call; without a cap, a persistently broken
    # config (bad partition, expired allocation) resubmits forever every
    # time someone — or the reconciler — clears the unknown state.
    MAX_SUBMISSION_ATTEMPTS = 3

    def __init__(self, manager: RunManager):
        self.manager = manager
        self.locks: dict[str, asyncio.Lock] = {}
        self.backend_locks: dict[str, asyncio.Lock] = {}

    def lock(self, run: Run) -> asyncio.Lock:
        return self.locks.setdefault(run.id, asyncio.Lock())

    def backend_lock(self, run: Run) -> asyncio.Lock:
        return self.backend_locks.setdefault(run.backend_name, asyncio.Lock())

    def save(self, run: Run) -> None:
        if self.manager.storage is None:
            raise RuntimeError("Slurm submission requires persistent run storage")
        self.manager.storage.save_run(run, durable=True)
        self.manager.notify_run(run.id)

    def destination(self, run: Run) -> str:
        client = self.manager.get_ssh_client(run.backend_name)
        return f"{run.backend_name}:{getattr(client, 'host', '')}:{getattr(client, 'port', 22)}"

    async def submit(
        self, run: Run, item: RunItem, script: str, backend: SlurmBackend
    ) -> bool | None:
        client = self.manager.get_ssh_client(run.backend_name)
        attempt_id = uuid.uuid4().hex
        prefix = re.sub(r"[^a-zA-Z0-9_.-]", "_", item.task.name)[:60]
        attempt = SubmissionAttempt(
            id=attempt_id,
            created_at=datetime.now(UTC),
            scheduler_name=f"{prefix}--sh-{attempt_id}",
            destination=self.destination(run),
            user=client.user if client else "",
        )
        item.submission_attempts.append(attempt)
        item.status = RunItemStatus.SUBMITTING
        try:
            self.save(run)
        except Exception as exc:
            # No remote command was sent. Keep the marker conservative in case
            # replace succeeded but directory fsync failed.
            item.status = RunItemStatus.SUBMISSION_UNKNOWN
            item.error = attempt.detail = f"Could not persist submission intent: {exc}"
            return None

        def accepted(job_id: str, output: str) -> None:
            item.job_id = attempt.job_id = job_id
            item.submit_output = output
            item.submitted_at = datetime.now(UTC)
            self.save(run)

        try:
            await backend.submit_attempt(script, attempt, accepted)
            await self.check(run, item, backend)
            return True if not item.submission_unresolved else None
        except SubmissionRejected as exc:
            attempt.resolution = "rejected"
            item.status = RunItemStatus.FAILED
            item.finished_at = datetime.now(UTC)
            item.error = attempt.detail = str(exc)
            try:
                self.save(run)
            except Exception:
                item.status = RunItemStatus.SUBMISSION_UNKNOWN
                attempt.resolution = "unknown"
                return None
            return False
        except (Exception, asyncio.CancelledError) as exc:
            item.status = RunItemStatus.SUBMISSION_UNKNOWN
            item.error = attempt.detail = f"Submission outcome unknown: {exc}"
            try:
                self.save(run)
            except Exception:
                pass  # The pre-submission marker remains sufficient to stop replay.
            if isinstance(exc, asyncio.CancelledError):
                raise
            return None

    async def _find_attempt_with_backoff(
        self, backend: SlurmBackend, attempt: SubmissionAttempt
    ) -> set[str]:
        """Look up an attempt's scheduler match, retrying briefly on a miss.

        Only empty results are retried — a query that raises (transport
        failure) or that finds multiple matches is a different, more
        urgent problem and is left to the caller immediately.
        """
        matches = await backend.find_attempt(attempt)
        for delay in self._FIND_ATTEMPT_RETRY_DELAYS:
            if matches:
                break
            await asyncio.sleep(delay)
            matches = await backend.find_attempt(attempt)
        return matches

    async def check(
        self, run: Run, item: RunItem, backend: SlurmBackend, job_id: str | None = None
    ) -> None:
        attempt = item.submission_attempts[-1]
        client = self.manager.get_ssh_client(run.backend_name)
        if (
            attempt.destination != self.destination(run)
            or client is None
            or attempt.user != client.user
        ):
            raise SubmissionConflict(
                "Submission destination or user changed; restore its original configuration"
            )
        matches = await self._find_attempt_with_backoff(backend, attempt)
        if len(matches) != 1:
            raise SubmissionConflict(
                f"Found {len(matches)} matching jobs; submission remains unknown"
            )
        found = next(iter(matches))
        if (job_id is not None and found != job_id) or (
            attempt.job_id is not None and found != attempt.job_id
        ):
            raise SubmissionConflict("Job ID contradicts submission evidence")
        item.job_id = attempt.job_id = found
        item.status = RunItemStatus.SUBMITTED
        item.submitted_at = item.submitted_at or attempt.created_at
        item.error = None
        attempt.resolution = "accepted"
        attempt.detail = "Verified unique scheduler match"
        try:
            self.save(run)
        except Exception:
            item.status = RunItemStatus.SUBMISSION_UNKNOWN
            attempt.resolution = "unknown"
            raise

    async def resolve(
        self,
        run: Run,
        item: RunItem,
        *,
        attempt_id: str | None = None,
        action: str = "check",
        job_id: str | None = None,
        confirm_not_submitted: bool = False,
    ) -> dict[str, Any]:
        async with self.lock(run):
            if not item.submission_unresolved or not item.submission_attempts:
                raise SubmissionConflict("Task has no unresolved submission")
            attempt = item.submission_attempts[-1]
            # attempt_id is an internal identity check, not something a
            # caller should have to fetch first: a bare "resolve this
            # task's submission" always means the latest attempt. It's
            # only required when a caller explicitly wants to guard
            # against acting on an attempt that changed underneath it
            # (e.g. a UI that fetched run view a while ago).
            if attempt_id is not None and attempt.id != attempt_id:
                raise SubmissionConflict("Submission attempt changed; refresh before resolving")
            backend = self.manager.get_job_backend(run.backend_name)
            if not isinstance(backend, SlurmBackend):
                raise SubmissionConflict("Original Slurm backend is unavailable")
            if action == "retry":
                if not confirm_not_submitted:
                    raise SubmissionConflict(
                        "Retry requires explicit confirmation that this attempt submitted no job"
                    )
                # Positive scheduler evidence overrides an accidental retry.
                # Failed queries never prove absence; the user's declaration
                # is the authority when they have verified externally.
                matches: set[str] = set()
                if self.manager.available(run.backend_name):
                    try:
                        matches = await self._find_attempt_with_backoff(backend, attempt)
                    except Exception:
                        pass
                if matches:
                    raise SubmissionConflict(
                        "Scheduler reports matching jobs; check or bind instead"
                    )
                attempt.resolution = "not_submitted"
                attempt.detail = "User explicitly confirmed no submission and authorized retry"
                item.status = RunItemStatus.PENDING
                item.job_id = None
                item.submitted_at = None
                item.error = None
                try:
                    self.save(run)
                except Exception:
                    item.status = RunItemStatus.SUBMISSION_UNKNOWN
                    item.job_id = attempt.job_id
                    attempt.resolution = "unknown"
                    raise
            elif action in ("check", "bind"):
                if action == "bind" and (job_id is None or not job_id.isdecimal()):
                    raise SubmissionConflict("Bind requires a numeric job ID")
                try:
                    await self.check(run, item, backend, job_id if action == "bind" else None)
                except Exception as exc:
                    item.status = RunItemStatus.SUBMISSION_UNKNOWN
                    item.error = attempt.detail = str(exc)
                    self.save(run)
                    raise SubmissionConflict(str(exc)) from exc
            else:
                raise SubmissionConflict("Unknown resolution action")
            return item.to_dict()

    async def reconcile(self, run: Run) -> None:
        for item in run.items:
            if item.submission_unresolved and item.submission_attempts:
                try:
                    await self.resolve(run, item, attempt_id=item.submission_attempts[-1].id)
                except Exception:
                    pass  # Query failures preserve unknown; other runs may progress.
