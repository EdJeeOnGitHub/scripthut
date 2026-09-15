"""Managed lifecycle evidence and races, exercised through the real owner."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripthut import main
from scripthut.backends.base import JobStats
from scripthut.config_schema import ScriptHutConfig
from scripthut.models import ConnectionStatus
from scripthut.runs.manager import (
    SCHEDULER_NO_RECORD_MARKER,
    SETTLING_UNCONFIRMED_MARKER,
    RunManager,
)
from scripthut.runs.models import Run, RunItem, TaskDefinition
from scripthut.runs.models import RunItemStatus as Status
from scripthut.runtime import BackendState

NOW = datetime(2026, 1, 1, tzinfo=UTC)


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW


@pytest.fixture
def lifecycle(monkeypatch):
    backend = SimpleNamespace(
        get_jobs=AsyncMock(return_value=[]),
        get_cluster_info=AsyncMock(return_value=None),
        get_disk_info=AsyncMock(return_value=None),
        get_job_stats=AsyncMock(return_value={}),
        terminal_states=frozenset({"COMPLETED", "FAILED"}),
        failure_states={"FAILED": "Non-zero exit code"},
    )
    manager = RunManager(ScriptHutConfig(), {}, job_backends={"b": backend})
    item = RunItem(
        TaskDefinition(id="task", name="Task", command="true"),
        status=Status.SUBMITTED,
        job_id="42",
        submitted_at=NOW - timedelta(seconds=65),
    )
    run = Run(
        id="run",
        workflow_name="wf",
        backend_name="b",
        created_at=NOW,
        items=[item],
        max_concurrent=1,
    )
    manager.runs[run.id] = run
    bs = BackendState(
        "b", "slurm", backend=backend, status=ConnectionStatus(connected=True, host="example")
    )
    state = main.AppState()
    state.run_manager = manager
    state.backends = {"b": bs}
    monkeypatch.setattr(main, "state", state)
    monkeypatch.setattr(main, "datetime", Clock)
    monkeypatch.setattr("scripthut.runs.manager.datetime", Clock)
    return manager, run, item, backend, bs


@pytest.mark.asyncio
@pytest.mark.parametrize("age,queried", [(60, False), (60.001, True)])
async def test_accounting_query_grace_is_strict(lifecycle, age, queried):
    _, _, item, backend, bs = lifecycle
    item.submitted_at = NOW - timedelta(seconds=age)
    await main.poll_backend(bs)
    assert bool(backend.get_job_stats.await_count) == queried
    assert item.status == Status.SUBMITTED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,age,expected",
    [
        (Status.SUBMITTED, 300, Status.SUBMITTED),
        (Status.SUBMITTED, 300.001, Status.FAILED),
        (Status.SETTLING, 600, Status.SETTLING),
        (Status.SETTLING, 600.001, Status.COMPLETED),
    ],
)
async def test_empty_accounting_timeout_boundaries(lifecycle, status, age, expected):
    _, _, item, _, bs = lifecycle
    item.status = status
    item.submitted_at = item.finished_at = NOW - timedelta(seconds=age)
    await main.poll_backend(bs)
    assert item.status == expected
    if expected == Status.FAILED:
        assert item.error == SCHEDULER_NO_RECORD_MARKER
    if expected == Status.COMPLETED:
        assert item.error == SETTLING_UNCONFIRMED_MARKER
        assert item.exit_code is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query", ["get_jobs", "get_job_stats", "get_cluster_info", "get_disk_info"]
)
async def test_failed_poll_cannot_supply_absence_evidence(lifecycle, query):
    _, _, item, backend, bs = lifecycle
    item.submitted_at = NOW - timedelta(days=1)
    getattr(backend, query).side_effect = OSError("unavailable")
    before = item.to_dict()
    await main.poll_backend(bs)
    assert item.to_dict() == before
    assert not bs.poll_fresh


@pytest.mark.asyncio
async def test_terminal_run_receives_late_failure_and_resource_evidence(lifecycle):
    manager, run, item, backend, bs = lifecycle
    item.status = Status.COMPLETED
    backend.get_job_stats.return_value = {
        "42": JobStats(
            80, "1G", "10s", NOW - timedelta(seconds=20), NOW - timedelta(seconds=1), "FAILED", 2
        )
    }
    await main.poll_backend(bs)
    assert item.status == Status.FAILED
    assert item.scheduler_state == "FAILED"
    assert item.exit_code == 2
    assert item.cpu_efficiency == 80
    assert item.finished_at == NOW - timedelta(seconds=1)
    assert manager._run_versions[run.id] > 0


def observation(manager, jobs=(), stats=None, *, outcome=None, when=NOW):
    from scripthut.runs.observations import AccountingOutcome, BackendObservation, QueueObservation

    plan = manager.plan_backend_observation("b", QueueObservation(tuple(jobs), when))
    return BackendObservation(plan, outcome or AccountingOutcome.SUCCEEDED, stats or {})


@pytest.mark.asyncio
async def test_unrequested_accounting_is_not_empty_success(lifecycle):
    from scripthut.runs.observations import AccountingOutcome

    manager, _, item, _, _ = lifecycle
    item.submitted_at = NOW - timedelta(days=1)
    obs = observation(manager, outcome=AccountingOutcome.NOT_REQUESTED)
    await manager.apply_backend_observations([obs])
    assert item.status == Status.SUBMITTED


@pytest.mark.asyncio
async def test_observation_copies_mutable_backend_stats(lifecycle):
    manager, _, item, _, _ = lifecycle
    stats = {"42": JobStats(80, "1G", "10s", state="COMPLETED", exit_code=0)}
    obs = observation(manager, stats=stats)
    stats["42"].state = "FAILED"
    obs.accounting["42"].state = "FAILED"
    stats.clear()
    await manager.apply_backend_observations([obs])
    assert item.status == Status.COMPLETED


@pytest.mark.asyncio
async def test_duplicate_and_older_observations_do_not_repeat_completion(lifecycle):
    manager, _, item, _, _ = lifecycle
    effects = []

    async def complete(run, completed):
        effects.append(completed.task.id)

    manager._after_item_completed = complete
    old = observation(manager, stats={"42": JobStats(0, "", "", state="FAILED")})
    new = observation(manager, stats={"42": JobStats(0, "", "", state="COMPLETED")})
    newest = observation(manager, stats={"42": JobStats(0, "", "", state="FAILED")})
    await manager.apply_backend_observations([new, new, old])
    assert item.status == Status.COMPLETED
    assert effects == ["task"]
    # Later, genuinely new failure evidence still corrects a false completion.
    await manager.apply_backend_observations([newest])
    assert item.status == Status.FAILED


@pytest.mark.asyncio
async def test_generated_job_is_not_subject_to_old_queue_snapshot(lifecycle):
    manager, run, item, _, _ = lifecycle
    generated = RunItem(
        TaskDefinition(id="child", name="Child", command="true"),
        status=Status.RUNNING,
        job_id="new",
        submitted_at=NOW,
    )

    async def complete(run, completed):
        run.items.append(generated)

    manager._after_item_completed = complete
    await manager.apply_backend_observations(
        [observation(manager, stats={"42": JobStats(0, "", "", state="COMPLETED")})]
    )
    assert item.status == Status.COMPLETED
    assert generated.status == Status.RUNNING


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["job", "attempt", "replacement", "submitted_at"])
async def test_changed_item_identity_rejects_captured_evidence(lifecycle, change):
    from scripthut.submission import SubmissionAttempt

    manager, run, item, _, _ = lifecycle
    old = observation(manager, stats={"42": JobStats(0, "", "", state="FAILED")})
    if change == "job":
        item.job_id = "43"
    elif change == "attempt":
        item.submission_attempts.append(
            SubmissionAttempt(
                id="new", created_at=NOW, scheduler_name="new", destination="b", user="test"
            )
        )
    elif change == "replacement":
        item = RunItem.from_dict(item.to_dict())
        run.items[0] = item
    else:
        item.submitted_at = NOW
    before = item.to_dict()
    await manager.apply_backend_observations([old])
    assert item.to_dict() == before


@pytest.mark.asyncio
async def test_newer_failed_poll_supersedes_older_success(lifecycle):
    from scripthut.runs.observations import AccountingOutcome

    manager, _, item, _, _ = lifecycle
    old = observation(manager, stats={"42": JobStats(0, "", "", state="FAILED")})
    failed = observation(manager, outcome=AccountingOutcome.FAILED)
    await manager.apply_backend_observations([failed, old])
    assert item.status == Status.SUBMITTED


@pytest.mark.asyncio
async def test_overlapping_application_serializes_completion(lifecycle):
    import asyncio

    manager, _, item, _, _ = lifecycle
    entered, release = asyncio.Event(), asyncio.Event()
    effects = []

    async def complete(run, completed):
        effects.append(completed.task.id)
        entered.set()
        await release.wait()

    manager._after_item_completed = complete
    first = observation(manager, stats={"42": JobStats(0, "", "", state="COMPLETED")})
    second = observation(manager, stats={"42": JobStats(0, "", "", state="COMPLETED")})
    applying = asyncio.create_task(manager.apply_backend_observations([first]))
    await entered.wait()
    repeating = asyncio.create_task(manager.apply_backend_observations([second]))
    release.set()
    await asyncio.gather(applying, repeating)
    assert effects == ["task"]
    assert item.status == Status.COMPLETED


@pytest.mark.asyncio
async def test_rerun_during_output_fetch_discards_old_result(lifecycle):
    manager, run, item, _, _ = lifecycle
    item.status = Status.COMPLETED

    async def command(cmd):
        await manager.rerun_in_place(run.id)
        return "old.txt\t1\n", "HAS_SUMMARY", 0

    manager.backends["b"] = SimpleNamespace(is_connected=True, run_command=command)

    async def submit(run, pending):
        pending.job_id = "new-job"
        pending.status = Status.SUBMITTED
        pending.submitted_at = NOW
        return True

    manager.submit_task = submit
    await manager._after_item_completed(run, item)
    assert item.job_id == "new-job"
    assert not item.outputs
    assert not item.has_run_summary


@pytest.mark.asyncio
async def test_cancel_during_generated_source_read_cannot_append_work(lifecycle):
    manager, run, item, _, _ = lifecycle
    item.status = Status.COMPLETED
    item.task.generates_source = "generated.json"
    run.items.append(RunItem(TaskDefinition(id="pending", name="Pending", command="true")))

    async def command(cmd):
        assert await manager.cancel_run(run.id)
        return '[{"id":"late","name":"Late","command":"true"}]', "", 0

    manager.backends["b"] = SimpleNamespace(is_connected=True, run_command=command)
    await manager._after_item_completed(run, item)
    assert [entry.task.id for entry in run.items] == ["task", "pending"]
    assert run.items[1].status == Status.FAILED


@pytest.mark.asyncio
async def test_filter_refresh_applies_queue_lifecycle(lifecycle, monkeypatch):
    from scripthut.models import JobState

    _, _, item, backend, _ = lifecycle
    backend.get_jobs.return_value = [SimpleNamespace(job_id="42", state=JobState.RUNNING)]
    monkeypatch.setattr(main, "jobs_partial", AsyncMock(return_value="html"))
    await main.toggle_filter(SimpleNamespace())
    assert item.status == Status.RUNNING
    assert item.started_at == NOW


@pytest.mark.asyncio
async def test_submission_during_queue_fetch_is_not_marked_missing(lifecycle):
    manager, _, item, backend, bs = lifecycle

    async def jobs(user=None):
        item.status = Status.RUNNING
        item.job_id = "just-submitted"
        item.submitted_at = NOW + timedelta(seconds=1)
        return []

    backend.get_jobs.side_effect = jobs
    await main.poll_backend(bs)
    assert item.status == Status.RUNNING


@pytest.mark.asyncio
async def test_later_arriving_older_queue_is_ignored(lifecycle):
    from scripthut.models import JobState

    manager, _, item, _, _ = lifecycle
    latest = observation(manager, [("42", JobState.RUNNING, None)])
    await manager.apply_backend_observations([latest])
    late = observation(manager, when=NOW - timedelta(seconds=1))
    await manager.apply_backend_observations([late])
    assert item.status == Status.RUNNING


@pytest.mark.asyncio
async def test_nested_resource_accounting_survives_observation_and_reporting(lifecycle, tmp_path):
    from scripthut.reports.resources import ResourceUsage
    from scripthut.reports.efficiency import EfficiencyStore

    manager, run, item, _, _ = lifecycle
    usage = ResourceUsage(elapsed_seconds=20, allocated_cpu_seconds=40,
                          consumed_cpu_seconds=30, reported_peak_bytes=1024)
    obs = observation(manager, stats={"42": JobStats(
        75, "1K", "30s", end_time=NOW, state="COMPLETED", exit_code=0,
        resource_usage=usage)})
    usage.consumed_cpu_seconds = 999
    first = obs.accounting["42"].resource_usage
    assert isinstance(first, ResourceUsage)
    assert first.consumed_cpu_seconds == 30
    first.consumed_cpu_seconds = 888
    assert obs.accounting["42"].resource_usage.consumed_cpu_seconds == 30
    await manager.apply_backend_observations([obs])
    assert item.to_dict()["resource_usage"]["consumed_cpu_seconds"] == 30
    store = EfficiencyStore(tmp_path / "efficiency.sqlite")
    store.record_runs([run])
    records = store.records(NOW - timedelta(days=1), NOW + timedelta(days=1))
    assert records[0]["usage"]["consumed_cpu_seconds"] == 30
