"""Submission boundaries use real durable storage and the real Slurm command path."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripthut.backends.slurm import SlurmBackend
from scripthut.runs.manager import RunManager
from scripthut.runs.models import Run, RunItem, TaskDefinition
from scripthut.runs.models import RunItemStatus as Status
from scripthut.runs.storage import RunStorageManager
from scripthut.ssh.transport import TransportError
from scripthut.submission import SubmissionConflict


@pytest.fixture
def setup(tmp_path):
    ssh = SimpleNamespace(host="cluster", port=22, user="alice", is_connected=True)
    ssh.run_command = AsyncMock()
    backend = SlurmBackend(ssh)
    config = SimpleNamespace(
        get_backend=lambda name: SimpleNamespace(max_concurrent=4),
        get_source=lambda name: None,
        get_project=lambda name: None,
        env=[],
    )
    manager = RunManager(config, {"b": ssh}, RunStorageManager(tmp_path), {"b": backend})
    manager._resolve_environment = lambda run, task: ({}, "")
    run = Run(
        "run",
        "workflow",
        "b",
        datetime.now(UTC),
        [
            RunItem(TaskDefinition(id="one", name="one", command="true")),
            RunItem(TaskDefinition(id="two", name="two", command="true", dependencies=["one"])),
        ],
        4,
        log_dir="/logs",
    )
    manager.runs[run.id] = run
    return manager, run, ssh, backend


def disk_item(manager):
    return manager.storage.load_all_runs()["run"].items[0]


def responses(manager, run, *, lose=False, matches=("42",), query_error=False):
    async def command(cmd, **kwargs):
        item = run.items[0]
        if cmd.startswith("sbatch"):
            stored = disk_item(manager)
            assert stored.status == Status.SUBMITTING
            assert stored.submission_attempts[-1].id == item.submission_attempts[-1].id
            assert item.submission_attempts[-1].scheduler_name in cmd
            if lose:
                raise TransportError("response lost after acceptance")
            return "Submitted batch job 42\n", "", 0
        if cmd.startswith(("squeue --local", "TZ=UTC sacct --local")):
            if not lose:
                assert disk_item(manager).job_id == "42"
            if query_error:
                return "", "accounting unavailable", 1
            attempt = item.submission_attempts[-1]
            return "".join(f"{jid}|{attempt.scheduler_name}|alice\n" for jid in matches), "", 0
        return "", "", 0

    return command


@pytest.mark.asyncio
async def test_durable_intent_and_id_before_verification(setup):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run)
    assert await manager.submit_task(run, run.items[0]) is True
    stored = disk_item(manager)
    assert stored.status == Status.SUBMITTED
    assert stored.submission_attempts[-1].resolution == "accepted"
    assert stored.job_id == "42"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "matches,query_error", [((), False), (("42", "43"), False), (("42",), True)]
)
async def test_unknown_never_replays_and_keeps_dependencies_pending(setup, matches, query_error):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(
        manager, run, lose=True, matches=matches, query_error=query_error
    )
    await manager.process_run(run)
    for _ in range(2):
        await manager.update_all_runs({"b": []})
    assert run.items[0].status == Status.SUBMISSION_UNKNOWN
    assert run.items[1].status == Status.PENDING
    assert run.running_count == 1
    assert sum(c.args[0].startswith("sbatch") for c in ssh.run_command.call_args_list) == 1
    with pytest.raises(SubmissionConflict):
        await manager.cancel_run(run.id)
    with pytest.raises(SubmissionConflict):
        await manager.rerun_in_place(run.id)
    assert not manager.delete_run(run.id)


@pytest.mark.asyncio
async def test_returned_id_survives_failed_verification_and_restart(setup):
    manager, run, ssh, backend = setup
    ssh.run_command.side_effect = responses(manager, run, query_error=True)
    await manager.process_run(run)
    assert disk_item(manager).job_id == "42"
    # Simulate a crash immediately after the ID write: disk says submitting.
    run.items[0].status = Status.SUBMITTING
    manager.storage.save_run(run, durable=True)
    ssh.run_command.side_effect = responses(manager, run)
    restored = RunManager(manager.config, manager.backends, manager.storage, {"b": backend})
    await restored.restore_from_storage()
    item = restored.runs[run.id].items[0]
    assert item.status == Status.SUBMITTED
    assert item.job_id == "42"
    assert sum(c.args[0].startswith("sbatch") for c in ssh.run_command.call_args_list) == 1


@pytest.mark.asyncio
async def test_lost_response_is_adopted_without_resubmission(setup):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run, lose=True)
    await manager.process_run(run)
    await manager.submissions.reconcile(run)
    assert run.items[0].status == Status.SUBMITTED
    assert disk_item(manager).job_id == "42"


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_on", [1, 2, 3])
async def test_disk_failure_boundaries(setup, monkeypatch, fail_on):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run)
    save = manager.storage.save_run
    count = 0

    def failing_save(run, *, durable=False):
        nonlocal count
        count += 1
        if count >= fail_on:
            raise OSError("disk full")
        save(run, durable=durable)

    monkeypatch.setattr(manager.storage, "save_run", failing_save)
    await manager.process_run(run)
    assert run.items[0].status == Status.SUBMISSION_UNKNOWN
    assert run.items[1].status == Status.PENDING
    submitted = sum(c.args[0].startswith("sbatch") for c in ssh.run_command.call_args_list)
    assert submitted == (0 if fail_on == 1 else 1)
    if fail_on > 1:
        assert disk_item(manager).status == Status.SUBMITTING
    await manager.process_run(run)
    assert submitted == sum(c.args[0].startswith("sbatch") for c in ssh.run_command.call_args_list)


@pytest.mark.asyncio
async def test_retry_requires_confirmation_and_fresh_attempt_id(setup):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run, lose=True, matches=())
    await manager.process_run(run)
    item = run.items[0]
    first = item.submission_attempts[-1].id
    for kwargs in ({"attempt_id": first}, {"attempt_id": "stale", "confirm_not_submitted": True}):
        with pytest.raises(SubmissionConflict):
            await manager.submissions.resolve(run, item, action="retry", **kwargs)
    await manager.submissions.resolve(
        run, item, attempt_id=first, action="retry", confirm_not_submitted=True
    )
    ssh.run_command.side_effect = responses(manager, run)
    await manager.process_run(run)
    assert item.submission_attempts[0].resolution == "not_submitted"
    assert item.submission_attempts[1].id != first
    assert disk_item(manager).status == Status.SUBMITTED


@pytest.mark.asyncio
async def test_concurrent_submissions_send_once(setup):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run)
    await asyncio.gather(*(manager.process_run(run) for _ in range(4)))
    assert sum(c.args[0].startswith("sbatch") for c in ssh.run_command.call_args_list) == 1


@pytest.mark.asyncio
async def test_unavailable_and_failed_poll_do_not_advance(setup):
    manager, run, ssh, _ = setup
    ssh.is_connected = False
    await manager.process_run(run)
    ssh.run_command.assert_not_called()
    ssh.is_connected = True
    await manager.update_all_runs({})
    ssh.run_command.assert_not_called()
    ssh.run_command.side_effect = TransportError("connection lost preparing logs")
    await manager.process_run(run)
    assert all(i.status == Status.PENDING for i in run.items)
    assert not run.items[0].submission_attempts


@pytest.mark.asyncio
async def test_bind_rejects_wrong_id_or_destination(setup):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run, lose=True)
    await manager.process_run(run)
    item = run.items[0]
    kwargs = dict(attempt_id=item.submission_attempts[-1].id, action="bind")
    with pytest.raises(SubmissionConflict):
        await manager.submissions.resolve(run, item, job_id="43", **kwargs)
    ssh.host = "other-cluster"
    with pytest.raises(SubmissionConflict):
        await manager.submissions.resolve(run, item, job_id="42", **kwargs)
    ssh.host = "cluster"
    await manager.submissions.resolve(run, item, job_id="42", **kwargs)
    assert item.status == Status.SUBMITTED


def test_legacy_item_loads_without_attempt_history():
    item = RunItem(TaskDefinition(id="old", name="old", command="true"))
    data = item.to_dict()
    data.pop("submission_attempts")
    assert RunItem.from_dict(data).submission_attempts == []


@pytest.mark.asyncio
async def test_api_and_remote_cli_require_current_attempt(setup):
    import httpx
    from fastapi import FastAPI

    from scripthut.api import make_api_router
    from scripthut.cli import RemoteClient, build_parser

    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run, lose=True)
    await manager.process_run(run)
    app = FastAPI()
    app.include_router(
        make_api_router(
            SimpleNamespace(
                run_manager=manager,
                notify_poll=lambda: None,
            )
        )
    )
    async with RemoteClient("http://test") as client:
        await client._client.aclose()
        client._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app),
            base_url="http://test/api/v1",
        )
        with pytest.raises(RuntimeError, match="409"):
            await client.resolve_submission("run", "one", attempt_id="stale")
        with pytest.raises(RuntimeError, match="409"):
            await client.cancel_run("run")
        with pytest.raises(RuntimeError, match="409"):
            await client.rerun("run")
        path = "/runs/run/tasks/one/submission"
        body = {"attempt_id": run.items[0].submission_attempts[-1].id}
        denied = await client._client.post(path, json=body, headers={"Origin": "http://elsewhere"})
        assert denied.status_code == 403
        result = await client.resolve_submission("run", "one", **body)
        assert result["status"] == "submitted"
        assert result["job_id"] == "42"
    args = build_parser().parse_args(
        [
            "run",
            "resolve",
            "run",
            "one",
            "--attempt",
            body["attempt_id"],
            "--action",
            "retry",
            "--confirm-not-submitted",
        ]
    )
    assert args.confirm_not_submitted
    assert args.action == "retry"


@pytest.mark.asyncio
async def test_debug_submission_is_durable_and_reused_after_loss(setup):
    manager, run, ssh, _ = setup

    async def command(cmd, **kwargs):
        if cmd.startswith("sbatch"):
            saved = [r for r in manager.storage.load_all_runs().values() if r.debug_source]
            assert len(saved) == 1
            assert saved[0].interactive_wait
            assert saved[0].items[0].status == Status.SUBMITTING
            assert "Waiting for continue signal" in cmd
            raise TransportError("debug response lost")
        return "", "", 0

    ssh.run_command.side_effect = command
    debug = await manager.create_debug_run(run, run.items[0])
    again = await manager.create_debug_run(run, run.items[0])
    assert debug.id == again.id
    assert debug.items[0].status == Status.SUBMISSION_UNKNOWN
    assert run.items[0].status == Status.PENDING
    assert sum(c.args[0].startswith("sbatch") for c in ssh.run_command.call_args_list) == 1


@pytest.mark.asyncio
async def test_failed_accounting_poll_preserves_cached_jobs_and_task(setup, monkeypatch):
    from scripthut import main
    from scripthut.models import ConnectionStatus
    from scripthut.runtime import BackendState

    manager, run, ssh, backend = setup
    item = run.items[0]
    item.status = Status.SUBMITTED
    item.job_id = "42"
    item.submitted_at = datetime(2000, 1, 1, tzinfo=UTC)
    backend.get_jobs = AsyncMock(return_value=[])
    backend.get_cluster_info = AsyncMock(return_value=None)
    backend.get_disk_info = AsyncMock(return_value=None)
    backend.get_job_stats = AsyncMock(side_effect=TransportError("accounting lost"))
    monkeypatch.setattr(main.state, "run_manager", manager)
    monkeypatch.setattr(main.state, "run_storage", None)
    bs = BackendState(
        "b",
        "slurm",
        ssh_client=ssh,
        backend=backend,
        status=ConnectionStatus(connected=True, host="cluster"),
    )
    await main.poll_backend(bs)
    assert not bs.poll_fresh
    assert not bs.status.connected
    assert item.status == Status.SUBMITTED
    assert run.items[1].status == Status.PENDING


def test_strict_storage_failure_propagates(setup, monkeypatch):
    import os

    manager, run, _, _ = setup

    def fail(fd):
        raise OSError("fsync failed")

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError, match="fsync"):
        manager.storage.save_run(run, durable=True)


@pytest.mark.asyncio
async def test_unknown_submission_ui_shows_history_and_deliberate_actions(setup):
    from scripthut.main import templates

    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run, lose=True)
    await manager.process_run(run)
    html = templates.get_template("run_items.html").render(run=run, job_nodes={})
    assert "Check scheduler" in html
    assert "Verify and bind ID" in html
    assert "I verified that this attempt submitted no job" in html
    assert run.items[0].submission_attempts[-1].id in html
    assert "resolution required" in html


@pytest.mark.asyncio
async def test_unknown_uses_slot_but_healthy_run_can_progress(setup):
    from dataclasses import replace

    manager, run, ssh, backend = setup
    ssh.run_command.side_effect = responses(manager, run, lose=True, matches=())
    await manager.process_run(run)
    healthy = replace(run, id="healthy", items=[RunItem(run.items[0].task)])
    manager.runs[healthy.id] = healthy

    async def accepted(script, attempt, callback):
        callback("99", "Submitted batch job 99")

    backend.submit_attempt = accepted
    backend.find_attempt = AsyncMock(return_value={"99"})
    await manager.process_run(healthy)
    assert healthy.items[0].status == Status.SUBMITTED
    assert run.items[0].status == Status.SUBMISSION_UNKNOWN
    assert manager._backend_running_count("b") == 2
    manager.config.get_backend = lambda name: SimpleNamespace(max_concurrent=2)
    blocked = replace(run, id="blocked", items=[RunItem(run.items[0].task)])
    manager.runs[blocked.id] = blocked
    await manager.process_run(blocked)
    assert blocked.items[0].status == Status.PENDING


@pytest.mark.asyncio
async def test_restart_loads_all_runs_before_scheduling(setup):
    from dataclasses import replace

    manager, run, _, _ = setup
    manager.storage.save_run(run)
    other = replace(run, id="other")
    manager.storage.save_run(other)
    manager.runs.clear()

    async def process(run):
        assert set(manager.runs) == {"run", "other"}

    manager.process_run = AsyncMock(side_effect=process)
    await manager.restore_from_storage()
    assert manager.process_run.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [255, -1])
async def test_asyncssh_transport_exit_is_unknown_not_rejected(setup, code):
    manager, run, ssh, _ = setup

    async def command(cmd, **kwargs):
        return ("", "lost", code) if cmd.startswith("sbatch") else ("", "", 0)

    ssh.run_command.side_effect = command
    await manager.process_run(run)
    assert run.items[0].status == Status.SUBMISSION_UNKNOWN
    assert run.items[1].status == Status.PENDING


@pytest.mark.asyncio
async def test_explicit_retry_retains_returned_id_in_history(setup):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run, query_error=True)
    await manager.process_run(run)
    item = run.items[0]
    first = item.submission_attempts[-1].id
    assert item.job_id == "42"
    # An operator has independently checked the original scheduler. The
    # accounting outage itself is never authority for an automatic retry.
    await manager.submissions.resolve(
        run,
        item,
        attempt_id=first,
        action="retry",
        confirm_not_submitted=True,
    )
    assert item.job_id is None
    assert disk_item(manager).submission_attempts[0].job_id == "42"
    assert disk_item(manager).submission_attempts[0].resolution == "not_submitted"


@pytest.mark.asyncio
async def test_retry_rejects_positive_scheduler_evidence(setup):
    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run, lose=True)
    await manager.process_run(run)
    item = run.items[0]
    with pytest.raises(SubmissionConflict, match="matching jobs"):
        await manager.submissions.resolve(
            run,
            item,
            attempt_id=item.submission_attempts[-1].id,
            action="retry",
            confirm_not_submitted=True,
        )
    assert item.status == Status.SUBMISSION_UNKNOWN


@pytest.mark.asyncio
async def test_dirty_save_preserves_durability_and_retries_disk_errors(setup, monkeypatch):
    import os

    manager, run, ssh, _ = setup
    ssh.run_command.side_effect = responses(manager, run)
    await manager.process_run(run)
    calls = []
    original = os.fsync

    def sync(fd):
        calls.append(fd)
        return original(fd)

    monkeypatch.setattr(os, "fsync", sync)
    manager.storage.mark_dirty(run.id)
    manager.save_dirty()
    assert calls  # Normal dirty saves must still fsync version-3 records.

    def fail(fd):
        raise OSError("disk unavailable")

    monkeypatch.setattr(os, "fsync", fail)
    manager.storage.mark_dirty(run.id)
    manager.save_dirty()
    assert run.id in manager.storage._dirty_runs
    assert disk_item(manager).job_id == "42"
    monkeypatch.setattr(os, "fsync", original)
    manager.save_dirty()
    assert run.id not in manager.storage._dirty_runs
