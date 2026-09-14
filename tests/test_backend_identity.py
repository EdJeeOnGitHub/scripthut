"""Identity remains stable across views, scheduler polling, and persisted jobs."""
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

import scripthut.main as main
from scripthut.config_schema import GlobalSettings, ScriptHutConfig, SlurmBackendConfig, SSHConfig
from scripthut.identity import JobFilter
from scripthut.models import ConnectionStatus
from scripthut.runs.manager import RunManager
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
from scripthut.runs.storage import RunStorageManager
from scripthut.runtime import BackendState, init_backend, init_local_backend


def task():
    return TaskDefinition(id="test", name="test", command="hostname")


@pytest.mark.parametrize("data, expected", [
    ({}, True), ({"my_jobs_only": False}, False),
    ({"filter_user": "ed"}, True), ({"filter_user": None}, False),
    ({"filter_user": "ed", "my_jobs_only": False}, False),
])
def test_settings_migration(data, expected):
    if "filter_user" in data:
        with pytest.warns(DeprecationWarning):
            settings = GlobalSettings(**data)
    else:
        settings = GlobalSettings(**data)
    assert settings.my_jobs_only is expected
    assert "filter_user" not in settings.model_dump()


def test_filter_identity_and_cloud():
    users = {"local": "ed", "quest": "bui4696", "cloud": None}
    mine = JobFilter(users, True)
    assert mine.query_user("quest") == "bui4696"
    assert mine.matches("local", "ed")
    assert mine.matches("quest", "bui4696")
    assert not mine.matches("quest", "ed")
    assert not mine.matches("quest", None)
    assert mine.matches("cloud", "aws-batch")
    everyone = JobFilter(users, False)
    assert everyone.query_user("quest") is None
    assert everyone.matches("quest", "someone-else")


@pytest.mark.asyncio
async def test_runtime_identity_and_managed_ownership(monkeypatch, tmp_path):
    ssh = AsyncMock()
    monkeypatch.setattr("scripthut.runtime.create_ssh_client", lambda cfg: ssh)
    cfg = SlurmBackendConfig(name="quest", ssh=SSHConfig(host="quest", user="bui4696"))
    bs = await init_backend(cfg)
    assert bs.current_user == "bui4696"
    manager = RunManager(ScriptHutConfig(backends=[cfg]), {},
                         backend_users={"quest": bs.current_user})
    monkeypatch.setattr(manager, "process_run", AsyncMock())
    run = await manager._build_run([task()], "smoke", "quest", 1, None)
    assert run.items[0].user == "bui4696"
    state = main.AppState()
    state.backends = {"quest": bs}
    state.run_manager = manager
    monkeypatch.setattr(main, "state", state)
    run.items[0].user = None  # Legacy managed job, without persisted ownership.
    for enabled in (True, False):
        state.filter_enabled = enabled
        views = main._apply_job_filters(main._collect_all_job_views())
        assert views[0].user == "bui4696"
    assert run.items[0].user is None
    assert await main.filter_status() == {"enabled": False, "users": {"quest": "bui4696"}}


@pytest.mark.asyncio
async def test_local_owner_is_not_filter(monkeypatch, tmp_path):
    from scripthut.config_schema import LocalBackendConfig
    monkeypatch.setenv("USER", "incorrect-environment-user")
    bs = init_local_backend(LocalBackendConfig(name="local"), tmp_path)
    assert bs.current_user == bs.backend.current_user
    assert bs.current_user != "incorrect-environment-user"
    assert await bs.backend.get_jobs(user="different-user") == []


def test_reconcile_only_observed_owners(tmp_path):
    storage = RunStorageManager(tmp_path)
    for job_id, user in [("1", "bui4696"), ("2", "other"), ("3", "")]:
        storage.add_external_job("quest", job_id, "job", user, "running")
    assert storage.reconcile_external_jobs("quest", set(), user="bui4696") == 1
    run = storage.get_or_create_weekly_run("quest", datetime.now(UTC))
    assert run.get_item_by_job_id("2").status == RunItemStatus.RUNNING
    assert run.get_item_by_job_id("3").status == RunItemStatus.RUNNING
    assert storage.reconcile_external_jobs("quest", set()) == 2


@pytest.mark.asyncio
async def test_poll_uses_identity_for_quota_and_unfiltered_accounting(monkeypatch):
    driver = MagicMock()
    driver.get_jobs = AsyncMock(return_value=[])
    driver.get_cluster_info = AsyncMock(return_value=None)
    driver.get_disk_info = AsyncMock(return_value=None)
    driver.get_job_stats = AsyncMock(return_value={})
    bs = BackendState(name="quest", backend_type="slurm", current_user="bui4696",
                      backend=driver, status=ConnectionStatus(connected=True, host="quest"))
    run = Run(id="r", workflow_name="smoke", backend_name="quest",
              created_at=datetime.now(UTC), max_concurrent=1,
              items=[RunItem(task=task(), job_id="1", status=RunItemStatus.SETTLING)])
    from scripthut.runs.manager import RunManager
    manager = RunManager(ScriptHutConfig(), {}, job_backends={"quest": driver})
    manager.runs = {"r": run}
    state = main.AppState()
    state.backends = {"quest": bs}
    state.run_manager = manager
    monkeypatch.setattr(main, "state", state)
    await main.poll_backend(bs, filter_user="bui4696")
    driver.get_jobs.assert_awaited_once_with(user="bui4696")
    driver.get_cluster_info.assert_awaited_once_with(user="bui4696")
    driver.get_job_stats.assert_awaited_once_with(["1"], user=None)
    assert bs.poll_fresh


@pytest.mark.asyncio
async def test_toggle_routes_each_user_and_hot_reload(monkeypatch, tmp_path):
    state = main.AppState()
    state.config = ScriptHutConfig(settings=GlobalSettings(data_dir=tmp_path))
    state.backends = {
        name: BackendState(name=name, backend_type="slurm", current_user=user)
        for name, user in [("local", "ed"), ("quest", "bui4696")]
    }
    monkeypatch.setattr(main, "state", state)
    poll = AsyncMock()
    monkeypatch.setattr(main, "poll_backend", poll)
    monkeypatch.setattr(main, "jobs_partial", AsyncMock(return_value="html"))
    await main.toggle_filter(MagicMock())
    assert [c.kwargs["filter_user"] for c in poll.await_args_list] == [None, None]
    poll.reset_mock()
    await main.toggle_filter(MagicMock())
    assert [c.kwargs["filter_user"] for c in poll.await_args_list] == ["ed", "bui4696"]
    monkeypatch.setattr(main, "set_config", lambda cfg: None)
    main.reload_runtime_config(ScriptHutConfig(
        settings=GlobalSettings(data_dir=tmp_path, my_jobs_only=False),
    ))
    assert state.filter_enabled is False
    assert state.backends["quest"].current_user == "bui4696"


@pytest.mark.asyncio
async def test_overlapping_job_ids_are_scoped_to_backend(monkeypatch, tmp_path):
    from scripthut.models import HPCJob, JobState
    storage = RunStorageManager(tmp_path)
    driver = MagicMock()
    remote_job = HPCJob(job_id="42", name="external", user="bui4696",
                        state=JobState.RUNNING, partition="kellogg", time_used="0:01",
                        nodes="remote-node", cpus=1, memory="256M",
                        submit_time=None, start_time=None)
    driver.get_jobs = AsyncMock(return_value=[remote_job])
    driver.get_cluster_info = AsyncMock(return_value=None)
    driver.get_disk_info = AsyncMock(return_value=None)
    bs = BackendState(name="quest", backend_type="slurm", current_user="bui4696",
                      backend=driver, status=ConnectionStatus(connected=True, host="quest"))
    run = Run(id="local-run", workflow_name="smoke", backend_name="local",
              created_at=datetime.now(UTC), max_concurrent=1,
              items=[RunItem(task=task(), job_id="42", status=RunItemStatus.RUNNING)])
    state = main.AppState()
    state.backends = {"quest": bs}
    state.run_storage = storage
    from scripthut.runs.manager import RunManager
    state.run_manager = RunManager(ScriptHutConfig(), {}, storage=storage,
                                  job_backends={"quest": driver})
    state.run_manager.runs[run.id] = run
    monkeypatch.setattr(main, "state", state)
    await main.poll_backend(bs, filter_user="bui4696")
    assert bs.poll_fresh
    external = storage.get_or_create_weekly_run("quest", datetime.now(UTC))
    assert external.get_item_by_job_id("42").user == "bui4696"
    assert main._get_job_nodes() == {("quest", "42"): "remote-node"}
