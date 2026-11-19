"""Exercise rental ownership across retries and process restarts."""

import asyncio

import asyncssh
import httpx
import pytest
from fastapi import FastAPI

from scripthut import main
from scripthut.api import make_api_router
from scripthut.backends.vast import VastBackend
from scripthut.config_schema import ScriptHutConfig
from scripthut.runs.models import RunItemStatus, TaskDefinition
from scripthut.runtime import init_runtime


class Marketplace:
    def __init__(self):
        self.instances = []
        self.creates = 0
        self.destroy_error = False

    async def list_instances(self):
        return self.instances.copy()

    async def search_offers(self, filters):
        assert filters["dph_total"] == {"lte": 0.5}
        assert filters["num_gpus"] == {"eq": 1}
        return [{"id": 421, "dph_total": 0.25}]

    async def create_instance(self, offer_id, options):
        assert offer_id == 421
        self.creates += 1
        self.instances.append({"id": 712, "label": options["label"], "actual_status": "loading"})
        raise httpx.ReadTimeout("booking response lost")

    async def destroy_instance(self, instance_id):
        assert instance_id == "712"
        if self.destroy_error:
            raise httpx.ReadTimeout("destroy response lost")
        self.instances.clear()


@pytest.fixture
def backend_factory(tmp_path, monkeypatch):
    market = Marketplace()
    monkeypatch.setattr(VastBackend, "api", property(lambda self: market))
    cfg = ScriptHutConfig.model_validate({
        "backends": [{
            "type": "vast", "name": "vast-gpu", "rental_namespace": "unit-test",
            "ssh_identity_file": "/test/key", "ssh_known_hosts_file": "/test/known_hosts",
            "default_image": "example.test/image:tag",
            "profiles": {"default": {"gpu_names": ["RTX_4090"], "max_hourly_usd": 0.5}},
        }],
    })
    return market, lambda: VastBackend(cfg.vast_backends[0], tmp_path)


@pytest.mark.asyncio
async def test_lost_create_response_and_restart_cancel(backend_factory):
    market, make_backend = backend_factory
    task = TaskDefinition(id="task", name="Task", command="true")
    env = {"SCRIPTHUT_RUN_ID": "run"}
    result = await make_backend().submit_task(task, "true", env)
    restarted = make_backend()
    assert (await restarted.submit_task(task, "true", env)).job_id == result.job_id
    assert market.creates == 1
    market.destroy_error = True
    with pytest.raises(httpx.ReadTimeout):
        await restarted.cancel_job(result.job_id)
    assert await restarted.get_job_stats([result.job_id]) == {}
    assert len(market.instances) == 1
    market.destroy_error = False
    restarted = make_backend()
    assert await restarted.get_jobs() == []
    assert (await restarted.get_job_stats([result.job_id]))[result.job_id].state == "CANCELLED"
    assert market.instances == []


@pytest.mark.asyncio
async def test_cleanup_retries_after_durable_output_collection(backend_factory):
    market, make_backend = backend_factory
    backend = make_backend()
    task = TaskDefinition(id="task", name="Task", command="true")
    result = await backend.submit_task(task, "true", {"SCRIPTHUT_RUN_ID": "run"})
    market.instances[0]["actual_status"] = "running"

    class WorkerFiles:
        async def exists(self, path):
            return path.endswith("/outputs")

    async def download(sftp, source, target):
        if source.endswith("/outputs"):
            target.mkdir()
            (target / "result.txt").write_text("result")
        else:
            target.write_text("log")

    backend._download = download

    async def observe(job_id, record, instance):
        await backend._collect(job_id, record, WorkerFiles(), 0)
        record.update(finishing="COMPLETED", code=0)
        backend._save(job_id, record)

    backend._observe = observe
    market.destroy_error = True
    assert len(await backend.get_jobs()) == 1
    assert await backend.get_job_stats([result.job_id]) == {}
    market.destroy_error = False
    restarted = make_backend()
    assert await restarted.get_jobs() == []
    assert market.instances == []
    assert (await restarted.get_job_stats([result.job_id]))[result.job_id].state == "COMPLETED"
    assert await restarted.fetch_log(result.job_id, "") == ("log", None)
    directory = restarted._directory(result.job_id)
    assert (directory / "outputs" / "result.txt").read_text() == "result"


@pytest.mark.asyncio
async def test_first_use_pin_survives_restart(backend_factory, monkeypatch):

    market, make_backend = backend_factory
    backend = make_backend()
    backend.config = backend.config.model_copy(update={
        "ssh_known_hosts_file": None, "ssh_trust_on_first_use": True,
    })
    task = TaskDefinition(id="task", name="Task", command="true")
    result = await backend.submit_task(task, "true", {"SCRIPTHUT_RUN_ID": "run"})
    assert market.creates == 1
    key = asyncssh.generate_private_key("ssh-ed25519").convert_to_public()

    async def first_key(*args, **kwargs):
        return key

    monkeypatch.setattr(asyncssh, "get_server_host_key", first_key)
    record = backend._records()[result.job_id]
    assert await backend._known_hosts(result.job_id, record, "test.host", 2200) == ([key], [], [])

    async def unexpected_key(*args, **kwargs):
        pytest.fail("Attempted to re-pin a previously pinned host")

    monkeypatch.setattr(asyncssh, "get_server_host_key", unexpected_key)
    restarted = make_backend()
    restarted.config = backend.config
    record = restarted._records()[result.job_id]
    assert await restarted._known_hosts(
        result.job_id, record, "test.host", 2200
    ) == ([key], [], [])


async def test_submit_api_runs_vast_task_over_ssh(tmp_path, backend_factory, monkeypatch):
    market, make_backend = backend_factory
    remote = tmp_path / "worker"
    monkeypatch.setattr("scripthut.backends.vast.REMOTE", str(remote))
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "data.txt").write_text("staged input\n")
    work = tmp_path / "work"
    work.mkdir()
    key = asyncssh.generate_private_key("ssh-ed25519")
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    identity = tmp_path / "identity"
    key.write_private_key(str(identity))
    authorized = tmp_path / "authorized"
    authorized.write_bytes(key.export_public_key())
    known = tmp_path / "known_hosts"

    async def run_command(process):
        child = await asyncio.create_subprocess_shell(
            process.command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await child.communicate()
        process.stdout.write(stdout.decode())
        process.stderr.write(stderr.decode())
        process.exit(child.returncode)

    async with asyncssh.listen(
        "127.0.0.1", 0, server_host_keys=[host_key],
        authorized_client_keys=str(authorized), process_factory=run_command,
        sftp_factory=asyncssh.SFTPServer,
    ) as worker:
        port = worker.get_port()
        known.write_text(f"[127.0.0.1]:{port} {host_key.export_public_key().decode()}")
        config = ScriptHutConfig.model_validate({
            "backends": [make_backend().config.model_dump() | {
                "input_root": inputs, "ssh_identity_file": str(identity),
                "ssh_known_hosts_file": str(known),
            }],
            "settings": {"data_dir": str(tmp_path / "controller")},
        })
        runtime = await init_runtime(config, restore_runs=False)
        state = main.AppState(
            config=config, backends=runtime.backends,
            run_manager=runtime.run_manager, run_storage=runtime.run_storage,
        )
        monkeypatch.setattr(main, "state", state)
        app = FastAPI()
        app.include_router(make_api_router(state))
        body = {
            "backend": "vast-gpu",
            "task": {
                "id": "task", "name": "Task", "project_id": "vast-test",
                "working_dir": str(work), "inputs": ["data.txt"],
                "outputs": ["*.txt"], "time_limit": "00:10",
                "command": 'cp "$SCRIPTHUT_INPUT_DIR/data.txt" result.txt; '
                           'cp result.txt "$SCRIPTHUT_OUTPUT_DIR/copied.txt"; cat result.txt',
            },
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test",
        ) as client:
            response = await client.post("/api/v1/tasks/run", json=body)
        assert response.is_success, response.text
        run_id = response.json()["id"]
        item = runtime.run_manager.runs[run_id].items[0]
        assert item.status == RunItemStatus.SUBMITTED
        market.instances[0].update(
            actual_status="running", public_ipaddr="127.0.0.1",
            ports={"22/tcp": [{"HostPort": str(port)}]},
        )
        async with asyncio.timeout(5):
            while item.status != RunItemStatus.COMPLETED:
                await main.poll_backend(runtime.backends["vast-gpu"])
                await asyncio.sleep(0.05)
        assert not market.instances
        output = tmp_path / "controller/vast-jobs/unit-test" / item.job_id / "outputs"
        assert (output / "copied.txt").read_text() == "staged input\n"
        assert (output / "declared-0" / "result.txt").read_text() == "staged input\n"
        log, error = await runtime.run_manager.fetch_log_file(run_id, "task")
        assert error is None and "staged input" in log
