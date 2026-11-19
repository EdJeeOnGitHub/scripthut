"""Exercise rental ownership across retries and process restarts."""

import httpx
import pytest

from scripthut.backends.vast import VastBackend
from scripthut.config_schema import ScriptHutConfig
from scripthut.runs.models import TaskDefinition


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
    import asyncssh

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
