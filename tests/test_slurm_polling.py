"""Scheduler budgets must hold across concurrent aliases and failed commands."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from scripthut.backends.slurm import SlurmBackend
from scripthut.backends.slurm_polling import SlurmCommandGate
from scripthut.config_schema import SlurmBackendConfig, SSHConfig
from scripthut.runtime import init_backend


@pytest.fixture
def clock():
    now = [1000.0]

    async def sleep(delay):
        now[0] += delay

    with (
        patch("scripthut.backends.slurm_polling.time.monotonic", side_effect=lambda: now[0]),
        patch("scripthut.backends.slurm_polling.asyncio.sleep", side_effect=sleep),
    ):
        yield now


async def test_aliases_coalesce_and_commands_are_spaced(clock):
    starts = []

    async def execute(command, **kwargs):
        starts.append((clock[0], command, kwargs))
        return "", "", 0

    clients = [AsyncMock(user="u"), AsyncMock(user="u")]
    for client in clients:
        client.run_command.side_effect = execute
    gate = SlurmCommandGate(60, 120)
    backends = [SlurmBackend(c, command_gate=gate) for c in clients]
    await asyncio.gather(*(b.get_jobs("u") for b in backends))
    assert len(starts) == 1
    await gate.run(clients[1], "sacct -j 1", timeout=15)
    await gate.run(clients[0], "scancel 1", mutation=True)
    assert [s[0] for s in starts] == [1000, 1060, 1120]
    assert starts[1][2]["timeout"] == 15


async def test_expiry_and_mutation_invalidate_queue(clock):
    client = AsyncMock()
    client.run_command.return_value = ("", "", 0)
    gate = SlurmCommandGate(60, 120)
    await gate.run(client, "squeue", cache_queue=True)
    clock[0] += 120
    await gate.run(client, "squeue", cache_queue=True)
    assert client.run_command.await_count == 2
    await gate.run(client, "sbatch", input="script", mutation=True)
    await gate.run(client, "squeue", cache_queue=True)
    assert client.run_command.await_count == 4


async def test_failures_consume_budget_and_are_not_cached(clock):
    client = AsyncMock()
    client.run_command.side_effect = [RuntimeError("disconnected"), ("", "bad", 1), ("", "", 0)]
    gate = SlurmCommandGate(60, 120)
    with pytest.raises(RuntimeError):
        await gate.run(client, "squeue", cache_queue=True)
    await gate.run(client, "squeue", cache_queue=True)
    await gate.run(client, "squeue", cache_queue=True)
    assert clock[0] == 1120
    assert client.run_command.await_count == 3


async def test_cancelled_wait_does_not_execute_or_consume_slot():
    gate = SlurmCommandGate(60)
    client = AsyncMock()
    client.run_command.return_value = ("", "", 0)
    await gate.run(client, "squeue")
    waiting = asyncio.Event()

    async def wait(delay):
        waiting.set()
        await asyncio.Event().wait()

    with patch("scripthut.backends.slurm_polling.asyncio.sleep", side_effect=wait):
        task = asyncio.create_task(gate.run(client, "sbatch", mutation=True))
        await waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert client.run_command.await_count == 1
    assert not gate._lock.locked()


async def test_disabled_capacity_sweep_sends_no_commands():
    client = AsyncMock()
    backend = SlurmBackend(client, poll_cluster_info=False)
    assert await backend.get_cluster_info() is None
    client.run_command.assert_not_called()


async def test_runtime_wires_budget_and_capacity_policy():
    cfg = SlurmBackendConfig(
        name="cluster", ssh=SSHConfig(host="cluster.example", user="u"),
        scheduler_min_interval=60, scheduler_query_cache_seconds=120,
        poll_cluster_info=False,
    )
    client = AsyncMock()
    with patch("scripthut.runtime.create_ssh_client", return_value=client):
        bs = await init_backend(cfg)
    assert bs.backend._command_gate.min_interval == 60
    assert bs.backend._command_gate.query_cache_seconds == 120
    assert await bs.backend.get_cluster_info() is None


async def test_defaults_keep_budget_disabled():
    gate = SlurmCommandGate()
    client = AsyncMock()
    client.run_command.return_value = ("", "", 0)
    await asyncio.gather(*(gate.run(client, "squeue", cache_queue=True) for _ in range(2)))
    assert client.run_command.await_count == 2
