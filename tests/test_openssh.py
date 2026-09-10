"""Real OpenSSH multiplexing against a disposable, key-authenticated SSH server."""

import asyncio
import base64
import os
import shlex
import shutil
import socket
from pathlib import Path

import asyncssh
import pytest

from scripthut.config_schema import SSHConfig
from scripthut.ssh.openssh import OpenSSHClient
from scripthut.ssh.transport import TransportError

pytestmark = pytest.mark.skipif(
    os.name != "posix" or shutil.which("ssh") is None,
    reason="Requires POSIX and an OpenSSH client",
)


@pytest.fixture
async def ssh_master(tmp_path):
    """Only this fixture owns the master; count every actual TCP connection."""
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    key = asyncssh.generate_private_key("ssh-ed25519")
    key_path = tmp_path / "key"
    key.write_private_key(str(key_path))
    key_path.chmod(0o600)
    authorized = tmp_path / "authorized"
    authorized.write_bytes(key.export_public_key())
    state = {"connections": 0, "commands": [], "active": 0, "peak": 0}
    tasks = set()

    class Server(asyncssh.SSHServer):
        def connection_made(self, connection):
            state["connections"] += 1

        def session_requested(self):
            if state.get("refuse_sessions"):
                raise asyncssh.ChannelOpenError(asyncssh.OPEN_RESOURCE_SHORTAGE, "Session limit")
            return asyncssh.SSHServerProcess(handle, None, 3, False)

    async def handle(process):
        task = asyncio.current_task()
        tasks.add(task)
        state["commands"].append(process.command)
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        child = None
        try:
            if process.command == "terminal-test":
                process.stdout.write(f"size:{process.term_size[:2]}\n")
                while True:
                    try:
                        line = await process.stdin.readline()
                    except asyncssh.TerminalSizeChanged:
                        process.stdout.write(f"size:{process.term_size[:2]}\n")
                        continue
                    if not line or line.strip() == "quit":
                        process.exit(7)
                        return
                    process.stdout.write(f"echo:{line}")
            else:
                child = await asyncio.create_subprocess_shell(
                    process.command or "true", cwd=tmp_path,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                out, err = await child.communicate()
                process.stdout.write(out.decode())
                process.stderr.write(err.decode())
                process.exit(child.returncode)
        finally:
            if child is not None and child.returncode is None:
                child.kill()
                await child.wait()
            state["active"] -= 1
            tasks.discard(task)

    async with asyncssh.listen(
        "127.0.0.1", 0, server_factory=Server, server_host_keys=[host_key],
        authorized_client_keys=str(authorized),
    ) as server:
        port = server.get_port()
        known_hosts = tmp_path / "known_hosts"
        known_hosts.write_text(f"[127.0.0.1]:{port} {host_key.export_public_key().decode()}")
        # Short socket paths also work on macOS's smaller Unix socket limit.
        import tempfile
        with tempfile.TemporaryDirectory(prefix="sh-mux-") as directory:
            path = Path(directory) / "master.sock"
            config = SSHConfig(
                host="127.0.0.1", user="test", port=port, transport="openssh",
                control_path=path, known_hosts=known_hosts,
            )

            async def start_master():
                master = await asyncio.create_subprocess_exec(
                    shutil.which("ssh"), "-F", "none", "-M", "-N", "-S", str(path),
                    "-i", str(key_path), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
                    "-o", "StrictHostKeyChecking=yes",
                    "-o", f"UserKnownHostsFile={known_hosts}",
                    "-p", str(port), "-l", "test", "127.0.0.1",
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
                )
                for _ in range(200):
                    if path.exists():
                        return master
                    if master.returncode is not None:
                        raise AssertionError((await master.stderr.read()).decode())
                    await asyncio.sleep(0.01)
                master.kill()
                await master.wait()
                raise AssertionError("Master did not create socket")

            masters = [await start_master()]

            async def restart():
                if masters[-1].returncode is None:
                    masters[-1].terminate()
                    await masters[-1].wait()
                masters.append(await start_master())

            client = OpenSSHClient(config)
            try:
                yield client, state, masters, restart, tmp_path
            finally:
                await client.disconnect()
                for master in masters:
                    if master.returncode is None:
                        master.terminate()
                    await master.wait()
                for task in list(tasks):
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)


async def test_commands_files_and_master_ownership(ssh_master):
    client, state, masters, _, tmp_path = ssh_master
    await client.connect()
    entries = []
    client.on_command = entries.append
    name = "file with spaces ' and $(touch SHOULD_NOT_EXIST)"
    quoted = shlex.quote(name)
    out, err, code = await client.run_command(
        f"cat > {quoted} <<'SCRIPT'\nprintf 'hello\\n'; printf 'stderr\\n' >&2; exit 7\nSCRIPT\n"
        f"sh {quoted}",
    )
    assert (out, err, code) == ("hello\n", "stderr\n", 7)
    assert not (tmp_path / "SHOULD_NOT_EXIST").exists()
    assert "printf" in (await client.run_command(f"cat {quoted}"))[0]
    binary = bytes(range(256))
    (tmp_path / "binary").write_bytes(binary)
    encoded, _, code = await client.run_command("base64 binary")
    assert code == 0 and base64.b64decode(encoded) == binary
    assert entries[0].exit_code == 7 and entries[0].stderr == "stderr\n"
    assert client.health.last_successful_check is not None
    assert client.health.first_observed_at is not None
    await client.disconnect()
    assert masters[0].returncode is None
    assert client.control_path.exists()
    assert state["connections"] == 1


async def test_missing_stale_and_replaced_socket(ssh_master):
    client, state, masters, restart, _ = ssh_master
    await client.connect()
    previous = client.health.first_observed_at
    masters[0].terminate()
    await masters[0].wait()
    health = await client.check_connection(force=True)
    assert health.state == "disconnected" and not health.socket_exists
    with pytest.raises(TransportError):
        await client.run_command("touch NEVER")
    client.control_path.write_text("stale regular file")
    health = await client.check_connection(force=True)
    assert health.state == "connection_failed" and health.socket_exists
    assert not health.master_responsive
    client.control_path.unlink()  # Fixture-owned path only.
    stale = socket.socket(socket.AF_UNIX)
    stale.bind(str(client.control_path))
    stale.close()
    health = await client.check_connection(force=True)
    assert health.state == "connection_failed"
    assert state["connections"] == 1
    client.control_path.unlink()
    await restart()
    await client.check_connection(force=True)
    assert client.is_connected
    assert client.health.first_observed_at > previous
    assert state["connections"] == 2  # Only the explicitly restarted fixture master.


async def test_master_dies_between_check_and_use_without_network_fallback(ssh_master, monkeypatch):
    client, state, masters, _, _ = ssh_master
    execute = client._execute

    async def race(argv, timeout):
        result = await execute(argv, timeout)
        if "check" in argv:
            masters[0].terminate()
            await masters[0].wait()
        return result

    monkeypatch.setattr(client, "_execute", race)
    health = await client.check_connection(force=True)
    assert health.state == "connection_failed"
    assert health.master_responsive  # Check succeeded; remote use did not.
    assert health.last_successful_check is None
    assert state["connections"] == 1
    assert state["commands"] == []
    assert not client._children


async def test_concurrency_timeout_and_cancellation(ssh_master):
    client, state, masters, _, _ = ssh_master
    await client.connect()
    await asyncio.gather(*(client.run_command("sleep 0.05") for _ in range(6)))
    assert state["peak"] == 2
    with pytest.raises(TransportError, match="timed out"):
        await client.run_command("sleep 0.1", timeout=0.01)
    assert not client._children
    pending = asyncio.create_task(client.run_command("sleep 0.1"))
    while not client._children:
        await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not client._children
    assert masters[0].returncode is None


async def test_probe_coalescing_backoff_and_force(ssh_master, monkeypatch):
    client, state, masters, _, _ = ssh_master
    await asyncio.gather(*(client.check_connection(force=True) for _ in range(8)))
    assert state["commands"] == ["true"]
    await client.check_connection()
    assert state["commands"] == ["true"]
    masters[0].terminate()
    await masters[0].wait()
    await client.check_connection(force=True)
    calls = 0
    original = client._socket_stat

    def counted():
        nonlocal calls
        calls += 1
        return original()

    monkeypatch.setattr(client, "_socket_stat", counted)
    await client.check_connection()
    assert calls == 0
    await client.check_connection(force=True)
    assert calls == 1 and client._backoff == 60


async def read_until(process, needle):
    data = b""
    async with asyncio.timeout(5):
        while needle not in data:
            chunk = await process.stdout.read(4096)
            assert chunk, data
            data += chunk
    return data


async def test_terminal_input_resize_exit_and_disconnect(ssh_master):
    client, state, masters, _, _ = ssh_master
    process = await client.create_interactive_session("terminal-test", term_size=(81, 25))
    await read_until(process, b"size:(81, 25)")
    process.stdin.write(b"hello\n")
    await read_until(process, b"echo:hello")
    process.change_terminal_size(103, 41)
    await read_until(process, b"size:(103, 41)")
    process.stdin.write(b"quit\n")
    await asyncio.wait_for(process.wait_closed(), 5)
    assert process.returncode == 7
    assert process.fd == -1
    second = await client.create_interactive_session("terminal-test")
    await read_until(second, b"size:")
    await client.disconnect()
    assert second.returncode is not None and second.fd == -1
    assert masters[0].returncode is None
    assert state["connections"] == 1


async def test_permission_error(ssh_master, monkeypatch):
    client, _, _, _, _ = ssh_master

    def denied():
        raise PermissionError("socket directory is inaccessible")

    with monkeypatch.context() as patch:
        patch.setattr(client, "_socket_stat", denied)
        health = await client.check_connection(force=True)
        assert health.state == "connection_failed"
        assert "inaccessible" in health.error

async def test_runtime_cli_backend_submission_and_log_retrieval(ssh_master):
    from scripthut.backends.slurm import SlurmBackend
    from scripthut.cli import _ssh_client_for
    from scripthut.config_schema import SlurmBackendConfig
    from scripthut.runtime import init_backend

    client, state, masters, _, tmp_path = ssh_master
    config = SlurmBackendConfig(name="socket", ssh=client.config)
    cli_client = _ssh_client_for(config)
    assert isinstance(cli_client, OpenSSHClient)
    await cli_client.connect()
    runtime = await init_backend(config)
    assert runtime.status.connected
    assert isinstance(runtime.ssh_client, OpenSSHClient)
    assert isinstance(runtime.backend, SlurmBackend)
    try:
        # Test-only scheduler commands exercise the real heredoc/verification path.
        original = runtime.ssh_client.run_command
        sbatch = tmp_path / "sbatch"
        sbatch.write_text('#!/bin/sh\ncat > submission.sh\necho "Submitted batch job 123"\n')
        sbatch.chmod(0o700)
        squeue = tmp_path / "squeue"
        squeue.write_text('#!/bin/sh\necho 123\n')
        squeue.chmod(0o700)

        async def with_path(command, timeout=30):
            prefixed = f"export PATH={shlex.quote(str(tmp_path))}:$PATH; {command}"
            return await original(prefixed, timeout)

        runtime.ssh_client.run_command = with_path
        script = "#!/bin/sh\nprintf 'quoted script\\n'\n"
        assert await runtime.backend.submit_job(script) == "123"
        assert (tmp_path / "submission.sh").read_text() == script + "\n"
        log = tmp_path / "job.log"
        log.write_text("first\nlast\n")
        content, error = await runtime.backend.fetch_log("123", str(log), tail_lines=1)
        assert error is None and content.strip() == "last"
        assert len(runtime.command_log) > 0
    finally:
        await runtime.ssh_client.disconnect()
        await cli_client.disconnect()
    assert masters[0].returncode is None and state["connections"] == 1


async def test_connection_api_and_card_preserve_cached_details(ssh_master):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    import httpx
    from fastapi import FastAPI

    from scripthut.api import make_api_router
    from scripthut.main import templates
    from scripthut.models import ConnectionStatus
    from scripthut.runtime import BackendState

    client, state, masters, _, _ = ssh_master
    bs = BackendState(
        name="socket", backend_type="slurm", ssh_client=client,
        status=ConnectionStatus(
            connected=False, host=client.host, job_count=12, disk_avail_bytes=99,
        ),
    )
    app = FastAPI()
    app.include_router(make_api_router(SimpleNamespace(
        backends={"socket": bs}, notify_poll=MagicMock(),
    )))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver",
    ) as http:
        url = "/api/v1/backends/socket/connection"
        assert (await http.get(url)).json()["last_successful_check"] is None
        denied = await http.post(url + "/check", headers={"Origin": "https://other.example"})
        assert denied.status_code == 403 and state["commands"] == []
        checked = await http.post(url + "/check", headers={"Origin": "http://testserver"})
        assert checked.status_code == 200 and checked.json()["state"] == "connected"
        last_success = checked.json()["last_successful_check"]
        masters[0].terminate()
        await masters[0].wait()
        failed = (await http.post(url + "/check")).json()
        assert failed["state"] == "disconnected"
        assert failed["last_successful_check"] == last_success
        assert failed["first_observed_at"] is None
        assert not bs.status.connected and bs.status.job_count == 12
        assert bs.status.disk_avail_bytes == 99
        assert (await http.get("/api/v1/backends/missing/connection")).status_code == 404
    html = templates.env.get_template("backends_status.html").render(
        backends={"socket": bs}, backend_usage={},
    )
    assert "Check connection" in html
    assert "Connection first observed" in html
    assert "Terminal connection command" in html
    assert "StrictHostKeyChecking=yes" in html
    assert "ControlPersist=8h" in html


async def test_interactive_no_fallback_when_master_disappears(ssh_master, monkeypatch):
    from scripthut.ssh.pty_process import PTYProcess

    client, state, masters, _, _ = ssh_master
    original = PTYProcess.start

    async def race(argv, env, size):
        masters[0].terminate()
        await masters[0].wait()
        return await original(argv, env, size)

    monkeypatch.setattr(PTYProcess, "start", race)
    process = await client.create_interactive_session("touch NEVER")
    await asyncio.wait_for(process.wait_closed(), 5)
    assert process.returncode == 255
    assert state["connections"] == 1 and state["commands"] == []


async def test_disconnect_during_spawn_reaps_owned_child(ssh_master, monkeypatch):
    client, _, masters, _, _ = ssh_master
    entered, release = asyncio.Event(), asyncio.Event()
    spawn = asyncio.create_subprocess_exec

    async def delayed_spawn(*args, **kwargs):
        entered.set()
        await release.wait()
        return await spawn(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_spawn)
    operation = asyncio.create_task(client.run_command("sleep 0.2"))
    await entered.wait()
    shutdown = asyncio.create_task(client.disconnect())
    await asyncio.sleep(0)
    assert not shutdown.done()
    release.set()
    await asyncio.wait_for(shutdown, 5)
    await asyncio.gather(operation, return_exceptions=True)
    assert not client._children
    assert client.health.state == "disconnected"
    assert masters[0].returncode is None


async def test_healthy_probe_does_not_replace_connection_age(ssh_master):
    client, state, masters, _, _ = ssh_master
    await client.connect()
    first = client.health.first_observed_at
    await client.check_connection(force=True)
    assert client.health.first_observed_at == first
    assert state["connections"] == 1 and masters[0].returncode is None


async def test_session_limit_does_not_trigger_direct_fallback(ssh_master):
    client, state, masters, _, _ = ssh_master
    await client.connect()
    state["refuse_sessions"] = True
    with pytest.raises(TransportError):
        await client.run_command("true")
    assert not client.is_connected
    assert state["connections"] == 1 and masters[0].returncode is None
    state["refuse_sessions"] = False
    assert (await client.run_command("true"))[2] == 0


async def test_connection_loss_during_command_is_not_retried(ssh_master):
    client, state, masters, _, _ = ssh_master
    command = "sleep 0.2"
    operation = asyncio.create_task(client.run_command(command))
    async with asyncio.timeout(5):
        while command not in state["commands"]:
            await asyncio.sleep(0.01)
    masters[0].terminate()
    await masters[0].wait()
    with pytest.raises(TransportError):
        await operation
    assert state["commands"].count(command) == 1
    assert state["connections"] == 1
    assert not client._children and not client.is_connected


async def test_submission_recovers_after_master_loss_and_controller_restart(ssh_master):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from scripthut.backends.slurm import SlurmBackend
    from scripthut.runs.manager import RunManager
    from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
    from scripthut.runs.storage import RunStorageManager

    client, observations, masters, restart, tmp_path = ssh_master
    await client.connect()
    sbatch = tmp_path / "sbatch"
    sbatch.write_text(
        '#!/bin/sh\ncat > submitted-script\n'
        'printf "123|%s|test\\n" "${1#--job-name=}" >> allocations\n'
        'sleep 1\necho "Submitted batch job 123"\n'
    )
    sbatch.chmod(0o700)
    for name in ("squeue", "sacct"):
        path = tmp_path / name
        path.write_text('#!/bin/sh\ncat allocations\n')
        path.chmod(0o700)
    original = client.run_command

    async def with_path(command, timeout=30):
        return await original(f"export PATH={shlex.quote(str(tmp_path))}:$PATH; {command}", timeout)

    client.run_command = with_path
    config = SimpleNamespace(get_backend=lambda name: SimpleNamespace(max_concurrent=4))
    storage = RunStorageManager(tmp_path / "runs")
    backend = SlurmBackend(client)
    manager = RunManager(config, {"b": client}, storage, {"b": backend})
    manager._resolve_environment = lambda run, task: ({}, "")
    run = Run("loss", "workflow", "b", datetime.now(UTC), [
        RunItem(TaskDefinition(id="task", name="task", command="true")),
    ], 1, log_dir=str(tmp_path / "logs"))
    manager.runs[run.id] = run
    task = asyncio.create_task(manager.process_run(run))
    try:
        async with asyncio.timeout(5):
            while not (tmp_path / "allocations").exists():
                await asyncio.sleep(0.01)
        masters[0].terminate()
        await masters[0].wait()
        await asyncio.wait_for(task, 5)
        assert run.items[0].status == RunItemStatus.SUBMISSION_UNKNOWN
        assert storage.load_all_runs()[run.id].items[0].submission_unresolved
        await restart()
        await client.check_connection(force=True)
        restored = RunManager(config, {"b": client}, storage, {"b": backend})
        await restored.restore_from_storage()
        assert restored.runs[run.id].items[0].job_id == "123"
        assert restored.runs[run.id].items[0].status == RunItemStatus.SUBMITTED
        assert len((tmp_path / "allocations").read_text().splitlines()) == 1
        assert observations["connections"] == 2
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
