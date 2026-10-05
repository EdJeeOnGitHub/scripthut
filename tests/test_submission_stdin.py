"""Large submissions cross a real process boundary without large argv entries."""

import asyncio
import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripthut.backends.local import LocalExecClient
from scripthut.backends.slurm import SlurmBackend
from scripthut.config_schema import SSHConfig
from scripthut.ssh.client import SSHClient
from scripthut.ssh.openssh import OpenSSHClient
from scripthut.ssh.transport import ExecutionNotStartedError, TransportError
from scripthut.submission import SubmissionAttempt


@pytest.mark.asyncio
async def test_large_stdin_through_real_openssh_process(tmp_path, monkeypatch):
    fake = tmp_path / "ssh"
    fake.write_text("""#!/usr/bin/env python3
import sys, hashlib
assert max(map(len, sys.argv)) < 1024
print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())
""")
    fake.chmod(0o700)
    client = OpenSSHClient(
        SSHConfig(
            transport="openssh",
            host="cluster",
            port=22,
            user="alice",
            control_path=tmp_path / "socket",
            known_hosts=tmp_path / "known_hosts",
        )
    )
    monkeypatch.setattr(client, "_argv", lambda: [str(fake)])
    monkeypatch.setattr(client, "_socket_stat", lambda: (1, 2))
    entries = []
    client.on_command = entries.append
    script = "#!/bin/bash\n# λ '$() `literal`\n" + "# padding\n" * 240000
    out, err, code = await client.run_command("sbatch --job-name=test", input=script)
    assert out.strip() == hashlib.sha256(script.encode()).hexdigest()
    assert code == 0 and not err
    assert entries[0].command == "sbatch --job-name=test"
    assert not client._children


@pytest.mark.asyncio
async def test_spawn_error_is_definitely_not_started(tmp_path, monkeypatch):
    client = OpenSSHClient(
        SSHConfig(
            transport="openssh",
            host="cluster",
            port=22,
            user="alice",
            control_path=tmp_path / "socket",
            known_hosts=tmp_path / "known_hosts",
        )
    )
    monkeypatch.setattr(client, "_argv", lambda: [str(tmp_path / "missing")])
    monkeypatch.setattr(client, "_socket_stat", lambda: (1, 2))
    with pytest.raises(ExecutionNotStartedError):
        await client.run_command("sbatch", input="true")
    assert not client._children


@pytest.mark.asyncio
async def test_timeout_and_cancellation_reap_stdin_process(tmp_path):
    client = OpenSSHClient(
        SSHConfig(
            transport="openssh",
            host="cluster",
            port=22,
            user="alice",
            control_path=tmp_path / "socket",
            known_hosts=tmp_path / "known_hosts",
        )
    )
    with pytest.raises(TransportError, match="timed out"):
        await client._execute(["/bin/sleep", "60"], 0.01, input="x" * 200000)
    assert not client._children
    task = asyncio.create_task(client._execute(["/bin/sleep", "60"], 60, input="x" * 200000))
    while not client._children:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not client._children


@pytest.mark.asyncio
async def test_asyncssh_passes_stdin_without_logging_payload(tmp_path):
    client = SSHClient(host="cluster", user="alice", key_path=tmp_path / "key")
    client._connection = SimpleNamespace(
        is_closed=lambda: False,
        run=AsyncMock(return_value=SimpleNamespace(stdout="ok", stderr="", exit_status=0)),
    )
    entries = []
    client.on_command = entries.append
    assert await client.run_command("cat", input="λ\n") == ("ok", "", 0)
    assert client._connection.run.call_args.kwargs["input"] == "λ\n"
    assert entries[0].command == "cat"


@pytest.mark.asyncio
async def test_local_stdin_and_no_input():
    client = LocalExecClient()
    assert await client.run_command("cat", input="λ\n") == ("λ\n", "", 0)
    assert await client.run_command("true") == ("", "", 0)


@pytest.mark.asyncio
async def test_sbatch_receives_script_as_stdin():
    ssh = SimpleNamespace(run_command=AsyncMock(return_value=("Submitted batch job 42", "", 0)))
    backend = SlurmBackend(ssh)
    attempt = SubmissionAttempt("id", datetime.now(UTC), "task--sh-id", "b", "alice")
    accepted = []
    script = "#!/bin/bash\n" + "# λ\n" * 600000
    await backend.submit_attempt(script, attempt, lambda *args: accepted.append(args))
    ssh.run_command.assert_awaited_once_with("sbatch --job-name=task--sh-id", input=script)
    assert accepted[0][0] == "42"


@pytest.mark.asyncio
async def test_asyncssh_stdin_against_real_server(tmp_path):
    import asyncssh
    host_key = asyncssh.generate_private_key('ssh-ed25519')
    key = asyncssh.generate_private_key('ssh-ed25519')
    key_path = tmp_path / 'key'
    key.write_private_key(str(key_path))
    authorized = tmp_path / 'authorized'
    authorized.write_bytes(key.export_public_key())

    async def handle(process):
        payload = await process.stdin.read()
        process.stdout.write(hashlib.sha256(payload.encode()).hexdigest())
        process.exit(0)

    async with asyncssh.listen('127.0.0.1', 0, server_host_keys=[host_key],
                              authorized_client_keys=str(authorized),
                              process_factory=handle) as server:
        port = server.get_port()
        known_hosts = tmp_path / 'known_hosts'
        known_hosts.write_text(f'[127.0.0.1]:{port} {host_key.export_public_key().decode()}')
        client = SSHClient(host='127.0.0.1', port=port, user='alice', key_path=key_path,
                           known_hosts=known_hosts)
        script = '# λ $()\n' * 300000
        try:
            out, err, code = await client.run_command('sbatch', input=script)
            assert out == hashlib.sha256(script.encode()).hexdigest()
            assert not err and code == 0
        finally:
            await client.disconnect()
