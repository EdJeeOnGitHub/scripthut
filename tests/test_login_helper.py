"""Real host-helper PTY/password/detach lifecycle against disposable SSH servers."""

import asyncio
import os
import shutil
import signal
import sys
from pathlib import Path

import asyncssh
import pytest

from scripthut.ssh.login_helper import Profile

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.name != "posix" or not shutil.which("ssh"),
        reason="POSIX OpenSSH required",
    ),
]
HELPER = Path(__file__).parents[1] / "src/scripthut/ssh/login_helper.py"
SECRET = "fixture-password-only"


@pytest.fixture
async def host(tmp_path):
    key = asyncssh.generate_private_key("ssh-ed25519")
    observations = {"auth": 0, "connections": 0, "commands": []}

    class Server(asyncssh.SSHServer):
        def connection_made(self, connection):
            observations["connections"] += 1

        def begin_auth(self, user):
            return True

        def password_auth_supported(self):
            return True

        def validate_password(self, user, password):
            observations["auth"] += 1
            return user == "test" and password == SECRET

    async def process(session):
        observations["commands"].append(session.command)
        if observations.get("probe_gate"):
            await observations["probe_gate"].wait()
        session.exit(0 if session.command == "true" else 1)

    async with asyncssh.listen(
        "127.0.0.1",
        0,
        server_factory=Server,
        server_host_keys=[key],
        process_factory=process,
    ) as server:
        port = server.get_port()
        known = tmp_path / "known_hosts"
        known.write_text(f"[127.0.0.1]:{port} {key.export_public_key().decode()}")
        # Keep the Unix socket path short on macOS too.
        import tempfile

        with tempfile.TemporaryDirectory(prefix="sh-login-") as directory:
            socket = Path(directory) / "master"
            profile_file = tmp_path / "profiles.toml"
            profile_file.write_text(
                '[profiles.test]\nhost="127.0.0.1"\nuser="test"\n'
                f'port={port}\ncontrol_path="{socket}"\nknown_hosts="{known}"\ntimeout=3\n'
            )
            profile_file.chmod(0o600)
            children = []

            async def start():
                child = await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(HELPER),
                    "--profiles",
                    str(profile_file),
                    "test",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                children.append(child)
                return child

            try:
                yield start, socket, observations, profile_file
            finally:
                for child in children:
                    if child.returncode is None:
                        child.terminate()
                    await asyncio.wait_for(child.wait(), 5)
                if socket.exists():
                    cleanup = await asyncio.create_subprocess_exec(
                        "ssh",
                        "-F",
                        "none",
                        "-S",
                        str(socket),
                        "-O",
                        "exit",
                        "127.0.0.1",
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await cleanup.wait()


async def prompt(child):
    data = bytearray()
    async with asyncio.timeout(5):
        while b"password:" not in data.lower():
            chunk = await child.stdout.read(512)
            assert chunk, (data, await child.stderr.read(), await child.wait())
            data.extend(chunk)
    return bytes(data)


async def test_success_detaches_without_echo_and_reuses_master(host):
    start, socket, seen, _ = host
    child = await start()
    output = await prompt(child)
    child.stdin.write((SECRET + "\n").encode())
    await child.stdin.drain()
    assert await asyncio.wait_for(child.wait(), 5) == 0
    output += await child.stdout.read()
    assert SECRET.encode() not in output
    assert socket.exists()
    assert seen["commands"] == ["true"]
    # Browser EOF after success must not destroy the master.
    child.stdin.close()
    reused = await start()
    assert await asyncio.wait_for(reused.wait(), 5) == 0
    assert seen["connections"] == 1 and seen["auth"] == 1
    assert seen["commands"] == ["true", "true"]


@pytest.mark.parametrize("action", ["eof", "term", "hup", "timeout", "wrong_password"])
async def test_unsuccessful_attempt_cleans_up(host, action):
    start, socket, seen, _ = host
    child = await start()
    await prompt(child)
    if action == "eof":
        child.stdin.close()
    elif action in ("term", "hup"):
        child.send_signal(signal.SIGTERM if action == "term" else signal.SIGHUP)
    elif action == "wrong_password":
        child.stdin.write(b"wrong-password\n")
        await child.stdin.drain()
    assert await asyncio.wait_for(child.wait(), 7) != 0
    assert not socket.exists()
    assert seen["auth"] <= 1


async def test_duplicate_is_locked_without_new_authentication(host):
    start, _, seen, _ = host
    first = await start()
    await prompt(first)
    duplicate = await start()
    assert await asyncio.wait_for(duplicate.wait(), 3) == 3
    assert seen["connections"] == 1
    first.stdin.close()
    await first.wait()


async def test_refuses_unowned_profile_permissions_and_stale_socket(host):
    start, socket, seen, path = host
    path.chmod(0o644)
    child = await start()
    assert await child.wait() == 7
    path.chmod(0o600)
    socket.write_text("do not remove")
    child = await start()
    assert await child.wait() == 7
    assert socket.read_text() == "do not remove"
    assert seen["connections"] == 0


async def test_profile_rejects_browser_shaped_commands(host):
    _, _, _, path = host
    with pytest.raises(ValueError):
        Profile.load(path, "test; touch /tmp/unsafe")


@pytest.mark.parametrize("succeed", [False, True])
async def test_verified_host_transport_has_no_echo_and_survives_controller_disconnect(
    host, tmp_path, succeed
):
    from scripthut.ssh.client import SSHClient

    _, socket, _, profiles = host
    key = asyncssh.generate_private_key("ssh-ed25519")
    active = set()
    modes = []

    class Gateway(asyncssh.SSHServer):
        def begin_auth(self, user):
            return False

    async def handle(session):
        task = asyncio.current_task()
        active.add(task)
        modes.append(session.get_terminal_mode(asyncssh.PTY_ECHO))
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            str(HELPER),
            "--profiles",
            str(profiles),
            "test",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )

        async def incoming():
            try:
                while data := await session.stdin.read(4096):
                    child.stdin.write(data)
                    await child.stdin.drain()
            finally:
                child.stdin.close()

        async def outgoing():
            while data := await child.stdout.read(4096):
                session.stdout.write(data)

        pipes = [asyncio.create_task(incoming()), asyncio.create_task(outgoing())]
        try:
            code = await child.wait()
            await pipes[1]
            session.exit(code)
        finally:
            for pipe in pipes:
                pipe.cancel()
            await asyncio.gather(*pipes, return_exceptions=True)
            if child.returncode is None:
                child.terminate()
            await child.wait()
            active.discard(task)

    async with asyncssh.listen(
        "127.0.0.1",
        0,
        server_factory=Gateway,
        server_host_keys=[key],
        process_factory=handle,
        encoding=None,
    ) as gateway:
        known = tmp_path / "gateway_known_hosts"
        port = gateway.get_port()
        known.write_text(f"[127.0.0.1]:{port} {key.export_public_key().decode()}")
        identity = tmp_path / "gateway_identity"
        key.write_private_key(str(identity))
        client = SSHClient("127.0.0.1", "test", identity, port=port, known_hosts=known)
        logged = []
        client.on_command = logged.append
        try:
            process = await client.create_interactive_session("fixed-helper-command", no_echo=True)
            output = await prompt(process)
            if succeed:
                process.stdin.write((SECRET + "\n").encode())
                await asyncio.wait_for(process.wait_closed(), 5)
                output += await process.stdout.read()
                assert process.returncode == 0
                assert socket.exists()
            await client.disconnect()
            async with asyncio.timeout(5):
                while active:
                    await asyncio.sleep(0.01)
            assert socket.exists() == succeed
            assert modes == [0]
            assert SECRET.encode() not in output
            assert logged == []
        finally:
            await client.disconnect()
            for task in list(active):
                task.cancel()
            await asyncio.gather(*active, return_exceptions=True)


async def test_cancel_after_fork_before_verification_removes_owned_master(host):
    start, socket, seen, _ = host
    gate = asyncio.Event()
    seen["probe_gate"] = gate
    child = await start()
    await prompt(child)
    child.stdin.write((SECRET + "\n").encode())
    await child.stdin.drain()
    try:
        async with asyncio.timeout(3):
            while "true" not in seen["commands"]:
                await asyncio.sleep(0.01)
        assert socket.exists()  # ssh -f has forked, but detach is not yet committed.
        child.terminate()
        assert await asyncio.wait_for(child.wait(), 5) == 2
        assert not socket.exists()
    finally:
        gate.set()
