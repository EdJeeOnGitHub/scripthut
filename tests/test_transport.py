"""Configuration, shared construction, and terminal relay contracts."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import WebSocketDisconnect
from pydantic import ValidationError

from scripthut.backends.local import LocalExecClient
from scripthut.cli import _ssh_client_for
from scripthut.config_schema import SlurmBackendConfig, SSHConfig
from scripthut.ssh.client import SSHClient
from scripthut.ssh.factory import create_ssh_client
from scripthut.terminal import handle_terminal_websocket, relay_terminal


def test_asyncssh_remains_default(tmp_path):
    config = SSHConfig(host="example.org", user="test", key_path=tmp_path / "key")
    backend = SlurmBackendConfig(name="cluster", ssh=config)
    assert config.transport == "asyncssh"
    assert isinstance(create_ssh_client(config), SSHClient)
    assert isinstance(_ssh_client_for(backend), SSHClient)


@pytest.mark.parametrize("fields", [
    {"control_path": None},
    {"control_path": "relative.sock"},
    {"control_path": "/tmp/%h.sock"},
    {"control_path": "/tmp/${HOME}.sock"},
    {"control_path": "/tmp/test\nsock"},
    {"host": "-oProxyCommand=anything"},
    {"host": "two words"},
    {"user": "test\nother"},
    {"port": 0},
    {"max_operations": 0},
    {"control_persist": "forever"},
    {"known_hosts": None},
])
def test_invalid_openssh_configuration(fields):
    args = dict(
        host="example.org", user="test", transport="openssh",
        control_path="/tmp/master.sock", known_hosts="/tmp/known_hosts",
    )
    args.update(fields)
    with pytest.raises(ValidationError):
        SSHConfig(**args)


def test_socket_requires_explicit_transport():
    with pytest.raises(ValidationError, match="transport: openssh"):
        SSHConfig(host="example.org", user="test", control_path="/tmp/socket")


def test_unsupported_platform_fails_when_selected(monkeypatch, tmp_path):
    from scripthut.ssh import openssh

    config = SSHConfig(
        host="example.org", user="test", transport="openssh",
        control_path=tmp_path / "socket", known_hosts=tmp_path / "known_hosts",
    )
    # No POSIX-only module should be imported by selecting the default transport.
    with monkeypatch.context() as patch:
        patch.setattr(openssh.os, "name", "nt")
        with pytest.raises(ValueError, match="requires a POSIX host"):
            create_ssh_client(config)
        assert isinstance(create_ssh_client(SSHConfig(host="host", user="test")), SSHClient)


async def test_local_executor_explicitly_rejects_terminal():
    async with LocalExecClient() as client:
        assert client.is_connected
        with pytest.raises(RuntimeError, match="does not support interactive"):
            await client.create_interactive_session()


async def test_relay_preserves_utf8_and_exit_and_reaps_process():
    process = MagicMock()
    chunks = iter([b"\xe2", b"\x82\xac", b""])
    process.stdout.read = AsyncMock(side_effect=lambda _: next(chunks))
    process.wait_closed = AsyncMock()
    process.returncode = 7
    websocket = MagicMock()
    websocket.receive_text = AsyncMock(side_effect=asyncio.Event().wait)
    websocket.send_json = AsyncMock()
    websocket.close = AsyncMock()
    await relay_terminal(websocket, process)
    assert [call.args[0] for call in websocket.send_json.call_args_list] == [
        {"type": "output", "data": "€"}, {"type": "exit", "code": 7},
    ]
    process.close.assert_called_once()
    assert process.wait_closed.await_count >= 1


async def test_relay_forwards_input_resize_and_closes_on_browser_exit():
    process = MagicMock()
    async def wait_for_output(_):
        await asyncio.Event().wait()
    process.stdout.read = AsyncMock(side_effect=wait_for_output)
    process.wait_closed = AsyncMock()
    websocket = MagicMock()
    websocket.receive_text = AsyncMock(side_effect=[
        json.dumps({"type": "input", "data": "hello\n"}),
        json.dumps({"type": "resize", "cols": 100, "rows": 40}),
        WebSocketDisconnect(),
    ])
    websocket.send_json = AsyncMock()
    websocket.close = AsyncMock()
    await relay_terminal(websocket, process)
    process.stdin.write.assert_called_once_with(b"hello\n")
    process.change_terminal_size.assert_called_once_with(100, 40)
    process.close.assert_called_once()
    process.wait_closed.assert_awaited_once()


async def test_terminal_handler_accepts_consumed_init(monkeypatch):
    client = MagicMock()
    client.create_interactive_session = AsyncMock()
    websocket = MagicMock()
    websocket.receive_json = AsyncMock()
    relay = AsyncMock()
    monkeypatch.setattr("scripthut.terminal.relay_terminal", relay)
    await handle_terminal_websocket(
        websocket, client, command="shell", init_data={"cols": 90, "rows": 30},
    )
    websocket.receive_json.assert_not_called()
    client.create_interactive_session.assert_awaited_once_with(
        command="shell", term_size=(90, 30),
    )
    relay.assert_awaited_once()
