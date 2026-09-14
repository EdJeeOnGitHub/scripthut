"""Browser authentication boundary and lifecycle tests; no real credentials."""

import asyncio
import re
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from scripthut.browser_login import LoginManager, make_login_router
from scripthut.login_config import BrowserLoginConfig
from scripthut.ssh.client import SSHClient

ORIGIN = "http://127.0.0.1"


class Process:
    def __init__(self):
        self.stdin = self
        self.stdout = asyncio.StreamReader()
        self.stdout.feed_data(b"Password: ")
        self.returncode = None
        self.closed = asyncio.Event()
        self.input = b""
        self.signal_count = 0
        self.win_race = False

    def write(self, data):
        self.input += data

    def change_terminal_size(self, width, height):
        pass

    def finish(self, code=0):
        if self.returncode is None:
            self.returncode = code
            self.stdout.feed_eof()
            self.closed.set()

    def terminate(self):
        self.signal_count += 1
        self.finish(0 if self.win_race else -15)

    def close(self):
        self.finish(-1)

    async def wait_closed(self):
        await self.closed.wait()


def application(monkeypatch, timeout=300):
    config = BrowserLoginConfig(
        enabled=True,
        private_access_verified=True,
        allowed_origins=[ORIGIN],
        host_backend="host",
        profiles_path="/private/profiles.toml",
        profiles={"cluster": "cluster"},
        timeout=timeout,
    )
    processes = []

    async def create(*args, **kwargs):
        assert kwargs["no_echo"] is True
        process = Process()
        processes.append(process)
        return process

    host = MagicMock(spec=SSHClient)
    host.create_interactive_session = AsyncMock(side_effect=create)
    state = SimpleNamespace(
        config=SimpleNamespace(browser_login=config),
        notify_poll=MagicMock(),
        backends={
            "cluster": SimpleNamespace(enabled=True),
            "host": SimpleNamespace(ssh_client=host),
        },
    )
    state.login_manager = LoginManager(state)
    monkeypatch.setattr(
        "scripthut.browser_login.check_backend_connection", AsyncMock(return_value=True)
    )
    app = FastAPI()
    app.include_router(make_login_router(state))
    return app, state.login_manager, processes, host


def csrf(page):
    return re.search(r'data-csrf="([^"]+)"', page.text)[1]


@pytest.mark.asyncio
async def test_http_ownership_origin_csrf_and_duplicates(monkeypatch):
    app, manager, processes, host = application(monkeypatch)
    transport = httpx.ASGITransport(app)
    async with httpx.AsyncClient(transport=transport, base_url=ORIGIN) as client:
        page = await client.get("/login/backend/cluster")
        assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
        assert "no-store" in page.headers["cache-control"]
        assert "HttpOnly" in page.headers["set-cookie"]
        assert "SameSite=strict" in page.headers["set-cookie"]
        token = csrf(page)
        path = "/login/backend/cluster/start"
        assert (await client.post(path)).status_code == 403
        assert (await client.post(path, headers={"Origin": ORIGIN})).status_code == 403
        headers = {"Origin": ORIGIN, "X-CSRF-Token": token}
        assert (
            await client.post(path, headers={**headers, "Origin": "https://evil.test"})
        ).status_code == 403
        started = (await client.post(path, headers=headers)).json()
        assert (await client.post(path, headers=headers)).json() == started
        assert processes == []  # A POST reserves; only the owned WebSocket can authenticate.
        async with httpx.AsyncClient(transport=transport, base_url=ORIGIN) as other:
            other_token = csrf(await other.get("/login/backend/cluster"))
            assert (await other.get(f"/login/attempt/{started['id']}")).status_code == 404
            assert (
                await other.post(path, headers={"Origin": ORIGIN, "X-CSRF-Token": other_token})
            ).status_code == 409
        cancelled = await client.post(f"/login/attempt/{started['id']}/cancel", headers=headers)
        assert cancelled.json()["state"] == "cancelled"
        host.create_interactive_session.assert_not_called()
    await manager.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("success_race", [False, True])
async def test_cancel_and_success_race(monkeypatch, success_race):
    _, manager, processes, _ = application(monkeypatch)
    attempt = manager.start("owner", "cluster")
    adapter = await manager.attach(attempt)
    processes[0].win_race = success_race
    adapter.close()
    await adapter.wait_closed()
    assert attempt.state == ("connected" if success_race else "cancelled")
    assert processes[0].signal_count == 1
    await manager.stop(attempt)
    assert processes[0].signal_count == 1
    await manager.close()


@pytest.mark.asyncio
async def test_timeout_and_controller_shutdown(monkeypatch):
    _, manager, processes, _ = application(monkeypatch, timeout=1)
    attempt = manager.start("owner", "cluster")
    await manager.attach(attempt)
    async with asyncio.timeout(3):
        while attempt.state in {"waiting", "authenticating"}:
            await asyncio.sleep(0.05)
    assert attempt.state == "timed_out"
    next_attempt = manager.start("owner", "cluster")
    await manager.attach(next_attempt)
    await manager.close()
    assert next_attempt.state == "cancelled"
    assert all(p.returncode is not None for p in processes)


@pytest.mark.asyncio
async def test_cancel_while_host_channel_is_opening(monkeypatch):
    _, manager, processes, host = application(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original = host.create_interactive_session.side_effect

    async def delayed(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    host.create_interactive_session.side_effect = delayed
    attempt = manager.start("owner", "cluster")
    opening = asyncio.create_task(manager.attach(attempt))
    await entered.wait()
    await manager.stop(attempt)
    release.set()
    await opening
    assert processes[0].returncode is not None
    assert attempt.state == "cancelled"
    await manager.close()


def test_websocket_origin_session_and_secret_log_exclusion(monkeypatch, caplog):
    app, manager, processes, host = application(monkeypatch)
    with TestClient(app, base_url=ORIGIN) as client:
        token = csrf(client.get("/login/backend/cluster"))
        headers = {"Origin": ORIGIN, "X-CSRF-Token": token}
        attempt = client.post("/login/backend/cluster/start", headers=headers).json()
        path = f"ws://127.0.0.1/login/attempt/{attempt['id']}/terminal"
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect(path, headers={"Origin": "https://evil.test"}):
                pass
        assert not processes
        with client.websocket_connect(path, headers={"Origin": ORIGIN}) as ws:
            ws.send_json({"csrf": "wrong"})
            with pytest.raises(WebSocketDisconnect):
                ws.receive_json()
        assert not processes
        with client.websocket_connect(path, headers={"Origin": ORIGIN}) as ws:
            ws.send_json({"csrf": token})
            assert ws.receive_json()["data"] == "Password: "
            ws.send_json({"type": "input", "data": "VERY_SECRET\n"})
            # A resize acknowledges an input boundary without echoing secrets.
            ws.send_json({"type": "resize", "cols": 80, "rows": 24})
        assert "VERY_SECRET" not in caplog.text
        assert "Password:" not in caplog.text
        host.run_command.assert_not_called()
        # Closing the websocket schedules helper cleanup; the watcher completes
        # asynchronously. Observe the public state without assuming TestClient
        # drains all background tasks before its context manager returns.
        deadline = time.monotonic() + 2
        while client.get(f"/login/attempt/{attempt['id']}").json()["state"] != "cancelled":
            assert time.monotonic() < deadline, "Websocket disconnect did not cancel authentication"
            time.sleep(0.01)
        assert processes[0].returncode is not None


@pytest.mark.parametrize(
    "change",
    [
        {"private_access_verified": False},
        {"allowed_origins": ["https://*.example.com"]},
        {"allowed_origins": ["http://remote.example.com"]},
        {"profiles_path": "relative"},
        {"profiles": {"cluster": "$(command)"}},
    ],
)
def test_config_requires_explicit_safe_boundary(change):
    values = dict(
        enabled=True,
        private_access_verified=True,
        allowed_origins=[ORIGIN],
        host_backend="host",
        profiles_path="/private/profiles",
        profiles={"cluster": "cluster"},
    )
    with pytest.raises(ValueError):
        BrowserLoginConfig(**{**values, **change})


@pytest.mark.asyncio
async def test_expired_reservation_never_starts_host_auth(monkeypatch):
    _, manager, processes, host = application(monkeypatch, timeout=1)
    attempt = manager.start("owner", "cluster")
    async with asyncio.timeout(3):
        while attempt.state == "waiting":
            await asyncio.sleep(0.05)
    assert attempt.state == "timed_out"
    host.create_interactive_session.assert_not_called()
    assert not processes
    await manager.close()


def test_verbose_packet_logging_cannot_capture_authentication(monkeypatch, caplog):
    import logging

    application(monkeypatch)
    with caplog.at_level(logging.DEBUG, logger="asyncssh"):
        logging.getLogger("asyncssh").debug(
            "Packet contains VERY_SECRET",
            extra={"packet": b"VERY_SECRET"},
        )
        logging.getLogger("asyncssh").info("Non-secret lifecycle message")
    assert "VERY_SECRET" not in caplog.text
    assert "Non-secret lifecycle message" in caplog.text


def test_enabled_config_requires_verified_host_and_openssh_targets():
    from scripthut.config_schema import ScriptHutConfig

    config = {
        "browser_login": dict(
            enabled=True,
            private_access_verified=True,
            allowed_origins=[ORIGIN],
            host_backend="host",
            profiles_path="/private/profiles",
            profiles={"cluster": "cluster"},
        ),
        "backends": [
            {
                "name": "host",
                "type": "slurm",
                "ssh": {
                    "host": "127.0.0.1",
                    "user": "operator",
                    "known_hosts": "/private/known_hosts",
                },
            },
            {
                "name": "cluster",
                "type": "slurm",
                "ssh": {
                    "host": "cluster.example",
                    "user": "operator",
                    "transport": "openssh",
                    "control_path": "/sockets/cluster",
                    "known_hosts": "/private/known_hosts",
                },
            },
        ],
    }
    assert ScriptHutConfig.model_validate(config).browser_login.enabled
    config["backends"][0]["ssh"]["known_hosts"] = None
    with pytest.raises(ValueError, match="verified AsyncSSH"):
        ScriptHutConfig.model_validate(config)
    config["backends"][0]["ssh"]["known_hosts"] = "/private/known_hosts"
    config["backends"][0]["ssh"]["host"] = "untrusted.example"
    with pytest.raises(ValueError, match="loopback"):
        ScriptHutConfig.model_validate(config)
