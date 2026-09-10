"""Session-bound browser authentication; transcripts exist only in the live relay."""

from __future__ import annotations

import asyncio
import html
import logging
import secrets
import shlex
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request, WebSocket
from fastapi.responses import FileResponse, HTMLResponse

from scripthut.login_config import BrowserLoginConfig
from scripthut.runtime import check_backend_connection
from scripthut.ssh.client import SSHClient
from scripthut.terminal import relay_terminal

COOKIE = "scripthut_login"
ASSETS = Path(__file__).parent / "login_assets"
CSP = (
    "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "
    "base-uri 'none'; frame-ancestors 'none'; form-action 'none'; object-src 'none'"
)
ACTIVE = {"waiting", "authenticating"}


class NoAuthenticationPackets(logging.Filter):
    """AsyncSSH's level-3 packet dumps contain plaintext channel input/output."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not hasattr(record, "packet")


@dataclass
class LoginAttempt:
    id: str
    backend: str
    owner: str = field(repr=False)
    state: str = "waiting"
    process: Any = field(default=None, repr=False)
    watcher: asyncio.Task[None] | None = field(default=None, repr=False)
    deadline: asyncio.Task[None] | None = field(default=None, repr=False)
    stop_reason: str | None = None
    attached: bool = False

    def public(self) -> dict[str, str]:
        return {"id": self.id, "backend": self.backend, "state": self.state}


class LoginManager:
    def __init__(self, state: Any):
        self.app_state = state
        # Disable raw packet dumps for this process when browser login is in
        # use, including unusually verbose third-party debug configurations.
        ssh_logger = logging.getLogger("asyncssh")
        if not any(isinstance(f, NoAuthenticationPackets) for f in ssh_logger.filters):
            ssh_logger.addFilter(NoAuthenticationPackets())
        self.sessions: dict[str, tuple[str, float]] = {}
        self.attempts: dict[str, LoginAttempt] = {}
        self.closed = False

    @property
    def config(self) -> BrowserLoginConfig:
        config = self.app_state.config
        if self.closed or config is None or not config.browser_login.enabled:
            raise HTTPException(404, "Browser login is not enabled")
        return cast(BrowserLoginConfig, config.browser_login)

    def origin(self, request: Any, *, required: bool = True) -> None:
        scheme = {"ws": "http", "wss": "https"}.get(request.url.scheme, request.url.scheme)
        actual = f"{scheme}://{request.url.netloc}"
        claimed = request.headers.get("origin")
        if actual not in self.config.allowed_origins or (
            (required or claimed is not None) and claimed != actual
        ):
            raise HTTPException(403, "Origin rejected")

    def session(self, request: Any, token: str | None = None) -> str:
        owner = str(request.cookies.get(COOKIE, ""))
        record = self.sessions.get(owner)
        if record is None or record[1] < time.monotonic():
            raise HTTPException(403, "Login session expired")
        if token is not None and not secrets.compare_digest(record[0], token):
            raise HTTPException(403, "CSRF token rejected")
        return owner

    def owned(self, owner: str, attempt_id: str) -> LoginAttempt:
        attempt = next((a for a in self.attempts.values() if a.id == attempt_id), None)
        if attempt is None or attempt.owner != owner:
            raise HTTPException(404, "Login attempt not found")
        return attempt

    def start(self, owner: str, backend: str) -> LoginAttempt:
        if backend not in self.config.profiles:
            raise HTTPException(404, "Backend is not configured for browser login")
        bs = self.app_state.backends.get(backend)
        if bs is None or not bs.enabled:
            raise HTTPException(409, "Backend unavailable")
        previous = self.attempts.get(backend)
        if previous and previous.state in ACTIVE:
            if previous.owner != owner:
                raise HTTPException(409, "An authentication attempt is already active")
            return previous
        attempt = LoginAttempt(secrets.token_urlsafe(24), backend, owner)
        self.attempts[backend] = attempt
        attempt.deadline = asyncio.create_task(self._expire(attempt, self.config.timeout))
        return attempt

    async def _expire(self, attempt: LoginAttempt, timeout: int) -> None:
        await asyncio.sleep(timeout)
        await self.stop(attempt, "timed_out")

    async def _watch(self, attempt: LoginAttempt) -> None:
        try:
            await attempt.process.wait_closed()
            if attempt.process.returncode == 0:
                healthy = await check_backend_connection(
                    self.app_state.backends[attempt.backend],
                    force=True,
                )
                attempt.state = "connected" if healthy else "failed"
            else:
                attempt.state = attempt.stop_reason or {
                    2: "cancelled",
                    5: "timed_out",
                }.get(attempt.process.returncode, "failed")
        except Exception:
            attempt.state = attempt.stop_reason or "failed"
        finally:
            if attempt.deadline and attempt.deadline is not asyncio.current_task():
                attempt.deadline.cancel()
            self.app_state.notify_poll()

    async def attach(self, attempt: LoginAttempt) -> LoginProcess:
        if attempt.attached or attempt.state != "waiting":
            raise HTTPException(409, "Attempt already attached or finished")
        attempt.attached = True
        attempt.state = "authenticating"
        host = self.app_state.backends.get(self.config.host_backend)
        client = host.ssh_client if host else None
        if not isinstance(client, SSHClient):
            attempt.state = "failed"
            raise HTTPException(503, "Verified host transport unavailable")
        command = shlex.join(
            [
                self.config.helper_path,
                "--profiles",
                self.config.profiles_path,
                self.config.profiles[attempt.backend],
            ]
        )
        try:
            # create_interactive_session never invokes command logging. ECHO=0
            # applies when the host PTY is allocated, before helper startup.
            attempt.process = await client.create_interactive_session(command, no_echo=True)
            attempt.watcher = asyncio.create_task(self._watch(attempt))
            if attempt.stop_reason:
                attempt.state = "authenticating"
                await self.stop(attempt, attempt.stop_reason)
            return LoginProcess(self, attempt)
        except BaseException:
            attempt.state = attempt.stop_reason or "failed"
            if attempt.deadline:
                attempt.deadline.cancel()
            raise

    async def stop(self, attempt: LoginAttempt, reason: str = "cancelled") -> None:
        if attempt.state not in ACTIVE:
            return
        attempt.stop_reason = reason
        if attempt.process is None:
            attempt.state = reason
            if attempt.deadline and attempt.deadline is not asyncio.current_task():
                attempt.deadline.cancel()
            return
        if attempt.process.returncode is None:
            attempt.process.terminate()  # Host helper handles TERM and cleans up.
        assert attempt.watcher is not None
        try:
            await asyncio.wait_for(asyncio.shield(attempt.watcher), 5)
        except TimeoutError:
            attempt.process.close()  # Host EOF/deadline remains independently enforced.
            await attempt.watcher

    async def close(self) -> None:
        self.closed = True
        await asyncio.gather(*(self.stop(a) for a in self.attempts.values()))
        for attempt in self.attempts.values():
            if attempt.deadline:
                attempt.deadline.cancel()
        self.sessions.clear()


class LoginProcess:
    """Relay adapter which signals the helper instead of killing a detached master."""

    def __init__(self, manager: LoginManager, attempt: LoginAttempt):
        self.manager, self.attempt = manager, attempt
        self.stdin, self.stdout = attempt.process.stdin, attempt.process.stdout
        self.stop_task: asyncio.Task[None] | None = None

    @property
    def returncode(self) -> int | None:
        return cast(int | None, self.attempt.process.returncode)

    def change_terminal_size(self, width: int, height: int) -> None:
        if not 1 <= width <= 1000 or not 1 <= height <= 1000:
            raise ValueError("Invalid terminal size")
        self.attempt.process.change_terminal_size(width, height)

    def close(self) -> None:
        if self.stop_task is None:
            self.stop_task = asyncio.create_task(self.manager.stop(self.attempt))

    async def wait_closed(self) -> None:
        if self.stop_task:
            await asyncio.shield(self.stop_task)
        elif self.attempt.watcher:
            await asyncio.shield(self.attempt.watcher)


def make_login_router(state: Any) -> APIRouter:
    router = APIRouter(prefix="/login")

    def manager() -> LoginManager:
        if state.login_manager is None:
            raise HTTPException(404, "Browser login is not enabled")
        return cast(LoginManager, state.login_manager)

    @router.get("/assets/{name}")
    async def asset(name: str) -> FileResponse:
        if name not in ("login-v1.js", "login-v1.css"):
            raise HTTPException(404)
        return FileResponse(ASSETS / name, headers={"X-Content-Type-Options": "nosniff"})

    @router.get("/backend/{backend}")
    async def page(backend: str, request: Request) -> HTMLResponse:
        m = manager()
        m.origin(request, required=False)
        if backend not in m.config.profiles:
            raise HTTPException(404, "Backend is not configured for browser login")
        m.sessions = {
            key: value for key, value in m.sessions.items() if value[1] > time.monotonic()
        }
        owner = str(request.cookies.get(COOKIE, ""))
        if owner not in m.sessions:
            if len(m.sessions) >= 512:
                raise HTTPException(429, "Too many login sessions")
            owner = secrets.token_urlsafe(32)
            m.sessions[owner] = (secrets.token_urlsafe(32), time.monotonic() + 1800)
        csrf = m.sessions[owner][0]
        content = (
            (ASSETS / "login.html")
            .read_text()
            .replace(
                "{{backend}}",
                html.escape(backend, quote=True),
            )
            .replace("{{csrf}}", csrf)
        )
        response = HTMLResponse(
            content,
            headers={
                "Content-Security-Policy": CSP,
                "Cache-Control": "no-store",
                "Referrer-Policy": "no-referrer",
                "Cross-Origin-Opener-Policy": "same-origin",
                "X-Content-Type-Options": "nosniff",
            },
        )
        response.set_cookie(
            COOKIE,
            owner,
            max_age=1800,
            httponly=True,
            secure=request.url.scheme == "https",
            samesite="strict",
            path="/login",
        )
        return response

    @router.post("/backend/{backend}/start")
    async def start(backend: str, request: Request) -> dict[str, str]:
        m = manager()
        m.origin(request)
        owner = m.session(request, request.headers.get("x-csrf-token", ""))
        return m.start(owner, backend).public()

    @router.get("/attempt/{attempt_id}")
    async def status(attempt_id: str, request: Request) -> dict[str, str]:
        m = manager()
        m.origin(request, required=False)
        return m.owned(m.session(request), attempt_id).public()

    @router.post("/attempt/{attempt_id}/cancel")
    async def cancel(attempt_id: str, request: Request) -> dict[str, str]:
        m = manager()
        m.origin(request)
        owner = m.session(request, request.headers.get("x-csrf-token", ""))
        attempt = m.owned(owner, attempt_id)
        await m.stop(attempt)
        return attempt.public()

    @router.websocket("/attempt/{attempt_id}/terminal")
    async def terminal(websocket: WebSocket, attempt_id: str) -> None:
        attempt = None
        m = None
        try:
            m = manager()
            m.origin(websocket)
            owner = m.session(websocket)
            attempt = m.owned(owner, attempt_id)
        except HTTPException:
            await websocket.close(code=1008)
            return
        await websocket.accept()
        authenticated = False
        try:
            init = await asyncio.wait_for(websocket.receive_json(), 10)
            m.session(websocket, init.get("csrf", ""))
            authenticated = True
            process = await m.attach(attempt)
            await relay_terminal(websocket, process)
        except Exception:
            # Do not log received frames, process output, or exception details.
            try:
                await websocket.close(code=1008)
            except RuntimeError:
                pass
        finally:
            if authenticated and not attempt.attached:
                await m.stop(attempt)

    return router
