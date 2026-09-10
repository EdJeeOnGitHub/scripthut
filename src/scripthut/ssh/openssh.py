"""Execute through an externally owned OpenSSH master, with no direct fallback."""

from __future__ import annotations

import asyncio
import os
import shlex
import shutil
import stat
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING

from scripthut.ssh.command_log import CommandLogEntry
from scripthut.ssh.transport import InteractiveProcess, TransportError

if TYPE_CHECKING:
    from scripthut.config_schema import SSHConfig


@dataclass
class SocketHealth:
    state: str = "disconnected"
    socket_exists: bool = False
    master_responsive: bool = False
    last_successful_check: datetime | None = None
    first_observed_at: datetime | None = None
    error: str | None = None


class OpenSSHClient:
    """Owns command children only; never starts, stops, or unlinks the master."""

    def __init__(self, config: SSHConfig) -> None:
        if os.name != "posix":
            raise ValueError("OpenSSH socket transport requires a POSIX host (Linux/macOS)")
        ssh = shutil.which("ssh")
        false = shutil.which("false")
        if not ssh or not false:
            raise ValueError("OpenSSH socket transport requires ssh and false executables")
        if config.control_path is None:
            raise ValueError("OpenSSH socket transport requires control_path")
        self.config = config
        self.host, self.user, self.port = config.host, config.user, config.port
        self.control_path = config.control_path.expanduser()
        self._ssh = str(Path(ssh).resolve())
        self._false = str(Path(false).resolve())
        self.health = SocketHealth()
        self.on_command: Callable[[CommandLogEntry], None] | None = None
        self._slots = asyncio.Semaphore(config.max_operations)
        self._lifecycle_lock = asyncio.Lock()
        self._children: set[asyncio.subprocess.Process] = set()
        self._terminals: set[InteractiveProcess] = set()
        self._probe_task: asyncio.Task[SocketHealth] | None = None
        self._next_probe = 0.0
        self._backoff = 0.0
        self._socket_identity: tuple[int, int] | None = None
        self._shutdown = False

    @property
    def is_connected(self) -> bool:
        return not self._shutdown and self.health.state == "connected"

    def _argv(self) -> list[str]:
        return [
            self._ssh, "-F", "none", "-S", str(self.control_path),
            "-o", "ControlMaster=no", "-o", "BatchMode=yes",
            "-o", f"ProxyCommand=exec {shlex.quote(self._false)}",
            "-o", "StrictHostKeyChecking=yes", "-o", "ClearAllForwardings=yes",
            "-o", "ForwardAgent=no", "-o", "ForwardX11=no",
            "-p", str(self.port), "-l", self.user,
        ]

    def _socket_stat(self) -> tuple[int, int]:
        info = self.control_path.lstat()
        self.health.socket_exists = True
        if not stat.S_ISSOCK(info.st_mode):
            raise TransportError("Control path is not a Unix socket")
        if info.st_uid != os.getuid():
            raise TransportError("Control socket must be owned by the controller user")
        return info.st_dev, info.st_ino

    def _failed(self, error: str) -> None:
        self.health.state = "connection_failed"
        self.health.error = error
        self.health.first_observed_at = None
        self._backoff = min(max(self._backoff * 2, 30.0), 300.0)
        self._next_probe = time.monotonic() + self._backoff

    def _succeeded(self, identity: tuple[int, int]) -> None:
        if self._shutdown:
            return
        now = datetime.now(UTC)
        if self._socket_identity != identity or self.health.first_observed_at is None:
            self.health.first_observed_at = now
        self._socket_identity = identity
        self.health.state = "connected"
        self.health.master_responsive = True
        self.health.last_successful_check = now
        self.health.error = None
        self._backoff = 0.0
        self._next_probe = time.monotonic() + 30.0

    async def _execute(self, argv: list[str], timeout: int) -> tuple[str, str, int]:
        async with self._slots:
            async with self._lifecycle_lock:
                if self._shutdown:
                    raise TransportError("OpenSSH client is closed")
                process = await asyncio.create_subprocess_exec(
                    *argv, stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                self._children.add(process)
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
                return (
                    stdout.decode("utf-8", errors="replace"),
                    stderr.decode("utf-8", errors="replace"),
                    process.returncode if process.returncode is not None else -1,
                )
            except TimeoutError as exc:
                raise TransportError(f"OpenSSH operation timed out after {timeout}s") from exc
            finally:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                await process.wait()
                self._children.discard(process)

    async def _probe(self, timeout: int) -> SocketHealth:
        self.health.socket_exists = False
        self.health.master_responsive = False
        try:
            identity = self._socket_stat()
            _, stderr, code = await self._execute(
                [*self._argv(), "-O", "check", "--", self.host], timeout,
            )
            if code:
                raise TransportError(stderr.strip() or "OpenSSH master did not answer")
            self.health.master_responsive = True
            _, stderr, code = await self._execute(
                [*self._argv(), "-T", "--", self.host, "true"], timeout,
            )
            if code:
                raise TransportError(stderr.strip() or "Remote connection check failed")
            self._succeeded(identity)
        except (OSError, TransportError) as exc:
            self._failed(str(exc))
            if isinstance(exc, FileNotFoundError):
                self.health.state = "disconnected"
        return self.health

    async def check_connection(self, *, force: bool = False, timeout: int = 15) -> SocketHealth:
        if self._probe_task is not None and not self._probe_task.done():
            return await asyncio.shield(self._probe_task)
        if not force and time.monotonic() < self._next_probe:
            return self.health
        self._probe_task = asyncio.create_task(self._probe(timeout))
        return await asyncio.shield(self._probe_task)

    async def connect(self, timeout: int = 15) -> None:
        self._shutdown = False
        health = await self.check_connection(timeout=timeout)
        if health.state != "connected":
            raise TransportError(health.error or "OpenSSH socket is disconnected")

    async def run_command(self, command: str, timeout: int = 30) -> tuple[str, str, int]:
        start = time.perf_counter()
        stdout = stderr = ""
        code: int | None = None
        error = None
        try:
            self.health.socket_exists = False
            identity = self._socket_stat()
            stdout, stderr, code = await self._execute(
                [*self._argv(), "-T", "--", self.host, command], timeout,
            )
            if code == 255 or code < 0:
                raise TransportError(stderr.strip() or f"OpenSSH exited with status {code}")
            self._succeeded(identity)
            return stdout, stderr, code
        except (OSError, TransportError) as exc:
            error = str(exc)
            if not self._shutdown:
                self._failed(error)
            raise TransportError(error) from exc
        finally:
            if self.on_command:
                self.on_command(CommandLogEntry(
                    timestamp=datetime.now(UTC), command=command, stdout=stdout,
                    stderr=stderr, exit_code=code, error=error,
                    duration_ms=int((time.perf_counter() - start) * 1000),
                ))

    async def create_interactive_session(
        self, command: str | None = None, term_type: str = "xterm-256color",
        term_size: tuple[int, int] = (80, 24),
    ) -> InteractiveProcess:
        from scripthut.ssh.pty_process import PTYProcess

        if self._shutdown:
            raise TransportError("OpenSSH client is closed")
        try:
            self.health.socket_exists = False
            self._socket_stat()
        except (OSError, TransportError) as exc:
            self._failed(str(exc))
            raise TransportError(str(exc)) from exc
        argv = [*self._argv(), "-tt", "-e", "none", "--", self.host]
        if command is not None:
            argv.append(command)
        async with self._lifecycle_lock:
            if self._shutdown:
                raise TransportError("OpenSSH client is closed")
            process = await PTYProcess.start(argv, {**os.environ, "TERM": term_type}, term_size)
            self._terminals.add(process)

        def finished(_: asyncio.Task[None]) -> None:
            self._terminals.discard(process)
            if process.returncode == 255 and not self._shutdown:
                self._failed("OpenSSH interactive session failed (exit 255)")

        process._waiter.add_done_callback(finished)
        return process

    async def disconnect(self) -> None:
        self._shutdown = True
        if self._probe_task is not None and not self._probe_task.done():
            self._probe_task.cancel()
            await asyncio.gather(self._probe_task, return_exceptions=True)
        # A child still being spawned must be registered before we take ownership
        # of the shutdown snapshot. Queued operations see _shutdown and cannot spawn.
        async with self._lifecycle_lock:
            terminals = list(self._terminals)
            children = list(self._children)
        for terminal in terminals:
            terminal.close()
        for process in children:
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
        await asyncio.gather(
            *(p.wait_closed() for p in terminals), *(p.wait() for p in children),
        )
        self.health.state = "disconnected"
        self._next_probe = 0.0

    def connection_details(self) -> dict[str, object]:
        details = asdict(self.health)
        details.update(
            transport="openssh", control_path=str(self.control_path),
            control_persist=self.config.control_persist,
            terminal_command=self.terminal_command(),
        )
        return details

    def terminal_command(self) -> str:
        if self.config.terminal_command is not None:
            return self.config.terminal_command
        known_hosts = self.config.known_hosts_resolved
        assert known_hosts is not None  # Validated by SSHConfig.
        return shlex.join([
            "ssh", "-F", "none", "-M", "-N", "-f", "-S", str(self.control_path),
            "-o", f"ControlPersist={self.config.control_persist}",
            "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3",
            "-o", "StrictHostKeyChecking=yes",
            "-o", f"UserKnownHostsFile={shlex.quote(str(known_hosts))}",
            "-p", str(self.port), "-l", self.user, "--", self.host,
        ])

    async def __aenter__(self) -> OpenSSHClient:
        await self.connect()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.disconnect()
