"""Structural interfaces shared by local and SSH command execution."""

from __future__ import annotations

from collections.abc import Callable
from types import TracebackType
from typing import Protocol

from scripthut.ssh.command_log import CommandLogEntry


class ByteReader(Protocol):
    async def read(self, n: int = -1) -> bytes: ...


class ByteWriter(Protocol):
    def write(self, data: bytes) -> None: ...


class InteractiveProcess(Protocol):
    @property
    def stdin(self) -> ByteWriter: ...
    @property
    def stdout(self) -> ByteReader: ...
    @property
    def returncode(self) -> int | None: ...
    def change_terminal_size(self, width: int, height: int) -> None: ...
    def close(self) -> None: ...
    async def wait_closed(self) -> None: ...


class ExecutionClient(Protocol):
    user: str
    on_command: Callable[[CommandLogEntry], None] | None

    @property
    def is_connected(self) -> bool: ...
    async def connect(self, timeout: int = 15) -> None: ...
    async def disconnect(self) -> None: ...
    async def run_command(self, command: str, timeout: int = 30) -> tuple[str, str, int]: ...
    async def create_interactive_session(
        self, command: str | None = None, term_type: str = "xterm-256color",
        term_size: tuple[int, int] = (80, 24),
    ) -> InteractiveProcess: ...
    async def __aenter__(self) -> ExecutionClient: ...
    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...


class TransportError(RuntimeError):
    """A command could not be completed; its remote side effects may be unknown."""
