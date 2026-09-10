"""Terminal session manager for web-based interactive terminals."""

import asyncio
import codecs
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from scripthut.ssh.transport import ExecutionClient, InteractiveProcess

logger = logging.getLogger(__name__)


@dataclass
class TerminalSession:
    """Tracks a single active terminal session."""

    id: str
    backend_name: str
    session_type: str  # "headnode", "attach", or "job"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    node: str | None = None
    job_id: str | None = None
    label: str | None = None
    websocket: WebSocket | None = field(default=None, repr=False)


class TerminalManager:
    """Manages active terminal sessions."""

    def __init__(self) -> None:
        self._sessions: dict[str, TerminalSession] = {}

    @property
    def sessions(self) -> dict[str, TerminalSession]:
        return self._sessions

    def create_session(
        self,
        backend_name: str,
        session_type: str,
        *,
        node: str | None = None,
        job_id: str | None = None,
        label: str | None = None,
    ) -> TerminalSession:
        session = TerminalSession(
            id=uuid.uuid4().hex[:12],
            backend_name=backend_name,
            session_type=session_type,
            node=node,
            job_id=job_id,
            label=label,
        )
        self._sessions[session.id] = session
        return session

    def remove_session(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    async def close_session(self, session_id: str) -> bool:
        """Close a session's WebSocket, triggering cleanup. Returns True if found."""
        session = self._sessions.get(session_id)
        if session is None:
            return False
        if session.websocket:
            try:
                await session.websocket.close()
            except Exception:
                pass
        return True


async def relay_terminal(websocket: WebSocket, process: InteractiveProcess) -> None:
    """Relay an owned process and always close/reap it on either endpoint's exit."""
    async def ws_to_ssh() -> None:
        while True:
            msg = json.loads(await websocket.receive_text())
            if msg["type"] == "input":
                process.stdin.write(msg["data"].encode())
            elif msg["type"] == "resize":
                process.change_terminal_size(msg.get("cols", 80), msg.get("rows", 24))

    async def ssh_to_ws() -> None:
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        while True:
            data = await process.stdout.read(4096)
            if not data:
                break
            text = decoder.decode(data)
            if text:
                await websocket.send_json({"type": "output", "data": text})
        tail = decoder.decode(b"", final=True)
        if tail:
            await websocket.send_json({"type": "output", "data": tail})
        await process.wait_closed()
        await websocket.send_json({
            "type": "exit", "code": process.returncode if process.returncode is not None else -1,
        })

    tasks = [asyncio.create_task(ws_to_ssh()), asyncio.create_task(ssh_to_ws())]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    except WebSocketDisconnect:
        pass
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        process.close()
        await process.wait_closed()
        try:
            await websocket.close()
        except RuntimeError:
            pass


async def handle_terminal_websocket(
    websocket: WebSocket,
    ssh_client: ExecutionClient,
    command: str | None = None,
    *,
    init_data: dict[str, Any] | None = None,
) -> None:
    """Initialize and relay a byte PTY over the existing terminal JSON protocol."""
    try:
        if init_data is None:
            init_data = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        if not isinstance(init_data, dict):
            raise ValueError("Expected terminal initialization object")
        process = await ssh_client.create_interactive_session(
            command=command,
            term_size=(init_data.get("cols", 80), init_data.get("rows", 24)),
        )
        await relay_terminal(websocket, process)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.error("Terminal session failed: %s", exc)
        try:
            await websocket.send_json({"type": "error", "message": str(exc)})
            await websocket.close()
        except (RuntimeError, WebSocketDisconnect):
            pass
