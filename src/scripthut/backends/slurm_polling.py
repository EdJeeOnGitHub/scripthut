"""A shared scheduler command budget for one Slurm login identity."""

from __future__ import annotations

import asyncio
import time

from scripthut.ssh.transport import ExecutionClient


class SlurmCommandGate:
    """Space command starts and coalesce identical read-only queue queries.

    Owned by a runtime, shared across backend aliases for the same SSH endpoint
    and user. The command timeout starts only after admission to the budget.
    Failed queries and mutations are never cached. A mutation invalidates queue
    snapshots before execution, including when the remote outcome is uncertain.
    """

    def __init__(self, min_interval: float = 0, query_cache_seconds: float = 0) -> None:
        self.min_interval = min_interval
        self.query_cache_seconds = query_cache_seconds
        self._lock = asyncio.Lock()
        self._last_start: float | None = None
        self._cache: dict[str, tuple[float, tuple[str, str, int]]] = {}

    async def run(
        self, client: ExecutionClient, command: str, timeout: int = 30,
        *, input: str | None = None, cache_queue: bool = False,
        mutation: bool = False,
    ) -> tuple[str, str, int]:
        # Preserve concurrent legacy behavior unless this endpoint opts in.
        if not self.min_interval and not self.query_cache_seconds:
            if input is None:
                return await client.run_command(command, timeout=timeout)
            return await client.run_command(command, timeout=timeout, input=input)
        async with self._lock:
            now = time.monotonic()
            self._cache = {
                key: value for key, value in self._cache.items()
                if now - value[0] < self.query_cache_seconds
            }
            if cache_queue and command in self._cache:
                return self._cache[command][1]
            if self._last_start is not None:
                delay = self.min_interval - (now - self._last_start)
                if delay > 0:
                    await asyncio.sleep(delay)
            if mutation:
                self._cache.clear()
            # Reserve even on failure: errors must not trigger rapid retries.
            self._last_start = time.monotonic()
            if input is None:
                result = await client.run_command(command, timeout=timeout)
            else:
                result = await client.run_command(command, timeout=timeout, input=input)
            if cache_queue and result[2] == 0 and self.query_cache_seconds:
                self._cache[command] = (self._last_start, result)
            return result
