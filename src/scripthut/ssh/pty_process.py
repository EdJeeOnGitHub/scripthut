"""POSIX PTY adapter. Imported only when an interactive OpenSSH session starts."""

from __future__ import annotations

import asyncio
import errno
import fcntl
import os
import signal
import struct
import termios
import tty


class PTYProcess:
    """An owned local SSH child with the byte-stream interface used by the relay."""

    def __init__(self, process: asyncio.subprocess.Process, fd: int) -> None:
        self.process = process
        self.fd = fd
        self.stdout = asyncio.StreamReader()
        self.stdin = self
        self._pending = bytearray()
        self._loop = asyncio.get_running_loop()
        os.set_blocking(fd, False)
        self._loop.add_reader(fd, self._read_ready)
        self._waiter = asyncio.create_task(self._finish())

    @classmethod
    async def start(
        cls, argv: list[str], env: dict[str, str], size: tuple[int, int],
    ) -> PTYProcess:
        master, slave = os.openpty()
        try:
            tty.setraw(slave)
            cls._resize(slave, *size)
            process = await asyncio.create_subprocess_exec(
                *argv, stdin=slave, stdout=slave, stderr=slave, env=env,
                start_new_session=True,
            )
        except BaseException:
            os.close(master)
            raise
        finally:
            os.close(slave)
        return cls(process, master)

    @property
    def returncode(self) -> int | None:
        return self.process.returncode

    @staticmethod
    def _resize(fd: int, width: int, height: int) -> None:
        if not 1 <= width <= 1000 or not 1 <= height <= 1000:
            raise ValueError("Terminal dimensions must be between 1 and 1000")
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", height, width, 0, 0))

    def change_terminal_size(self, width: int, height: int) -> None:
        if self.fd >= 0:
            self._resize(self.fd, width, height)
            # The detached child has no controlling terminal/foreground group,
            # so the kernel does not deliver SIGWINCH from TIOCSWINSZ for us.
            if self.returncode is None:
                try:
                    self.process.send_signal(signal.SIGWINCH)
                except ProcessLookupError:
                    pass

    def _read_ready(self) -> None:
        if self.fd < 0:
            return
        try:
            while True:
                data = os.read(self.fd, 65536)
                if not data:
                    break
                self.stdout.feed_data(data)
        except BlockingIOError:
            return
        except OSError as exc:
            if exc.errno != errno.EIO:  # Linux reports EOF as EIO on a PTY.
                self.stdout.set_exception(exc)
        self._loop.remove_reader(self.fd)
        self.stdout.feed_eof()

    def write(self, data: bytes) -> None:
        if self.fd < 0 or self.returncode is not None:
            raise BrokenPipeError("Terminal is closed")
        if len(self._pending) + len(data) > 1024 * 1024:
            raise BufferError("Terminal input buffer is full")
        self._pending.extend(data)
        self._write_ready()

    def _write_ready(self) -> None:
        try:
            while self._pending:
                count = os.write(self.fd, self._pending)
                del self._pending[:count]
        except BlockingIOError:
            self._loop.add_writer(self.fd, self._write_ready)
            return
        except OSError:
            self._pending.clear()
            self.close()
        if self.fd >= 0:
            self._loop.remove_writer(self.fd)

    async def _finish(self) -> None:
        await self.process.wait()
        self._read_ready()  # Drain the final output before closing the descriptor.
        self._loop.remove_reader(self.fd)
        self._loop.remove_writer(self.fd)
        os.close(self.fd)
        self.fd = -1
        self.stdout.feed_eof()

    def close(self) -> None:
        if self.process.returncode is None:
            try:
                self.process.kill()
            except ProcessLookupError:
                pass

    async def wait_closed(self) -> None:
        await asyncio.shield(self._waiter)
