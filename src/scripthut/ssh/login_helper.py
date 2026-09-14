#!/usr/bin/env python3
"""Standalone POSIX host helper. Requires Python 3.11+, OpenSSH, and no packages.

Install this file as an executable; invoke only with an operator-owned profile
file and a configured profile ID. stdout is ephemeral authentication traffic.
Exit 0 means verified detach; every other exit is a non-secret lifecycle result.
"""

from __future__ import annotations

import argparse
import os
import re
import selectors
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from types import FrameType


class CancelledError(Exception):
    pass


@dataclass(frozen=True)
class Profile:
    host: str
    user: str
    control_path: str
    known_hosts: str
    port: int = 22
    identity_file: str | None = None
    timeout: int = 300
    control_persist: str = "8h"

    @classmethod
    def load(cls, file: Path, name: str) -> Profile:
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            raise ValueError("Invalid profile ID")
        info = file.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("Profiles must be an owner-only regular file")
        entries = tomllib.loads(file.read_text())["profiles"]
        profiles = {key: cls(**value) for key, value in entries.items()}
        for profile in profiles.values():
            profile.validate()
        sockets = [profile.control_path for profile in profiles.values()]
        if len(set(sockets)) != len(sockets):
            raise ValueError("Profiles must use distinct sockets")
        return profiles[name]

    def validate(self) -> None:
        for value in (self.host, self.user):
            if not value or value.startswith("-") or re.search(r"\s|[\x00-\x1f\x7f]", value):
                raise ValueError("Host and user must be literal tokens")
        for path in (self.control_path, self.known_hosts, self.identity_file):
            if path is not None and (
                not Path(path).is_absolute() or re.search(r"[%$\x00-\x1f\x7f]", path)
            ):
                raise ValueError("Profile paths must be absolute and have no expansion tokens")
        if not 1 <= self.port <= 65535 or not 1 <= self.timeout <= 300:
            raise ValueError("Invalid port or timeout")
        if not re.fullmatch(r"(?:[1-9][0-9]*[smhdw]?)+", self.control_persist):
            raise ValueError("Invalid persistence interval")


class LoginHelper:
    def __init__(self, profile: Profile):
        self.profile = profile
        ssh = shutil.which("ssh")
        false = shutil.which("false")
        if not ssh or not false:
            raise ValueError("OpenSSH and false are required")
        self.ssh, self.false = ssh, false
        self.child: subprocess.Popen[bytes] | None = None
        self.owned = False
        self.committed = False

    def argv(self, *, reuse: bool) -> list[str]:
        p = self.profile
        args = [
            self.ssh,
            "-F",
            "none",
            "-e",
            "none",
            "-S",
            p.control_path,
            "-p",
            str(p.port),
            "-l",
            p.user,
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={p.known_hosts}",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "ForwardAgent=no",
            "-o",
            "ForwardX11=no",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "ConnectTimeout=30",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "LogLevel=ERROR",
        ]
        if reuse:
            args += [
                "-o",
                "BatchMode=yes",
                "-o",
                "ControlMaster=no",
                "-o",
                f"ProxyCommand=exec {shlex.quote(self.false)}",
            ]
        else:
            args += [
                "-o",
                "BatchMode=no",
                "-o",
                "NumberOfPasswordPrompts=1",
                "-o",
                f"ControlPersist={p.control_persist}",
                "-M",
                "-N",
                "-f",
            ]
            if p.identity_file:
                args += ["-o", "IdentitiesOnly=yes", "-i", p.identity_file]
        return args

    def probe(self, *, exit_master: bool = False) -> bool:
        args = self.argv(reuse=True)
        args += ["-O", "exit"] if exit_master else ["-T"]
        args += ["--", self.profile.host]
        if not exit_master:
            args += ["true"]
        try:
            return (
                subprocess.run(
                    args,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                    check=False,
                ).returncode
                == 0
            )
        except (OSError, subprocess.TimeoutExpired):
            return False

    def cleanup(self) -> None:
        if self.committed or not self.owned:
            return
        if self.child and self.child.poll() is None:
            try:
                os.killpg(self.child.pid, signal.SIGTERM)
                self.child.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(self.child.pid, signal.SIGKILL)
                self.child.wait()
            except ProcessLookupError:
                self.child.wait()
        # ssh -f may fork just as cancellation arrives. Reap the foreground
        # parent first, then close any master it created while holding the lock.
        until = time.monotonic() + 1
        while time.monotonic() < until:
            if Path(self.profile.control_path).exists():
                self.probe(exit_master=True)
            time.sleep(0.05)

    def run(self) -> int:
        import fcntl
        import termios
        import tty

        p = self.profile
        directory = Path(p.control_path).parent
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("Socket directory must be owned and mode 0700")
        lock = os.open(
            directory / ("." + Path(p.control_path).name + ".login.lock"),
            os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
            0o600,
        )
        try:
            lock_info = os.fstat(lock)
            if lock_info.st_uid != os.getuid() or not stat.S_ISREG(lock_info.st_mode):
                raise ValueError("Invalid profile lock")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 3  # Another helper owns this profile.
            socket = Path(p.control_path)
            if socket.exists() or socket.is_symlink():
                info = socket.lstat()
                if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
                    raise ValueError("Invalid existing socket")
                return 0 if self.probe() else 4  # Never replace an existing master.
            master, slave = os.openpty()
            original = termios.tcgetattr(0) if os.isatty(0) else None
            old_handlers = {}

            def cancelled(signum: int, frame: FrameType | None) -> None:
                if signum == signal.SIGALRM:
                    raise TimeoutError()
                raise CancelledError()

            def controlling_tty() -> None:
                # This standalone helper is single-threaded; no Python threads
                # exist across fork. OpenSSH reads passwords through /dev/tty.
                os.setsid()
                fcntl.ioctl(0, termios.TIOCSCTTY, 0)

            try:
                tty.setraw(slave)
                if original is not None:
                    tty.setraw(0)
                for signum in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGALRM):
                    old_handlers[signum] = signal.signal(signum, cancelled)
                signal.alarm(p.timeout)
                self.owned = True
                self.child = subprocess.Popen(
                    [*self.argv(reuse=False), "--", p.host],
                    stdin=slave,
                    stdout=slave,
                    stderr=slave,
                    preexec_fn=controlling_tty,
                    env={**os.environ, "SSH_ASKPASS_REQUIRE": "never", "DISPLAY": ""},
                )
                os.close(slave)
                slave = -1
                deadline = time.monotonic() + p.timeout
                with selectors.DefaultSelector() as selector:
                    selector.register(master, selectors.EVENT_READ)
                    selector.register(0, selectors.EVENT_READ)
                    while self.child.poll() is None:
                        if time.monotonic() >= deadline:
                            return 5
                        for key, _ in selector.select(0.1):
                            try:
                                data = os.read(key.fd, 4096)
                            except OSError:
                                if key.fd == master:
                                    selector.unregister(master)
                                    continue
                                raise
                            if not data:
                                if key.fd == 0:
                                    raise CancelledError()
                                selector.unregister(master)
                                continue
                            os.write(master if key.fd == 0 else 1, data)
                if self.child.returncode != 0 or not self.probe():
                    return 6
                self.committed = True
                return 0
            except TimeoutError:
                return 5
            except (CancelledError, BrokenPipeError):
                return 2
            finally:
                signal.alarm(0)
                # Ignore repeated disconnect signals while host-side cleanup
                # finishes. Never rely on the browser/controller to clean up.
                for signum in old_handlers:
                    signal.signal(signum, signal.SIG_IGN)
                self.cleanup()
                os.close(master)
                if slave >= 0:
                    os.close(slave)
                if original is not None:
                    try:
                        termios.tcflush(0, termios.TCIFLUSH)
                        termios.tcsetattr(0, termios.TCSANOW, original)
                    except termios.error:
                        pass
                for signum, handler in old_handlers.items():
                    signal.signal(signum, handler)
        finally:
            os.close(lock)


def main() -> int:
    parser = argparse.ArgumentParser(description="Authenticate a configured OpenSSH master")
    parser.add_argument("--profiles", required=True, type=Path)
    parser.add_argument("profile")
    args = parser.parse_args()
    try:
        if os.name != "posix":
            raise ValueError("POSIX required")
        return LoginHelper(Profile.load(args.profiles, args.profile)).run()
    except Exception:
        # Exception text may contain authentication output or configuration.
        # Status is deliberately generic, and transcripts are never logged.
        return 7


if __name__ == "__main__":
    sys.exit(main())
