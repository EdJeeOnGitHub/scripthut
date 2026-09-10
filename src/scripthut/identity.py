"""Backend identity and the shared policy for the My Jobs view."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripthut.config_schema import BackendConfig


def local_user() -> str:
    """Resolve the effective process owner, never a display filter or USER override."""
    import pwd

    return pwd.getpwuid(os.geteuid()).pw_name


def backend_user(config: BackendConfig) -> str | None:
    if config.type in ("slurm", "pbs"):
        return config.ssh.user
    if config.type == "local":
        return local_user()
    return None


@dataclass(frozen=True)
class JobFilter:
    users: Mapping[str, str | None]
    enabled: bool

    def query_user(self, backend_name: str) -> str | None:
        return self.users.get(backend_name) if self.enabled else None

    def matches(self, backend_name: str, owner: str | None) -> bool:
        user = self.query_user(backend_name)
        return user is None or owner == user
