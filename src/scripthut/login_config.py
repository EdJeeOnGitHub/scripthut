"""Explicit opt-in and private-access requirements for browser authentication."""

import re
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, model_validator


class BrowserLoginConfig(BaseModel):
    enabled: bool = False
    private_access_verified: bool = False
    allowed_origins: list[str] = Field(default_factory=list)
    host_backend: str = ""
    helper_path: str = "/usr/local/libexec/scripthut-ssh-login"
    profiles_path: str = ""
    profiles: dict[str, str] = Field(default_factory=dict)
    timeout: int = Field(default=300, ge=1, le=300)

    @model_validator(mode="after")
    def validate_boundary(self) -> "BrowserLoginConfig":
        if not self.enabled:
            return self
        if not self.private_access_verified or not self.allowed_origins or not self.host_backend:
            raise ValueError(
                "Browser login requires verified private access, origins, and host_backend"
            )
        for origin in self.allowed_origins:
            url = urlsplit(origin)
            if (
                url.scheme not in ("https", "http")
                or not url.hostname
                or url.path
                or url.query
                or url.fragment
                or url.username
                or "*" in origin
                or (url.scheme == "http" and url.hostname not in ("localhost", "127.0.0.1", "::1"))
            ):
                raise ValueError("Origins must be exact HTTPS origins or HTTP loopback origins")
        for path in (self.helper_path, self.profiles_path):
            if not Path(path).is_absolute() or any(ord(c) < 32 for c in path):
                raise ValueError("Helper/profile paths must be absolute")
        if not self.profiles or any(
            not re.fullmatch(r"[a-zA-Z0-9_-]+", p) for p in self.profiles.values()
        ):
            raise ValueError("Configure backend IDs mapped to literal host profile IDs")
        if len(set(self.profiles.values())) != len(self.profiles):
            raise ValueError("Each backend must use a distinct login profile")
        return self
