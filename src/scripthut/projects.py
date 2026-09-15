"""Project identity at the submission boundary; never infer from job names."""

from __future__ import annotations

import os
import subprocess
from urllib.parse import urlsplit


def require_project(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(
            "task.project_id must be a non-empty string without surrounding whitespace"
        )
    if len(value) > 200 or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError(
            "task.project_id must be at most 200 characters without control characters"
        )
    return value


def repository_name(remote: str) -> str:
    """Use the repository basename for HTTPS, SSH and local Git remotes."""
    path = urlsplit(remote).path if "://" in remote else remote.split(":", 1)[-1]
    name = path.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")
    return require_project(name)


def submission_project(explicit: object = None) -> str:
    """Explicit value, then current Git origin, then workspace fallback outside Git.

    Remote identity survives renamed checkouts and worktrees. Multiple remotes
    without origin are ambiguous; never silently choose a different project.
    """
    if explicit is not None:
        return require_project(explicit)

    def git(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", *args],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ValueError("Cannot inspect Git identity; pass --project explicitly") from exc
        return result.stdout.strip() if result.returncode == 0 else None

    if git("rev-parse", "--git-dir") is not None:
        remotes = (git("remote") or "").splitlines()
        remote = "origin" if "origin" in remotes else remotes[0] if len(remotes) == 1 else None
        url = git("remote", "get-url", remote) if remote else None
        if not url:
            raise ValueError("Git repository has no unambiguous remote; pass --project explicitly")
        return repository_name(url)
    fallback = os.environ.get("SCRIPTHUT_PROJECT")
    if fallback is not None:
        return require_project(fallback)
    raise ValueError(
        "No project identity: run inside a Git checkout, pass --project, or set SCRIPTHUT_PROJECT"
    )


def attributed_task(task: dict) -> dict:
    return dict(task, project_id=submission_project(task.get("project_id")))
