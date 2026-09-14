"""Project identity for reporting; never changes workflow routing or checkout."""
from urllib.parse import urlsplit


def project_name(run, task=None):
    if task is not None and task.project_id:
        return task.project_id
    if task is None:
        projects = {item.task.project_id for item in run.items if item.task.project_id}
        if len(projects) > 1:
            return "Multiple projects"
        if projects:
            return next(iter(projects))
    if run.source_name:
        return run.source_name
    repo = run.git_repo
    if not isinstance(repo, str) or not repo.strip():
        return None
    # Accept recorded HTTPS, ssh:// and Git's user@host:path syntax.
    path = urlsplit(repo).path if "://" in repo else repo.split(":", 1)[-1]
    name = path.rstrip("/").rsplit("/", 1)[-1]
    return name.removesuffix(".git") or None
