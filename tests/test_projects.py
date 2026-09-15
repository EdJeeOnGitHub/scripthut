import json
import subprocess
from datetime import UTC
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.project_audit import audit
from scripthut.projects import attributed_task, repository_name, submission_project


@pytest.mark.parametrize(
    "remote",
    [
        "https://example.com/team/real-repo.git",
        "git@example.com:team/real-repo.git",
        "ssh://git@example.com/team/real-repo.git/",
        "/srv/git/real-repo.git",
    ],
)
def test_repository_identity(remote):
    assert repository_name(remote) == "real-repo"


def test_git_identity_precedes_workspace_fallback_and_survives_worktree(tmp_path, monkeypatch):
    repo = tmp_path / "misleading-directory"
    repo.mkdir()
    subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(repo), "remote", "add", "origin", "git@example.com:team/real-repo.git"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "-m",
            "init",
        ],
        check=True,
        capture_output=True,
    )
    tree = tmp_path / "different-worktree-name"
    subprocess.run(
        ["git", "-C", str(repo), "worktree", "add", "-b", "feature", str(tree)],
        check=True,
        capture_output=True,
    )
    monkeypatch.chdir(tree)
    monkeypatch.setenv("SCRIPTHUT_PROJECT", "container-default")
    assert submission_project() == "real-repo"
    assert submission_project("explicit") == "explicit"
    original = {"id": "task"}
    assert attributed_task(original)["project_id"] == "real-repo"
    assert original == {"id": "task"}


def test_outside_git_and_ambiguous_git(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SCRIPTHUT_PROJECT", raising=False)
    with pytest.raises(ValueError, match="No project identity"):
        submission_project()
    monkeypatch.setenv("SCRIPTHUT_PROJECT", "primary-repo")
    assert submission_project() == "primary-repo"
    subprocess.run(["git", "init"], check=True, capture_output=True)
    with pytest.raises(ValueError, match="unambiguous remote"):
        submission_project()
    for remote in ("first", "second"):
        subprocess.run(
            ["git", "remote", "add", remote, f"https://example.com/{remote}.git"], check=True
        )
    with pytest.raises(ValueError, match="unambiguous remote"):
        submission_project()


@pytest.mark.parametrize("value", [None, "", "  ", " padded ", 123, ["project"], "bad\nname"])
def test_api_rejects_missing_or_invalid_project_before_submission(value):
    state = MagicMock()
    state.config_error = None
    state.run_manager.create_adhoc_run = AsyncMock()
    app = FastAPI()
    app.include_router(make_api_router(state))
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/tasks/run",
            json={
                "backend": "test",
                "task": dict(id="t", name="t", command="true", project_id=value),
            },
        )
    assert response.status_code == 422
    state.run_manager.create_adhoc_run.assert_not_awaited()


def test_historical_audit_uses_only_recorded_repo_and_never_writes(tmp_path):
    folder = tmp_path / "workflow" / "run"
    folder.mkdir(parents=True)
    path = folder / "run.json"
    data = dict(
        id="run1",
        workflow_name="guess-me",
        git_repo=None,
        items=[
            {"task": {"id": "missing", "command": "cd /work/guess-me"}},
            {"task": {"id": "known", "project_id": "keep-me"}},
        ],
    )
    path.write_text(json.dumps(data))
    before = path.read_bytes()
    assert audit(tmp_path) == [
        dict(run_id="run1", task_ids=["missing"], project_id=None, evidence="requires review")
    ]
    assert path.read_bytes() == before
    data["git_repo"] = "git@example.com:team/actual.git"
    path.write_text(json.dumps(data))
    assert audit(tmp_path)[0]["project_id"] == "actual"


def test_cli_missing_project_is_actionable_error(tmp_path, monkeypatch, capsys):
    from scripthut.cli import main

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SCRIPTHUT_PROJECT", raising=False)
    assert main(["task", "run", "true", "--backend", "test", "--dry-run"]) == 1
    assert "pass --project" in capsys.readouterr().err


def test_task_runtime_project_is_available_for_nested_submissions():
    from datetime import datetime

    from scripthut.config import ScriptHutConfig
    from scripthut.runs.env import resolve_for_task
    from scripthut.runs.models import TaskDefinition

    env, _ = resolve_for_task(
        ScriptHutConfig(),
        backend_name="test",
        workflow_name="workflow",
        run_id="run",
        created_at=datetime.now(UTC),
        task=TaskDefinition(
            id="coordinator", name="coordinator", command="true", project_id="repo"
        ),
    )
    assert env["SCRIPTHUT_PROJECT"] == "repo"
