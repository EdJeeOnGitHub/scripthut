"""Read-only inventory of historical tasks missing explicit project labels.

Run with ``python -m scripthut.project_audit /path/to/workflows``. No controller
state is loaded or written. Candidates use only recorded Git repository URLs;
unknowns require an operator-reviewed run-to-project mapping before migration.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripthut.projects import repository_name


def audit(root: Path) -> list[dict]:
    rows = []
    for path in sorted(root.glob("*/*/run.json")):
        data = json.loads(path.read_text())
        if data.get("workflow_name", "").startswith("_default"):
            continue  # Scheduler-discovered jobs were not submitted by ScriptHut.
        missing = [
            item["task"]["id"]
            for item in data.get("items", [])
            if not item["task"].get("project_id")
        ]
        if not missing:
            continue
        repo = data.get("git_repo")
        try:
            candidate = repository_name(repo) if isinstance(repo, str) and repo else None
        except ValueError:
            candidate = None
        rows.append(
            dict(
                run_id=data["id"],
                task_ids=missing,
                project_id=candidate,
                evidence="recorded git_repo" if candidate else "requires review",
            )
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workflows", type=Path)
    args = parser.parse_args()
    if not args.workflows.is_dir():
        parser.error("workflows must be an existing directory")
    print(json.dumps(audit(args.workflows), indent=2))


if __name__ == "__main__":
    main()
