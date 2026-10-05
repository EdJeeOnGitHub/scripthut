"""Read-only deployment readiness view, protected by the normal API middleware."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException


def status_root() -> Path:
    value = os.environ.get("SCRIPTHUT_DEPLOYMENT_STATUS_DIR")
    if not value:
        raise HTTPException(503, "Deployment readiness is not configured")
    return Path(value)


def read_status(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
        if not isinstance(value, dict):
            raise ValueError("Invalid status")
        return value
    except (OSError, ValueError) as exc:
        raise HTTPException(
            503, "Deployment readiness is unavailable; wait before retrying"
        ) from exc


def cancellation_failure(deployment_id: str) -> dict[str, Any]:
    gate = read_status(status_root() / "current.json")
    if gate.get("accepting_submissions") is not False or gate.get("deployment_id") != deployment_id:
        raise HTTPException(409, "Cancellation must reference the active host deployment")
    return {
        "code": "deployment_interrupted",
        "deployment_id": deployment_id,
        "message": (
            "Stopped by the operator for deployment; wait for verified readiness before retrying."
        ),
        "retry_status_url": "/api/v1/deployments/" + deployment_id,
    }


def make_maintenance_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.get("/maintenance")
    async def maintenance() -> dict[str, Any]:
        return read_status(status_root() / "current.json")

    @router.get("/deployments/{deployment_id}")
    async def deployment(deployment_id: str) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", deployment_id):
            raise HTTPException(404, "Unknown deployment")
        root = status_root()
        path = root / (deployment_id + ".json")
        if not path.exists():
            raise HTTPException(404, "Unknown deployment")
        # Read gate last, so a newer deployment cannot be bypassed using an old ID.
        record = read_status(path)
        gate = read_status(root / "current.json")
        return dict(
            record,
            retry_allowed=gate.get("accepting_submissions") is True
            and gate.get("verified") is True
            and record.get("verified") is True,
        )

    return router
