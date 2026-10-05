import json

import httpx
import pytest
from fastapi import FastAPI

from scripthut.maintenance import cancellation_failure, make_maintenance_router


@pytest.mark.asyncio
async def test_readiness_survives_restart_and_old_id_cannot_bypass_gate(tmp_path, monkeypatch):
    monkeypatch.setenv("SCRIPTHUT_DEPLOYMENT_STATUS_DIR", str(tmp_path))
    (tmp_path / "old.json").write_text(json.dumps({"deployment_id": "old", "verified": True}))
    gate = tmp_path / "current.json"
    gate.write_text(
        json.dumps({"deployment_id": "new", "accepting_submissions": False, "verified": False})
    )
    for _ in range(2):
        app = FastAPI()
        app.include_router(make_maintenance_router())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/api/v1/maintenance")).json()[
                "accepting_submissions"
            ] is False
            assert (await client.get("/api/v1/deployments/old")).json()["retry_allowed"] is False
            assert (await client.get("/api/v1/deployments/unknown")).status_code == 404
    gate.write_text(
        json.dumps({"deployment_id": "new", "accepting_submissions": True, "verified": True})
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/api/v1/deployments/old")).json()["retry_allowed"] is True
        gate.unlink()
        assert (await client.get("/api/v1/deployments/old")).status_code == 503


def test_only_active_host_deployment_can_supply_failure_reason(tmp_path, monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setenv("SCRIPTHUT_DEPLOYMENT_STATUS_DIR", str(tmp_path))
    gate = tmp_path / "current.json"
    gate.write_text(json.dumps({"deployment_id": "active", "accepting_submissions": False}))
    assert cancellation_failure("active")["code"] == "deployment_interrupted"
    with pytest.raises(HTTPException):
        cancellation_failure("forged")
    gate.write_text(json.dumps({"deployment_id": "active", "accepting_submissions": True}))
    with pytest.raises(HTTPException):
        cancellation_failure("active")


@pytest.mark.asyncio
async def test_launcher_paused_still_returns_structured_maintenance(tmp_path, monkeypatch):
    from scripthut.research_api import make_research_router

    monkeypatch.setenv("SCRIPTHUT_DEPLOYMENT_STATUS_DIR", str(tmp_path))
    monkeypatch.delenv("SCRIPTHUT_RESEARCH_SOCKET", raising=False)
    (tmp_path / "current.json").write_text(
        json.dumps(
            {
                "accepting_submissions": False,
                "deployment_id": "active",
                "retry_status_url": "/api/v1/deployments/active",
            }
        )
    )
    app = FastAPI()
    app.include_router(make_research_router())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for path in ("/api/v1/research-runs", "/api/v1/research-runs/" + "a" * 32 + "/retry"):
            response = await client.post(path, json={})
            assert response.status_code == 503
            assert response.json()["code"] == "deployment_in_progress"
            assert response.json()["deployment_id"] == "active"
