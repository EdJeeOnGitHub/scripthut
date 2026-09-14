import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.runs.manager import RunManager
from scripthut.runs.models import RunItemStatus
from scripthut.runs.request_journal import RequestBusy, RequestConflict, RequestJournal
from scripthut.runs.storage import RunStorageManager

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Durable journal requires POSIX locking")

PAYLOAD = {
    "backend": "test",
    "task": {"id": "bootstrap", "name": "prepare", "command": "true"},
    "retain_until_archived": True,
}


def manager(root):
    config = MagicMock()
    config.cache = None
    config.get_backend.return_value.account = None
    config.get_backend.return_value.login_shell = False
    rm = RunManager(config, {}, RunStorageManager(root), job_backends={"test": MagicMock()})
    rm.process_run = AsyncMock()
    return rm


@pytest.mark.asyncio
async def test_durable_before_schedule_and_retry_after_lost_response(tmp_path):
    rm = manager(tmp_path)

    async def check(run):
        assert run.id in rm.storage.load_all_runs()
        assert rm.request_journal.lookup("key")["phase"] == "accepted"

    rm.process_run.side_effect = check
    first = await rm.create_keyed_adhoc_run("key", PAYLOAD)
    second = await rm.create_keyed_adhoc_run("key", PAYLOAD)
    assert first.id == second.id
    assert rm.process_run.await_count == 1
    fresh = manager(tmp_path)
    assert (await fresh.create_keyed_adhoc_run("key", PAYLOAD)).id == first.id
    fresh.process_run.assert_not_awaited()
    with pytest.raises(RequestConflict):
        await fresh.create_keyed_adhoc_run("key", dict(PAYLOAD, run_name="changed"))


@pytest.mark.asyncio
async def test_reservation_and_durable_save_crash_recovery(tmp_path):
    rm = manager(tmp_path)
    reserved = rm.request_journal.reserve("key", PAYLOAD)
    rm.request_journal.accepted = MagicMock(side_effect=OSError("crash after run save"))
    with pytest.raises(OSError):
        await rm.create_keyed_adhoc_run("key", PAYLOAD)
    rm.process_run.assert_not_awaited()
    fresh = manager(tmp_path)
    run = await fresh.create_keyed_adhoc_run("key", PAYLOAD)
    assert run.id == reserved["run_id"]
    assert fresh.request_journal.lookup("key")["phase"] == "accepted"
    # The normal restored-run scheduler, not HTTP replay, resumes the durable run.
    await fresh.restore_from_storage()
    assert fresh.process_run.await_count == 1


@pytest.mark.asyncio
async def test_retention_protection_ack_and_no_reexecution_after_expiry(tmp_path):
    rm = manager(tmp_path)
    run = await rm.create_keyed_adhoc_run("key", PAYLOAD)
    original = rm.storage._run_dir(run)
    assert rm.storage.delete_run(run) is False
    run.created_at = datetime.now(UTC) - timedelta(days=40)
    original.rename(rm.storage._run_dir(run))
    run.items[0].status = RunItemStatus.COMPLETED
    rm.storage.save_run(run, durable=True)
    assert rm.storage.cleanup_old_runs() == 0
    rm.request_journal.acknowledge("key", "a" * 64)
    assert rm.storage.cleanup_old_runs() == 1
    fresh = manager(tmp_path)
    assert await fresh.create_keyed_adhoc_run("key", PAYLOAD) is None
    fresh.process_run.assert_not_awaited()
    assert fresh.request_journal.lookup("key")["run_id"] == run.id


def test_separate_instances_exclude_concurrent_creation(tmp_path):
    a, b = RequestJournal(tmp_path), RequestJournal(tmp_path)
    with a.lease("key"):
        with pytest.raises(RequestBusy):
            with b.lease("key"):
                pytest.fail("Must not enter overlapping creation")
    with b.lease("key"):
        assert a.reserve("key", PAYLOAD)["run_id"] == b.reserve("key", PAYLOAD)["run_id"]


@pytest.mark.asyncio
async def test_http_contract_conflict_lookup_and_archive_ack(tmp_path):
    import httpx
    from fastapi import FastAPI

    from scripthut.api import make_api_router

    rm = manager(tmp_path)
    state = MagicMock(run_manager=rm, config_error=None)
    app = FastAPI()
    app.include_router(make_api_router(state))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        missing = await client.get("/api/v1/submission-requests/research-run:key")
        assert missing.status_code == 404
        payload = dict(PAYLOAD, request_key="research-run:key")
        first = await client.post("/api/v1/tasks/run", json=payload)
        second = await client.post("/api/v1/tasks/run", json=payload)
        assert first.status_code == second.status_code == 200
        assert first.json()["id"] == second.json()["id"]
        changed = await client.post("/api/v1/tasks/run", json=dict(payload, run_name="other"))
        assert changed.status_code == 409
        premature = await client.post(
            "/api/v1/submission-requests/research-run:key/archive",
            json={"archive_receipt_sha256": "a" * 64},
        )
        assert premature.status_code == 422
        rm.runs[first.json()["id"]].items[0].status = RunItemStatus.COMPLETED
        response = await client.post(
            "/api/v1/submission-requests/research-run:key/archive",
            json={"archive_receipt_sha256": "a" * 64},
        )
        assert response.status_code == 200
        record = (await client.get("/api/v1/submission-requests/research-run:key")).json()
        assert record["phase"] == "accepted" and record["archive_receipt"] == "a" * 64
        assert "payload" not in record
        rm.process_run.assert_awaited_once()


def test_journal_identity_survives_restart_and_migrates_old_rows(tmp_path):
    import sqlite3

    root = tmp_path / "_submission_requests"
    root.mkdir()
    with sqlite3.connect(root / "journal.sqlite3") as db:
        db.execute(
            "CREATE TABLE requests (key TEXT PRIMARY KEY, digest TEXT NOT NULL, "
            "payload TEXT NOT NULL, run_id TEXT NOT NULL UNIQUE, phase TEXT NOT NULL, "
            "retain INTEGER NOT NULL, archive_receipt TEXT)"
        )
        db.execute(
            "INSERT INTO requests VALUES (?,?,?,?,?,?,?)",
            ("old", "digest", "{}", "original", "accepted", 1, None),
        )
    first = RequestJournal(tmp_path)
    assert first.lookup("old")["run_id"] == "original"
    assert RequestJournal(tmp_path).journal_id == first.journal_id
    assert first.protected("original")


@pytest.mark.asyncio
async def test_concurrent_http_requests_and_validation(tmp_path):
    import asyncio

    import httpx
    from fastapi import FastAPI

    from scripthut.api import make_api_router

    rm = manager(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()

    async def schedule(run):
        entered.set()
        await release.wait()

    rm.process_run.side_effect = schedule
    app = FastAPI()
    app.include_router(make_api_router(MagicMock(run_manager=rm)))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        caps = (await client.get("/api/v1/capabilities")).json()
        headers = {"X-ScriptHut-Journal-ID": caps["journal_id"]}
        payload = dict(PAYLOAD, request_key="key")
        assert (await client.post("/api/v1/submission-requests", json=payload)).status_code == 428
        wrong = {"X-ScriptHut-Journal-ID": "wrong"}
        assert (
            await client.post("/api/v1/submission-requests", json=payload, headers=wrong)
        ).status_code == 409
        assert (
            await client.post("/api/v1/tasks/run", json=dict(payload, typo=True))
        ).status_code == 422
        assert (await client.post("/api/v1/tasks/run", json=PAYLOAD)).status_code == 422
        first = asyncio.create_task(
            client.post("/api/v1/submission-requests", json=payload, headers=headers)
        )
        await asyncio.wait_for(entered.wait(), 2)
        second = await client.post("/api/v1/submission-requests", json=payload, headers=headers)
        assert second.status_code == 503
        release.set()
        original = await first
        retry = await client.post("/api/v1/submission-requests", json=payload, headers=headers)
        assert original.json()["id"] == retry.json()["id"]
        rm.process_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_scheduler_side_effect_after_failed_run_save(tmp_path):
    rm = manager(tmp_path)
    rm.storage.save_run = MagicMock(side_effect=OSError("disk unavailable"))
    with pytest.raises(OSError):
        await rm.create_keyed_adhoc_run("key", PAYLOAD)
    rm.process_run.assert_not_awaited()
    assert not rm.runs
    reserved = rm.request_journal.lookup("key")
    fresh = manager(tmp_path)
    await fresh.restore_from_storage()
    assert next(iter(fresh.runs)) == reserved["run_id"]
    fresh.process_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_restart_accepts_durable_run_before_scheduling(tmp_path):
    rm = manager(tmp_path)
    rm.request_journal.accepted = MagicMock(side_effect=OSError("crash before acceptance"))
    with pytest.raises(OSError):
        await rm.create_keyed_adhoc_run("key", PAYLOAD)
    fresh = manager(tmp_path)

    async def schedule(run):
        assert fresh.request_journal.lookup("key")["phase"] == "accepted"
        assert fresh.storage.load_all_runs()[run.id].request_key == "key"

    fresh.process_run.side_effect = schedule
    await fresh.restore_from_storage()
    fresh.process_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_all_later_keyed_run_saves_remain_durable(tmp_path):
    from unittest.mock import patch

    rm = manager(tmp_path)
    run = await rm.create_keyed_adhoc_run("key", PAYLOAD)
    loaded = rm.storage.load_all_runs()[run.id]
    with patch("scripthut.runs.storage.os.fsync") as sync:
        assert rm.storage.save_run(loaded)
    assert sync.call_count >= 2


@pytest.mark.asyncio
async def test_capability_unavailable_without_persistent_storage(tmp_path):
    import httpx
    from fastapi import FastAPI

    from scripthut.api import make_api_router

    rm = manager(tmp_path)
    rm.request_journal = None
    app = FastAPI()
    app.include_router(make_api_router(MagicMock(run_manager=rm)))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        caps = (await client.get("/api/v1/capabilities")).json()
        assert caps["capabilities"]["keyed_submission"] == 0
        assert caps["journal_id"] is None
        result = await client.post("/api/v1/tasks/run", json=dict(PAYLOAD, request_key="key"))
        assert result.status_code == 503
        rm.process_run.assert_not_awaited()


def test_storage_disables_journal_without_posix_support(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import patch

    storage = RunStorageManager(tmp_path)
    with patch("scripthut.runs.storage.os", SimpleNamespace(name="nt")):
        assert storage.request_journal is None
    assert not (tmp_path / "_submission_requests").exists()
