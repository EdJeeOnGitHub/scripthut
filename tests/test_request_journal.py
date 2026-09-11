from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.runs.manager import RunManager
from scripthut.runs.models import RunItemStatus
from scripthut.runs.request_journal import RequestBusy, RequestConflict, RequestJournal
from scripthut.runs.storage import RunStorageManager

PAYLOAD = {'backend': 'test', 'task': {'id': 'bootstrap', 'name': 'prepare', 'command': 'true'},
           'retain_until_archived': True}


def manager(root):
    config = MagicMock()
    config.cache = None
    config.get_backend.return_value.account = None
    config.get_backend.return_value.login_shell = False
    rm = RunManager(config, {}, RunStorageManager(root), job_backends={'test': MagicMock()})
    rm.process_run = AsyncMock()
    return rm


@pytest.mark.asyncio
async def test_durable_before_schedule_and_retry_after_lost_response(tmp_path):
    rm = manager(tmp_path)
    async def check(run):
        assert run.id in rm.storage.load_all_runs()
        assert rm.request_journal.lookup('key')['phase'] == 'accepted'
    rm.process_run.side_effect = check
    first = await rm.create_keyed_adhoc_run('key', PAYLOAD)
    second = await rm.create_keyed_adhoc_run('key', PAYLOAD)
    assert first.id == second.id
    assert rm.process_run.await_count == 1
    fresh = manager(tmp_path)
    assert (await fresh.create_keyed_adhoc_run('key', PAYLOAD)).id == first.id
    fresh.process_run.assert_not_awaited()
    with pytest.raises(RequestConflict):
        await fresh.create_keyed_adhoc_run('key', dict(PAYLOAD, run_name='changed'))


@pytest.mark.asyncio
async def test_reservation_and_durable_save_crash_recovery(tmp_path):
    rm = manager(tmp_path)
    reserved = rm.request_journal.reserve('key', PAYLOAD)
    rm.request_journal.accepted = MagicMock(side_effect=OSError('crash after run save'))
    with pytest.raises(OSError):
        await rm.create_keyed_adhoc_run('key', PAYLOAD)
    rm.process_run.assert_not_awaited()
    fresh = manager(tmp_path)
    run = await fresh.create_keyed_adhoc_run('key', PAYLOAD)
    assert run.id == reserved['run_id']
    assert fresh.request_journal.lookup('key')['phase'] == 'accepted'
    # The normal restored-run scheduler, not HTTP replay, resumes the durable run.
    await fresh.restore_from_storage()
    assert fresh.process_run.await_count == 1


@pytest.mark.asyncio
async def test_retention_protection_ack_and_no_reexecution_after_expiry(tmp_path):
    rm = manager(tmp_path)
    run = await rm.create_keyed_adhoc_run('key', PAYLOAD)
    # Remove the original fixture path before saving the backdated record.
    rm.storage.delete_run(run)
    run.created_at = datetime.now(UTC) - timedelta(days=40)
    run.items[0].status = RunItemStatus.COMPLETED
    rm.storage.save_run(run, durable=True)
    assert rm.storage.cleanup_old_runs() == 0
    rm.request_journal.acknowledge('key', 'a' * 64)
    assert rm.storage.cleanup_old_runs() == 1
    fresh = manager(tmp_path)
    assert await fresh.create_keyed_adhoc_run('key', PAYLOAD) is None
    fresh.process_run.assert_not_awaited()
    assert fresh.request_journal.lookup('key')['run_id'] == run.id


def test_separate_instances_exclude_concurrent_creation(tmp_path):
    a, b = RequestJournal(tmp_path), RequestJournal(tmp_path)
    with a.lease('key'):
        with pytest.raises(RequestBusy):
            with b.lease('key'):
                pytest.fail('Must not enter overlapping creation')
    with b.lease('key'):
        assert a.reserve('key', PAYLOAD)['run_id'] == b.reserve('key', PAYLOAD)['run_id']


@pytest.mark.asyncio
async def test_http_contract_conflict_lookup_and_archive_ack(tmp_path):
    from fastapi import FastAPI
    import httpx
    from scripthut.api import make_api_router
    rm = manager(tmp_path)
    state = MagicMock(run_manager=rm, config_error=None)
    app = FastAPI()
    app.include_router(make_api_router(state))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
        missing = await client.get('/api/v1/submission-requests/research-run:key')
        assert missing.status_code == 404
        payload = dict(PAYLOAD, request_key='research-run:key')
        first = await client.post('/api/v1/tasks/run', json=payload)
        second = await client.post('/api/v1/tasks/run', json=payload)
        assert first.status_code == second.status_code == 200
        assert first.json()['id'] == second.json()['id']
        changed = await client.post('/api/v1/tasks/run', json=dict(payload, run_name='other'))
        assert changed.status_code == 409
        premature = await client.post('/api/v1/submission-requests/research-run:key/archive',
                                      json={'archive_receipt_sha256': 'a' * 64})
        assert premature.status_code == 422
        rm.runs[first.json()['id']].items[0].status = RunItemStatus.COMPLETED
        response = await client.post('/api/v1/submission-requests/research-run:key/archive',
                                     json={'archive_receipt_sha256': 'a' * 64})
        assert response.status_code == 200
        record = (await client.get('/api/v1/submission-requests/research-run:key')).json()
        assert record['phase'] == 'accepted' and record['archive_receipt'] == 'a' * 64
        assert 'payload' not in record
        rm.process_run.assert_awaited_once()
