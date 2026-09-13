from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.runs.models import RunItemStatus
from scripthut.runs.manager import RunManager
from scripthut.runs.storage import RunStorageManager

PAYLOAD = {'backend': 'test', 'task': {'id': 'bootstrap', 'name': 'prepare', 'command': 'true'},
           'retain_until_archived': True}


@pytest.mark.asyncio
async def test_refs_are_durable_before_schedule_and_outputs_retry_without_execution(tmp_path):
    references = {'schema_version': 1, 'request_id': 'a' * 32, 'source_commit': 'b' * 40,
                  'inputs': [{'name': 'data', 'version': 'sha256:' + 'c' * 64}], 'outputs': []}
    config = MagicMock()
    config.cache = None
    config.get_backend.return_value.account = None
    config.get_backend.return_value.login_shell = False
    rm = RunManager(config, {}, RunStorageManager(tmp_path), job_backends={'test': MagicMock()})

    async def scheduled(run):
        assert rm.storage.load_all_runs()[run.id].artifact_refs == references

    rm.process_run = AsyncMock(side_effect=scheduled)
    run = await rm.create_keyed_adhoc_run('artifact-key', dict(PAYLOAD, artifact_refs=references))
    output = dict(references, outputs=[{'name': 'reports', 'version': 'sha256:' + 'd' * 64}])
    with pytest.raises(ValueError, match='completed'):
        await rm.attach_artifact_outputs(run.id, output)
    run.items[0].status = RunItemStatus.COMPLETED
    first = await rm.attach_artifact_outputs(run.id, output)
    assert await rm.attach_artifact_outputs(run.id, output) == first
    assert rm.storage.load_all_runs()[run.id].artifact_refs == output
    rm.process_run.assert_awaited_once()
    with pytest.raises(ValueError, match='new launcher request'):
        await rm.rerun_in_place(run.id)
    with pytest.raises(ValueError, match='cannot change'):
        await rm.attach_artifact_outputs(run.id, dict(output, source_commit='e' * 40))
    with pytest.raises(ValueError, match='another version'):
        await rm.attach_artifact_outputs(run.id, dict(output, outputs=[{'name': 'reports', 'version': 'sha256:' + 'f' * 64}]))
