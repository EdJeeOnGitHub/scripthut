from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from fastapi import FastAPI

from scripthut.api import make_api_router
from scripthut.cli import RemoteClient, _cmd_efficiency, _format_efficiency
from scripthut.config_schema import EfficiencyPolicy, ScriptHutConfig
from scripthut.reports.efficiency import summarize, report
from scripthut.reports.query import query
from scripthut.runs.storage import RunStorageManager
from tests.test_resource_efficiency import run


@pytest.fixture
def state(tmp_path):
    storage = RunStorageManager(tmp_path)
    value = run()
    item = value.items[0]
    now = datetime.now(timezone.utc)
    item.submitted_at = item.started_at = now - timedelta(hours=1)
    item.finished_at = now - timedelta(minutes=1)
    item.scheduler_state = 'COMPLETED'
    storage.save_run(value)
    return SimpleNamespace(config=ScriptHutConfig(), run_storage=storage,
                           run_manager=SimpleNamespace(runs={value.id: value}))


def test_query_scope_policy_pagination_and_retained_run(state):
    result = query(state, 'summary', project='study')
    assert result['overall']['cpu_efficiency'] == 50
    assert result['overall']['assessment']['reliability'] == 'limited_evidence'
    assert result['targets']['cpu_min_percent'] == 80
    assert query(state, 'summary', project='other')['evidence'] == 'no_history'
    assert query(state, 'jobs', offset=1)['jobs'] == []
    assert query(state, 'jobs')['total'] == 1
    state.run_manager.runs.clear()
    assert query(state, 'run', run_id='r1')['total'] == 1
    with pytest.raises(LookupError):
        query(state, 'run', run_id='missing')
    with pytest.raises(ValueError):
        query(state, 'summary', days=0)
    with pytest.raises(ValueError):
        query(state, 'jobs', limit=1001)


def test_early_oom_and_cancellation_without_measurements(state):
    value = state.run_manager.runs['r1']
    item = value.items[0]
    from scripthut.runs.models import RunItemStatus
    item.status = RunItemStatus.FAILED
    item.started_at = None
    item.resource_usage = None
    item.cpu_efficiency = None
    item.max_rss = None
    item.scheduler_state = 'OUT_OF_MEMORY'
    result = query(state, 'summary')['overall']
    assert result['oom'] == 1 and result['eligible_attempts'] == 1
    assert result['cpu_efficiency'] is None
    assert result['assessment']['reliability'] == 'outside_target'
    item.scheduler_state = 'CANCELLED by 123'
    result = query(state, 'summary')['overall']
    assert result['eligible_attempts'] == 0 and result['cancelled'] == 1
    assert result['resource_failure_percent'] is None
    item.scheduler_state = None
    assert query(state, 'summary')['overall']['unknown_outcomes'] == 1


def test_weighted_totals_and_failure_boundary(state):
    row = query(state, 'jobs')['jobs'][0]
    failed = dict(row, status='failed', scheduler_state='TIMEOUT')
    assert summarize([row]*99+[failed])['assessment']['reliability'] == 'outside_target'
    assert summarize([row]*100+[failed])['assessment']['reliability'] == 'within_target'
    assert summarize([row]*99)['assessment']['reliability'] == 'limited_evidence'
    larger = dict(row, usage=dict(row['usage'], allocated_cpu_seconds=144000, consumed_cpu_seconds=144000))
    assert summarize([row, larger])['cpu_efficiency'] == pytest.approx(100*151200/158400)
    partial = dict(row, usage=dict(row['usage'], memory_scope='task'))
    assert summarize([partial])['memory_measured'] == 0
    assert report([row])['overall'] == summarize([row])


def test_time_bounds_and_unfinished_run(state):
    value = state.run_manager.runs['r1']
    store = state.run_storage.efficiency_store
    finish = value.items[0].finished_at
    assert len(store.records(finish, finish+timedelta(seconds=1))) == 1
    assert not store.records(finish-timedelta(seconds=1), finish)
    value.id = 'unfinished'
    value.items[0].finished_at = None
    state.run_manager.runs = {value.id: value}
    assert query(state, 'run', run_id='unfinished')['evidence'] == 'no_final_attempts'


@pytest.mark.asyncio
async def test_api_remote_client_and_error_contract(state):
    app = FastAPI()
    app.include_router(make_api_router(state))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test/api/v1') as http:
        client = RemoteClient('http://test')
        await client._client.aclose()
        client._client = http
        data = await client.efficiency('summary', days=7, project='study')
        assert data['window']['days'] == 7
        assert data['overall']['jobs'] == 1
        assert 'OOMs: 0' in _format_efficiency(data)
        assert (await http.get('/efficiency/jobs?limit=0')).status_code == 422
        assert (await http.get('/efficiency/runs/absent')).status_code == 404
        state.run_storage = None
        assert (await http.get('/efficiency/summary')).status_code == 503


@pytest.mark.asyncio
async def test_cli_project_inference_and_all_projects(state, capsys):
    class Client:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def efficiency(self, kind, **params):
            assert params['project'] == expected
            return query(state, kind, **params)
    args = SimpleNamespace(efficiency_cmd='summary', days=7, backend=None, workflow=None,
                           all_projects=False, project=None, json=True)
    expected = 'study'
    with patch('scripthut.cli._make_client', return_value=Client()), patch('scripthut.projects.submission_project', return_value='study') as infer:
        assert await _cmd_efficiency(args) == 0
        infer.assert_called_once_with(None)
        args.all_projects = True
        expected = ''
        assert await _cmd_efficiency(args) == 0
        assert infer.call_count == 1
    assert '"targets"' in capsys.readouterr().out


def test_policy_validation():
    with pytest.raises(ValueError): EfficiencyPolicy(cpu_min_percent=101)
    with pytest.raises(ValueError): EfficiencyPolicy(memory_target_percent=95)
    with pytest.raises(ValueError): EfficiencyPolicy(resource_failure_max_percent=0)


def test_cli_parser_and_explicit_project():
    from scripthut.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(['efficiency', 'summary', '--project', 'study', '--days', '7', '--json'])
    assert args.project == 'study' and args.days == 7 and args.json
    args = parser.parse_args(['efficiency', 'run', 'r1', '--offset', '100'])
    assert args.id == 'r1' and args.offset == 100
    with pytest.raises(SystemExit):
        parser.parse_args(['efficiency', 'summary', '--project', 'study', '--all-projects'])


@pytest.mark.asyncio
async def test_old_server_has_actionable_error():
    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(lambda r: httpx.Response(404))) as http:
        client = RemoteClient('http://test')
        await client._client.aclose()
        client._client = http
        with pytest.raises(RuntimeError, match='upgrade the server'):
            await client.efficiency('summary')
