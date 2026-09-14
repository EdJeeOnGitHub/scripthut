from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock
import pytest

from scripthut.reports.resources import ResourceUsage, slurm_resources
from scripthut.reports.efficiency import EfficiencyStore, summarize, report
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
from scripthut.runs.storage import RunStorageManager

NOW = datetime(2026, 9, 14, tzinfo=timezone.utc)


def run(job='12', status=RunItemStatus.COMPLETED, usage=None):
    item = RunItem(TaskDefinition(id='fit', name='fit', command='true', cpus=4, memory='8G',
                   project_id='study', workflow_id='model'), status=status,
                   job_id=job, submitted_at=NOW, started_at=NOW, finished_at=NOW+timedelta(hours=1),
                   resource_usage=usage or ResourceUsage(3600, 14400, 7200, 8*1024**3, 8*1024**3,
                                                        2*1024**3, 'single_task_job', 'test'))
    return Run(id='r1', workflow_name='anonymous/request-123', backend_name='cluster',
               created_at=NOW, items=[item], max_concurrent=None)


def row(jid, cpu, peak='', tres='', req='', nodes='', tasks=''):
    return f'{jid}|{cpu}|01:00:00|4|{peak}|2026-09-14T00:00:00|2026-09-14T01:00:00|COMPLETED|0:0|{tres}|{req}|{nodes}|{tasks}\n'


def test_parent_cpu_aggregate_is_not_replaced_by_batch_or_added_to_steps():
    raw = row('12', '02:00:00', tres='cpu=4,mem=8G', req='2Gc', nodes='1', tasks='4')
    raw += row('12.batch', '00:01:00', '1G', tasks='1')
    raw += row('12.0', '01:59:00', '2G', tasks='4')
    usage = slurm_resources(raw)['12']
    assert usage.cpu_efficiency == 50
    assert usage.average_cores == 2
    assert usage.requested_memory_bytes == 8*1024**3
    assert usage.reported_peak_bytes == 2*1024**3
    assert usage.memory_scope == 'task'
    assert usage.memory_percent is None


def test_step_fallback_excludes_extern_and_requires_complete_cpu_observations():
    raw = row('12','00:00:00')+row('12.batch','00:01:00')+row('12.0','01:59:00')+row('12.extern','00:20:00')
    assert slurm_resources(raw)['12'].consumed_cpu_seconds == 7200
    usage = slurm_resources(row('12','')+row('12.batch',''))['12']
    assert usage.consumed_cpu_seconds is None
    assert usage.reported_peak_bytes is None
    assert usage.cpu_efficiency is None


def test_single_task_peak_can_be_compared_and_measured_zero_survives():
    raw = row('12','00:00:00',tres='cpu=4,mem=8G',req='8Gn',nodes='1',tasks='1')
    raw += row('12.batch','00:00:00','2G',nodes='1',tasks='1')
    usage = slurm_resources(raw)['12']
    assert usage.memory_percent == 25
    assert usage.cpu_efficiency == 0
    assert ResourceUsage.from_dict(usage.to_dict()) == usage
    assert ResourceUsage.from_dict({'reported_peak_bytes': float('nan')}) is None


def test_persisted_metrics_survive_retry_and_deleted_run_and_use_project_metadata(tmp_path):
    storage = RunStorageManager(tmp_path)
    first = run()
    assert storage.save_run(first)
    assert storage.load_all_runs()['r1'].items[0].resource_usage.cpu_efficiency == 50
    assert storage.load_all_runs()['r1'].items[0].task.project_id == 'study'
    second = run('13')
    second.items[0].submitted_at += timedelta(hours=2)
    second.items[0].started_at += timedelta(hours=2)
    second.items[0].finished_at += timedelta(hours=2)
    storage.save_run(second)
    storage.efficiency_store.record_runs([second])
    for path in tmp_path.rglob('run.json'):
        path.unlink()
    rows = storage.efficiency_store.records(NOW, NOW+timedelta(days=1))
    assert len(rows) == 2
    assert {r['job_id'] for r in rows} == {'12', '13'}
    assert rows[0]['project'] == 'study'
    assert rows[0]['workflow'] == 'model'
    assert len(report(rows)['projects']) == 1


def test_weighted_cpu_and_success_only_memory_sizing(tmp_path):
    store = EfficiencyStore(tmp_path/'metrics.sqlite3')
    a = run()
    b = run('13', RunItemStatus.FAILED, ResourceUsage(100,100,100,100,100,100,'single_task_job'))
    b.items[0].scheduler_state='OUT_OF_MEMORY'
    store.record_runs([a,b])
    rows = store.records(NOW, NOW+timedelta(days=1))
    result = summarize(rows)
    assert result['cpu_efficiency'] == pytest.approx(7300/14500*100)
    assert result['memory_measured'] == 1
    assert result['peak_p95'] == 2*1024**3
    assert result['oom'] == 1
    assert result['failed'] == 1
    assert store.records(NOW, NOW+timedelta(days=1), project='other') == []


def test_external_and_cache_hits_excluded_and_history_not_fabricated(tmp_path):
    store = EfficiencyStore(tmp_path/'metrics.sqlite3')
    old = run(); old.items[0].resource_usage=None; old.items[0].max_rss='1G'; old.items[0].cpu_efficiency=90
    external=run('14'); external.workflow_name='_default'
    cached=run('15'); cached.items[0].cache_hit=True
    store.record_runs([old,external,cached])
    rows=store.records(NOW,NOW+timedelta(days=1))
    assert len(rows)==1
    assert rows[0]['usage']['reported_peak_bytes']==1024**3
    assert summarize(rows)['cpu_measured']==0
    assert summarize(rows)['memory_measured']==0


def test_efficiency_page_has_filters_coverage_and_archived_measurements(tmp_path, monkeypatch):
    import scripthut.main as main
    from fastapi.testclient import TestClient
    storage=RunStorageManager(tmp_path); storage.save_run(run())
    monkeypatch.setattr(main.state, 'run_storage', storage)
    manager=MagicMock(); manager.runs={}
    monkeypatch.setattr(main.state,'run_manager',manager)
    response=TestClient(main.app).get('/efficiency?start=2026-09-14&end=2026-09-14')
    assert response.status_code==200
    assert 'CPU coverage 1/1' in response.text
    assert 'Archived metrics' in response.text
    assert 'study' in response.text
    assert 'Mean RAM over time unavailable' in response.text
    assert TestClient(main.app).get('/efficiency?start=not-a-date').status_code==400


@pytest.mark.parametrize('repo', ['https://github.com/org/aggregate-transfers.git',
                                  'git@github.com:org/aggregate-transfers.git',
                                  'ssh://git@github.com/org/aggregate-transfers.git'])
def test_project_attribution_priority_and_repository_fallback(repo, tmp_path):
    from scripthut.reports.attribution import project_name
    from scripthut.runs.usage import UsageLog, records_from_runs
    r = run()
    r.git_repo = repo
    r.source_name = 'configured-source'
    assert project_name(r, r.items[0].task) == 'study'
    r.items[0].task.project_id = None
    assert project_name(r) == 'configured-source'
    r.source_name = None
    assert project_name(r) == 'aggregate-transfers'
    ledger = UsageLog(tmp_path/'usage.jsonl')
    r.git_repo = None
    assert project_name(r) is None
    assert ledger.record([r]) == 1
    r.git_repo = repo
    assert ledger.record([r]) == 1
    assert ledger.record([r]) == 0
    records = UsageLog(ledger.path).records()
    assert len(records) == 1
    assert records[0].source_name == 'aggregate-transfers'
    assert next(records_from_runs([r])).source_name == 'aggregate-transfers'
