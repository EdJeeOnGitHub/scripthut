from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from scripthut.config_schema import EfficiencyPolicy, ScriptHutConfig
from scripthut.reports.dashboard import dashboard, resolve_window, attention_key, chart
from scripthut.reports.efficiency import summarize
from scripthut.runs.storage import RunStorageManager
from tests.test_resource_efficiency import run

NOW = datetime(2026, 9, 16, 12, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize('period,start,prior', [
    ('today', '2026-09-16T00:00:00+00:00', '2026-09-15T00:00:00+00:00'),
    ('week', '2026-09-14T00:00:00+00:00', '2026-09-07T00:00:00+00:00'),
    ('last_week', '2026-09-07T00:00:00+00:00', '2026-08-31T00:00:00+00:00'),
    ('30_days', '2026-08-18T00:00:00+00:00', '2026-07-19T00:00:00+00:00'),
])
def test_periods(period, start, prior):
    w = resolve_window(NOW, period)
    assert w['start'].isoformat() == start
    assert w['comparison_start'].isoformat() == prior
    assert w['end']-w['start'] == w['comparison_end']-w['comparison_start']


def test_custom_and_boundary_windows():
    w = resolve_window(NOW, start='2026-09-14', end='2026-09-15')
    assert w['end'] == NOW.replace(hour=0, minute=0)
    assert not w['partial']
    w = resolve_window(NOW, start='2026-09-14', end='2026-09-20')
    assert w['end'] == NOW and w['partial']
    assert w['comparison_end'] == NOW-timedelta(days=7)
    jan = resolve_window(datetime(2027, 1, 1, tzinfo=timezone.utc),'week')
    assert jan['start'].isoformat().startswith('2026-12-28')
    monday = resolve_window(datetime(2026, 9, 14, tzinfo=timezone.utc),'week')
    assert monday['start'] == monday['end']
    for args in [dict(start='bad',end='2026-09-15'),dict(start='2026-09-16',end='2026-09-15'),dict(start='2027-01-01',end='2027-01-02'),dict(period='missing')]:
        with pytest.raises(ValueError): resolve_window(NOW,**args)


@pytest.fixture
def state(tmp_path):
    storage = RunStorageManager(tmp_path)
    current = run()
    prior = run('prior')
    prior.id = 'prior-run'
    prior.items[0].finished_at -= timedelta(days=7)
    prior.items[0].task.project_id = 'study'
    storage.save_run(current)
    storage.save_run(prior)
    return SimpleNamespace(config=ScriptHutConfig(), run_storage=storage,
                           run_manager=SimpleNamespace(runs={}))


def test_dashboard_aggregation_gaps_comparison_and_filters(state):
    d = dashboard(state, now=NOW)
    assert d['report']['overall']['jobs'] == 1
    assert d['previous']['jobs'] == 1
    assert d['report']['overall']['changes']['cpu_efficiency'] == 0
    assert sum(b['allocated_hours'] for b in d['series']) == d['report']['overall']['allocated_hours']
    assert len(d['series']) == 3
    assert d['series'][1]['cpu_efficiency'] is None
    assert len(d['charts'][0]['points']) == 1
    assert len(d['charts'][3]['points']) == 1
    assert d['series'][-1]['partial']
    assert 'period=week' in d['link'](project='study')
    empty = dashboard(state,now=NOW,project='absent')
    assert empty['total'] == 0 and empty['report']['overall']['cpu_efficiency'] is None
    assert 'absent' in empty['choices']['project']
    today = dashboard(state,now=NOW,period='today')
    assert len(today['series']) == 13 and today['series'][-1]['partial']


def test_pagination_does_not_change_totals(state):
    for i in range(60):
        r=run('extra-'+str(i)); r.id=str(i); state.run_storage.save_run(r)
    a=dashboard(state,now=NOW,project='study')
    b=dashboard(state,now=NOW,project='study',offset=50)
    assert a['total']==b['total']==61
    assert len(a['jobs'])==50 and len(b['jobs'])==11
    assert a['report']['overall']==b['report']['overall']
    assert not {r['identity'] for r in a['jobs']} & {r['identity'] for r in b['jobs']}


def test_attention_prefers_impact_and_reliability(state):
    policy=EfficiencyPolicy()
    base=dashboard(state,now=NOW)['report']['projects'][0]
    small=dict(base,project='small',allocated_hours=1,consumed_hours=0)
    big=dict(base,project='big',allocated_hours=100,consumed_hours=70)
    fail=dict(base,project='failure',resource_failures=1,assessment=dict(base['assessment'],reliability='outside_target'))
    assert sorted([small,big,fail],key=lambda s: attention_key(s,policy)) == [fail,big,small]
    unknown=dict(summarize([]),project='unknown')
    assert attention_key(unknown,policy)[0]==2


def test_chart_breaks_gaps_and_preserves_values_above_100():
    series=[dict(cpu_efficiency=v) for v in [20,None,120]]
    c=chart(series,'cpu_efficiency','CPU',80)
    assert len(c['paths'])==2 and c['maximum']==120
    assert c['points'][-1]['value']==120


def test_page_overview_drilldown_and_errors(state, monkeypatch):
    import scripthut.main as main
    for key in ['config', 'run_storage', 'run_manager']:
        monkeypatch.setattr(main.state, key, getattr(state, key))
    client=TestClient(main.app)
    response=client.get('/efficiency?start=2026-09-14&end=2026-09-15')
    assert response.status_code==200
    assert 'Completed-job efficiency' in response.text
    assert 'View exact trend data' in response.text
    assert 'Archived metrics' not in response.text
    assert 'period=custom' in response.text
    detail=client.get('/efficiency?start=2026-09-14&end=2026-09-15&project=study')
    assert detail.status_code==200
    assert 'Archived metrics' in detail.text and 'All projects' in detail.text
    assert 'Workflows and backends' in detail.text
    assert client.get('/efficiency?start=bad').status_code==400
    monkeypatch.setattr(main.state, 'run_storage', None)
    response=client.get('/efficiency')
    assert response.status_code==503
    assert 'temporarily unavailable' in response.text
    assert 'metric' not in response.text.split('<body>')[1]
