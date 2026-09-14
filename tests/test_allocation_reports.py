from datetime import datetime, timezone, timedelta
import json
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import pytest
from scripthut.reports.allocations import AllocationReader, view

NOW = datetime(2026, 9, 14, tzinfo=timezone.utc)


def sample(**kwargs):
    return dict(id='shared', account='group', backends=['cluster-a', 'cluster-b'],
        unit='SU', allowance_type='budget', period_label='2026', observed_at=NOW.isoformat(),
        source='site accounting', used=40, allowance=100, balance=60,
        running={'jobs': 1, 'unpriced_jobs': 0, 'estimate': 10},
        queued={'jobs': 2, 'unpriced_jobs': 0, 'estimate': 20}, **kwargs)


def test_distinguishes_balance_from_projected_headroom():
    row = view(sample(), NOW)
    assert row['remaining'] == 60
    assert row['headroom'] == 30
    assert row['widths'] == [40, 10, 20]
    assert not row['stale']


def test_overbudget_and_unknown_costs():
    row = sample(); row.update(used=120, balance=-20)
    row['queued'] = {'jobs': 2, 'unpriced_jobs': 2, 'estimate': None}
    result = view(row, NOW)
    assert result['widths'] == [100, 0, 0]
    assert result['remaining'] == -20
    assert result['partial']
    row.update(allowance=None, balance=None)
    assert view(row, NOW)['remaining'] is None
    row.update(allowance=0, balance=0)
    assert view(row, NOW)['widths'] == [0, 0, 0]


def test_shared_record_and_failed_refresh_preserve_observation(tmp_path, monkeypatch):
    path = tmp_path/'snapshot.json'
    path.write_text(json.dumps({'schema_version': 1, 'allocations': [sample()]}))
    monkeypatch.setenv('SCRIPTHUT_ALLOCATION_REPORT_FILE', str(path))
    reader = AllocationReader()
    assert len(reader.read(now=NOW)) == 1
    assert set(reader.by_backend()) == {'cluster-a', 'cluster-b'}
    path.write_text('{')
    assert reader.read(now=NOW)['shared']['stale']
    assert reader.read(now=NOW)['shared']['used'] == 40
    assert view(sample(), NOW+timedelta(minutes=31))['stale']


@pytest.mark.parametrize('value', [-1, float('nan'), float('inf'), True])
def test_invalid_usage_is_not_zero(value):
    row = sample(); row['used'] = value
    with pytest.raises(ValueError): view(row, NOW)


def test_render_has_bounded_bar_shared_label_and_explicit_unknowns():
    row = sample(); row['queued']['estimate'] = None; row['queued']['unpriced_jobs'] = 2
    env = Environment(loader=FileSystemLoader(str(Path(__file__).parents[1]/'templates')), autoescape=True)
    html = env.get_template('_allocation.html').module.allocation_bar(view(row, NOW))
    assert 'Shared balance: cluster-a, cluster-b' in html
    assert '2 unpriced' in html
    assert 'Forecast incomplete' in html
    assert 'Remaining 60.0' in html
