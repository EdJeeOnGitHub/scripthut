"""Presentation queries for completed-job efficiency; CLI windows stay unchanged."""
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from scripthut.config_schema import EfficiencyPolicy
from .efficiency import report, summarize

UTC = timezone.utc
PERIODS = [('today', 'Today'), ('week', 'This week'), ('last_week', 'Last week'),
           ('30_days', 'Last 30 days'), ('custom', 'Custom')]
SORTS = [('attention', 'Needs attention'), ('allocated_hours', 'Allocated CPU-hours'),
         ('cpu_efficiency', 'Lowest CPU efficiency'), ('memory_median_percent', 'Lowest memory utilization')]


def resolve_window(now, period='week', start='', end=''):
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    monday = midnight - timedelta(days=now.weekday())
    if start or end:
        period = 'custom'
    if period not in dict(PERIODS):
        raise ValueError('Choose a valid reporting period.')
    if period == 'custom':
        if not start or not end:
            raise ValueError('Choose both From and Through dates for a custom period.')
        try:
            beginning = datetime.strptime(start, '%Y-%m-%d').replace(tzinfo=UTC)
            calendar_end = datetime.strptime(end, '%Y-%m-%d').replace(tzinfo=UTC) + timedelta(days=1)
        except ValueError as exc:
            raise ValueError('Enter dates as YYYY-MM-DD.') from exc
        if calendar_end <= beginning or beginning > now:
            raise ValueError('End date must be on or after start date; the period cannot be wholly in the future.')
        shift = calendar_end - beginning
        ending = min(calendar_end, now)
    elif period == 'today':
        beginning, ending, shift = midnight, now, timedelta(days=1)
        calendar_end = midnight + shift
    elif period == 'week':
        beginning, ending, shift = monday, now, timedelta(days=7)
        calendar_end = monday + shift
    elif period == 'last_week':
        beginning, ending, shift = monday - timedelta(days=7), monday, timedelta(days=7)
        calendar_end = ending
    else:
        beginning, ending, shift = midnight - timedelta(days=29), now, timedelta(days=30)
        calendar_end = midnight + timedelta(days=1)
    if shift > timedelta(days=3660):
        raise ValueError('Choose a period of at most 3660 days.')
    return dict(period=period, start=beginning, end=ending,
                comparison_start=beginning-shift, comparison_end=ending-shift,
                partial=ending < calendar_end, hourly=period == 'today')


def attention_key(s, policy):
    assessment = s['assessment']
    gap = max(0, s['allocated_hours'] * policy.cpu_min_percent / 100 - s['consumed_hours'])
    memory = s['memory_median_percent']
    distance = max(0, abs(memory-policy.memory_target_percent)-policy.memory_tolerance_percent) if memory is not None else 0
    if assessment['reliability'] == 'outside_target':
        key = (0, -s['resource_failures'], -s['allocated_hours'])
    elif assessment['cpu'] == 'below_target' or assessment['memory'] in {'below_target', 'above_target'}:
        key = (1, -gap, -distance, -s['allocated_hours'])
    elif 'unknown' in assessment.values() or s['limited_evidence']:
        key = (2, -s['allocated_hours'])
    else:
        key = (3, -s['allocated_hours'])
    return (*key, s['project'])


def buckets(rows, window, policy):
    step = timedelta(hours=1) if window['hourly'] else timedelta(days=1)
    groups = {}
    for row in rows:
        stamp = datetime.fromisoformat(row['finished']).astimezone(UTC)
        key = stamp.replace(minute=0, second=0, microsecond=0)
        if not window['hourly']:
            key = key.replace(hour=0)
        groups.setdefault(key, []).append(row)
    result, cursor = [], window['start']
    while cursor < window['end']:
        end = min(cursor+step, window['end'])
        result.append(dict(start=cursor, end=end, partial=end < cursor+step,
                           label=cursor.strftime('%H:%M' if window['hourly'] else '%d %b'),
                           **summarize(groups.get(cursor, []), policy)))
        cursor += step
    return result


def chart(series, key, title, target=None, band=None, percent=True):
    values = [b[key] for b in series if b[key] is not None]
    maximum = max([100 if percent and key != 'resource_failure_percent' else 1,
                   target or 0, *(band or []), *values]) * (1.05 if key == 'resource_failure_percent' or not percent else 1)
    def y(value):
        return round(105 - 90*value/maximum, 2)
    points, paths, segment = [], [], []
    for i, bucket in enumerate(series):
        x = round(45 + 710 * (i+.5)/max(1, len(series)), 2)
        value = bucket[key]
        # A bucket with no CPU measurement is not measured zero workload.
        if key == 'allocated_hours' and not bucket['cpu_measured']:
            value = None
        if value is None:
            if segment:
                paths.append(' '.join(segment)); segment = []
            continue
        segment.append(f'{x},{y(value)}')
        points.append(dict(x=x, y=y(value), value=value, bucket=bucket))
    if segment:
        paths.append(' '.join(segment))
    return dict(key=key, title=title, percent=percent, maximum=maximum, points=points, paths=paths,
                target=target, target_y=y(target) if target is not None else None,
                band=band, band_top=y(band[1]) if band else None,
                band_height=y(band[0])-y(band[1]) if band else None,
                bar_width=max(1, min(24, 560/max(1,len(series)))))


def dashboard(state, *, now=None, period='week', start='', end='', project='', backend='', workflow='', sort='attention', offset=0):
    now = now or datetime.now(UTC)
    window = resolve_window(now, period, start, end)
    if offset < 0:
        raise ValueError('Job offset must be nonnegative.')
    if sort not in dict(SORTS):
        raise ValueError('Choose a valid sort order.')
    if state.run_storage is None:
        raise RuntimeError('Efficiency history is temporarily unavailable.')
    policy = state.config.settings.efficiency if state.config else EfficiencyPolicy()
    store = state.run_storage.efficiency_store
    runs = list(state.run_manager.runs.values()) if state.run_manager else []
    store.record_runs(runs)
    all_rows = store.records(window['comparison_start'], window['end'])
    choices = {key: sorted({r[key] for r in all_rows} | ({value} if value else set()))
               for key, value in [('project',project), ('backend',backend), ('workflow',workflow)]}
    filtered = [r for r in all_rows if all(not v or r[k] == v for k,v in
                [('project',project), ('backend',backend), ('workflow',workflow)])]
    current, previous = [], []
    for row in filtered:
        stamp = datetime.fromisoformat(row['finished']).astimezone(UTC)
        if window['start'] <= stamp < window['end']:
            current.append(row)
        elif window['comparison_start'] <= stamp < window['comparison_end']:
            previous.append(row)
    result = report(current, sort=sort, policy=policy)
    prior = report(previous, policy=policy)
    prior_projects = {p['project']: p for p in prior['projects']}
    metrics = ['cpu_efficiency', 'memory_median_percent', 'resource_failure_percent']
    def compare(item, old):
        item['changes'] = {key: item[key]-old[key] if old and item[key] is not None and old[key] is not None else None for key in metrics}
        item['cpu_gap_hours'] = max(0,item['allocated_hours']*policy.cpu_min_percent/100-item['consumed_hours'])
    compare(result['overall'],prior['overall'])
    for item in result['projects']:
        compare(item,prior_projects.get(item['project']))
    if sort == 'attention':
        result['projects'].sort(key=lambda s: attention_key(s,policy))
    series = buckets(current,window,policy)
    charts = [chart(series,'cpu_efficiency','CPU efficiency',policy.cpu_min_percent),
              chart(series,'memory_median_percent','Median peak / requested memory',band=(policy.memory_target_percent-policy.memory_tolerance_percent,policy.memory_target_percent+policy.memory_tolerance_percent)),
              chart(series,'resource_failure_percent','Resource failures',policy.resource_failure_max_percent),
              chart(series,'allocated_hours','Measured allocated CPU-hours',percent=False)]
    scale = max([100,policy.memory_target_percent+policy.memory_tolerance_percent,
                 *[p[k] for p in result['projects'] for k in ['cpu_efficiency','memory_median_percent'] if p[k] is not None]])
    params = dict(period=window['period'],project=project,backend=backend,workflow=workflow,sort=sort)
    if window['period']=='custom':
        params.update(start=start,end=end)
    def link(**changes):
        values = dict(params, **changes)
        return '?' + urlencode({k:v for k,v in values.items() if v is not None and v != ''})
    return dict(report=result, previous=prior['overall'], policy=policy, window=window, generated_at=now,
                latest_completion=current[0]['finished'] if current else None,
                choices=choices, filters=dict(params,start=start,end=end), periods=PERIODS, sorts=SORTS,
                series=series, charts=charts, scale=scale, link=link,
                jobs=current[offset:offset+50], offset=offset, total=len(current),
                live_run_ids={r.id for r in runs})
