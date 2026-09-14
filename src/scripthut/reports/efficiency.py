"""Derived per-attempt metrics, independent of run retention and job lifecycle."""
from __future__ import annotations
import json
import logging
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median

from .attribution import project_name
from .resources import ResourceUsage, bytes_value

log = logging.getLogger(__name__)


def historic_usage(item):
    if item.resource_usage is not None:
        return item.resource_usage
    # Retain only the historical values actually recorded. A CPU percentage
    # without its accounting denominator cannot supply weighted CPU totals.
    return ResourceUsage(reported_peak_bytes=bytes_value(item.max_rss),
                         source='historical run record; accounting scope unavailable')


class EfficiencyStore:
    def __init__(self, path):
        self.path = Path(path)

    @contextmanager
    def connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=3)
        connection.execute('''CREATE TABLE IF NOT EXISTS metrics (
            identity TEXT PRIMARY KEY, finished TEXT NOT NULL, backend TEXT NOT NULL,
            project TEXT NOT NULL, workflow TEXT NOT NULL, payload TEXT NOT NULL)''')
        connection.execute('CREATE INDEX IF NOT EXISTS metrics_finished ON metrics(finished)')
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def record_runs(self, runs):
        rows = []
        for run in runs:
            if run.workflow_name.startswith('_default'):
                continue
            for item in run.items:
                if item.status.value not in ('completed', 'failed') or not item.job_id or not item.started_at or not item.finished_at or item.cache_hit:
                    continue
                # Submission time distinguishes reused job IDs and reruns even
                # when old records predate explicit submission attempt IDs.
                attempt = next((a.id for a in reversed(item.submission_attempts) if a.job_id == item.job_id), None)
                stamp = attempt or (item.submitted_at or item.started_at).isoformat()
                identity = json.dumps([run.backend_name, item.job_id, stamp])
                project = project_name(run, item.task) or 'Unattributed'
                resource = historic_usage(item)
                row = dict(identity=identity, run_id=run.id, task_id=item.task.id,
                    job_id=item.job_id, backend=run.backend_name, project=project,
                    workflow=item.task.workflow_id or run.workflow_name, started=item.started_at.isoformat(),
                    finished=item.finished_at.astimezone(timezone.utc).isoformat(),
                    status=item.status.value, scheduler_state=item.scheduler_state,
                    requested_cpus=item.task.cpus, requested_memory=item.task.memory,
                    historical_cpu_efficiency=item.cpu_efficiency, usage=resource.to_dict())
                rows.append((identity, row['finished'], row['backend'], project, row['workflow'], json.dumps(row, allow_nan=False)))
        if not rows:
            return
        with self.connect() as db:
            db.executemany('''INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(identity) DO UPDATE SET finished=excluded.finished,
                backend=excluded.backend, project=excluded.project,
                workflow=excluded.workflow, payload=excluded.payload
                WHERE metrics.payload != excluded.payload''', rows)

    def safe_record(self, runs):
        try:
            self.record_runs(runs)
        except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
            # Reporting cannot make an otherwise successful run save fail.
            log.warning('Efficiency metrics unavailable: %s', exc)

    def records(self, start, end, *, backend='', project='', workflow=''):
        if not self.path.exists():
            return []
        query = 'SELECT payload FROM metrics WHERE finished >= ? AND finished < ?'
        args = [start.astimezone(timezone.utc).isoformat(), end.astimezone(timezone.utc).isoformat()]
        for key, value in [('backend', backend), ('project', project), ('workflow', workflow)]:
            if value:
                query += f' AND {key} = ?'
                args.append(value)
        with self.connect() as db:
            return [json.loads(row[0]) for row in db.execute(query+' ORDER BY finished DESC', args)]


def percentile(values, fraction):
    values = sorted(values)
    if not values:
        return None
    position = (len(values)-1)*fraction
    low = int(position)
    high = min(low+1, len(values)-1)
    return values[low] + (values[high]-values[low])*(position-low)


def summarize(rows):
    cpu_pairs, peaks, ratios = [], [], []
    for row in rows:
        usage = ResourceUsage.from_dict(row['usage'])
        if not usage:
            continue
        if usage.allocated_cpu_seconds and usage.consumed_cpu_seconds is not None:
            cpu_pairs.append((usage.allocated_cpu_seconds, usage.consumed_cpu_seconds))
        if row['status'] == 'completed' and usage.memory_percent is not None:
            peaks.append(usage.reported_peak_bytes)
            ratios.append(usage.memory_percent)
    allocated = sum(a for a, _ in cpu_pairs)
    consumed = sum(c for _, c in cpu_pairs)
    return dict(jobs=len(rows), cpu_measured=len(cpu_pairs), memory_measured=len(peaks),
        allocated_hours=allocated/3600, consumed_hours=consumed/3600,
        cpu_efficiency=100*consumed/allocated if allocated else None,
        peak_mean=mean(peaks) if peaks else None, peak_median=median(peaks) if peaks else None,
        peak_p95=percentile(peaks, .95), peak_max=max(peaks) if peaks else None,
        memory_median_percent=median(ratios) if ratios else None,
        oom=sum(r.get('scheduler_state') == 'OUT_OF_MEMORY' for r in rows),
        timeouts=sum(r.get('scheduler_state') == 'TIMEOUT' for r in rows),
        failed=sum(r['status'] == 'failed' for r in rows))


def report(rows, sort='allocated_hours'):
    groups = defaultdict(list)
    for row in rows:
        groups[(row['project'], row['workflow'], row['backend'])].append(row)
    summaries = [dict(project=key[0], workflow=key[1], backend=key[2], **summarize(items))
                 for key, items in groups.items()]
    sort = sort if sort in ('allocated_hours', 'cpu_efficiency', 'memory_median_percent') else 'allocated_hours'
    summaries.sort(key=lambda r: (r[sort] is None, -(r[sort] or 0) if sort == 'allocated_hours' else (r[sort] or 0)))
    projects = defaultdict(list)
    for row in rows:
        projects[row['project']].append(row)
    project_summaries = [dict(project=key, **summarize(items)) for key, items in projects.items()]
    project_summaries.sort(key=lambda r: (r[sort] is None, -(r[sort] or 0) if sort == 'allocated_hours' else (r[sort] or 0)))
    return dict(overall=summarize(rows), projects=project_summaries, groups=summaries, jobs=rows)
