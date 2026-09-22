"""Shared read-only efficiency queries for HTTP and local clients."""
from datetime import datetime, timedelta, timezone

from .efficiency import report, measured_job


def query(state, kind, *, days=30, project='', backend='', workflow='', run_id='', limit=100, offset=0):
    if kind not in {'summary', 'jobs', 'run'}:
        raise ValueError('Unknown efficiency report')
    if not 1 <= days <= 3660 or not 1 <= limit <= 1000 or offset < 0:
        raise ValueError('days must be 1–3660; limit 1–1000; offset nonnegative')
    if state.run_storage is None or state.config is None:
        raise RuntimeError('Efficiency history is unavailable')
    store = state.run_storage.efficiency_store
    # Surface failures to the caller instead of returning a misleading empty report.
    runs = list(state.run_manager.runs.values()) if state.run_manager else []
    store.record_runs(runs)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    rows = store.records(None if kind == 'run' else start, None if kind == 'run' else end,
                         project=project, backend=backend, workflow=workflow, run_id=run_id)
    exists = any(r.id == run_id for r in runs)
    if kind == 'run' and not rows and not exists:
        exists = run_id in state.run_storage.load_all_runs()
        if not exists:
            raise LookupError('Unknown run or no retained efficiency history')
    policy = state.config.settings.efficiency
    result = dict(schema_version=1, generated_at=end.isoformat(), targets=policy.model_dump(),
                  filters=dict(project=project or None, backend=backend or None, workflow=workflow or None, run_id=run_id or None),
                  window=None if kind == 'run' else dict(start=start.isoformat(), end=end.isoformat(), days=days),
                  evidence='measured' if rows else 'no_final_attempts' if kind == 'run' else 'no_history')
    summary = report(rows, policy=policy)
    result['overall'] = summary['overall']
    if kind == 'summary':
        result.update(projects=summary['projects'], groups=summary['groups'])
    else:
        result.update(total=len(rows), limit=limit, offset=offset,
                      jobs=[measured_job(r, policy) for r in rows[offset:offset+limit]])
    return result
