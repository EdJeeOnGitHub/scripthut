"""Versioned, site-neutral allocation snapshots produced by host collectors."""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)
MAX_BYTES = 1024 * 1024


def instant(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timestamps must include a timezone')
    return result


def number(value, *, signed=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('Expected a finite number')
    if value < 0 and not signed:
        raise ValueError('Expected a nonnegative number')
    return value


def validate(record):
    for key in ('id', 'account', 'unit', 'period_label', 'source'):
        if not isinstance(record.get(key), str) or not record[key]:
            raise ValueError(f'Missing {key}')
    backends = record.get('backends')
    if not isinstance(backends, list) or not backends or any(not isinstance(x, str) or not x for x in backends):
        raise ValueError('Expected backend names')
    if record.get('allowance_type') not in ('budget', 'guideline', 'unknown'):
        raise ValueError('Unknown allowance type')
    instant(record['observed_at'])
    number(record['used'])
    for key in ('allowance', 'balance'):
        if record.get(key) is not None:
            number(record[key], signed=key == 'balance')
    for kind in ('running', 'queued'):
        forecast = record[kind]
        for key in ('jobs', 'unpriced_jobs'):
            if type(forecast.get(key)) is not int or forecast[key] < 0:
                raise ValueError('Invalid forecast counts')
        if forecast['unpriced_jobs'] > forecast['jobs']:
            raise ValueError('Unpriced jobs exceed jobs')
        if forecast.get('estimate') is not None:
            number(forecast['estimate'])
    return record


def view(record, now=None):
    record = dict(validate(record))
    now = now or datetime.now(timezone.utc)
    record['stale'] = bool(record.get('error')) or (now - instant(record['observed_at'])).total_seconds() > 1800
    allowance = record.get('allowance')
    balance = record.get('balance')
    if balance is None and allowance is not None:
        balance = allowance - record['used']
    record['remaining'] = balance
    estimate = sum(record[k]['estimate'] or 0 for k in ('running', 'queued'))
    record['partial'] = any(record[k]['unpriced_jobs'] or record[k]['estimate'] is None for k in ('running', 'queued'))
    record['headroom'] = balance - estimate if balance is not None else None
    record['excess'] = max(0, record['used'] + estimate - allowance) if allowance is not None else 0
    widths = []
    available = 100.0
    for value in (record['used'], record['running']['estimate'] or 0, record['queued']['estimate'] or 0):
        width = min(available, value / allowance * 100) if allowance else 0
        widths.append(width)
        available -= width
    record['widths'] = widths
    return record


class AllocationReader:
    """Keep valid prior records if an atomic snapshot is temporarily unreadable."""
    def __init__(self):
        self._path = None
        self._records = {}

    def read(self, path=None, now=None):
        path = path or os.environ.get('SCRIPTHUT_ALLOCATION_REPORT_FILE')
        if path != self._path:
            self._records = {}
            self._path = path
        if not path:
            return {}
        try:
            with Path(path).open('rb') as stream:
                data = stream.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise ValueError('Allocation snapshot exceeds 1 MiB')
            bundle = json.loads(data)
            if bundle.get('schema_version') != 1 or not isinstance(bundle.get('allocations'), list):
                raise ValueError('Unsupported allocation snapshot')
            records = {}
            for row in bundle['allocations']:
                try:
                    row = validate(row)
                    if row['id'] in records:
                        raise ValueError('Duplicate allocation identity')
                    records[row['id']] = row
                except (ValueError, KeyError, TypeError) as exc:
                    log.warning('Skipping invalid allocation: %s', exc)
                    key = row.get('id') if isinstance(row, dict) else None
                    if key in self._records:
                        records[key] = dict(self._records[key], error='Invalid latest allocation snapshot')
            self._records = records
        except (OSError, ValueError, KeyError, TypeError) as exc:
            log.warning('Allocation snapshot unavailable: %s', exc)
            self._records = {key: dict(row, error='Allocation snapshot unavailable') for key, row in self._records.items()}
        return {key: view(row, now) for key, row in self._records.items()}

    def by_backend(self):
        result = {}
        for row in self.read().values():
            for name in row['backends']:
                result.setdefault(name, []).append(row)
        return result
