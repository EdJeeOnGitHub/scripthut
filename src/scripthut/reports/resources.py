"""Normalized accounting measurements; unknown is distinct from measured zero."""
from __future__ import annotations
from dataclasses import asdict, dataclass
import math
import re



def bytes_value(value):
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r'([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)(?:i?B)?', value.strip(), re.I)
    if not match:
        return None
    amount, suffix = match.groups()
    return int(float(amount) * 1024**('KMGTPE'.find(suffix.upper()) + 1 if suffix else 0))


def time_value(value):
    if not isinstance(value, str) or not re.fullmatch(r'(?:\d+-)?\d+:\d+(?::\d+(?:\.\d+)?)?(?:\.\d+)?', value):
        return None
    days, sep, rest = value.partition("-")
    days, rest = (int(days), rest) if sep else (0, days)
    seconds = 0.0
    for component in rest.split(":"):
        seconds = seconds * 60 + float(component)
    return seconds + days * 86400


@dataclass
class ResourceUsage:
    elapsed_seconds: float | None = None
    allocated_cpu_seconds: float | None = None
    consumed_cpu_seconds: float | None = None
    requested_memory_bytes: int | None = None
    allocated_memory_bytes: int | None = None
    reported_peak_bytes: int | None = None
    memory_scope: str = 'unknown'  # single_task_job, task, unknown
    source: str = 'scheduler accounting'

    @property
    def cpu_efficiency(self):
        if not self.allocated_cpu_seconds or self.consumed_cpu_seconds is None:
            return None
        return 100 * self.consumed_cpu_seconds / self.allocated_cpu_seconds

    @property
    def average_cores(self):
        if not self.elapsed_seconds or self.consumed_cpu_seconds is None:
            return None
        return self.consumed_cpu_seconds / self.elapsed_seconds

    @property
    def memory_percent(self):
        request = self.allocated_memory_bytes or self.requested_memory_bytes
        if self.memory_scope != 'single_task_job' or not request or self.reported_peak_bytes is None:
            return None
        return self.reported_peak_bytes / request * 100

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict):
            return None
        fields = {key: value for key, value in data.items() if key in cls.__dataclass_fields__}
        for key, value in fields.items():
            if key.endswith(('_seconds', '_bytes')) and value is not None:
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                    return None
        return cls(**fields)


def slurm_resources(output):
    """Consume the extended sacct query; never add a parent and its steps."""
    groups = {}
    for line in output.splitlines():
        fields = line.strip().split('|')
        if len(fields) < 9:
            continue
        groups.setdefault(fields[0].split('.')[0], []).append(fields)
    result = {}
    for job, rows in groups.items():
        parent = next((r for r in rows if r[0] == job), None)
        if parent is None:
            continue
        elapsed = time_value(parent[2])
        try:
            cpus = int(parent[3])
        except ValueError:
            cpus = 0
        consumed = time_value(parent[1])
        steps = [r for r in rows if '.' in r[0] and not r[0].endswith('.extern')]
        step_cpu = [time_value(r[1]) for r in steps]
        # Parent is Slurm's aggregate. Some sites only fill step accounting.
        if step_cpu and all(v is not None for v in step_cpu):
            # Some sites report an incomplete parent during final accounting.
            # Use the larger complete observation, never parent + children.
            consumed = max(consumed or 0, sum(step_cpu))
        peaks = [bytes_value(r[4]) for r in (steps or [parent])]
        peaks = [p for p in peaks if p is not None]
        requested = allocated = None
        scope = 'task' if peaks else 'unknown'
        if len(parent) >= 13:
            tres, reqmem, nodes, tasks = parent[9:13]
            memory = next((v[4:] for v in tres.split(',') if v.startswith('mem=')), '')
            allocated = bytes_value(memory + "M" if memory.isdigit() else memory)
            match = re.fullmatch(r'(.+)([cn])', reqmem)
            if match:
                amount = bytes_value(match[1] + "M" if match[1].isdigit() else match[1])
                try:
                    requested = amount * (cpus if match[2] == 'c' else int(nodes)) if amount is not None else None
                except ValueError:
                    pass
            elif reqmem:
                requested = bytes_value(reqmem + "M" if reqmem.isdigit() else reqmem)
            # A single single-task step on one node is comparable with the
            # whole allocation. Multiple task/step maxima are not summed.
            measured = steps or [parent]
            if nodes == '1' and len(measured) == 1 and len(measured[0]) >= 13 and measured[0][12] == '1':
                scope = 'single_task_job'
        result[job] = ResourceUsage(elapsed, elapsed*cpus if elapsed is not None and cpus > 0 else None,
            consumed, requested, allocated, max(peaks) if peaks else None, scope, 'Slurm sacct')
    return result
