# Resource efficiency

`/efficiency` reports tracked jobs overall, by project, by workflow/backend, and per
job. Dates select completion time in UTC (inclusive start and end dates); the
default is the last 30 days. Tables can sort by measured allocated CPU-hours,
lowest CPU efficiency, or lowest comparable memory/request ratio. Job detail
retains numeric data when a run has been removed, with the newest 200 matching
jobs shown. No requests are adjusted automatically.

CPU efficiency is consumed CPU-seconds divided by allocated CPU-seconds. Weighted
summaries sum numerator and denominator over the same measured jobs. Average
cores used divides consumed seconds by elapsed seconds. Failed measured jobs
contribute to CPU cost totals, but only successful comparable memory measurements
contribute to sizing distributions. Coverage, OOMs, and timeouts are explicit.

Slurm resource records prefer the parent CPU aggregate or a larger complete sum
of non-extern step observations, never parent plus its children. This preserves
sites whose parent accounting is incomplete while including non-batch compute.
Unknown CPU time stays unknown. `MaxRSS` is retained as a reported task peak;
comparison with allocation memory is enabled only for one single-task step on
one node. Multiple task or step peaks are never summed as simultaneous usage.
Other backends and historical records can retain displayed peaks without claiming
comparable memory scope or a known CPU denominator.

Mean/median/p95/max RAM values are distributions **of job peaks**, not time averages.
True time-averaged RAM requires a future sampling provider. Slurm AveRSS is not used
as time-averaged memory. OOM, workload variation, and unmeasured jobs must be reviewed
before reducing requests.

New optional task metadata `project_id` and `workflow_id` provides explicit
reporting attribution. It does not affect scheduler resource selection. The
launcher stamps these from the captured project's ID and experiment; bootstrap
preparation has a separate workflow suffix. Existing source attribution is the
fallback, with missing attribution labelled `Unattributed`. Existing request keys
continue to hash the exact submitted payload, including any metadata present.

`JobStats.resource_usage` and `RunItem.resource_usage` carry optional normalized
accounting records. Existing state, exit-code, queue observations and transitions
remain owned by RunManager. Its accounting handoff only copies the optional record.
The independent reports module does not own a scheduler connection or poll loop.

`resource-metrics.sqlite3` under the run-storage directory is a derived, local
history. Saving a run also records its terminal measurements; retries preserve
records before reset. Identities include backend, scheduler job and explicit
submission attempt (or legacy submission timestamp). Upserts allow later accounting
corrections without duplicating attempts. Startup and report reads reconcile retained
run records. Old runs cannot supply measurements that were never recorded.
The existing run JSON and API retain their old fields with one optional normalized
record added. Missing metrics are not displayed as zero.

Deploy through the canonical production release workflow. The schema addition is
backward compatible and requires no rewrite of old runs. Rolling back the UI or
producer leaves the derived SQLite history in place; it does not change submissions,
Slurm policy, source repositories, or artifacts. The metrics database may be rebuilt
from retained runs, but measurements for already deleted runs would then be lost.


Project attribution is shared with the existing activity charts and overview cards.
The priority is explicit task `project_id`, configured run source, then the recorded
Git repository basename. The launcher supplies `project_id` from project configuration;
scripthut has no site or repository name mapping. Tasks can additionally specify
`workflow_id`. Mixed-project runs display “Multiple projects” while accounting remains
per task. Missing metadata remains unattributed; job names and filesystem paths are
not treated as evidence. Retained runs reconcile improved attribution into both
reporting stores. Usage ledger corrections are appended and deduplicated by task
identity, so deleting the original run does not lose its corrected project label.
