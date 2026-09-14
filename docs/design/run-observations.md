# Managed run observations

RunManager owns managed task transitions, scheduler evidence, completion effects,
dependency consequences, persistence and watcher notifications. Polling owns the
backend calls and connection/display state. External-job history remains a separate
storage concern; it does not control managed RunItems.

## Poll boundary

`plan_backend_observation` captures a backend's immutable queue snapshot, current
run/task/job/attempt identities and accounting query IDs. `apply_backend_observations`
applies completed evidence under a per-backend observation lock. Accounting can be
successful, failed or unrequested; only a successful requested query supplies
absence evidence. A stale or duplicate observation is ignored. Query timestamps
refer to the start of queue collection, preventing newly submitted jobs from being
marked missing by an older snapshot.

Accounting corrections and existing completion effects precede queue transitions.
Only captured identities still current after awaits receive evidence. Submission
reconciliation retains its own locks and unknown-outcome semantics. Fair-share
scheduling and its greedy follow-up pass remain manager-owned. Queue-only callers
can use the compatibility adapters, which delegate to the same transition logic.

Rerun and cancellation invalidate captured identities. Completion I/O rechecks
identity before attaching results, so an old output listing or generated workflow
cannot modify a replacement attempt. No observation fields are persisted; restart
uses the existing run and submission journal formats.

## Intentional differences and limits

Filter-triggered refresh now applies the same full lifecycle cycle as background
polling. Overlapping/duplicate observations and observations collected before a
submission or cancellation can no longer overwrite newer state. These race guards
do not change scheduler timeout constants, error markers or accounting precedence.

This refactor does not make completion effects transactional across crashes.
The existing state-before-effect crash window remains. The legacy false-failure
correction still uncascades dependencies without rerunning completion hooks.
Explicit queue COMPLETED still enters SETTLING without supplying a missing finish
timestamp; unlike queue disappearance, that existing path may not reach the long
fallback until accounting supplies timing. Those behavioral issues are separate
from consolidating ownership and require their own tests and review.
