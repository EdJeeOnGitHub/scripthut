# Conserving scheduler queries

The controller normally polls all backends using `settings.poll_interval`. For
clusters with scheduler RPC limits, configure an endpoint budget as well:

```yaml
settings:
  poll_interval: 120
backends:
  - name: cluster
    type: slurm
    ssh:
      host: login.cluster.example
      user: researcher
    scheduler_min_interval: 60
    scheduler_query_cache_seconds: 120
    poll_cluster_info: false
```

Backend aliases with the same SSH host (case insensitive), port, and user share
one budget within the controller runtime. The largest configured interval and
cache lifetime apply to all those aliases. Identical successful job-list queries
reuse a snapshot; account-specific queries retain their own results. Every Slurm
backend command, including accounting, submission and cancellation, waits its
turn. Failures consume a slot too. Submissions and cancellations are never cached
or automatically retried, and invalidate cached queue results before execution.
Remote command timeouts start after admission, rather than expiring while waiting
for the budget. A force refresh cannot bypass the budget or snapshot lifetime.

Disabling `poll_cluster_info` removes the automatic partition/node availability,
pending-reason and fair-share/quota sweep. Those optional details are unavailable
in the dashboard; job monitoring and accounting continue. This avoids spending
most of a restricted command budget on dashboard metadata. A two-minute poll is
a nominal cadence, not a latency guarantee: accounting or queued mutations can
extend it. Polling all backends shares one loop, so a slow cycle also delays the
next cycle for other backends.

These settings require a controller restart. The default budget/cache are zero
and capacity polling remains enabled, preserving existing installations. The
budget is process-local; run one controller for a login identity. Independent
controllers, direct SSH commands, and external agent scripts must coordinate
their own usage. DNS aliases for one cluster must use a consistent SSH hostname
to share the budget.
