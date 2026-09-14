# Allocation reporting

Set `SCRIPTHUT_ALLOCATION_REPORT_FILE` to a version-1 JSON snapshot produced by a
trusted host collector. Mount the containing directory read-only in the controller;
mounting the file itself prevents atomic replacements becoming visible. Without
this setting the feature is disabled. No scheduler calls occur while rendering.

The bundle contains `schema_version: 1` and an `allocations` array. Each record has
`id`, `account`, `backends` (linked names), `unit`, `allowance_type` (`budget`,
`guideline`, `unknown`), `period_label`, timezone-aware `observed_at`, `source`,
`used`, nullable `allowance`/`balance`, and `running`/`queued` objects containing
`jobs`, `unpriced_jobs`, and nullable `estimate`. Optional fields are `notes`,
`source_as_of`, `period_start`, `period_end`, and `error`.

Used is reported consumption; running is additional estimated cost from now;
queued assumes immediate start. Remaining is the balance before forecasts. Gray
headroom subtracts priced estimates, and unknown costs are explicitly incomplete.
Allowance zero and missing allowance are distinct. Overages retain negative
balances while meter widths stay within 100%. Estimates are not reservations.
Reports older than 30 minutes or with errors are marked stale. Read failures keep
the last valid snapshot in memory; collector persistence survives restarts.

An allocation shared by multiple backends is one record with multiple links, not
multiple balances. Card labels make shared identity explicit. Site tools, billing
rules, reporting windows and deployment are owned by agent-infra's
`allocation-reports` package; there are no hostnames or site commands in this UI.
