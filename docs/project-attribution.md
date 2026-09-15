# Historical project attribution

New submissions follow the [project attribution contract](cli.md#project-attribution).
Existing records remain readable without `project_id`; this change does not rewrite
history or relabel scheduler-discovered jobs.

Inventory historical records without changing them:

```sh
python -m scripthut.project_audit /path/to/workflows > project-candidates.json
```

Use the controller's persisted workflows directory. Each result lists a run ID,
missing task IDs, a proposed project, and the evidence category. A recorded Git URL
provides a repository-name candidate. Existing task labels are excluded. Runs with
no recorded Git identity require review: neither command paths nor workflow names
are reliable ownership evidence. The audit does not read the current Git checkout
or container environment to guess a historical name.

For those unknowns, establish an explicit run-ID-to-project mapping from submission
records or confirmed ownership. This PR only produces the inventory, not a mutation
command. A reviewed migration must preserve existing labels, back up controller
state, update the resource-metrics attribution as well as `run.json`, and coordinate
with the controller so a live save cannot overwrite changes. Never rewrite durable
request payloads or their digests; those preserve retry identity. Metrics for runs
whose JSON has expired need separate evidence from the retained metrics or launcher
records and will not appear in this inventory.
