# Submission recovery

ScriptHut sends Slurm scripts through stdin to a short `sbatch` command, supporting
large scripts without placing their contents in SSH arguments or command logs.
An execution failure before the command starts is a definite submission failure.
A lost response after execution starts remains unknown to avoid duplicate jobs.
Submission history retains the original error separately from later scheduler checks.

Resolve unknown attempts through `POST /api/v1/runs/{run_id}/tasks/{task_id}/submission`
or `scripthut run resolve`. Available actions are `check`, `bind`, `retry`, and `abandon`.

To close an attempt without retrying it, first independently verify that no job was
submitted to the original scheduler. Then supply its current attempt ID:

```sh
scripthut run resolve RUN TASK --action abandon --attempt ATTEMPT --confirm-not-submitted
```

The run detail page provides the same action. Abandon requires operator confirmation,
unchanged scheduler destination and user, successful queue and accounting checks with
no matching jobs, and no previously recorded job ID. Query failures or scheduler
matches prevent abandonment. It marks the task failed, keeps its history, and allows
normal dependency failure processing; it does not resubmit or cancel scheduler jobs.
Genuinely ambiguous submissions never expire automatically.
