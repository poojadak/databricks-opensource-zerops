# Databricks Job Failure Self-Healing (proposal-first)

This is a separate project from the repository's data-quality monitor. It does
**not** read `molina-new.teamsint.failed_jobs_diagnosis` or depend on the
existing daily-load notebook. It uses that notebook as a reference for Jobs API
authentication, failed task output retrieval, and the `ai_query` diagnosis
format.

The scheduled Databricks notebook discovers failed runs directly from
`system.lakeflow.job_run_timeline` and failed task runs from
`system.lakeflow.job_task_run_timeline`, filtered to one explicitly configured
workspace. It then calls the Databricks Jobs API to retrieve error output,
classifies the failure, creates a remediation proposal, and writes one
idempotent incident row per failed job run to a new table.

## Safety defaults

- The Asset Bundle defines a 15-minute schedule, but it is **paused by default**.
- It never retries, repairs, or changes production jobs.
- Candidate fixes are text proposals only: no generated code is executed.
- No GitHub or Azure Repos branch is pushed and no pull request is opened.
- Databricks `ai_query` is optional and advisory. A deterministic classifier
  chooses the category and policy-approved action. If the model call fails,
  deterministic diagnosis is used.
- The incident ledger is idempotent on workspace + job + run. A failed API
  lookup is not marked processed, so the next scheduled run can retry it.
- `validation_status=NOT_RUN_NO_SOURCE_MAPPING` is explicit: this project does
  not claim code-level sandbox validation when the job-to-source mapping and
  isolated test environment have not been configured.

## Workflow

1. Query recent job runs with `FAILED`, `ERROR`, or `TIMED_OUT` states from
   `system.lakeflow.job_run_timeline`.
2. Find task run IDs with those same result states from
   `system.lakeflow.job_task_run_timeline`.
3. Skip runs already recorded in the new incident ledger.
4. Call the Jobs API for each new parent run and retrieve error output for each
   failed task. If task details are not returned for the parent run, use
   system-table task run IDs to retrieve output.
5. Redact common credential patterns, classify the error, and optionally ask
   the configured Databricks model for a root cause and candidate fix.
6. Apply the deterministic recommendation policy and `MERGE` one review record
   per failed job run.

Multi-task failures are grouped under their parent run in `task_failures_json`;
failed tasks retain their task keys and task-run IDs.

## Requirements

- Deploy and run in the same Databricks workspace whose job runs are monitored.
- Unity Catalog access to both Lakeflow system tables and create/write access
  for the configured incident table.
- The job identity must be allowed to read failed run/task output through the
  Jobs API.
- `ai_query` must be available if `llm_enabled=true`. Confirm client-data/model
  handling policies before sending redacted error text to the model.
- System-table records are region-scoped and retained according to Databricks'
  system-table retention policy. The job additionally limits discovery to
  `lookback_days`.

System tables can contain activity for multiple workspaces in one region. This
job filters to the explicit `workspace_id`, and its API identity is scoped to
the workspace where the bundle is deployed. Deploy one instance per workspace.

## Validate and deploy

From this directory:

```powershell
python -m unittest discover -s tests -v
databricks bundle validate -t dev --profile molina-new --var="workspace_id=7405611780316702"
databricks bundle deploy -t dev --profile molina-new --var="workspace_id=7405611780316702"
```

The workspace ID matches the one used by the reference loader's workspace-name
mapping; confirm it with the workspace administrator before deployment. The
schedule remains paused after deployment. Review access, failure volumes, and
sample incident records before deliberately unpausing it.

The Azure Pipelines definition runs unit tests and builds/publishes the wheel on
pushes to `development`; it does not have Databricks credentials and cannot
deploy or access client job data.

## Low-cost failure test fixture

`notebooks/02_cost_guarded_failure_scenarios.py` is a synthetic fixture for
testing the failure discovery workflow. Its manual-only companion job uses five
independent tasks to raise immediate `ValueError`, `ZeroDivisionError`,
`PermissionError`, `TimeoutError`, and `MemoryError` exceptions. No task reads
data, touches a protected resource, sleeps, allocates memory, retries, or triggers
an external service.

The workflow uses serverless job compute in **standard (non-performance-
optimized) mode**, has no schedule, allows one concurrent job run, and sets
short task timeouts with retries disabled. Job creation itself does not start
compute. A manual run can incur serverless DBU charges; run it only when ready,
and check the workspace's usage/budget policy for exact rates. The synthetic
timeout and OOM cases fail immediately rather than consuming time or memory.

## Current boundary and next integration

The workflow retrieves task output and proposes a fix, but it does not know
which source repository, source file, owner, branch, or test command belongs
to each job. Add an approved job/task-to-repository mapping and isolated
validation environment before enabling code generation, sandbox execution, or
PR creation. Automatic retry/repair remains disabled until a reviewed,
allowlisted policy is defined.
