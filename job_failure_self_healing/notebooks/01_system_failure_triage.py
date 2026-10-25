# Databricks notebook source
# MAGIC %md
# MAGIC # System-table job failure triage
# MAGIC
# MAGIC This scheduled notebook delegates the reusable discovery, Jobs API,
# MAGIC classification, and incident-ledger logic to the project package. It does
# MAGIC not read the legacy failed-jobs diagnosis table.

# COMMAND ----------

dbutils.widgets.text("workspace_id", "", "Workspace ID to monitor")
dbutils.widgets.text("system_run_table", "system.lakeflow.job_run_timeline", "Job run system table")
dbutils.widgets.text(
    "system_task_run_table",
    "system.lakeflow.job_task_run_timeline",
    "Task run system table",
)
dbutils.widgets.text(
    "incident_table",
    "molina-new.teamsint.job_healing_incidents",
    "New incident ledger table",
)
dbutils.widgets.text("lookback_days", "7", "Recent failed-run window")
dbutils.widgets.text("max_runs_per_run", "100", "Maximum failed runs per scheduled run")
dbutils.widgets.dropdown("llm_enabled", "true", ["true", "false"], "Use Databricks AI diagnosis")
dbutils.widgets.text(
    "llm_model",
    "databricks-meta-llama-3-3-70b-instruct",
    "Databricks model endpoint",
)

# COMMAND ----------

import logging
from databricks.sdk import WorkspaceClient
from job_failure_self_healing.runner import run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

workspace_id = dbutils.widgets.get("workspace_id").strip()
if not workspace_id:
    raise ValueError("Set the workspace_id widget to the workspace running this job.")

notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = notebook_context.apiToken().get()
workspace_host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
jobs_client = WorkspaceClient(host=workspace_host, token=api_token)

processed_count = run(
    workspace_id=workspace_id,
    system_run_table=dbutils.widgets.get("system_run_table").strip(),
    system_task_run_table=dbutils.widgets.get("system_task_run_table").strip(),
    incident_table=dbutils.widgets.get("incident_table").strip(),
    lookback_days=int(dbutils.widgets.get("lookback_days")),
    max_runs_per_run=int(dbutils.widgets.get("max_runs_per_run")),
    llm_enabled=dbutils.widgets.get("llm_enabled").lower() == "true",
    llm_model=dbutils.widgets.get("llm_model").strip(),
    jobs_client=jobs_client,
)

print(f"New failed-run incidents recorded: {processed_count}")
