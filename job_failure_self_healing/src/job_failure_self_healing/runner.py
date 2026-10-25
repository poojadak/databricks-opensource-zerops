import argparse
import datetime as dt
import json
import logging
from typing import Any

from databricks.sdk import WorkspaceClient

from job_failure_self_healing.core import (
    classify_failure,
    make_incident_key,
    make_recommendation,
    parse_diagnosis,
    redact_failure_text,
    render_review_packet,
    validate_proposal,
)

LOGGER = logging.getLogger("job_failure_self_healing")


def _identifier(name: str) -> str:
    parts = name.split(".")
    if len(parts) != 3 or any(not part.strip("`") for part in parts):
        raise ValueError(f"Expected a catalog.schema.table name, got {name!r}")
    return ".".join(f"`{part.strip('`').replace('`', '``')}`" for part in parts)


def _enum_text(value: Any) -> str:
    return str(getattr(value, "value", value) or "").upper()


def _get_task_failures(
    client: WorkspaceClient,
    run_id: int,
    system_tasks: list[dict[str, Any]],
) -> tuple[Any, list[dict[str, Any]]]:
    details = client.jobs.get_run(run_id=run_id)
    failed = []
    for task in getattr(details, "tasks", None) or []:
        state = getattr(task, "state", None)
        if _enum_text(getattr(state, "result_state", None)) != "FAILED":
            continue

        task_run_id = getattr(task, "run_id", None)
        task_key = getattr(task, "task_key", None) or "unknown_task"
        message = getattr(state, "state_message", None) or ""
        output_error = ""
        if task_run_id:
            try:
                output = client.jobs.get_run_output(run_id=int(task_run_id))
                output_error = (
                    getattr(output, "error", None)
                    or getattr(output, "error_trace", None)
                    or ""
                )
            except Exception as exc:
                LOGGER.warning(
                    "Could not read output for task run %s (%s); using task state message.",
                    task_run_id,
                    exc,
                )

        failed.append(
            {
                "task_key": task_key,
                "task_run_id": int(task_run_id) if task_run_id else None,
                "task_name": getattr(task, "description", None) or task_key,
                "failure_reason": redact_failure_text(output_error or message),
            }
        )
    known_task_run_ids = {task["task_run_id"] for task in failed if task["task_run_id"]}
    for task in system_tasks:
        task_run_id = task["task_run_id"]
        if task_run_id in known_task_run_ids:
            continue
        output_error = ""
        try:
            output = client.jobs.get_run_output(run_id=task_run_id)
            output_error = (
                getattr(output, "error", None)
                or getattr(output, "error_trace", None)
                or ""
            )
        except Exception as exc:
            LOGGER.warning(
                "Could not read output for system-table task run %s (%s).",
                task_run_id,
                exc,
            )
        failed.append(
            {
                "task_key": task["task_key"],
                "task_run_id": task_run_id,
                "task_name": task["task_key"],
                "failure_reason": redact_failure_text(output_error),
            }
        )
    return details, failed


def _try_llm_diagnosis(spark: Any, model: str, failure_text: str) -> dict[str, str] | None:
    prompt = (
        "Analyze this redacted Databricks task failure. Return JSON only with keys "
        '"category", "root_cause", and "candidate_fix". category must be one of '
        "OOM, TIMEOUT, PERMISSION, DATA_ISSUE, CODE_ERROR, CLUSTER, NETWORK, "
        "CONFIGURATION, UNKNOWN. Suggest a reviewable change only; do not suggest "
        "granting permissions, disabling controls, or blindly retrying production.\n\n"
        f"Failure evidence:\n{failure_text}"
    )
    quote = lambda text: "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"
    try:
        result = spark.sql(
            f"SELECT ai_query({quote(model)}, {quote(prompt)}) AS diagnosis"
        ).first()["diagnosis"]
        return parse_diagnosis(result)
    except Exception as exc:
        LOGGER.warning("Databricks LLM diagnosis unavailable; deterministic rules will be used (%s).", exc)
        return None


def _system_failed_runs(
    spark: Any,
    run_table: str,
    workspace_id: str,
    lookback_days: int,
    max_runs: int,
) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    runs = spark.table(_identifier(run_table))
    required = {
        "workspace_id",
        "job_id",
        "run_id",
        "period_end_time",
        "result_state",
    }
    missing = required - set(runs.columns)
    if missing:
        raise ValueError(
            f"System run table is missing required columns: {sorted(missing)}"
        )

    cutoff = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - dt.timedelta(
        days=lookback_days
    )
    result = (
        runs.filter(
            (F.col("workspace_id") == workspace_id)
            & F.upper(F.col("result_state")).isin("FAILED", "ERROR", "TIMED_OUT")
            & (F.col("period_end_time") >= F.lit(cutoff))
        )
        .groupBy("workspace_id", "job_id", "run_id")
        .agg(F.max("period_end_time").alias("failure_time_utc"))
        .orderBy(F.col("failure_time_utc").desc())
        .limit(max_runs + 1)
        .collect()
    )
    if len(result) > max_runs:
        raise ValueError(
            f"System tables contain more than max_runs_per_run={max_runs} "
            "failed, errored, or timed-out runs in the lookback window; "
            "increase the cap after reviewing volume."
        )
    return [
        {
            "workspace_id": row["workspace_id"],
            "job_id": int(row["job_id"]),
            "run_id": int(row["run_id"]),
            "failure_time_utc": row["failure_time_utc"],
        }
        for row in result
    ]


def _system_failed_tasks(
    spark: Any,
    task_run_table: str,
    workspace_id: str,
    candidate_runs: list[dict[str, Any]],
) -> dict[tuple[int, int], list[dict[str, Any]]]:
    from pyspark.sql import functions as F

    if not candidate_runs:
        return {}
    tasks = spark.table(_identifier(task_run_table))
    needed = {
        "workspace_id",
        "job_id",
        "job_run_id",
        "run_id",
        "task_key",
        "result_state",
    }
    missing = needed - set(tasks.columns)
    if missing:
        raise ValueError(
            f"System task-run table is missing required columns: {sorted(missing)}"
        )
    run_ids = [str(run["run_id"]) for run in candidate_runs]
    selected = (
        tasks.filter(
            (F.col("workspace_id") == workspace_id)
            & F.upper(F.col("result_state")).isin("FAILED", "ERROR", "TIMED_OUT")
            & F.col("job_run_id").isin(run_ids)
        )
        .select(
            F.col("job_id").cast("long").alias("job_id"),
            F.col("job_run_id").cast("long").alias("job_run_id"),
            F.col("run_id").cast("long").alias("task_run_id"),
            "task_key",
        )
        .dropDuplicates(["job_id", "job_run_id", "task_run_id", "task_key"])
        .collect()
    )
    failures: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in selected:
        failures.setdefault(
            (int(row["job_id"]), int(row["job_run_id"])), []
        ).append(
            {
                "task_key": row["task_key"] or "unknown_task",
                "task_run_id": int(row["task_run_id"]),
            }
        )
    return failures


def _ensure_incident_table(spark: Any, table_name: str) -> None:
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema_name = ".".join(_identifier(table_name).split(".")[:2])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    schema = StructType(
        [
            StructField("incident_key", StringType(), False),
            StructField("workspace_id", StringType(), False),
            StructField("job_id", LongType(), False),
            StructField("run_id", LongType(), False),
            StructField("job_name", StringType(), True),
            StructField("run_page_url", StringType(), True),
            StructField("failure_time_utc", TimestampType(), True),
            StructField("category", StringType(), False),
            StructField("root_cause", StringType(), True),
            StructField("task_failures_json", StringType(), False),
            StructField("recommendation_json", StringType(), False),
            StructField("candidate_fix", StringType(), True),
            StructField("validation_status", StringType(), False),
            StructField("proposal_policy_status", StringType(), False),
            StructField("incident_status", StringType(), False),
            StructField("review_packet", StringType(), False),
            StructField("created_at_utc", TimestampType(), False),
        ]
    )
    if not spark.catalog.tableExists(_identifier(table_name)):
        spark.createDataFrame([], schema).write.format("delta").saveAsTable(_identifier(table_name))


def _diagnose(
    spark: Any, failure_text: str, llm_enabled: bool, llm_model: str
) -> dict[str, str]:
    diagnosis = _try_llm_diagnosis(spark, llm_model, failure_text) if llm_enabled else None
    rule_category = classify_failure(failure_text)
    if diagnosis:
        return {
            "category": (
                rule_category if rule_category != "UNKNOWN" else diagnosis["category"]
            ),
            "root_cause": redact_failure_text(diagnosis["root_cause"]),
            "candidate_fix": redact_failure_text(diagnosis["candidate_fix"]),
        }
    return {
        "category": rule_category,
        "root_cause": "Rule-based classification from the available task failure evidence.",
        "candidate_fix": "",
    }


def run(
    *,
    workspace_id: str,
    system_run_table: str,
    system_task_run_table: str,
    incident_table: str,
    jobs_client: WorkspaceClient | None = None,
    lookback_days: int = 7,
    max_runs_per_run: int = 100,
    llm_enabled: bool = True,
    llm_model: str = "databricks-meta-llama-3-3-70b-instruct",
) -> int:
    from pyspark.sql import SparkSession

    if not workspace_id.strip():
        raise ValueError("workspace_id must be explicitly configured.")
    if lookback_days < 1 or max_runs_per_run < 1:
        raise ValueError("lookback_days and max_runs_per_run must be positive.")
    if not system_run_table.startswith("system.lakeflow.") or not system_task_run_table.startswith(
        "system.lakeflow."
    ):
        raise ValueError("Run and task discovery must use system.lakeflow system tables.")

    spark = SparkSession.builder.getOrCreate()
    client = jobs_client or WorkspaceClient()
    _ensure_incident_table(spark, incident_table)
    failed_runs = _system_failed_runs(
        spark, system_run_table, workspace_id, lookback_days, max_runs_per_run
    )
    if not failed_runs:
        LOGGER.info("No failed job runs found for workspace %s.", workspace_id)
        return 0

    system_tasks = _system_failed_tasks(
        spark, system_task_run_table, workspace_id, failed_runs
    )

    keys = [
        make_incident_key(workspace_id, run["job_id"], run["run_id"])
        for run in failed_runs
    ]
    from pyspark.sql import functions as F

    existing = {
        row["incident_key"]
        for row in spark.table(_identifier(incident_table))
        .filter(F.col("incident_key").isin(keys))
        .select("incident_key")
        .collect()
    }
    pending = [
        run
        for run in failed_runs
        if make_incident_key(workspace_id, run["job_id"], run["run_id"]) not in existing
    ]
    if not pending:
        LOGGER.info("All %s recent failed runs are already recorded.", len(failed_runs))
        return 0

    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    incident_rows = []
    for run in pending:
        job_id = run["job_id"]
        run_id = run["run_id"]
        try:
            run_details, tasks = _get_task_failures(
                client, run_id, system_tasks.get((job_id, run_id), [])
            )
        except Exception as exc:
            LOGGER.exception(
                "Failed to fetch task details for job_id=%s run_id=%s; will retry next schedule: %s",
                job_id,
                run_id,
                exc,
            )
            continue

        if not tasks:
            tasks = [
                {
                    "task_key": "job_run_failure",
                    "task_run_id": None,
                    "task_name": "Job run failure",
                    "failure_reason": redact_failure_text(
                        getattr(getattr(run_details, "state", None), "state_message", "")
                        or "No failed task details were returned by the Jobs API."
                    ),
                }
            ]

        task_text = "\n".join(
            f"[{task['task_key']}] {task['failure_reason']}" for task in tasks
        )
        task_text = redact_failure_text(task_text, max_chars=16000)
        diagnosis = _diagnose(spark, task_text, llm_enabled, llm_model)
        recommendation = make_recommendation(diagnosis["category"])
        policy_status = validate_proposal(diagnosis["category"], recommendation)
        run_url = getattr(run_details, "run_page_url", None) or ""
        job_name = getattr(run_details, "run_name", None) or str(job_id)
        packet = render_review_packet(
            job_name=job_name,
            job_id=job_id,
            run_id=run_id,
            run_page_url=run_url,
            category=diagnosis["category"],
            root_cause=diagnosis["root_cause"],
            recommendation=recommendation,
            candidate_fix=diagnosis["candidate_fix"],
        )
        incident_rows.append(
            {
                "incident_key": make_incident_key(workspace_id, job_id, run_id),
                "workspace_id": workspace_id,
                "job_id": job_id,
                "run_id": run_id,
                "job_name": job_name,
                "run_page_url": run_url,
                "failure_time_utc": run["failure_time_utc"],
                "category": diagnosis["category"],
                "root_cause": diagnosis["root_cause"],
                "task_failures_json": json.dumps(tasks, ensure_ascii=True),
                "recommendation_json": json.dumps(recommendation, ensure_ascii=True),
                "candidate_fix": diagnosis["candidate_fix"],
                "validation_status": "NOT_RUN_NO_SOURCE_MAPPING",
                "proposal_policy_status": policy_status,
                "incident_status": "AWAITING_HUMAN_REVIEW",
                "review_packet": packet,
                "created_at_utc": now,
            }
        )

    if not incident_rows:
        LOGGER.warning("No incidents were written; API failures will be retried next schedule.")
        return 0

    updates = spark.createDataFrame(incident_rows)
    updates.createOrReplaceTempView("job_healing_incident_updates")
    spark.sql(
        f"""
        MERGE INTO {_identifier(incident_table)} AS target
        USING job_healing_incident_updates AS source
        ON target.incident_key = source.incident_key
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    LOGGER.info("Recorded %s new failed run review packet(s).", len(incident_rows))
    return len(incident_rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace_id", required=True)
    parser.add_argument("--system_run_table", required=True)
    parser.add_argument("--system_task_run_table", required=True)
    parser.add_argument("--incident_table", required=True)
    parser.add_argument("--lookback_days", type=int, default=7)
    parser.add_argument("--max_runs_per_run", type=int, default=100)
    parser.add_argument("--llm_enabled", type=lambda value: value.lower() == "true", default=True)
    parser.add_argument("--llm_model", default="databricks-meta-llama-3-3-70b-instruct")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(
        workspace_id=args.workspace_id,
        system_run_table=args.system_run_table,
        system_task_run_table=args.system_task_run_table,
        incident_table=args.incident_table,
        lookback_days=args.lookback_days,
        max_runs_per_run=args.max_runs_per_run,
        llm_enabled=args.llm_enabled,
        llm_model=args.llm_model,
    )


if __name__ == "__main__":
    main()
