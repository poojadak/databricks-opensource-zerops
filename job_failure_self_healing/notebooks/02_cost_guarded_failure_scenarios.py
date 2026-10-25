# Databricks notebook source
# MAGIC %md
# MAGIC # Cost-guarded job failure scenarios
# MAGIC
# MAGIC Each task in the companion manual-only workflow invokes this notebook with
# MAGIC a different `failure_type`. Every case raises a small, immediate Python
# MAGIC exception; no external resource is accessed and no large allocation or
# MAGIC deliberate wait is performed.

# COMMAND ----------

dbutils.widgets.dropdown(
    "failure_type",
    "DATA_ISSUE",
    ["DATA_ISSUE", "CODE_ERROR", "PERMISSION", "TIMEOUT", "OOM"],
    "Simulated failure category",
)

failure_type = dbutils.widgets.get("failure_type")
print(f"Intentionally raising a simulated {failure_type} failure for monitoring tests.")

# COMMAND ----------

if failure_type == "DATA_ISSUE":
    raise ValueError("SIMULATED_DATA_ISSUE: malformed test input; required field is missing.")
elif failure_type == "CODE_ERROR":
    raise ZeroDivisionError("SIMULATED_CODE_ERROR: deliberate divide-by-zero in test code.")
elif failure_type == "PERMISSION":
    raise PermissionError("SIMULATED_PERMISSION: permission denied by synthetic test; no resource was accessed.")
elif failure_type == "TIMEOUT":
    raise TimeoutError("SIMULATED_TIMEOUT: synthetic timeout; no sleep or long operation was performed.")
elif failure_type == "OOM":
    raise MemoryError("SIMULATED_OOM: synthetic out of memory condition; no memory was allocated.")
else:
    raise ValueError(f"Unsupported failure_type: {failure_type}")
