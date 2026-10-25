import unittest
from types import SimpleNamespace

from job_failure_self_healing.runner import _get_task_failures


class FakeJobsClient:
    def __init__(self):
        self.jobs = self
        self.requested_outputs = []

    def get_run(self, run_id):
        self.parent_run_id = run_id
        return SimpleNamespace(
            run_name="daily-multi-task",
            run_page_url="https://workspace/jobs/1/runs/2",
            tasks=[
                SimpleNamespace(
                    state=SimpleNamespace(
                        result_state="FAILED",
                        state_message="task failed",
                    ),
                    run_id=201,
                    task_key="extract",
                    description="Extract data",
                )
            ],
        )

    def get_run_output(self, run_id):
        self.requested_outputs.append(run_id)
        return SimpleNamespace(error=f"failure output for {run_id}", error_trace=None)


class JobsApiEnrichmentTests(unittest.TestCase):
    def test_uses_parent_task_details_and_system_table_fallback(self):
        client = FakeJobsClient()
        details, failures = _get_task_failures(
            client,
            100,
            [
                {"task_key": "extract", "task_run_id": 201},
                {"task_key": "transform", "task_run_id": 202},
            ],
        )

        self.assertEqual(client.parent_run_id, 100)
        self.assertEqual(client.requested_outputs, [201, 202])
        self.assertEqual(details.run_name, "daily-multi-task")
        self.assertEqual(len(failures), 2)
        self.assertEqual(failures[0]["task_key"], "extract")
        self.assertEqual(failures[0]["failure_reason"], "failure output for 201")
        self.assertEqual(failures[1]["task_key"], "transform")
        self.assertEqual(failures[1]["failure_reason"], "failure output for 202")


if __name__ == "__main__":
    unittest.main()
