"""
A lightweight, local-safe Databricks mock operator.

Why this exists:
- The assignment repo runs Airflow locally (Docker Compose) without real Databricks connectivity.
- But I still want to demonstrate the production shape: "trigger Databricks job / notebook".
- This operator behaves like a Databricks trigger, but only logs + (optionally) sleeps.

Usage (in DAGs):
    from databricks_mock import DatabricksMockOperator
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class DatabricksMockOperator(BaseOperator):
    """
    Mock Databricks operator used for local / interview demo pipelines.

    Parameters
    ----------
    job_name:
        Logical name of the Databricks job we would trigger in production.
    notebook_path:
        Notebook path we would run in production (optional, for realism).
    payload:
        Arbitrary dict that represents what we'd pass to Databricks (job params, cluster spec, etc.).
    simulate_seconds:
        If > 0, sleep to simulate remote execution time.
    succeed:
        If False, raise an exception to simulate a Databricks job failure.

    Notes
    -----
    - This is intentionally minimal and safe (no network calls).
    - In production, we'd replace this with DatabricksRunNowOperator / DatabricksSubmitRunOperator
      from airflow.providers.databricks.
    """

    template_fields = ("job_name", "notebook_path", "payload")

    def __init__(
        self,
        *,
        job_name: str,
        notebook_path: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        simulate_seconds: int = 0,
        succeed: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.notebook_path = notebook_path
        self.payload = payload or {}
        self.simulate_seconds = int(simulate_seconds)
        self.succeed = bool(succeed)

    def execute(self, context: Context) -> Dict[str, Any]:
        run_id = context.get("run_id")
        logical_date = context.get("logical_date")

        self.log.info("===========================================")
        self.log.info("[DATABRICKS MOCK] Triggering Databricks job")
        self.log.info("job_name=%s", self.job_name)
        self.log.info("notebook_path=%s", self.notebook_path)
        self.log.info("airflow_run_id=%s", run_id)
        self.log.info("logical_date=%s", logical_date)
        self.log.info("payload=%s", self.payload)
        self.log.info("===========================================")

        if self.simulate_seconds > 0:
            self.log.info(
                "[DATABRICKS MOCK] Simulating runtime: %ss", self.simulate_seconds
            )
            time.sleep(self.simulate_seconds)

        if not self.succeed:
            raise RuntimeError(
                f"[DATABRICKS MOCK] Simulated failure for job={self.job_name}"
            )

        # Return a deterministic “fake” response that downstream tasks could consume
        result = {
            "databricks_job_name": self.job_name,
            "databricks_notebook_path": self.notebook_path,
            "airflow_run_id": run_id,
            "status": "SUCCESS",
        }
        self.log.info("[DATABRICKS MOCK] Completed successfully: %s", result)
        return result
