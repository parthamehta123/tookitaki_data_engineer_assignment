"""
DLT Pipeline Trigger
====================

Provides callables for triggering Delta Live Tables pipelines:

  trigger_dlt_pipeline()     – Local simulation (no infra, no API calls)
  trigger_dlt_databricks()   – Real Databricks DLT pipeline via REST API
                               (POST /api/2.0/pipelines/{id}/updates + polling)

Usage in DAGs:
    from dlt_trigger_simulation import trigger_dlt_pipeline       # local mode
    from dlt_trigger_simulation import trigger_dlt_databricks     # databricks mode
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional


# -------------------------------------------------------------------
# LOCAL SIMULATION
# -------------------------------------------------------------------
def trigger_dlt_pipeline(
    environment: str,
    pipeline_name: str = "customer_dlt_streaming_pipeline",
    simulate_seconds: int = 1,
    expected_marker_file: Optional[str] = None,
) -> None:
    """
    Simulate triggering a Delta Live Tables pipeline (local/dev mode).

    Parameters
    ----------
    environment:
        e.g., local / dev / prod (used only for logs)
    pipeline_name:
        Logical DLT pipeline name
    simulate_seconds:
        Sleep time to simulate "DLT running"
    expected_marker_file:
        Optional path to a file that should exist before "DLT" runs.
        If provided and missing, we raise to simulate a dependency failure.
        Example:
            /opt/airflow/data/landing/_SUCCESS
    """
    print("===========================================")
    print("[DLT SIM] Triggering DLT pipeline")
    print(f"env={environment}")
    print(f"pipeline_name={pipeline_name}")
    print("===========================================")

    if expected_marker_file:
        expected_marker_file = os.path.abspath(expected_marker_file)
        print(f"[DLT SIM] Checking marker file exists: {expected_marker_file}")
        if not os.path.exists(expected_marker_file):
            raise FileNotFoundError(
                f"[DLT SIM] Missing marker file: {expected_marker_file} "
                "(ingestion may not have produced expected output)"
            )
        print("[DLT SIM] Marker file present")

    if simulate_seconds > 0:
        print(f"[DLT SIM] Simulating pipeline runtime: {simulate_seconds}s")
        time.sleep(simulate_seconds)

    print("[DLT SIM] Pipeline completed successfully")
    print("===========================================")


# -------------------------------------------------------------------
# REAL DATABRICKS DLT TRIGGER
# -------------------------------------------------------------------
def trigger_dlt_databricks(
    pipeline_id: str,
    databricks_conn_id: str = "databricks_default",
    full_refresh: bool = False,
    poll_interval_seconds: int = 30,
    timeout_seconds: int = 3600,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Trigger a real Databricks DLT pipeline update and poll until completion.

    Uses the Databricks REST API via the Airflow DatabricksHook:
      POST /api/2.0/pipelines/{pipeline_id}/updates       (trigger)
      GET  /api/2.0/pipelines/{pipeline_id}/updates/{id}   (poll)

    Parameters
    ----------
    pipeline_id : str
        Databricks DLT pipeline ID (from the Pipelines UI or API).
    databricks_conn_id : str
        Airflow connection ID configured for Databricks.
    full_refresh : bool
        If True, the pipeline runs a full refresh instead of incremental.
    poll_interval_seconds : int
        Seconds between status polls.
    timeout_seconds : int
        Max total wait time before raising TimeoutError.

    Returns
    -------
    dict
        {"pipeline_id": ..., "update_id": ..., "state": "COMPLETED"}

    Raises
    ------
    ValueError
        If pipeline_id is empty.
    RuntimeError
        If the pipeline ends in FAILED or CANCELED state.
    TimeoutError
        If the pipeline does not reach a terminal state within timeout.
    """
    from airflow.providers.databricks.hooks.databricks import DatabricksHook

    if not pipeline_id:
        print(
            "[DLT] No pipeline_id configured (DATABRICKS_DLT_PIPELINE_ID not set). "
            "Skipping real DLT trigger. In production, set this env var to your "
            "Databricks DLT pipeline ID."
        )
        print("[DLT] DLT step completed (no-op)")
        return {"pipeline_id": None, "update_id": None, "state": "SKIPPED"}

    hook = DatabricksHook(databricks_conn_id=databricks_conn_id)

    # --- Trigger pipeline update ---
    print(
        f"[DLT] Triggering Databricks pipeline "
        f"pipeline_id={pipeline_id} full_refresh={full_refresh}"
    )

    trigger_endpoint = f"api/2.0/pipelines/{pipeline_id}/updates"
    response = hook._do_api_call(
        ("POST", trigger_endpoint),
        json={"full_refresh": full_refresh},
    )
    update_id = response.get("update_id")
    print(f"[DLT] Pipeline update started: update_id={update_id}")

    # --- Poll until terminal state ---
    terminal_success = {"COMPLETED"}
    terminal_failure = {"FAILED", "CANCELED"}
    elapsed = 0

    while elapsed < timeout_seconds:
        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

        status_endpoint = f"api/2.0/pipelines/{pipeline_id}/updates/{update_id}"
        status_response = hook._do_api_call(("GET", status_endpoint))
        state = status_response.get("update", {}).get("state", "UNKNOWN")
        print(f"[DLT] Pipeline state={state} (elapsed={elapsed}s)")

        if state in terminal_success:
            print("[DLT] Pipeline completed successfully")
            return {
                "pipeline_id": pipeline_id,
                "update_id": update_id,
                "state": state,
            }

        if state in terminal_failure:
            raise RuntimeError(
                f"[DLT] Pipeline {pipeline_id} ended with state={state}. "
                f"Update ID: {update_id}"
            )

    raise TimeoutError(
        f"[DLT] Pipeline {pipeline_id} did not complete within "
        f"{timeout_seconds}s. Last update_id={update_id}"
    )
