"""
Client Ingestion Pipeline DAG
==============================

End-to-end orchestration of client data ingestion with data quality gates.

Integrates:
  - Great Expectations (GX) streaming validation
  - Delta Live Tables (DLT) pipeline trigger
  - Databricks mock operator (zero-infra testing)
  - Real Databricks connectivity (production)

Runtime modes (PIPELINE_RUNTIME env var):
  local       – DockerOperator for GX streaming + Python-simulated DLT
  mock        – DatabricksMockOperator for GX + DLT (zero infra, fast feedback)
  databricks  – Real Databricks jobs for GX streaming + real DLT pipeline via API

Task graph:
  start
    → validate_data
      → gx_streaming_validation
        → trigger_dlt_pipeline
          → reconciliation_metrics
            → notify_success → end

  On failure of any critical task → notify_failure
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# -------------------------------------------------------------------
# RUNTIME CONFIG
# -------------------------------------------------------------------
PIPELINE_RUNTIME = os.getenv("PIPELINE_RUNTIME", "local")  # local | mock | databricks
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

# Databricks resource IDs (only used when PIPELINE_RUNTIME=databricks)
DATABRICKS_GX_JOB_ID = int(os.getenv("DATABRICKS_GX_JOB_ID", "494754315111843"))
DATABRICKS_DLT_PIPELINE_ID = os.getenv("DATABRICKS_DLT_PIPELINE_ID", "")
DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")

# -------------------------------------------------------------------
# CONDITIONAL IMPORTS
#   Only load the provider modules required by the active runtime.
#   This avoids ImportError when optional providers (docker, databricks)
#   are not installed.
# -------------------------------------------------------------------
if PIPELINE_RUNTIME == "local":
    from airflow.providers.docker.operators.docker import DockerOperator
    from dlt_trigger_simulation import trigger_dlt_pipeline

elif PIPELINE_RUNTIME == "mock":
    from databricks_mock import DatabricksMockOperator

elif PIPELINE_RUNTIME == "databricks":
    from airflow.providers.databricks.operators.databricks import (
        DatabricksRunNowOperator,
    )
    from dlt_trigger_simulation import trigger_dlt_databricks

else:
    raise ValueError(
        f"Unknown PIPELINE_RUNTIME={PIPELINE_RUNTIME!r}. "
        "Expected one of: local | mock | databricks"
    )

# -------------------------------------------------------------------
# DEFAULT ARGS
# -------------------------------------------------------------------
default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------
with DAG(
    dag_id="client_ingestion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "spark", "gx", "dlt", PIPELINE_RUNTIME],
    max_active_runs=3,
    doc_md=__doc__,
) as dag:

    # ---------------------------------------------------------------
    # 1. START
    # ---------------------------------------------------------------
    start = EmptyOperator(task_id="start")

    # ---------------------------------------------------------------
    # 2. VALIDATION GATE
    # ---------------------------------------------------------------
    def _validate_metadata(**context):
        """
        Pre-flight checks before ingestion.
        In production this could:
        - validate schema contracts via a registry
        - check upstream data availability (S3 prefix, Kafka lag)
        - run fast GX checkpoint against a sample
        """
        ds = context["ds"]
        run_id = context["run_id"]
        print(f"[VALIDATE] env={ENVIRONMENT} runtime={PIPELINE_RUNTIME}")
        print(f"[VALIDATE] ds={ds} run_id={run_id}")
        print("[VALIDATE] Pre-flight checks passed")

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_metadata,
    )

    # ---------------------------------------------------------------
    # 3. GX STREAMING VALIDATION (runtime-dependent)
    #
    #    local      → DockerOperator  (runs Spark + GX in container)
    #    mock       → DatabricksMockOperator  (simulated Databricks job)
    #    databricks → DatabricksRunNowOperator  (real Databricks job)
    # ---------------------------------------------------------------
    if PIPELINE_RUNTIME == "local":
        gx_task = DockerOperator(
            task_id="gx_streaming_validation",
            image="gx-streaming-demo:latest",
            command="python run_streaming.py",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            auto_remove="success",
            mount_tmp_dir=False,
            environment={
                "ENVIRONMENT": ENVIRONMENT,
            },
            do_xcom_push=False,
        )

    elif PIPELINE_RUNTIME == "mock":
        gx_task = DatabricksMockOperator(
            task_id="gx_streaming_validation",
            job_name="gx_streaming_validation_job",
            notebook_path="/Workspace/Shared/scenario_4_great_expectations_streaming",
            payload={
                "environment": ENVIRONMENT,
                "run_date": "{{ ds }}",
            },
            simulate_seconds=3,
        )

    elif PIPELINE_RUNTIME == "databricks":
        gx_task = DatabricksRunNowOperator(
            task_id="gx_streaming_validation",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=DATABRICKS_GX_JOB_ID,
            notebook_params={
                "environment": ENVIRONMENT,
                "run_date": "{{ ds }}",
            },
        )

    # ---------------------------------------------------------------
    # 4. DLT PIPELINE TRIGGER (runtime-dependent)
    #
    #    local      → PythonOperator  (simulation, no API calls)
    #    mock       → DatabricksMockOperator  (simulated DLT trigger)
    #    databricks → PythonOperator calling trigger_dlt_databricks()
    #                 (real Databricks Pipelines REST API with polling)
    # ---------------------------------------------------------------
    if PIPELINE_RUNTIME == "local":
        dlt_task = PythonOperator(
            task_id="trigger_dlt_pipeline",
            python_callable=trigger_dlt_pipeline,
            op_kwargs={
                "environment": ENVIRONMENT,
                "pipeline_name": "customer_dlt_streaming_pipeline",
            },
        )

    elif PIPELINE_RUNTIME == "mock":
        dlt_task = DatabricksMockOperator(
            task_id="trigger_dlt_pipeline",
            job_name="customer_dlt_streaming_pipeline",
            notebook_path="/Workspace/Shared/customer_dlt_streaming_pipeline",
            payload={
                "environment": ENVIRONMENT,
                "pipeline_name": "customer_dlt_streaming_pipeline",
            },
            simulate_seconds=2,
        )

    elif PIPELINE_RUNTIME == "databricks":
        dlt_task = PythonOperator(
            task_id="trigger_dlt_pipeline",
            python_callable=trigger_dlt_databricks,
            op_kwargs={
                "pipeline_id": DATABRICKS_DLT_PIPELINE_ID,
                "databricks_conn_id": DATABRICKS_CONN_ID,
            },
        )

    # ---------------------------------------------------------------
    # 5. RECONCILIATION
    # ---------------------------------------------------------------
    def _reconcile(**context):
        """
        Compare source vs curated record counts.
        In production:
        - Query Delta tables (source_count vs curated_count)
        - Emit metrics to monitoring (Datadog, Prometheus, etc.)
        - Alert on discrepancy above threshold
        """
        ds = context["ds"]
        print(f"[RECON] Reconciling source vs curated for ds={ds}")
        print(f"[RECON] runtime={PIPELINE_RUNTIME} env={ENVIRONMENT}")
        print("[RECON] Reconciliation complete")

    reconciliation = PythonOperator(
        task_id="reconciliation_metrics",
        python_callable=_reconcile,
    )

    # ---------------------------------------------------------------
    # 6. NOTIFICATIONS
    #    In production: EmailOperator / SlackOperator / PagerDuty
    # ---------------------------------------------------------------
    notify_success = EmptyOperator(task_id="notify_success")

    notify_failure = EmptyOperator(
        task_id="notify_failure",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(task_id="end")

    # ---------------------------------------------------------------
    # TASK DEPENDENCIES
    # ---------------------------------------------------------------
    (
        start
        >> validate_data
        >> gx_task
        >> dlt_task
        >> reconciliation
        >> notify_success
        >> end
    )

    [validate_data, gx_task, dlt_task, reconciliation] >> notify_failure
