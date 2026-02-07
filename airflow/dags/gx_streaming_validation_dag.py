"""
GX Streaming Validation DAG (standalone)
=========================================

Manual/event-driven DAG for running GX streaming validation independently
of the full ingestion pipeline.

Supports all three runtime modes:
  local       – DockerOperator (Spark + GX in container)
  mock        – DatabricksMockOperator (zero infra)
  databricks  – DatabricksRunNowOperator (real Databricks job)
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

PIPELINE_RUNTIME = os.getenv("PIPELINE_RUNTIME", "local")
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

default_args = {
    "owner": "data-platform",
    "retries": 1,
}

with DAG(
    dag_id="gx_streaming_validation",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual / event-driven
    catchup=False,
    default_args=default_args,
    tags=["gx", "spark", "streaming", PIPELINE_RUNTIME],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    if PIPELINE_RUNTIME == "local":
        from airflow.providers.docker.operators.docker import DockerOperator

        gx_streaming = DockerOperator(
            task_id="run_gx_streaming",
            image="gx-streaming-demo:latest",
            command="python run_streaming.py",
            auto_remove="never",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            environment={"ENVIRONMENT": ENVIRONMENT},
            mount_tmp_dir=False,
        )

    elif PIPELINE_RUNTIME == "mock":
        from databricks_mock import DatabricksMockOperator

        gx_streaming = DatabricksMockOperator(
            task_id="run_gx_streaming",
            job_name="gx_streaming_validation_job",
            notebook_path="/Workspace/Shared/scenario_4_great_expectations_streaming",
            payload={"environment": ENVIRONMENT},
            simulate_seconds=3,
        )

    elif PIPELINE_RUNTIME == "databricks":
        from airflow.providers.databricks.operators.databricks import (
            DatabricksRunNowOperator,
        )

        gx_streaming = DatabricksRunNowOperator(
            task_id="run_gx_streaming",
            databricks_conn_id=os.getenv("DATABRICKS_CONN_ID", "databricks_default"),
            job_id=int(os.getenv("DATABRICKS_GX_JOB_ID", "494754315111843")),
            notebook_params={
                "environment": ENVIRONMENT,
                "run_date": "{{ ds }}",
            },
        )

    else:
        raise ValueError(
            f"Unknown PIPELINE_RUNTIME={PIPELINE_RUNTIME!r}. "
            "Expected one of: local | mock | databricks"
        )

    end = EmptyOperator(task_id="end")

    start >> gx_streaming >> end
