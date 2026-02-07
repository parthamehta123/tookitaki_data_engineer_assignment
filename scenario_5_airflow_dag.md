# Scenario 5: Airflow DAG Design — Client Ingestion Pipeline

This scenario demonstrates a **production-grade Airflow DAG** that orchestrates client data ingestion end-to-end, integrating GX validation, DLT pipeline triggers, reconciliation, and notifications.

It complements **Scenario 4 (Great Expectations / DLT)** by handling orchestration, retries, backfills, and operational visibility.

---

## End-to-End Flow

```text
start
  → validate_data (pre-flight checks)
    → gx_streaming_validation (Great Expectations on Databricks / Docker / Mock)
      → trigger_dlt_pipeline (DLT pipeline via REST API / simulation)
        → reconciliation_metrics (source vs curated counts)
          → notify_success → end

(any failure) → notify_failure
```

---

## Three Runtime Modes

The DAG supports three runtime modes via the `PIPELINE_RUNTIME` environment variable:

| Mode | GX Validation | DLT Trigger | Use Case |
|---|---|---|---|
| `local` | DockerOperator (Spark + GX in container) | PythonOperator (simulation) | Local Docker development |
| `mock` | DatabricksMockOperator (simulated) | DatabricksMockOperator (simulated) | Zero-infra fast feedback |
| `databricks` | DatabricksRunNowOperator (real job) | PythonOperator (REST API) | Production Databricks |

### Mode selection
```python
PIPELINE_RUNTIME = os.getenv("PIPELINE_RUNTIME", "local")  # local | mock | databricks
```

Conditional imports ensure only the required provider modules are loaded per mode, avoiding `ImportError` when optional providers (docker, databricks) are not installed.

---

## DAG Steps

### 1. Start
- `EmptyOperator` — entry point for the DAG graph.

### 2. Validate Data (Pre-flight)
- `PythonOperator` — lightweight pre-flight checks.
- In production: validate schema contracts, check upstream data availability (S3 prefix, Kafka lag), run fast GX checkpoint against a sample.

### 3. GX Streaming Validation (runtime-dependent)

| Runtime | Operator | Details |
|---|---|---|
| `local` | `DockerOperator` | Runs `gx-streaming-demo:latest` container with `python run_streaming.py` |
| `mock` | `DatabricksMockOperator` | Simulates a Databricks notebook job (configurable sleep) |
| `databricks` | `DatabricksRunNowOperator` | Triggers real Databricks job (notebook: `scenario_4_great_expectations_streaming`) |

- On failure: task raises exception, downstream tasks are blocked, `notify_failure` triggers.
- Databricks mode passes `environment` and `run_date` as notebook parameters.

### 4. Trigger DLT Pipeline (runtime-dependent)

| Runtime | Operator | Details |
|---|---|---|
| `local` | `PythonOperator` | Calls `trigger_dlt_pipeline()` — local simulation with optional marker file check |
| `mock` | `DatabricksMockOperator` | Simulates DLT pipeline trigger |
| `databricks` | `PythonOperator` | Calls `trigger_dlt_databricks()` — real Databricks REST API (`POST /api/2.0/pipelines/{id}/updates`) with polling |

- Real DLT trigger polls `GET /api/2.0/pipelines/{id}/updates/{update_id}` until terminal state.
- Gracefully skips if `DATABRICKS_DLT_PIPELINE_ID` is not configured (logs message, returns `SKIPPED`).

### 5. Reconciliation Metrics
- `PythonOperator` — compares source vs curated record counts.
- In production: query Delta tables, emit metrics to Datadog/Prometheus, alert on discrepancies.

### 6. Notifications
- `notify_success`: `EmptyOperator` — runs after successful pipeline completion.
- `notify_failure`: `EmptyOperator` with `TriggerRule.ONE_FAILED` — triggers if any upstream task fails.
- In production: replace with `EmailOperator`, `SlackOperator`, or PagerDuty integration.

---

## Task Dependencies

```text
start
  >> validate_data
    >> gx_streaming_validation
      >> trigger_dlt_pipeline
        >> reconciliation_metrics
          >> notify_success
            >> end

[validate_data, gx_task, dlt_task, reconciliation] >> notify_failure
```

---

## Retry and Backoff Strategy

| Setting | Value |
|---|---|
| `retries` | 2 |
| `retry_delay` | 5 minutes |
| `retry_exponential_backoff` | True |
| `max_retry_delay` | 30 minutes |

These are DAG-level defaults. Individual tasks can override as needed.

---

## Preventing Reprocessing

1. **Checkpointing** — Spark Structured Streaming checkpoints ensure exactly-once semantics.
2. **Idempotent ingestion** — Delta Lake MERGE logic prevents duplicates on retry.
3. **Run metadata** — `run_id` and `execution_date` passed to downstream jobs for deterministic replays.

---

## Backfill Strategy

- `catchup=False` for regular `@hourly` schedule.
- `max_active_runs=3` prevents resource exhaustion during backfills.
- Backfills handled via manual DAG runs with historical dates.
- Validation and ingestion logic remains unchanged for backfills.

---

## Configuration

| Environment Variable | Default | Purpose |
|---|---|---|
| `PIPELINE_RUNTIME` | `local` | Runtime mode: `local` / `mock` / `databricks` |
| `ENVIRONMENT` | `local` | Environment label (for logging) |
| `DATABRICKS_GX_JOB_ID` | `494754315111843` | Databricks job ID for GX notebook |
| `DATABRICKS_DLT_PIPELINE_ID` | (empty) | Databricks DLT pipeline ID |
| `DATABRICKS_CONN_ID` | `databricks_default` | Airflow connection ID for Databricks |

---

## Mapping to Scenario 4 (GX / DLT)

| Layer | Responsibility |
|---|---|
| Airflow (Scenario 5) | Orchestration, retries, backfills, alerts |
| GX / DLT (Scenario 4) | Data quality enforcement |
| Spark + Delta | Idempotent ingestion & storage |

Airflow **does not validate data itself** — it enforces *when* validation and ingestion occur.

---

## Implementation Files

| File | Purpose |
|---|---|
| `airflow/dags/client_ingestion_pipeline_dag.py` | Main DAG definition (3 runtime modes) |
| `airflow/plugins/dlt_trigger_simulation.py` | DLT trigger (local simulation + real Databricks API) |
| `airflow/plugins/databricks_mock.py` | DatabricksMockOperator for zero-infra testing |
| `notebooks/scenario_4_great_expectations_streaming.ipynb` | GX validation notebook (Databricks) |

---

## What This Scenario Demonstrates

- Clear task dependencies with linear flow + failure branch
- Three runtime modes (local, mock, databricks) for development velocity
- Robust retry & exponential backoff strategy
- Prevention of duplicate processing via checkpoints and idempotent MERGE
- Real Databricks integration (DatabricksRunNowOperator + DLT REST API)
- Zero-infra testing via DatabricksMockOperator
- Separation of orchestration, validation, and ingestion concerns
