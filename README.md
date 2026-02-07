# Tookitaki — Data Engineer Technical Assessment

## Overview

This repository contains my solutions for the Tookitaki Data Engineer take-home assessment.
The focus is on correctness, scalability, idempotency, and production-readiness.

Each scenario is implemented as working PySpark / SQL code **and** accompanied by a dedicated
design document (Markdown) that explains the reasoning, trade-offs, and architecture behind
the solution.

**Tools & Frameworks**
- Python 3 / PySpark
- SQL (Spark SQL / Delta)
- Databricks
- Delta Lake
- Delta Live Tables (DLT)
- Apache Airflow
- Docker

---

## Scenario Design Documents

Each scenario has a standalone Markdown file at the repo root that provides a detailed
write-up — design rationale, key concepts, execution instructions, and architecture decisions.

| # | File | Topic |
|---|------|-------|
| 1 | [scenario_1_dedup_idempotent.md](scenario_1_dedup_idempotent.md) | De-duplication & Idempotent Ingestion |
| 2 | [scenario_2_large_ingestion.md](scenario_2_large_ingestion.md) | Handling Large Datasets (~60 GB) — Design & Simulation |
| 3 | [scenario_3_data_validation.md](scenario_3_data_validation.md) | Data Validation Before Ingestion (Hard & Soft Checks) |
| 4 | [scenario_4_great_expectations.md](scenario_4_great_expectations.md) | Real-Time Data Quality Validation (GX + DLT) |
| 5 | [scenario_5_airflow_dag.md](scenario_5_airflow_dag.md) | Airflow DAG Design — Client Ingestion Pipeline |
| 6 | [scenario_6_spark_sql.md](scenario_6_spark_sql.md) | Spark Optimization & SQL Reasoning |

> **Reviewers:** Start with these Markdown files — they provide the full context and
> reasoning for every implementation decision. The source code in `src/` and `notebooks/`
> is the executable counterpart.

---

## Repository Structure

```
├── scenario_1_dedup_idempotent.md          # Scenario design documents (start here)
├── scenario_2_large_ingestion.md
├── scenario_3_data_validation.md
├── scenario_4_great_expectations.md
├── scenario_5_airflow_dag.md
├── scenario_6_spark_sql.md
│
├── src/                                    # PySpark source modules
│   ├── scenario_1_dedup_idempotent.py
│   ├── scenario_2_large_ingestion.py
│   ├── scenario_3_data_validation.py
│   ├── scenario_4_customer_validation_suite.py
│   └── scenario_4_realtime_ingestion.py
│
├── notebooks/                              # Databricks-ready notebooks
│   ├── scenario_1_dedup_idempotent.ipynb
│   ├── scenario_2_large_dataset_ingestion.ipynb
│   ├── scenario_3_data_validation.ipynb
│   ├── scenario_4_great_expectations_streaming.ipynb
│   └── customer_dlt_streaming_pipeline/    # DLT Bronze/Silver/Gold pipeline
│
├── sql/
│   └── analysis_queries.sql                # SQL queries for data quality analysis
│
├── airflow/
│   ├── dags/
│   │   └── client_ingestion_pipeline_dag.py
│   ├── plugins/
│   │   ├── databricks_mock.py              # Mock operator for zero-infra testing
│   │   └── dlt_trigger_simulation.py       # DLT trigger (simulation + real API)
│   ├── docker-compose.yaml
│   └── Dockerfile
│
├── spark/jobs/
│   └── client_ingestion.py                 # Spark job entry point
│
├── tests/
│   └── test_scenario_3_data_validation.py
│
├── run_streaming.py                        # Streaming entry point (GX demo)
├── requirements.txt
├── Dockerfile
└── README.md
```

---

## Assumptions

- Input data is append-only daily deltas (CDC)
- Spark jobs are rerunnable and idempotent
- Curated layer supports overwrite or MERGE semantics
- Soft deletes are preserved for auditability
- Delta Lake is the storage format throughout

---

## How to Review

1. **Read the scenario Markdown files** — they explain *why* each design choice was made.
2. **Browse `src/`** — production-style PySpark modules for Scenarios 1–4.
3. **Open `notebooks/`** — self-contained Databricks notebooks that can be imported and run.
4. **Check `airflow/`** — the orchestration DAG (Scenario 5) with three runtime modes.
5. **See `sql/analysis_queries.sql`** — data quality and reconciliation queries (Scenario 6).

---

## How to Run

- **Notebooks**: Import `.ipynb` files directly into Databricks.
- **PySpark modules**: Run via `spark-submit` (e.g., `spark-submit src/scenario_1_dedup_idempotent.py`).
- **Airflow DAG**: Use the provided `docker-compose.yaml` in `airflow/` to spin up a local Airflow instance.
- **Streaming demo**: `python run_streaming.py` (requires Spark and GX installed).
- **Tests**: `pytest tests/`

---

## Author

Partha Mehta
