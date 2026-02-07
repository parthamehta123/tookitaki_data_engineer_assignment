# Scenario 4: Real-Time Data Quality Validation (GX + DLT)

This scenario demonstrates **production-grade, real-time data quality enforcement** using **Spark Structured Streaming**, **Great Expectations (GX)**, **Kafka**, **Delta Lake**, and **Delta Live Tables (DLT)**.

It intentionally covers **two complementary implementations** to show both **framework-agnostic validation** and **Databricks-native pipelines**.

---

## What this scenario demonstrates

### Core capabilities
- Declarative data quality rules
- Streaming ingestion (Kafka / rate source)
- Fail-fast vs continue semantics
- Deterministic handling of bad data
- Auditability via metrics, quarantine, and data docs

### Two execution models
1. **GX + foreachBatch (framework-agnostic)**
2. **DLT-native expectations (Databricks-managed)**

---

## Implementation options

### Option A — Great Expectations (GX) + foreachBatch
**Use when:**
- Running outside Databricks-managed DLT pipelines
- You want full GX feature set (Data Docs, suites, validation history)
- You need explicit fail-fast control per micro-batch

**Pattern**
```
Kafka
  → Spark Structured Streaming
    → foreachBatch
      → GX validation
        → FAIL → stream stops
        → PASS → write to Delta
```

**Key properties**
- Validation runs per micro-batch
- Entire batch rejected on failure
- Strong consistency guarantees
- Optional GX Data Docs generation

---

### Option B — Delta Live Tables (DLT) streaming pipeline
**Use when:**
- Running inside Databricks
- You want managed orchestration, retries, and observability
- You prefer SQL-style expectations and lineage

**Pattern**
```
Streaming Source
  → Bronze (raw)
    → Silver (enriched + quarantine)
      → Gold (curated)
```

**Key properties**
- Native expectations (`expect`, `expect_or_fail`, `expect_or_drop`)
- Built-in metrics and lineage
- Continuous streaming execution
- Declarative Bronze / Silver / Gold layering

---

## Data quality rules enforced

| Rule | Description |
|----|----|
| party_key_not_null | Primary key must be present |
| party_key_unique | Unique within micro-batch (GX only) |
| valid_country | Country in {US, IN, UK, CA} |
| source_updated_at_not_null | Required source timestamp |
| ingested_at_not_null | Required ingestion timestamp |
| dob_not_future | DOB must be parseable and <= today |
| name_mostly_not_null | Name completeness threshold |

> Note: Global uniqueness must be enforced downstream (MERGE / constraints).

---

## Failure semantics

### Fail-fast (STOP)
- GX: raise exception in `foreachBatch`
- DLT: `@dlt.expect_or_fail`

**Behavior**
- Stream/table update fails
- No partial writes
- Safe restart using checkpoints

---

### Continue with quarantine (RETRY-friendly)
- DLT quarantine pattern (Silver layer)
- Bad rows captured with reasons
- Good rows continue flowing

**Behavior**
- Pipeline stays alive
- Full audit trail preserved
- Ideal for production systems

---

### Drop invalid rows
- DLT: `@dlt.expect_or_drop`

**Behavior**
- Pipeline continues
- Bad rows silently dropped
- Not recommended unless explicitly required

---

## Kafka message format (GX path)

```json
{
  "party_key": "1",
  "source_updated_at": "2024-01-10T10:00:00Z",
  "name": "Alice",
  "dob": "1990-01-01",
  "country": "US",
  "is_deleted": false,
  "ingested_at": "2024-01-10T10:05:00Z"
}
```

---

## DLT tables produced

| Layer | Table | Purpose |
|----|----|----|
| Bronze | bronze_raw_customers | Raw streaming ingestion |
| Silver | silver_customers_enriched | Validation flags + reasons |
| Silver | silver_customers_quarantine | Bad records (audit/debug) |
| Silver | silver_customers_valid | Clean validated rows |
| Gold | gold_customer_curated | Business-ready dataset |

---

## Observability

### GX
- Validation results per batch
- Optional Data Docs (HTML)
- Explicit failure signals

### DLT
- Built-in expectation metrics
- Table lineage
- Quarantine visibility
- Pipeline health dashboard

---

## When to choose which

| Requirement | Use GX | Use DLT |
|----|----|----|
| Non-Databricks runtime | ✅ | ❌ |
| Data Docs required | ✅ | ❌ |
| Managed retries & lineage | ❌ | ✅ |
| Strong fail-fast demo | ✅ | ✅ |
| Bronze/Silver/Gold | ❌ | ✅ |

---

## Summary

This scenario proves:
- Real-time data quality can be enforced deterministically
- Bad data can be stopped, dropped, or quarantined intentionally
- Streaming pipelines can remain consistent and restart-safe
- Both portable (GX) and managed (DLT) approaches are valid

---

## Airflow Integration (Scenario 5)

The GX validation notebook is triggered as part of the `client_ingestion_pipeline` DAG:

```
start → validate_data → gx_streaming_validation → trigger_dlt_pipeline → reconciliation → notify
```

- Airflow `DatabricksRunNowOperator` triggers the GX notebook on Databricks
- On validation failure: task fails, `notify_failure` triggers (TriggerRule.ONE_FAILED)
- On success: DLT pipeline is triggered next via REST API
- Three runtime modes: `local` (Docker), `mock` (zero-infra), `databricks` (real)

See: `airflow/dags/client_ingestion_pipeline_dag.py`

---

## Implementation files

| File | Purpose |
|---|---|
| `src/scenario_4_customer_validation_suite.py` | GX expectation suite (reusable) |
| `src/scenario_4_realtime_ingestion.py` | Streaming pipeline with foreachBatch |
| `notebooks/scenario_4_great_expectations_streaming.ipynb` | Databricks notebook (self-contained) |
| `notebooks/customer_dlt_streaming_pipeline/` | DLT pipeline (Bronze/Silver/Gold) |
| `airflow/dags/client_ingestion_pipeline_dag.py` | Airflow DAG orchestration |
| `airflow/plugins/dlt_trigger_simulation.py` | DLT trigger (simulation + real API) |

---

## Next extensions (optional)
- Expectation metrics export
- Alerting integration
- Replay/backfill workflows
- Schema evolution controls
