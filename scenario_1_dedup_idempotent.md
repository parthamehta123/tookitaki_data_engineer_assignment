# Scenario 1: De-duplication & Idempotent Ingestion

## Overview
This module implements Scenario 1 of the Tookitaki Data Engineer assessment.

It derives the **latest customer record per `party_key`** from a daily customer delta dataset using **PySpark and Delta Lake**, ensuring correctness, idempotency, and production safety.

---

## Key Concepts
- Deterministic window-based de-duplication
- Idempotent Delta Lake MERGE writes
- Soft-delete preservation
- Retry-safe pipeline behavior

---

## How It Works
1. Read raw customer deltas from a managed Delta table
2. Rank records per `party_key` using business and ingestion timestamps
3. Select the latest record per key
4. Write results using Delta MERGE INTO

---

## Execution

```bash
spark-submit scenario_1_dedup_idempotent.py \
  --input_table demo_raw_customers_delta \
  --output_table demo_curated_customers_latest
```

---

## Why This Design
- Safe for retries and backfills
- Handles late-arriving data
- Prevents duplicate records
- Aligns with enterprise data lake best practices

---

## Summary
This solution demonstrates a robust, idempotent approach to customer
de-duplication suitable for large-scale production pipelines.
