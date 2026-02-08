# Scenario 1: De-duplication & Idempotent Ingestion

## Overview

This module implements **Scenario 1** of the Tookitaki Data Engineer assessment.

It derives the **latest customer record per `party_key`** from a daily customer delta dataset using **PySpark and Delta Lake**, with strong guarantees around **correctness, idempotency, and production safety**.

The design mirrors real-world lakehouse ingestion patterns and is safe for retries, backfills, and reprocessing.

---

## What Problem This Solves

Customer ingestion pipelines commonly face:

- Duplicate records for the same business key
- Late-arriving updates from upstream systems
- Soft deletes represented as state changes (`is_deleted = true`)
- Reprocessing due to retries or pipeline failures

This scenario demonstrates how to handle all of these cases **deterministically and safely**.

---

## Key Concepts

- **Deterministic window-based de-duplication**
- **Idempotent Delta Lake MERGE writes**
- **Soft-delete preservation as state transitions**
- **Retry-safe and backfill-safe pipeline behavior**

---

## Soft Delete Semantics (Important)

This pipeline **does not perform physical deletes**.

Instead, deletes are modeled as **state transitions**:

- Each incoming record represents the latest known state of a customer
- If the most recent record for a `party_key` has `is_deleted = true`, that state is preserved
- The curated table always reflects the **latest state per business key**

This approach prevents accidental data loss and maintains correctness across reprocessing.

---

## How It Works

1. Read raw customer delta records from a managed Delta table
2. Rank records per `party_key` using deterministic ordering:
   - `source_updated_at` (primary business timestamp)
   - `ingested_at` (tie-breaker)
3. Select the latest record per business key
4. Write results using a Delta Lake `MERGE INTO` operation

This guarantees **exactly one output row per `party_key`**.

---

## Idempotency Guarantees

The pipeline is fully **idempotent**:

- Reprocessing the same input produces no net change
- Late-arriving updates overwrite older records correctly
- Soft deletes are preserved as state transitions
- Retries and backfills are safe by design

Idempotency is validated by re-running the transformation and comparing outputs.

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
- Handles late-arriving data correctly  
- Prevents duplicate records  
- Preserves delete semantics without physical removal  
- Aligns with enterprise lakehouse best practices  

---

## Summary

This scenario demonstrates a **robust, idempotent, and production-ready** approach to customer de-duplication using PySpark and Delta Lake.

By modeling deletes as **state transitions rather than physical deletes**, the pipeline ensures correctness, safety, and long-term maintainability.
