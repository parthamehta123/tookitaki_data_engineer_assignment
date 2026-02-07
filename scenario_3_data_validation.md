# Scenario 3: Data Validation Before Ingestion

## Objective
Before data is ingested into the curated layer, it must pass a set of **data quality validations**.
This scenario demonstrates how to classify validations into **hard** and **soft** checks and
enforce them using PySpark in a production-friendly way.

---

## Validation Strategy

### Hard Validations (Block Ingestion)
These validations protect **data correctness** and **pipeline safety**.

- `party_key` must not be NULL
- `source_updated_at` must not be NULL
- `ingested_at >= source_updated_at`
- Schema-level type enforcement

If **any hard validation fails**, the job **fails fast**.

---

### Soft Validations (Alert Only)
These validations indicate **data quality degradation** but do not block ingestion.

- High NULL percentage in `name`
- Invalid or unexpected `country` values

Soft validation failures are **logged and emitted as metrics**.

---

## Execution Flow

1. Read raw dataset
2. Run hard validations → fail if violated
3. Run soft validations → log warnings
4. Allow downstream ingestion to proceed

---

## Why This Matters

- Prevents corrupt data from entering curated tables
- Preserves availability by not blocking on non-critical issues
- Aligns with real-world Service Delivery expectations

---

## Output

- Clean, validated DataFrame
- Clear validation failure messages
- Production-ready validation pattern
