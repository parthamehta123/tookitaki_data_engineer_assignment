# Scenario 2: Handling Large Datasets (Design + Simulation)

This scenario focuses on designing a **robust PySpark ingestion pipeline** for handling a **large daily CSV file (~60GB)** arriving in cloud storage.

Since this environment cannot load an actual large file, the objective is to demonstrate **production-grade ingestion design**, not raw throughput benchmarking.

---

## What This Scenario Demonstrates

- Explicit schema usage (no schema inference)
- Scalable ingestion patterns for very large files
- Partitioning strategy to avoid executor OOMs
- Small-files avoidance during Delta writes
- Safe restart and retry behavior using Delta Lake

---

## Design Assumptions

- Data arrives daily as a **large CSV file (~60GB)** in cloud storage (e.g., S3 / ADLS / GCS)
- Files are **append-only CDC deltas**, not full snapshots
- Raw ingestion is **decoupled from downstream de-duplication** (handled in Scenario 1)
- Delta Lake provides **atomic writes and retry safety**

---

## Key Design Considerations

### Explicit Schema
- Prevents expensive schema inference
- Avoids schema drift

### Large File Handling
- Spark distributes reads across executors
- Task sizes are controlled to avoid memory pressure

### Small-Files Avoidance
- Output partitions are explicitly controlled
- Improves downstream query performance

### Failure Recovery
- Jobs can be safely retried
- Partial writes do not corrupt tables

---

## Production Ingestion Pattern (Documented)

```python
spark.read \
  .format("csv") \
  .schema(customer_schema) \
  .option("header", "true") \
  .option("mode", "FAILFAST") \
  .load("s3://bucket/raw/customers/2024-01-10/")
```

---

## Simulation Strategy

Because a real 60GB file cannot be loaded here, ingestion is simulated using in-memory data while applying the same schema and write logic.

---

## Outcome

This scenario demonstrates a **safe, scalable, production-ready ingestion design** that integrates cleanly with downstream processing (Scenario 1).
