# Scenario 6: Spark Optimization & SQL Reasoning

This scenario demonstrates **Spark performance tuning** and **SQL-based data quality analysis** in the context of a large-scale customer ingestion pipeline.

---

## Part 1: Spark Job Optimization

### Problem Statement

A Spark ingestion job processing ~60GB daily customer deltas exhibits:
1. Heavy shuffles during deduplication and joins
2. Data skew on `party_key` (some clients have disproportionately more records)
3. Too many small output files in the Delta table

Below is how I would diagnose and resolve each issue.

---

### 1.1 Heavy Shuffles

**Root cause:** Operations like `GROUP BY party_key`, `Window.partitionBy("party_key")`, and `JOIN` trigger full data shuffles across the cluster. In our deduplication logic (Scenario 1), the window function `partitionBy("party_key").orderBy(source_updated_at.desc())` causes a full shuffle of all records by `party_key`.

**Optimization strategies:**

#### A. Broadcast joins for reference data

When joining customer deltas against small reference tables (country codes, validation rules), broadcast the smaller side to avoid shuffle entirely.

```python
from pyspark.sql.functions import broadcast

# BAD: shuffle join (both sides redistributed)
enriched = customers.join(country_ref, "country")

# GOOD: broadcast join (no shuffle, reference sent to all executors)
enriched = customers.join(broadcast(country_ref), "country")
```

**When to use:** Reference table fits in memory (< 10MB default, configurable via `spark.sql.autoBroadcastJoinThreshold`).

```python
# Increase broadcast threshold for medium reference tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB
```

#### B. Reduce shuffle partition count

The default `spark.sql.shuffle.partitions = 200` is often excessive for medium datasets or too low for large ones. Tune it based on actual data volume.

```python
# For ~60GB input, target ~128MB per partition
# 60GB / 128MB ≈ 480 partitions
spark.conf.set("spark.sql.shuffle.partitions", 480)
```

**Rule of thumb:** Target 128MB–256MB per shuffle partition. Too many partitions = scheduler overhead and small files. Too few = OOM or GC pressure.

#### C. Enable Adaptive Query Execution (AQE)

AQE dynamically optimizes shuffle partitions, join strategies, and skew handling at runtime based on actual data statistics.

```python
spark.conf.set("spark.sql.adaptive.enabled", True)                          # Enable AQE
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)       # Auto-merge small partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")  # Target partition size
```

AQE is the single most impactful setting for shuffle-heavy workloads. It is enabled by default in Databricks Runtime 7.3+ and Spark 3.2+.

#### D. Pre-partition input data

If the raw Delta table is repeatedly read and shuffled by `party_key`, partition or Z-ORDER the table on that column to co-locate records.

```sql
-- Z-ORDER: co-locates records with similar party_key values in the same files
OPTIMIZE bronze_raw_customers ZORDER BY (party_key);
```

This doesn't eliminate shuffles but reduces shuffle data volume because records with the same key are already partially co-located.

---

### 1.2 Data Skew

**Root cause:** Some `party_key` values have orders of magnitude more records than others (e.g., a single corporate client with 10M updates vs retail clients with 1–2 updates). During `partitionBy("party_key")`, all records for a hot key land on a single executor, creating a straggler task.

**Diagnosis:**

```python
# Identify skewed keys
(
    df.groupBy("party_key")
    .count()
    .orderBy(F.col("count").desc())
    .show(20)
)

# Look for: top keys with count >> median count
# If top key has 10M rows and median is 5, you have severe skew
```

In the Spark UI, skew manifests as:
- One task taking 10x–100x longer than others in a stage
- Uneven "Shuffle Read Size" across tasks
- Possible OOM on the straggler executor

**Optimization strategies:**

#### A. AQE Skew Join Optimization (preferred)

Spark 3.x AQE can automatically detect and split skewed partitions during joins.

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)        # Partition is skewed if > 5x median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

AQE will automatically split oversized partitions and replicate the other side of the join to balance work.

#### B. Key salting (manual approach)

When AQE is unavailable or insufficient, manually distribute skewed keys across multiple partitions using a salt.

```python
import pyspark.sql.functions as F

NUM_SALT_BUCKETS = 10

# Step 1: Add random salt to break up hot keys
salted_df = df.withColumn(
    "salt", (F.rand() * NUM_SALT_BUCKETS).cast("int")
)

# Step 2: Window function now partitions by (party_key, salt)
# This splits a 10M-row key into ~10 partitions of ~1M each
window_spec = Window.partitionBy("party_key", "salt").orderBy(
    F.col("source_updated_at").desc(),
    F.col("ingested_at").desc(),
)

# Step 3: First pass — deduplicate within each salt bucket
first_pass = (
    salted_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn", "salt")
)

# Step 4: Second pass — final dedup across buckets (much smaller dataset now)
final_window = Window.partitionBy("party_key").orderBy(
    F.col("source_updated_at").desc(),
    F.col("ingested_at").desc(),
)

deduplicated = (
    first_pass
    .withColumn("rn", F.row_number().over(final_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
```

**Trade-off:** Two-pass dedup adds a stage but eliminates the straggler. Net wall-clock time is typically 3x–10x faster for severely skewed data.

#### C. Isolate and handle hot keys separately

For extreme skew (one key dominates), process the hot key independently.

```python
HOT_KEYS = ["CORP_CLIENT_001"]

# Split into hot and cold paths
hot_df = df.filter(F.col("party_key").isin(HOT_KEYS))
cold_df = df.filter(~F.col("party_key").isin(HOT_KEYS))

# Cold path: standard dedup (balanced partitions)
cold_deduped = deduplicate_latest_per_party(cold_df)

# Hot path: collect + local dedup (small output, large input)
hot_deduped = deduplicate_latest_per_party(
    hot_df.repartition(50, "party_key")  # Force spread across 50 tasks
)

# Union results
final = cold_deduped.unionByName(hot_deduped)
```

---

### 1.3 Too Many Small Output Files

**Root cause:** Each Spark task writes one output file. With 200 shuffle partitions, you get 200 files per write. Compounded over hourly appends (24 writes/day), this produces ~4,800 files/day. Small files degrade read performance due to excessive metadata overhead and per-file I/O cost.

**Optimization strategies:**

#### A. Coalesce before write

Reduce the number of output partitions before writing.

```python
# Target: 128MB per file for Delta tables
# For a 60GB dataset: 60GB / 128MB ≈ 480 files
TARGET_FILE_COUNT = 480

(
    deduplicated_df
    .coalesce(TARGET_FILE_COUNT)  # Narrow shuffle — no data movement
    .write.format("delta")
    .mode("append")
    .saveAsTable("demo_curated_customers_latest")
)
```

**`coalesce` vs `repartition`:**
- `coalesce(N)`: Merges partitions in-place without a shuffle. Use when reducing partition count.
- `repartition(N)`: Full shuffle to create exactly N evenly-sized partitions. Use when increasing partitions or when data distribution matters.

```python
# Use repartition when you need even distribution (e.g., before a join)
df.repartition(8, "party_key")

# Use coalesce when you just want fewer output files (cheaper, no shuffle)
df.coalesce(8)
```

This is exactly what we do in [scenario_2_large_ingestion.py](src/scenario_2_large_ingestion.py):

```python
def write_raw_delta(df, table_name, num_partitions=8):
    df.repartition(num_partitions).write.format("delta").mode("append").saveAsTable(table_name)
```

#### B. Delta Lake Auto-Optimize

Enable automatic file compaction on the Delta table itself.

```sql
-- Enable auto-compaction (merges small files after each write)
ALTER TABLE gold_customer_curated
SET TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

- **`optimizeWrite`**: Spark coalesces partitions before writing (like adaptive coalesce).
- **`autoCompact`**: After each write, Delta runs a lightweight compaction to merge small files.

#### C. Scheduled OPTIMIZE (maintenance job)

For tables with frequent appends, run periodic compaction.

```sql
-- Compact files and co-locate by party_key for faster lookups
OPTIMIZE gold_customer_curated ZORDER BY (party_key);

-- Check file statistics
DESCRIBE DETAIL gold_customer_curated;
-- Look at: numFiles, sizeInBytes, sizeInBytes/numFiles (target > 100MB)
```

**Best practice:** Schedule `OPTIMIZE` as a daily maintenance task in Airflow, running after the last hourly ingestion.

#### D. Target file size (Delta Lake)

Configure Delta to target a specific file size.

```sql
ALTER TABLE gold_customer_curated
SET TBLPROPERTIES ('delta.targetFileSize' = '128mb');
```

---

### Optimization Summary

| Problem | Primary Fix | Secondary Fix | Impact |
|---|---|---|---|
| Heavy shuffles | AQE + broadcast joins | Reduce shuffle partitions, Z-ORDER | 2x–5x faster |
| Data skew | AQE skew join | Key salting (2-pass dedup) | 3x–10x faster for skewed keys |
| Small files | coalesce/repartition before write | Auto-Optimize + scheduled OPTIMIZE | 2x–5x faster reads |

---

## Part 2: SQL Queries for Data Quality Analysis

These queries are designed for Delta Lake tables and use the table names from this project's implementation.

### 2.1 Identify Duplicate `party_key` Records

Duplicates in the raw layer are expected (multiple updates per customer). Duplicates in the curated layer indicate a deduplication bug.

#### Query A: Find duplicate keys in raw table with full context

```sql
-- Identify party_keys with multiple records and show all duplicates
-- ranked by business timestamp (most recent first)
WITH duplicate_keys AS (
    SELECT party_key,
           COUNT(*)                                AS record_count,
           COUNT(DISTINCT source_updated_at)       AS distinct_timestamps,
           MIN(source_updated_at)                  AS earliest_update,
           MAX(source_updated_at)                  AS latest_update
    FROM   bronze_raw_customers
    GROUP BY party_key
    HAVING COUNT(*) > 1
)
SELECT dk.party_key,
       dk.record_count,
       dk.distinct_timestamps,
       dk.earliest_update,
       dk.latest_update,
       r.name,
       r.country,
       r.source_updated_at,
       r.ingested_at,
       r.is_deleted
FROM   duplicate_keys dk
JOIN   bronze_raw_customers r ON dk.party_key = r.party_key
ORDER BY dk.record_count DESC, dk.party_key, r.source_updated_at DESC;
```

**Why this query matters:**
- `record_count` shows severity of duplication
- `distinct_timestamps` distinguishes genuine updates (multiple timestamps) from true duplicates (same timestamp)
- Ordered by `source_updated_at DESC` to see which record the dedup logic would keep

#### Query B: Verify curated table has no duplicates (post-dedup assertion)

```sql
-- This should return ZERO rows after deduplication
-- If it returns rows, the MERGE logic has a bug
SELECT   party_key,
         COUNT(*) AS duplicate_count
FROM     gold_customer_curated
GROUP BY party_key
HAVING   COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

#### Query C: Duplicate distribution histogram

```sql
-- Distribution of duplicates: how many keys have 2 copies, 3 copies, etc.
-- Useful for sizing the dedup workload and detecting bulk ingestion issues
WITH counts AS (
    SELECT party_key, COUNT(*) AS copies
    FROM   bronze_raw_customers
    GROUP BY party_key
)
SELECT   copies          AS records_per_key,
         COUNT(*)        AS num_keys,
         SUM(copies)     AS total_records
FROM     counts
GROUP BY copies
ORDER BY copies;
```

---

### 2.2 Compare Record Counts Between Raw and Curated Tables

Reconciliation ensures the pipeline didn't silently drop or duplicate data.

#### Query A: Basic count comparison

```sql
-- Side-by-side raw vs curated counts
-- Expected: curated_count <= raw_count (dedup removes duplicates)
-- Alert if: curated_count = 0 (pipeline failure) or curated_count > raw_distinct_keys (duplication bug)
SELECT
    raw.total_records       AS raw_total_records,
    raw.distinct_keys       AS raw_distinct_keys,
    curated.total_records   AS curated_total_records,
    raw.distinct_keys - curated.total_records AS missing_keys,
    ROUND(
        curated.total_records * 100.0 / NULLIF(raw.distinct_keys, 0), 2
    ) AS coverage_pct
FROM (
    SELECT COUNT(*)                AS total_records,
           COUNT(DISTINCT party_key) AS distinct_keys
    FROM   bronze_raw_customers
) raw
CROSS JOIN (
    SELECT COUNT(*) AS total_records
    FROM   gold_customer_curated
) curated;
```

**Interpreting results:**
- `coverage_pct = 100%`: All distinct keys in raw are present in curated (ideal)
- `coverage_pct < 100%`: Some keys were filtered (check quarantine/validation)
- `missing_keys > 0`: Investigate quarantine table for rejected records

#### Query B: Per-date reconciliation (for daily pipeline runs)

```sql
-- Compare ingestion volumes per day
-- Detects anomalies: sudden drops (data loss) or spikes (duplicate ingestion)
SELECT
    CAST(r.source_updated_at AS DATE)   AS update_date,
    COUNT(DISTINCT r.party_key)         AS raw_distinct_keys,
    COUNT(*)                            AS raw_total_records,
    c.curated_count,
    COUNT(DISTINCT r.party_key) - COALESCE(c.curated_count, 0) AS delta
FROM bronze_raw_customers r
LEFT JOIN (
    SELECT CAST(source_updated_at AS DATE) AS update_date,
           COUNT(*)                        AS curated_count
    FROM   gold_customer_curated
    GROUP BY CAST(source_updated_at AS DATE)
) c ON CAST(r.source_updated_at AS DATE) = c.update_date
GROUP BY CAST(r.source_updated_at AS DATE), c.curated_count
ORDER BY update_date DESC;
```

#### Query C: Reconciliation across all pipeline layers (Bronze → Silver → Gold)

```sql
-- Full pipeline layer reconciliation (DLT pipeline tables)
SELECT
    'bronze_raw_customers'          AS layer, COUNT(*) AS record_count FROM bronze_raw_customers
UNION ALL
SELECT
    'silver_customers_enriched'     AS layer, COUNT(*) FROM silver_customers_enriched
UNION ALL
SELECT
    'silver_customers_valid'        AS layer, COUNT(*) FROM silver_customers_valid
UNION ALL
SELECT
    'silver_customers_quarantine'   AS layer, COUNT(*) FROM silver_customers_quarantine
UNION ALL
SELECT
    'gold_customer_curated'         AS layer, COUNT(*) FROM gold_customer_curated
ORDER BY
    CASE layer
        WHEN 'bronze_raw_customers'        THEN 1
        WHEN 'silver_customers_enriched'   THEN 2
        WHEN 'silver_customers_valid'      THEN 3
        WHEN 'silver_customers_quarantine' THEN 4
        WHEN 'gold_customer_curated'       THEN 5
    END;

-- Expected relationship:
--   bronze = silver_enriched (1:1 enrichment)
--   silver_valid + silver_quarantine = silver_enriched (partitioned by quality)
--   gold <= silver_valid (gold may apply additional business filters)
```

---

### 2.3 Find Records Rejected During Ingestion

Rejected records are captured in the quarantine table with explicit failure reasons.

#### Query A: All quarantined records with rejection reasons

```sql
-- Records that failed DLT data quality expectations
-- dq_reasons contains the list of violated rules per record
SELECT   party_key,
         name,
         country,
         dob,
         source_updated_at,
         ingested_at,
         dq_reasons,
         dq_checked_at
FROM     silver_customers_quarantine
ORDER BY dq_checked_at DESC;
```

#### Query B: Rejection reason breakdown (most common failures)

```sql
-- Explode the dq_reasons array to count each violation type
-- Identifies systemic data quality issues in upstream sources
SELECT   reason,
         COUNT(*)        AS violation_count,
         ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_customers_quarantine), 2)
                         AS pct_of_rejections
FROM     silver_customers_quarantine
LATERAL VIEW EXPLODE(dq_reasons) AS reason
GROUP BY reason
ORDER BY violation_count DESC;
```

**Expected output example:**

| reason | violation_count | pct_of_rejections |
|---|---|---|
| invalid_country | 45 | 38.46 |
| name_null | 30 | 25.64 |
| party_key_null | 20 | 17.09 |
| dob_invalid_or_future | 15 | 12.82 |
| source_updated_at_null | 7 | 5.98 |

#### Query C: Rejection rate over time (trend analysis)

```sql
-- Track rejection rate per day to detect upstream data quality degradation
-- Alert if rejection_rate exceeds threshold (e.g., > 5%)
SELECT
    CAST(dq_checked_at AS DATE)        AS check_date,
    COUNT(*)                           AS rejected_count,
    total.enriched_count,
    ROUND(
        COUNT(*) * 100.0 / NULLIF(total.enriched_count, 0), 2
    ) AS rejection_rate_pct
FROM silver_customers_quarantine q
LEFT JOIN (
    SELECT CAST(dq_checked_at AS DATE) AS check_date,
           COUNT(*)                    AS enriched_count
    FROM   silver_customers_enriched
    GROUP BY CAST(dq_checked_at AS DATE)
) total ON CAST(q.dq_checked_at AS DATE) = total.check_date
GROUP BY CAST(q.dq_checked_at AS DATE), total.enriched_count
ORDER BY check_date DESC;
```

#### Query D: Records that failed hard validations (Scenario 3 pattern)

```sql
-- Identify records that would fail hard validation rules
-- (party_key null, source_updated_at null, temporal constraint violation)
-- Useful for pre-ingestion analysis of raw data quality
SELECT
    'party_key_null'          AS violation,
    COUNT(*)                  AS count
FROM bronze_raw_customers
WHERE party_key IS NULL

UNION ALL

SELECT
    'source_updated_at_null'  AS violation,
    COUNT(*)
FROM bronze_raw_customers
WHERE source_updated_at IS NULL

UNION ALL

SELECT
    'ingested_before_source'  AS violation,
    COUNT(*)
FROM bronze_raw_customers
WHERE ingested_at < source_updated_at

UNION ALL

SELECT
    'invalid_country'         AS violation,
    COUNT(*)
FROM bronze_raw_customers
WHERE country NOT IN ('US', 'IN', 'UK', 'CA')

UNION ALL

SELECT
    'dob_future'              AS violation,
    COUNT(*)
FROM bronze_raw_customers
WHERE TRY_CAST(dob AS DATE) > CURRENT_DATE()

ORDER BY count DESC;
```

---

## Part 3: How These Optimizations Connect to the Pipeline

### In our implementation

| Scenario | Optimization Applied |
|---|---|
| Scenario 1 (Dedup) | Window `partitionBy(party_key)` — benefits from AQE + salting for skew |
| Scenario 2 (Large Ingestion) | `repartition(8)` before write prevents small files; explicit schema avoids inference overhead |
| Scenario 3 (Validation) | Validation before write avoids wasted shuffle on bad data |
| Scenario 4 (GX/DLT) | `foreachBatch` validates per micro-batch; DLT auto-manages compaction |
| Scenario 5 (Airflow) | Orchestrates OPTIMIZE as post-ingestion maintenance |

### Production tuning checklist

```python
# 1. Enable AQE (handles shuffle + skew + small files automatically)
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)

# 2. Set shuffle partitions based on data volume
spark.conf.set("spark.sql.shuffle.partitions", 480)  # ~60GB / 128MB

# 3. Set max partition bytes for reads
spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))

# 4. Broadcast threshold for reference tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
```

```sql
-- 5. Delta table maintenance (scheduled daily)
OPTIMIZE gold_customer_curated ZORDER BY (party_key);

-- 6. Auto-optimize for streaming tables
ALTER TABLE gold_customer_curated SET TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.targetFileSize' = '128mb'
);
```

---

## Summary

This scenario demonstrates:
- Spark internals (shuffle, partitioning, skew)
- Practical optimization strategies with trade-offs (AQE vs manual salting)
- SQL for data quality analysis across Bronze/Silver/Gold layers
- Connection between optimization techniques and the actual pipeline implementation
- Monitoring and alerting patterns (rejection rate trends, reconciliation)
