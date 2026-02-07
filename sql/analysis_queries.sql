-- ============================================================
-- Scenario 6: SQL Queries for Data Quality Analysis
-- ============================================================
-- These queries are designed for Delta Lake tables produced by
-- the DLT pipeline (Bronze/Silver/Gold) and the dedup pipeline.
-- ============================================================


-- ------------------------------------------------------------
-- 1. DUPLICATE party_key DETECTION
-- ------------------------------------------------------------

-- 1A. Find duplicate keys in raw table with full context
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


-- 1B. Verify curated table has no duplicates (post-dedup assertion)
SELECT   party_key,
         COUNT(*) AS duplicate_count
FROM     gold_customer_curated
GROUP BY party_key
HAVING   COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- 1C. Duplicate distribution histogram
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


-- ------------------------------------------------------------
-- 2. RAW vs CURATED RECONCILIATION
-- ------------------------------------------------------------

-- 2A. Basic count comparison
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


-- 2B. Per-date reconciliation (for daily pipeline runs)
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


-- 2C. Full pipeline layer reconciliation (Bronze → Silver → Gold)
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


-- ------------------------------------------------------------
-- 3. REJECTED / QUARANTINED RECORDS
-- ------------------------------------------------------------

-- 3A. All quarantined records with rejection reasons
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


-- 3B. Rejection reason breakdown (most common failures)
SELECT   reason,
         COUNT(*)        AS violation_count,
         ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_customers_quarantine), 2)
                         AS pct_of_rejections
FROM     silver_customers_quarantine
LATERAL VIEW EXPLODE(dq_reasons) AS reason
GROUP BY reason
ORDER BY violation_count DESC;


-- 3C. Rejection rate over time (trend analysis)
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


-- 3D. Records that would fail hard validation rules (pre-ingestion analysis)
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
