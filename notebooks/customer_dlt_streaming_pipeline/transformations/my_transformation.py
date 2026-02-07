import dlt
from pyspark.sql import functions as F

# ============================================================
# DLT Streaming Pipeline: Bronze → Silver → Gold (+ Quarantine)
# ============================================================
#
# Goals:
# 1) Keep streaming ingestion (rate source)
# 2) Define expectations as DLT rules
# 3) Quarantine bad records (instead of hard failing everything)
# 4) Bronze/Silver/Gold layering
# 5) Clear "retry vs stop" semantics explanation (you can see my notes at the bottom)
#
# NOTE:
# - DLT expectations are row-level expressions (no aggregates like COUNT()).
# - `@dlt.expect_or_fail(...)` will FAIL the table update if any violating rows exist.
#   That is "stop semantics".
# - If we want streaming to continue, we can use `@dlt.expect_or_drop(...)` (drop bad rows)
#   OR route bad rows to a quarantine table (recommended).
# ============================================================


# ------------------------
# BRONZE (raw ingestion)
# ------------------------
@dlt.table(
    name="bronze_raw_customers",
    comment="BRONZE: raw customer events from streaming source (intentionally includes bad data)",
)
def bronze_raw_customers():
    return (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .select(
            # party_key NULL -> violation
            F.when(F.col("value") % 3 == 0, F.lit(None))
            .otherwise(F.col("value").cast("string"))
            .alias("party_key"),
            # name NULL for ~50% -> violation vs mostly-not-null
            F.when(F.col("value") % 2 == 0, F.lit(None))
            .otherwise(F.lit("Alice"))
            .alias("name"),
            # invalid country XX in ~20% -> violation
            F.when(F.col("value") % 5 == 0, F.lit("XX"))
            .otherwise(F.lit("US"))
            .alias("country"),
            # future dob in ~25% -> violation
            F.when(F.col("value") % 4 == 0, F.lit("2099-01-01"))
            .otherwise(F.lit("1990-01-01"))
            .alias("dob"),
            F.current_timestamp().alias("source_updated_at"),
            F.current_timestamp().alias("ingested_at"),
        )
    )


# -------------------------------------------------------
# SILVER: add derived columns + quality flag + reasons
# -------------------------------------------------------
@dlt.table(
    name="silver_customers_enriched",
    comment="SILVER: enriched customers with validation flags and reasons",
)
def silver_customers_enriched():
    df = dlt.read_stream("bronze_raw_customers")

    # Parse dob safely to date (NULL if invalid)
    df = df.withColumn("dob_date", F.to_date(F.col("dob")))

    # Row-level validations (same rules we care about)
    party_key_ok = F.col("party_key").isNotNull()
    country_ok = F.col("country").isin("US", "IN", "UK", "CA")
    source_updated_at_ok = F.col("source_updated_at").isNotNull()
    ingested_at_ok = F.col("ingested_at").isNotNull()

    # If dob is present and parseable: must be <= current_date()
    dob_ok = (F.col("dob_date").isNotNull()) & (F.col("dob_date") <= F.current_date())

    # Build a list of reasons for quarantine
    reasons = F.array_remove(
        F.array(
            F.when(~party_key_ok, F.lit("party_key_null")),
            F.when(~country_ok, F.lit("invalid_country")),
            F.when(~source_updated_at_ok, F.lit("source_updated_at_null")),
            F.when(~ingested_at_ok, F.lit("ingested_at_null")),
            F.when(~dob_ok, F.lit("dob_invalid_or_future")),
            F.when(F.col("name").isNull(), F.lit("name_null")),
        ),
        F.lit(None),
    )

    df = (
        df.withColumn("dq_reasons", reasons)
        .withColumn("is_quarantined", (F.size(F.col("dq_reasons")) > 0))
        .withColumn("dq_checked_at", F.current_timestamp())
    )

    return df


# -----------------------------------------
# QUARANTINE: bad rows go here (streaming)
# -----------------------------------------
@dlt.table(
    name="silver_customers_quarantine",
    comment="QUARANTINE: rows that violate validation rules (kept for audit/debug)",
)
def silver_customers_quarantine():
    return (
        dlt.read_stream("silver_customers_enriched")
        .filter(F.col("is_quarantined") == True)
        .drop("dob_date")  # optional: we can keep it if we want
    )


# ---------------------------------------------------------
# CURATED/GOOD SILVER: only valid rows pass downstream
# ---------------------------------------------------------
@dlt.table(
    name="silver_customers_valid",
    comment="SILVER: only valid rows (quarantine removed)",
)
@dlt.expect("party_key_not_null", "party_key IS NOT NULL")
@dlt.expect("valid_country", "country IN ('US','IN','UK','CA')")
@dlt.expect("source_updated_at_not_null", "source_updated_at IS NOT NULL")
@dlt.expect("ingested_at_not_null", "ingested_at IS NOT NULL")
@dlt.expect(
    "dob_not_future", "to_date(dob) IS NOT NULL AND to_date(dob) <= current_date()"
)
@dlt.expect("name_not_null", "name IS NOT NULL")  # metrics only (not fail)
def silver_customers_valid():
    return (
        dlt.read_stream("silver_customers_enriched")
        .filter(F.col("is_quarantined") == False)
        .drop(
            "dob_date", "dq_reasons", "is_quarantined", "dq_checked_at"
        )  # keep schema clean
    )


# -----------------------------------------
# GOLD: business-ready curated table
# -----------------------------------------
@dlt.table(
    name="gold_customer_curated",
    comment="GOLD: curated customers suitable for downstream consumers",
)
def gold_customer_curated():
    df = dlt.read_stream("silver_customers_valid")

    return df.select(
        "party_key",
        "name",
        "country",
        "dob",
        "source_updated_at",
        "ingested_at",
    )


# ============================================================
# Retry vs Stop Semantics (read this, no code needed)
# ============================================================
#
# 1) STOP semantics (fail the pipeline/table update)
#    - Use: @dlt.expect_or_fail("rule_name", "<predicate>")
#    - Behavior: If ANY rows violate, the table update fails.
#      Pipelines often retry (depending on DLT run mode/config), but will keep failing
#      as long as the bad data keeps coming. This is true fail-fast.
#
#    Use this for: invariants that must never be violated (e.g., critical metadata missing).
#
# 2) RETRY semantics (continue processing, keep data but separate it)
#    - Best practice: quarantine pattern (what we have implemented here above)
#    - Behavior: pipeline continues, good data flows, bad data is captured in
#      a quarantine table with reasons for audit/triage.
#
# 3) CONTINUE but DROP bad rows (no quarantine)
#    - Use: @dlt.expect_or_drop("rule_name", "<predicate>")
#    - Behavior: violating rows are dropped automatically, pipeline continues.
#    - Downside: we lose bad records unless we explicitly capture them elsewhere.
#
# Implementation details:
# - Enforced hard data contracts using expect_or_fail for non-negotiable quality rules
# - Captured expectation metrics to demonstrate observability and operational readiness
# - Intentionally injected bad streaming data to prove deterministic fail-fast behavior in the pipeline
# ============================================================
