"""
Scenario 3: Data Validation Before Ingestion

This module defines a production-style data validation layer
for customer delta ingestion pipelines.

Validation philosophy:
- HARD validations: Fail fast and block ingestion
- SOFT validations: Emit warnings and metrics, but allow ingestion

This pattern is commonly used in large-scale data platforms
to balance data quality with pipeline availability.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import List


# -------------------------------------------------------------------
# Hard Validations
# -------------------------------------------------------------------
def run_hard_validations(df: DataFrame) -> None:
    """
    Execute hard (blocking) validations.

    Any failure raises an exception and prevents ingestion.
    These checks protect data correctness and downstream integrity.
    """

    # party_key must always be present
    if df.filter(F.col("party_key").isNull()).count() > 0:
        raise ValueError("Hard validation failed: party_key is NULL")

    # source_updated_at is required for deterministic ordering
    if df.filter(F.col("source_updated_at").isNull()).count() > 0:
        raise ValueError("Hard validation failed: source_updated_at is NULL")

    # ingested_at must never be earlier than source_updated_at
    if df.filter(F.col("ingested_at") < F.col("source_updated_at")).count() > 0:
        raise ValueError("Hard validation failed: ingested_at < source_updated_at")


# -------------------------------------------------------------------
# Soft Validations
# -------------------------------------------------------------------
def run_soft_validations(df: DataFrame) -> None:
    """
    Execute soft (non-blocking) validations.

    These checks surface data quality issues but do NOT
    prevent ingestion. In production, these would typically
    emit metrics to monitoring systems.
    """

    total_rows = df.count()
    if total_rows == 0:
        print("[WARN] Soft validation skipped: DataFrame is empty")
        return

    # ------------------------------------------------------------
    # Null name percentage
    # ------------------------------------------------------------
    null_name_count = df.filter(F.col("name").isNull()).count()
    null_name_pct = null_name_count / total_rows

    if null_name_pct > 0.10:
        print(
            f"[WARN] High null name percentage: "
            f"{null_name_pct:.2%} ({null_name_count}/{total_rows})"
        )

    # ------------------------------------------------------------
    # Invalid country codes
    # ------------------------------------------------------------
    allowed_countries: List[str] = ["US", "IN", "UK", "CA"]

    invalid_country_count = df.filter(~F.col("country").isin(allowed_countries)).count()

    if invalid_country_count > 0:
        print(
            f"[WARN] Found {invalid_country_count} records "
            f"with invalid country codes"
        )


# -------------------------------------------------------------------
# Public Validation Entry Point
# -------------------------------------------------------------------
def validate(df: DataFrame) -> DataFrame:
    """
    Run all validations in the correct order.

    Returns:
        DataFrame: The original DataFrame if validation passes

    Raises:
        ValueError: If any hard validation fails
    """

    run_hard_validations(df)
    run_soft_validations(df)
    return df


# -------------------------------------------------------------------
# Example Usage (Local / Databricks Test)
# -------------------------------------------------------------------
if __name__ == "__main__":
    """
    Example execution for local testing or Databricks notebooks.
    Demonstrates:
    - Hard validation failure
    - Soft validation warnings
    """

    spark = SparkSession.builder.appName("scenario_3_data_validation").getOrCreate()

    # Sample test data
    data = [
        # Valid record
        ("1", "2024-01-10 10:00:00", "Alice", "US", "2024-01-10 10:05:00"),
        # Hard validation failure (source_updated_at is NULL)
        ("2", None, "Bob", "IN", "2024-01-10 09:55:00"),
        # Soft validation warnings (null name + invalid country)
        ("3", "2024-01-10 08:00:00", None, "XX", "2024-01-10 08:05:00"),
    ]

    columns = [
        "party_key",
        "source_updated_at",
        "name",
        "country",
        "ingested_at",
    ]

    df = spark.createDataFrame(data, columns)

    try:
        validated_df = validate(df)
        print("Data validation passed")
        validated_df.show(truncate=False)

    except ValueError as exc:
        print(f"Data validation failed: {exc}")

    spark.stop()
