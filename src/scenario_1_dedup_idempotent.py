"""
Scenario 1: De-duplication & Idempotent Ingestion (Delta Lake Ready)

This job derives the latest customer record per `party_key` from a daily
customer delta dataset using deterministic ordering.

Ordering logic:
- `source_updated_at` (primary business timestamp)
- `ingested_at` (tie-breaker for identical business timestamps)

Key guarantees:
- Idempotent re-runs (safe for retries, backfills, and restarts)
- Correct handling of duplicate records
- Soft deletes are handled as state transitions rather than physical deletes. The deduplication logic selects the
  latest record per business key, so if the most recent event represents a deletion (`is_deleted = true`), that state
  is preserved downstream. The merge logic remains idempotent and retry-safe.

The job is designed to run in:
- Databricks Jobs
- spark-submit environments with Delta Lake support
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import argparse


def get_spark_session(app_name: str = "scenario_1_deduplication") -> SparkSession:
    """
    Create or retrieve a SparkSession.

    In Databricks, a SparkSession is usually pre-created, but calling this
    method is safe and ensures compatibility with spark-submit execution.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Active Spark session
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def deduplicate_latest_per_party(df: DataFrame) -> DataFrame:
    """
    Deduplicate customer delta records and return the latest record
    per `party_key`.

    The function uses a deterministic window specification to rank records:
    1. `source_updated_at` (most recent business update wins)
    2. `ingested_at` (tie-breaker for identical source timestamps)

    This guarantees:
    - Exactly one output row per `party_key`
    - Repeatable results across re-runs (idempotency)
    - Correct handling of duplicate and late-arriving records

    Args:
        df (DataFrame): Input DataFrame containing customer delta records

    Returns:
        DataFrame: Deduplicated DataFrame with the latest record per `party_key`
    """

    window_spec = Window.partitionBy("party_key").orderBy(
        F.col("source_updated_at").desc(), F.col("ingested_at").desc()
    )

    latest_df = (
        df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    return latest_df


def write_idempotent_delta(
    spark: SparkSession, latest_df: DataFrame, target_table: str
) -> None:
    """
    Write the curated customer dataset to a Delta Lake table using an
    idempotent MERGE pattern.

    Behavior:
    - If the target table does not exist, it is created via an initial overwrite
    - If the table exists, a Delta Lake MERGE is performed on `party_key`

    Why MERGE:
    - Ensures idempotent behavior across retries
    - Allows late-arriving updates to overwrite older records
    - Preserves soft deletes as state transitions
    - Prevents duplicate records

    Args:
        spark (SparkSession): Active Spark session
        latest_df (DataFrame): Deduplicated DataFrame with latest customer records
        target_table (str): Fully qualified Delta table name for curated output
    """

    if not spark.catalog.tableExists(target_table):
        # Initial load: create the curated Delta table
        (latest_df.write.format("delta").mode("overwrite").saveAsTable(target_table))
    else:
        # Incremental / retry-safe load using MERGE INTO
        delta_target = DeltaTable.forName(spark, target_table)

        (
            delta_target.alias("t")
            .merge(latest_df.alias("s"), "t.party_key = s.party_key")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def main(input_table: str, output_table: str) -> None:
    """
    Main job entrypoint.

    Workflow:
    1. Read raw customer delta data from a managed Delta table
    2. Deduplicate records to derive the latest state per `party_key`
    3. Perform a safety check to avoid accidental empty writes
    4. Write results to the curated table using an idempotent Delta MERGE

    Args:
        input_table (str): Source Delta table containing raw customer deltas
        output_table (str): Target Delta table for curated latest records
    """

    spark = get_spark_session()

    # Read raw customer delta data
    input_df = spark.table(input_table)

    # Deduplicate and select the latest record per party_key
    latest_df = deduplicate_latest_per_party(input_df)

    # Fail fast if no data is produced to avoid accidental data loss
    if latest_df.count() == 0:
        raise RuntimeError(
            "Deduplicated DataFrame is empty. Aborting write to prevent data loss."
        )

    # Write curated data using an idempotent Delta Lake MERGE
    write_idempotent_delta(spark=spark, latest_df=latest_df, target_table=output_table)


if __name__ == "__main__":
    """
    Entry point for command-line execution.

    Example:
        spark-submit scenario_1_dedup_idempotent.py \
            --input_table demo_raw_customers_delta \
            --output_table demo_curated_customers_latest
    """

    parser = argparse.ArgumentParser(
        description="Scenario 1: De-duplication & Idempotent Ingestion"
    )

    parser.add_argument(
        "--input_table",
        required=True,
        help="Source Delta table containing raw customer delta data",
    )

    parser.add_argument(
        "--output_table",
        required=True,
        help="Target Delta table for curated latest customer records",
    )

    args = parser.parse_args()

    main(input_table=args.input_table, output_table=args.output_table)
