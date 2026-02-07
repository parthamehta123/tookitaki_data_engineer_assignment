"""
Client Ingestion Spark Job
==========================

This Spark job performs an idempotent ingestion using Delta Lake MERGE semantics.

Key guarantees:
- Safe re-runs (idempotent)
- Partition-aware ingestion
- Airflow-compatible arguments
- Local or cluster Spark compatible

Used by:
- Scenario 5 (Airflow orchestration)
- Scenario 1 (idempotent ingestion)
"""

import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable


# ---------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Client ingestion Spark job")

    parser.add_argument(
        "--run-date", required=True, help="Airflow execution date (YYYY-MM-DD)"
    )
    parser.add_argument("--run-id", required=True, help="Airflow run_id")
    parser.add_argument("--environment", required=True, help="Execution environment")

    return parser.parse_args()


# ---------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------
def create_spark_session():
    return (
        SparkSession.builder.appName("client_ingestion_job")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


# ---------------------------------------------------------
# MAIN INGESTION LOGIC
# ---------------------------------------------------------
def main():
    args = parse_args()

    run_date = args.run_date
    run_id = args.run_id
    environment = args.environment

    print(f"Starting ingestion | run_date={run_date} run_id={run_id} env={environment}")

    spark = create_spark_session()

    # -----------------------------------------------------
    # INPUT / OUTPUT PATHS (local-friendly)
    # -----------------------------------------------------
    source_path = f"/opt/airflow/data/raw/client_data/date={run_date}"
    target_path = "/opt/airflow/data/delta/client_table"

    # -----------------------------------------------------
    # READ SOURCE DATA
    # -----------------------------------------------------
    try:
        source_df = spark.read.parquet(source_path)
    except Exception as e:
        print(f"No source data found for {run_date}: {e}")
        sys.exit(0)

    # -----------------------------------------------------
    # ENRICH METADATA
    # -----------------------------------------------------
    enriched_df = (
        source_df.withColumn("ingestion_date", lit(run_date))
        .withColumn("ingestion_ts", lit(datetime.utcnow().isoformat()))
        .withColumn("run_id", lit(run_id))
    )

    # -----------------------------------------------------
    # CREATE TABLE IF NOT EXISTS
    # -----------------------------------------------------
    if not DeltaTable.isDeltaTable(spark, target_path):
        print("Target table does not exist. Creating new Delta table.")
        (
            enriched_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("ingestion_date")
            .save(target_path)
        )
        print("Initial load completed.")
        spark.stop()
        return

    # -----------------------------------------------------
    # IDEMPOTENT MERGE (UPSERT)
    # -----------------------------------------------------
    delta_table = DeltaTable.forPath(spark, target_path)

    (
        delta_table.alias("t")
        .merge(
            enriched_df.alias("s"),
            """
            t.client_id = s.client_id
            AND t.ingestion_date = s.ingestion_date
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Merge completed successfully.")

    # -----------------------------------------------------
    # BASIC RECONCILIATION LOG
    # -----------------------------------------------------
    src_count = enriched_df.count()
    tgt_count = spark.read.format("delta").load(target_path).count()

    print(
        f"Reconciliation | source_count={src_count} " f"target_total_count={tgt_count}"
    )

    spark.stop()
    print("Spark ingestion job finished successfully.")


# ---------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------
if __name__ == "__main__":
    main()
