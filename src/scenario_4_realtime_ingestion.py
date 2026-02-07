from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import great_expectations as ge

from src.scenario_4_customer_validation_suite import apply_customer_expectations


def validate_with_gx(batch_df: DataFrame, batch_id: int):
    """
    Executed for each micro-batch via foreachBatch.

    Flow:
    1. Receive micro-batch
    2. Convert to Pandas (small batches only)
    3. Apply Great Expectations
    4. Fail fast on validation error
    5. Write validated data exactly once
    """

    # NOTE:
    # Spark will not call foreachBatch for truly empty batches,
    # so an explicit count() check is unnecessary and expensive.

    # Convert Spark -> Pandas (GX requirement)
    pdf = batch_df.toPandas()

    ge_df = ge.from_pandas(pdf)
    ge_df = apply_customer_expectations(ge_df)

    results = ge_df.validate()

    if not results["success"]:
        failed = [
            r["expectation_config"]["expectation_type"]
            for r in results["results"]
            if not r["success"]
        ]
        raise RuntimeError(
            f"GX validation failed in batch {batch_id}. "
            f"Failed expectations: {failed}"
        )

    # # For local testing, we could write to Parquet instead.
    # # Uncomment the following block and comment out the below for local runs.
    # (
    #     batch_df.write.format("parquet")
    #     .mode("append")
    #     .save(f"/tmp/demo_curated_customers_validated/batch_{batch_id}")
    # )

    # SINGLE SINK ONLY (exactly-once semantics)
    # In production, this would be a Delta Lake table with proper schema and partitioning.
    (
        batch_df.write.format("delta")
        .mode("append")
        .saveAsTable("demo_curated_customers_validated")
    )


def start_streaming_pipeline(environment: str):
    """
    Entry point for Databricks notebooks / Airflow triggers.

    Responsibilities:
    - Start streaming query
    - Return query handle
    - Do NOT block execution
    """

    spark = SparkSession.builder.appName("scenario_4_realtime_ingestion").getOrCreate()

    # Simulated streaming source (replace with Kafka/Kinesis in prod)
    input_stream = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .select(
            F.col("value").cast("string").alias("party_key"),
            F.current_timestamp().alias("source_updated_at"),
            F.lit("Alice").alias("name"),
            F.lit("US").alias("country"),
            F.current_timestamp().alias("ingested_at"),
        )
    )

    query = (
        input_stream.writeStream.foreachBatch(validate_with_gx)
        .option("checkpointLocation", f"/tmp/gx_checkpoint/{environment}")
        # Deterministic demo runs
        .trigger(availableNow=True)
        .start()
    )

    return query
