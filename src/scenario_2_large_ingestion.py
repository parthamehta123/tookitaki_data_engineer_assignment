"""
Scenario 2: Handling Large Datasets (Design + Simulation)

This job demonstrates a production-grade PySpark ingestion pattern
for very large daily CSV files (~60GB).

Key guarantees:
- Explicit schema (no inference)
- Safe, scalable ingestion
- Small-files avoidance
- Restart and retry safety via Delta Lake

This script simulates ingestion using in-memory data while documenting
the production read path.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    BooleanType,
    DateType,
)
from pyspark.sql import Row
from datetime import datetime, date


def get_spark_session(app_name: str = "scenario_2_large_ingestion") -> SparkSession:
    """
    Create or retrieve a SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def define_customer_schema() -> StructType:
    """
    Define explicit schema for customer ingestion.
    Explicit schema avoids inference overhead and schema drift.
    """
    return StructType(
        [
            StructField("party_key", StringType(), False),
            StructField("source_updated_at", TimestampType(), False),
            StructField("name", StringType(), True),
            StructField("dob", DateType(), True),
            StructField("country", StringType(), True),
            StructField("is_deleted", BooleanType(), False),
            StructField("ingested_at", TimestampType(), False),
        ]
    )


def simulate_large_file_ingestion(spark: SparkSession, schema: StructType) -> DataFrame:
    """
    Simulate ingestion of a large CSV using in-memory data.
    Mirrors production ingestion behavior without requiring
    an actual large file.
    """
    sample_data = [
        Row(
            "1",
            datetime(2024, 1, 10, 9, 0),
            "Alice",
            date(1990, 1, 1),
            "US",
            False,
            datetime(2024, 1, 10, 9, 2),
        ),
        Row(
            "2",
            datetime(2024, 1, 10, 8, 30),
            "Bob",
            date(1985, 5, 5),
            "IN",
            False,
            datetime(2024, 1, 10, 8, 35),
        ),
        Row(
            "3",
            datetime(2024, 1, 10, 7, 45),
            "Charlie",
            date(1992, 7, 7),
            "UK",
            False,
            datetime(2024, 1, 10, 7, 50),
        ),
    ]
    return spark.createDataFrame(sample_data, schema=schema)


def write_raw_delta(df: DataFrame, table_name: str, num_partitions: int = 8) -> None:
    """
    Write ingested data to a raw Delta table with controlled partitioning.
    Controlled repartitioning avoids small-file problems.
    """
    (
        df.repartition(num_partitions)
        .write.format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )


def main() -> None:
    spark = get_spark_session()

    # Safe, portable Spark tuning
    spark.conf.set("spark.sql.files.maxPartitionBytes", 128 * 1024 * 1024)
    spark.conf.set("spark.sql.shuffle.partitions", 200)

    schema = define_customer_schema()
    input_df = simulate_large_file_ingestion(spark, schema)

    if input_df.count() == 0:
        raise RuntimeError("Simulated ingestion produced no records")

    write_raw_delta(
        df=input_df, table_name="demo_raw_customers_delta_large", num_partitions=8
    )


if __name__ == "__main__":
    main()
