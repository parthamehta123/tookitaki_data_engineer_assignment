"""
Unit-style tests for Scenario 3: Data Validation

These tests validate hard and soft validation behavior using
a local Spark session.

Designed to be:
- Fast
- Deterministic
- CI-friendly
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

from src.scenario_3_data_validation import validate


# -------------------------------------------------------------------
# Explicit Test Schema (MATCHES PRODUCTION)
# -------------------------------------------------------------------
TEST_SCHEMA = StructType(
    [
        StructField("party_key", StringType(), False),
        StructField("source_updated_at", TimestampType(), True),
        StructField("name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)


# -------------------------------------------------------------------
# Spark Test Fixture
# -------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("scenario_3_validation_tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# -------------------------------------------------------------------
# Test: Hard Validation Failure (NULL source_updated_at)
# -------------------------------------------------------------------
def test_hard_validation_fails_on_null_source_timestamp(spark):
    data = [
        ("1", None, "Alice", "US", datetime(2024, 1, 10, 10, 5)),
    ]

    df = spark.createDataFrame(data, schema=TEST_SCHEMA)

    with pytest.raises(ValueError, match="source_updated_at"):
        validate(df)


# -------------------------------------------------------------------
# Test: Hard Validation Failure (ingested_at < source_updated_at)
# -------------------------------------------------------------------
def test_hard_validation_fails_on_invalid_timestamps(spark):
    data = [
        (
            "1",
            datetime(2024, 1, 10, 10, 0),
            "Alice",
            "US",
            datetime(2024, 1, 10, 9, 55),
        ),
    ]

    df = spark.createDataFrame(data, schema=TEST_SCHEMA)

    with pytest.raises(ValueError, match="ingested_at < source_updated_at"):
        validate(df)


# -------------------------------------------------------------------
# Test: Soft Validation Only (Invalid Country Code)
# -------------------------------------------------------------------
def test_soft_validation_allows_invalid_country(spark, capsys):
    data = [
        (
            "1",
            datetime(2024, 1, 10, 10, 0),
            "Alice",
            "XX",
            datetime(2024, 1, 10, 10, 5),
        ),
    ]

    df = spark.createDataFrame(data, schema=TEST_SCHEMA)

    validated_df = validate(df)

    assert validated_df.count() == 1

    captured = capsys.readouterr()
    assert "invalid country" in captured.out.lower()


# -------------------------------------------------------------------
# Test: Fully Valid Dataset
# -------------------------------------------------------------------
def test_validation_passes_for_clean_data(spark):
    data = [
        (
            "1",
            datetime(2024, 1, 10, 10, 0),
            "Alice",
            "US",
            datetime(2024, 1, 10, 10, 5),
        ),
        (
            "2",
            datetime(2024, 1, 10, 9, 0),
            "Bob",
            "IN",
            datetime(2024, 1, 10, 9, 3),
        ),
    ]

    df = spark.createDataFrame(data, schema=TEST_SCHEMA)

    validated_df = validate(df)

    assert validated_df.count() == 2


# -------------------------------------------------------------------
# Test: Empty DataFrame (Defensive Case)
# -------------------------------------------------------------------
def test_validation_handles_empty_dataframe(spark):
    empty_df = spark.createDataFrame([], schema=TEST_SCHEMA)

    validated_df = validate(empty_df)

    assert validated_df.count() == 0
