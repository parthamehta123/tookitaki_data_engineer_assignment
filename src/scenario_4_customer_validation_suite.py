from datetime import date


def apply_customer_expectations(ge_df):
    """
    EXPECTATION SUITE DEFINITION
    ----------------------------
    This function defines the Great Expectations validation rules
    for customer ingestion data.

    The expectations are applied directly to an in-memory
    Great Expectations dataframe (Pandas-backed), which is the
    recommended and streaming-safe approach for GX.

    Expectations enforced:
    - party_key must be non-null and unique
    - country must be within an allowed set
    - source_updated_at must be present (valid timestamp enforced by Spark schema)
    - dob, if present, must be before today
    - name null percentage must be below a defined threshold
    """

    # party_key must be present
    ge_df.expect_column_values_to_not_be_null(column="party_key")

    # party_key must be unique within the micro-batch
    # (global uniqueness should be enforced downstream via MERGE / constraints)
    ge_df.expect_column_values_to_be_unique(column="party_key")

    # country must be within allowed set
    ge_df.expect_column_values_to_be_in_set(
        column="country",
        value_set=["US", "IN", "UK", "CA"],
    )

    # source_updated_at must exist
    # Timestamp type validity is enforced upstream by Spark schema
    # (TimestampType casting rejects non-timestamp values before GX runs)
    ge_df.expect_column_values_to_not_be_null(column="source_updated_at")

    # dob must be before today IF present
    if "dob" in ge_df.columns:
        ge_df.expect_column_values_to_be_between(
            column="dob",
            min_value="1900-01-01",
            max_value=date.today().isoformat(),
            allow_cross_type_comparisons=True,
            mostly=1.0,
        )

    # name null percentage must be below threshold (90% non-null)
    ge_df.expect_column_values_to_not_be_null(
        column="name",
        mostly=0.90,
    )

    return ge_df
