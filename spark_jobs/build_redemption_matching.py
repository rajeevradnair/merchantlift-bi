"""Build matched offer redemption candidates from Silver tables."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import SILVER_DIR
from merchantlift.spark import create_spark_session_local


MATCH_RULE_VERSION = "redemption_match_rules_v1"
MATCHED_REDEMPTION_TABLE = "fact_matched_offer_redemptions_clean"
TRANSACTIONS_TABLE = "fact_transactions_clean"
ACTIVATIONS_TABLE = "fact_offer_activations_clean"
OFFERS_TABLE = "dim_offer_clean"

ELIGIBLE_TRANSACTION_STATUSES = (
    "approved",
    "posted",
    "settled",
)

def build_pipeline_run_id() -> str:
    """Create a unique matching pipeline run identifier.

    Returns:
        Pipeline run identifier.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"redemption_match_run_{timestamp}"


def get_silver_table_path(table_name: str):
    """Return a Silver Delta table path.

    Args:
        table_name: Silver table name.

    Returns:
        Silver table path.
    """
    return SILVER_DIR / table_name


def read_silver_table(
    spark,
    table_name: str,
) -> DataFrame:
    """Read one Silver Delta table.

    Args:
        spark: Active Spark session.
        table_name: Silver table name.

    Returns:
        Silver Spark DataFrame.
    """
    table_path = get_silver_table_path(table_name)

    return (
        spark.read
        .format("delta")
        .load(str(table_path))
    )


def inspect_table(
    table_name: str,
    df: DataFrame,
    sample_size: int = 5,
) -> int:
    """Print schema, row count, and sample rows for a DataFrame.

    Args:
        table_name: Human-readable table name.
        df: DataFrame to inspect.
        sample_size: Number of sample rows to show.

    Returns:
        Row count.
    """
    print("\n" + "=" * 80)
    print(f"Inspecting table: {table_name}")
    print("=" * 80)

    print("\nSchema:")
    df.printSchema()

    row_count = df.count()

    print("\nRow count:")
    print(f"{table_name}: {row_count:,} rows")

    print("\nSample rows:")
    df.show(sample_size, truncate=False)

    return row_count


def require_columns(
    df: DataFrame,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    """Validate that a DataFrame contains required columns.

    Args:
        df: DataFrame to validate.
        table_name: Human-readable table name.
        required_columns: Required column names.

    Raises:
        ValueError: If required columns are missing.
    """
    missing_columns = sorted(set(required_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing required columns: "
            f"{', '.join(missing_columns)}"
        )


def build_match_ready_transactions(
    transactions_df: DataFrame,
) -> DataFrame:
    """Prepare Silver transactions for redemption matching.

    Args:
        transactions_df: Silver transactions DataFrame.

    Returns:
        Filtered transaction DataFrame with matching columns.
    """
    required_columns = (
        "transaction_id",
        "tokenized_cardmember_id",
        "merchant_id",
        "transaction_timestamp",
        "transaction_date",
        "transaction_amount",
    )

    require_columns(
        df=transactions_df,
        table_name=TRANSACTIONS_TABLE,
        required_columns=required_columns,
    )

    filtered_df = (
        transactions_df
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("tokenized_cardmember_id").isNotNull())
        .filter(F.col("merchant_id").isNotNull())
        .filter(F.col("transaction_timestamp").isNotNull())
        .filter(F.col("transaction_date").isNotNull())
        .filter(F.col("transaction_amount").isNotNull())
        .filter(F.col("transaction_amount") >= 0)
    )

    if "transaction_status" in filtered_df.columns:
        filtered_df = filtered_df.filter(
            F.lower(F.col("transaction_status")).isin(
                *ELIGIBLE_TRANSACTION_STATUSES
            )
        )

    selected_columns = [
        "transaction_id",
        "tokenized_cardmember_id",
        "merchant_id",
        "transaction_timestamp",
        "transaction_date",
        "transaction_amount",
    ]

    if "transaction_status" in filtered_df.columns:
        selected_columns.append("transaction_status")

    return filtered_df.select(*selected_columns)


def build_match_ready_activations(
    activations_df: DataFrame,
) -> DataFrame:
    """Prepare Silver activations for redemption matching.

    Args:
        activations_df: Silver activations DataFrame.

    Returns:
        Filtered activation DataFrame with matching columns.
    """
    required_columns = (
        "activation_id",
        "tokenized_cardmember_id",
        "offer_id",
        "activation_timestamp",
        "offer_expiry_timestamp",
    )

    require_columns(
        df=activations_df,
        table_name=ACTIVATIONS_TABLE,
        required_columns=required_columns,
    )

    filtered_df = (
        activations_df
        .filter(F.col("activation_id").isNotNull())
        .filter(F.col("tokenized_cardmember_id").isNotNull())
        .filter(F.col("offer_id").isNotNull())
        .filter(F.col("activation_timestamp").isNotNull())
        .filter(F.col("offer_expiry_timestamp").isNotNull())
        .filter(F.col("offer_expiry_timestamp") >= F.col("activation_timestamp"))
    )

    if "activation_status" in filtered_df.columns:
        filtered_df = filtered_df.filter(
            F.lower(F.col("activation_status")).isin(
                "active",
                "activated",
                "eligible",
            )
        )

    selected_columns = [
        "activation_id",
        "tokenized_cardmember_id",
        "offer_id",
        "activation_timestamp",
        "offer_expiry_timestamp",
    ]

    if "activation_status" in filtered_df.columns:
        selected_columns.append("activation_status")

    return filtered_df.select(*selected_columns)


def write_silver_table(
    df: DataFrame,
    table_name: str,
    partition_column: str | None = None,
) -> None:
    """Write one Silver Delta table.

    Args:
        df: DataFrame to write.
        table_name: Silver output table name.
        partition_column: Optional partition column.
    """
    output_path = get_silver_table_path(table_name)

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_column is not None and partition_column in df.columns:
        writer = writer.partitionBy(partition_column)

    writer.save(str(output_path))

    print(f"Wrote Silver Delta table: {output_path}")


def main(spark_session=None) -> None:
    """Run redemption matching job."""

    if spark_session is None:
        spark = create_spark_session_local("merchantlift-read-bronze-table")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    pipeline_run_id = build_pipeline_run_id()

    print("Starting redemption matching")
    print("=" * 80)
    print(f"Silver directory: {SILVER_DIR}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Match rule version: {MATCH_RULE_VERSION}")
    print(f"Output table: {MATCHED_REDEMPTION_TABLE}")
    print("=" * 80)

    transactions_df = read_silver_table(
        spark=spark,
        table_name=TRANSACTIONS_TABLE,
    )

    activations_df = read_silver_table(
        spark=spark,
        table_name=ACTIVATIONS_TABLE,
    )

    offers_df = read_silver_table(
        spark=spark,
        table_name=OFFERS_TABLE,
    )

    inspect_table(
        table_name=TRANSACTIONS_TABLE,
        df=transactions_df,
    )

    inspect_table(
        table_name=ACTIVATIONS_TABLE,
        df=activations_df,
    )

    inspect_table(
        table_name=OFFERS_TABLE,
        df=offers_df,
    )

    match_ready_transactions_df = build_match_ready_transactions(
        transactions_df=transactions_df,
    )

    match_ready_activations_df = build_match_ready_activations(
        activations_df=activations_df,
    )

    print("\nMatch-ready input counts")
    print("=" * 80)

    original_transaction_count = transactions_df.count()
    filtered_transaction_count = match_ready_transactions_df.count()

    original_activation_count = activations_df.count()
    filtered_activation_count = match_ready_activations_df.count()

    print(
        f"{'transactions original':<40} "
        f"{original_transaction_count:>12,}"
    )
    print(
        f"{'transactions match-ready':<40} "
        f"{filtered_transaction_count:>12,}"
    )
    print(
        f"{'transactions removed':<40} "
        f"{original_transaction_count - filtered_transaction_count:>12,}"
    )

    print(
        f"{'activations original':<40} "
        f"{original_activation_count:>12,}"
    )
    print(
        f"{'activations match-ready':<40} "
        f"{filtered_activation_count:>12,}"
    )
    print(
        f"{'activations removed':<40} "
        f"{original_activation_count - filtered_activation_count:>12,}"
    )

    print("\nMatch-ready transaction sample")
    match_ready_transactions_df.show(10, truncate=False)

    print("\nMatch-ready activation sample")
    match_ready_activations_df.show(10, truncate=False)

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()