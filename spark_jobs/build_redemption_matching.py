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

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()