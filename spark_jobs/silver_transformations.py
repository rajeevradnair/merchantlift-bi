"""Transform Bronze MerchantLift BI tables into Silver clean tables."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import BRONZE_DIR, SILVER_DIR
from merchantlift.spark_read_bronze import create_spark_session_local
from merchantlift.silver_config import (
    SILVER_RELATIONSHIPS,
    SILVER_TABLES,
    SilverRelationshipConfig,
    SilverTableConfig,
)

VALIDATION_RULE_VERSION = "silver_rules_v1"
REQUIRED_SILVER_METADATA_COLUMNS = (
    "silver_transformed_at",
    "silver_pipeline_run_id",
    "quality_status",
    "validation_rule_version",
)

def get_silver_table_path(table_name: str):
    """Return the Silver Delta table path.

    Args:
        table_name: Silver table name.

    Returns:
        Silver Delta table path.
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


def write_silver_delta_table(
    df: DataFrame,
    table_name: str,
    partition_column: str | None = None,
) -> None:
    """Write one Silver Delta table.

    Args:
        df: Silver DataFrame.
        table_name: Silver table name.
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


def validate_required_columns_exist(
    df: DataFrame,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    """Validate that required columns exist in a DataFrame.

    Args:
        df: DataFrame to validate.
        table_name: Table being validated.
        required_columns: Required column names.

    Raises:
        ValueError: If required columns are missing.
    """
    actual_columns = set(df.columns)
    missing_columns = sorted(set(required_columns) - actual_columns)

    if missing_columns:
        raise ValueError(
            f"Silver table {table_name} is missing required columns: "
            f"{', '.join(missing_columns)}"
        )

def cast_columns_if_present(
    df: DataFrame,
    column_names: tuple[str, ...],
    target_type: str,
) -> DataFrame:
    """Cast columns when they exist in the DataFrame.

    Args:
        df: Input DataFrame.
        column_names: Column names to cast.
        target_type: Spark SQL target type.

    Returns:
        DataFrame with selected columns cast.
    """
    result_df = df

    for column_name in column_names:
        if column_name in result_df.columns:
            result_df = result_df.withColumn(
                column_name,
                F.col(column_name).cast(target_type),
            )

    return result_df

def clean_bronze_table(
    bronze_df: DataFrame,
    table_config: SilverTableConfig,
    pipeline_run_id: str,
) -> DataFrame:
    """Clean one Bronze table into a Silver-ready DataFrame.

    Args:
        bronze_df: Bronze DataFrame.
        table_config: Silver transformation configuration.
        pipeline_run_id: Silver pipeline run identifier.

    Returns:
        Cleaned Silver DataFrame.
    """
    cleaned_df = bronze_df

    if table_config.primary_key in cleaned_df.columns:
        cleaned_df = (
            cleaned_df
            .filter(F.col(table_config.primary_key).isNotNull())
            .dropDuplicates([table_config.primary_key])
        )

    cleaned_df = cast_columns_if_present(
        df=cleaned_df,
        column_names=table_config.numeric_columns,
        target_type="double",
    )

    for timestamp_column in table_config.timestamp_columns:
        if timestamp_column in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                timestamp_column,
                F.to_timestamp(timestamp_column),
            )

    for date_column in table_config.date_columns:
        if date_column in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                date_column,
                F.to_date(date_column),
            )

    return add_silver_metadata(
        df=cleaned_df,
        pipeline_run_id=pipeline_run_id,
    )


def validate_written_silver_table(
    spark,
    table_name: str,
    expected_row_count: int,
) -> None:
    """Validate that a written Silver table can be read back.

    Args:
        spark: Active Spark session.
        table_name: Silver table name.
        expected_row_count: Expected row count.
    """
    silver_table_path = get_silver_table_path(table_name)

    silver_df = (
        spark.read
        .format("delta")
        .load(str(silver_table_path))
    )

    actual_row_count = silver_df.count()

    print("\nWritten Silver table validation:")
    print("=" * 80)
    print(f"Silver table: {table_name}")
    print(f"Silver path: {silver_table_path}")
    print(f"Expected rows: {expected_row_count:,}")
    print(f"Actual rows:   {actual_row_count:,}")
    print("=" * 80)

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"Silver row count mismatch for {table_name}: "
            f"expected {expected_row_count:,}, got {actual_row_count:,}"
        )

    print("Silver table read-back validation passed.")


def build_pipeline_run_id() -> str:
    """Create a unique Silver pipeline run identifier.

    Returns:
        Pipeline run identifier.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"silver_run_{timestamp}"


def read_bronze_table(spark, table_name: str) -> DataFrame:
    """Read a Bronze Delta table.

    Args:
        spark: Active Spark session.
        table_name: Bronze table name.

    Returns:
        Bronze Spark DataFrame.
    """
    table_path = BRONZE_DIR / table_name

    return (
        spark.read
        .format("delta")
        .load(str(table_path))
    )


def add_silver_metadata(
    df: DataFrame,
    pipeline_run_id: str,
) -> DataFrame:
    """Add Silver transformation metadata.

    Args:
        df: Cleaned Spark DataFrame.
        pipeline_run_id: Silver pipeline run identifier.

    Returns:
        DataFrame with Silver metadata columns.
    """
    return (
        df
        .withColumn("silver_transformed_at", F.current_timestamp())
        .withColumn("silver_pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("quality_status", F.lit("passed"))
        .withColumn("validation_rule_version", F.lit(VALIDATION_RULE_VERSION))
    )


def clean_fact_transactions(
    bronze_df: DataFrame,
    pipeline_run_id: str,
) -> DataFrame:
    """Clean Bronze transaction rows into trusted Silver transaction rows.

    Args:
        bronze_df: Bronze fact_transactions DataFrame.
        pipeline_run_id: Silver pipeline run identifier.

    Returns:
        Cleaned transaction DataFrame.
    """
    cleaned_df = (
        bronze_df
        .dropDuplicates(["transaction_id"])
        .withColumn(
            "transaction_amount",
            F.col("transaction_amount").cast("double"),
        )
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp("transaction_timestamp"),
        )
        .withColumn(
            "transaction_date",
            F.to_date("transaction_date"),
        )
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("tokenized_cardmember_id").isNotNull())
        .filter(F.col("merchant_id").isNotNull())
        .filter(F.col("transaction_amount").isNotNull())
        .filter(F.col("transaction_amount") >= 0)
        .filter(F.col("transaction_timestamp").isNotNull())
        .filter(F.col("transaction_date").isNotNull())
    )

    return add_silver_metadata(
        df=cleaned_df,
        pipeline_run_id=pipeline_run_id,
    )


def transform_one_silver_table(
    spark,
    table_config: SilverTableConfig,
    pipeline_run_id: str,
) -> tuple[str, int, int]:
    """Transform one Bronze table into one Silver table.

    Args:
        spark: Active Spark session.
        table_config: Silver transformation configuration.
        pipeline_run_id: Silver pipeline run identifier.

    Returns:
        Tuple of Silver table name, Bronze row count, and Silver row count.
    """
    print("\n" + "=" * 80)
    print(f"Transforming Bronze table: {table_config.bronze_table_name}")
    print(f"Writing Silver table: {table_config.silver_table_name}")
    print(f"Primary key: {table_config.primary_key}")
    print("=" * 80)

    bronze_df = read_bronze_table(
        spark=spark,
        table_name=table_config.bronze_table_name,
    )

    bronze_count = bronze_df.count()

    silver_df = clean_bronze_table(
        bronze_df=bronze_df,
        table_config=table_config,
        pipeline_run_id=pipeline_run_id,
    )

    silver_count = silver_df.count()

    partition_column = None

    if table_config.date_columns:
        first_date_column = table_config.date_columns[0]

        if first_date_column in silver_df.columns:
            partition_column = first_date_column

    write_silver_delta_table(
        df=silver_df,
        table_name=table_config.silver_table_name,
        partition_column=partition_column,
    )

    validate_silver_table(
        spark=spark,
        table_config=table_config,
        bronze_row_count=bronze_count,
        silver_row_count=silver_count,
    )

    print(f"Silver validation passed: {table_config.silver_table_name}")

    print(f"Completed Silver table: {table_config.silver_table_name}")
    print(f"Bronze rows: {bronze_count:,}")
    print(f"Silver rows: {silver_count:,}")
    print(f"Rows removed: {bronze_count - silver_count:,}")

    return table_config.silver_table_name, bronze_count, silver_count


def validate_silver_table(
    spark,
    table_config: SilverTableConfig,
    bronze_row_count: int,
    silver_row_count: int,
) -> None:
    """Validate one written Silver table.

    Args:
        spark: Active Spark session.
        table_config: Silver table configuration.
        bronze_row_count: Number of rows in the Bronze table.
        silver_row_count: Number of rows in the Silver table.

    Raises:
        ValueError: If validation fails.
    """
    silver_table_path = get_silver_table_path(table_config.silver_table_name)

    silver_df = (
        spark.read
        .format("delta")
        .load(str(silver_table_path))
    )

    read_back_count = silver_df.count()

    if read_back_count != silver_row_count:
        raise ValueError(
            f"Silver table {table_config.silver_table_name} read-back "
            f"count mismatch: expected {silver_row_count:,}, "
            f"got {read_back_count:,}"
        )

    if silver_row_count > bronze_row_count:
        raise ValueError(
            f"Silver table {table_config.silver_table_name} has more rows "
            f"than Bronze input: Bronze={bronze_row_count:,}, "
            f"Silver={silver_row_count:,}"
        )

    required_columns = (
        (table_config.primary_key,)
        + table_config.timestamp_columns
        + table_config.date_columns
        + table_config.numeric_columns
        + REQUIRED_SILVER_METADATA_COLUMNS
    )

    validate_required_columns_exist(
        df=silver_df,
        table_name=table_config.silver_table_name,
        required_columns=required_columns,
    )


def validate_silver_relationships(
    spark,
    relationship_config: SilverRelationshipConfig,
) -> tuple[str, int]:
    """Validate one parent-child relationship between Silver tables.

    Args:
        spark: Active Spark session.
        relationship_config: Relationship validation config.

    Returns:
        Tuple of relationship name and orphan key count.

    Raises:
        ValueError: If required columns are missing.
    """
    child_df = read_silver_table(
        spark=spark,
        table_name=relationship_config.child_table_name,
    )

    parent_df = read_silver_table(
        spark=spark,
        table_name=relationship_config.parent_table_name,
    )

    if relationship_config.child_column not in child_df.columns:
        raise ValueError(
            f"Child column {relationship_config.child_column} missing from "
            f"{relationship_config.child_table_name}"
        )

    if relationship_config.parent_column not in parent_df.columns:
        raise ValueError(
            f"Parent column {relationship_config.parent_column} missing from "
            f"{relationship_config.parent_table_name}"
        )

    child_keys = (
        child_df
        .select(relationship_config.child_column)
        .where(F.col(relationship_config.child_column).isNotNull())
        .dropDuplicates()
    )

    parent_keys = (
        parent_df
        .select(relationship_config.parent_column)
        .where(F.col(relationship_config.parent_column).isNotNull())
        .dropDuplicates()
    )

    orphan_keys = child_keys.join(
        parent_keys,
        child_keys[relationship_config.child_column]
        == parent_keys[relationship_config.parent_column],
        "left_anti",
    )

    orphan_count = orphan_keys.count()

    return relationship_config.relationship_name, orphan_count



def main(spark_session=None) -> None:
    """Clean one Bronze table and inspect the Silver output."""
    
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-read-bronze-table")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False
    
    pipeline_run_id = build_pipeline_run_id()

    print("Starting Silver transformations")
    print("=" * 80)
    print(f"Bronze directory: {BRONZE_DIR}")
    print(f"Silver directory: {SILVER_DIR}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Configured table count: {len(SILVER_TABLES)}")
    print("=" * 80)

    table_counts: list[tuple[str, int, int]] = []

    try:
        for table_config in SILVER_TABLES:
            table_name, bronze_count, silver_count = transform_one_silver_table(
                spark=spark,
                table_config=table_config,
                pipeline_run_id=pipeline_run_id,
            )

            table_counts.append(
                (
                    table_name,
                    bronze_count,
                    silver_count,
                )
            )

        print("\nSilver transformation summary")
        print("=" * 80)
        print(
            f"{'silver_table':<45} "
            f"{'bronze_rows':>12} "
            f"{'silver_rows':>12} "
            f"{'removed':>12}"
        )
        print("-" * 80)

        total_bronze_rows = 0
        total_silver_rows = 0

        for table_name, bronze_count, silver_count in table_counts:
            removed_count = bronze_count - silver_count
            total_bronze_rows += bronze_count
            total_silver_rows += silver_count

            print(
                f"{table_name:<45} "
                f"{bronze_count:>12,} "
                f"{silver_count:>12,} "
                f"{removed_count:>12,}"
            )

        print("-" * 80)
        print(
            f"{'TOTAL':<45} "
            f"{total_bronze_rows:>12,} "
            f"{total_silver_rows:>12,} "
            f"{total_bronze_rows - total_silver_rows:>12,}"
        )
        print("=" * 80)

        validate_silver_relationships(spark)

        print("\nSilver transformations complete.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()