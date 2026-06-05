"""Ingest raw MerchantLift BI tables into Bronze Delta tables."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import BRONZE_DIR, RAW_DATA_DIR
from merchantlift.spark import create_spark_session

from merchantlift.bronze_config import BRONZE_TABLES, BronzeTableConfig

def build_pipeline_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"bronze_run_{timestamp}"

def get_bronze_table_path(table_name: str):
    """Return the Bronze Delta table path.

    Args:
        table_name: Bronze table name.

    Returns:
        Bronze Delta table path.
    """
    return BRONZE_DIR / table_name

def write_bronze_delta_table(
    df: DataFrame,
    table_name: str,
    partition_column: str | None = None,
) -> None:
    """Write one Bronze Delta table.

    Args:
        df: Bronze DataFrame.
        table_name: Bronze table name.
        partition_column: Optional partition column.
    """
    output_path = BRONZE_DIR / table_name

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_column is not None and partition_column in df.columns:
        writer = writer.partitionBy(partition_column)

    writer.save(str(output_path))

    print(f"Wrote Bronze Delta table: {output_path}")


def add_bronze_metadata(
    df: DataFrame,
    table_name: str,
    source_file_path: str,
    pipeline_run_id: str,
) -> DataFrame:
    """Add Bronze ingestion metadata columns.

    Args:
        df: Raw Spark DataFrame.
        table_name: Source raw table name.
        source_file_path: Source raw table path.
        pipeline_run_id: Pipeline run identifier.

    Returns:
        DataFrame with ingestion metadata.
    """
    row_as_text = F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column_name).cast("string"), F.lit(""))
            for column_name in df.columns
        ],
    )

    return (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_table_name", F.lit(table_name))
        .withColumn("source_file_path", F.lit(source_file_path))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("record_hash", F.sha2(row_as_text, 256))
    )


def ingest_one_bronze_table(
    spark,
    table_config: BronzeTableConfig,
    pipeline_run_id: str,
) -> int:
    """Ingest one raw table into the Bronze Delta layer.

    Args:
        spark: Active Spark session.
        table_config: Bronze table ingestion configuration.
        pipeline_run_id: Pipeline run identifier.

    Returns:
        Number of rows written to the Bronze table.
    """
    table_name = table_config.table_name
    raw_table_path = RAW_DATA_DIR / table_name

    if not raw_table_path.exists():
        raise FileNotFoundError(
            f"Raw table path does not exist: {raw_table_path}"
        )

    print("\n" + "=" * 80)
    print(f"Ingesting raw table: {table_name}")
    print(f"Raw path: {raw_table_path}")
    print(f"Partition column: {table_config.partition_column}")
    print("=" * 80)

    raw_df = spark.read.parquet(str(raw_table_path))

    bronze_df = add_bronze_metadata(
        df=raw_df,
        table_name=table_name,
        source_file_path=str(raw_table_path),
        pipeline_run_id=pipeline_run_id,
    )

    row_count = bronze_df.count()

    write_bronze_delta_table(
        df=bronze_df,
        table_name=table_name,
        partition_column=table_config.partition_column,
    )

    print(f"Completed Bronze table: {table_name}")
    print(f"Rows written: {row_count:,}")

    return row_count

def validate_bronze_row_count(
    spark,
    table_config: BronzeTableConfig,
) -> tuple[str, int, int, str]:
    """Compare raw and Bronze row counts for one table.

    Args:
        spark: Active Spark session.
        table_config: Bronze table ingestion configuration.

    Returns:
        Tuple with table name, raw count, Bronze count, and status.
    """
    table_name = table_config.table_name

    raw_table_path = RAW_DATA_DIR / table_name
    bronze_table_path = get_bronze_table_path(table_name)

    raw_df = spark.read.parquet(str(raw_table_path))

    bronze_df = (
        spark.read
        .format("delta")
        .load(str(bronze_table_path))
    )

    if raw_df.count() == bronze_df.count():
        status = "PASSED"
    else:
        status = "FAILED"

    return table_name, raw_df.count(), bronze_df.count(), status


def validate_bronze_tables(spark) -> None:
    """Validate that Bronze tables preserve raw row counts.

    Args:
        spark: Active Spark session.

    Raises:
        ValueError: If any Bronze row-count check fails.
    """
    print("\nBronze validation summary")
    print("=" * 80)
    print(
        f"{'table_name':<45} "
        f"{'raw_rows':>12} "
        f"{'bronze_rows':>12} "
        f"{'status':>10}"
    )
    print("-" * 80)

    failed_tables = []
    total_raw_rows = 0
    total_bronze_rows = 0

    for table_config in BRONZE_TABLES:
        table_name, raw_count, bronze_count, status = validate_bronze_row_count(
            spark=spark,
            table_config=table_config,
        )

        total_raw_rows += raw_count
        total_bronze_rows += bronze_count

        if status == "FAILED":
            failed_tables.append(table_name)

        print(
            f"{table_name:<45} "
            f"{raw_count:>12,} "
            f"{bronze_count:>12,} "
            f"{status:>10}"
        )

    print("-" * 80)
    print(
        f"{'TOTAL':<45} "
        f"{total_raw_rows:>12,} "
        f"{total_bronze_rows:>12,} "
        f"{'':>10}"
    )
    print("=" * 80)

    if failed_tables:
        raise ValueError(
            "Bronze validation failed for tables: "
            + ", ".join(failed_tables)
        )

    print("Bronze validation passed.")

def main() -> None:
    """Ingest all configured raw tables into the Bronze Delta layer."""
    spark = create_spark_session("merchantlift-bronze-ingestion")

    pipeline_run_id = build_pipeline_run_id()

    print("Starting Bronze ingestion")
    print("=" * 80)
    print(f"Raw data directory: {RAW_DATA_DIR}")
    print(f"Bronze directory: {BRONZE_DIR}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Configured table count: {len(BRONZE_TABLES)}")
    print("=" * 80)

    table_row_counts: dict[str, int] = {}

    try:
        for table_config in BRONZE_TABLES:
            row_count = ingest_one_bronze_table(
                spark=spark,
                table_config=table_config,
                pipeline_run_id=pipeline_run_id,
            )

            table_row_counts[table_config.table_name] = row_count

        print("\nBronze ingestion summary")
        print("=" * 80)

        total_rows = 0

        for table_name, row_count in table_row_counts.items():
            total_rows += row_count
            print(f"{table_name:<45} {row_count:>12,} rows")

        print("-" * 80)
        print(f"{'TOTAL':<45} {total_rows:>12,} rows")
        print("=" * 80)

        print("\nBronze ingestion complete.")

        validate_bronze_tables(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()