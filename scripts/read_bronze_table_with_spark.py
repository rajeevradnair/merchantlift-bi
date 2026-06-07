"""Read one Bronze Delta table with Spark."""

from merchantlift.paths import BRONZE_DIR
from merchantlift.spark_read_bronze import create_spark_session, create_spark_session_local


def main(spark_session=None) -> None:
    """Read and inspect one Bronze Delta table."""
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-read-bronze-table")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    table_name = "fact_transactions"
    bronze_table_path = BRONZE_DIR / table_name

    print("=" * 80)
    print(f"Reading Bronze table: {table_name}")
    print(f"Bronze table path: {bronze_table_path}")
    print("=" * 80)

    bronze_df = (
        spark.read
        .format("delta")
        .load(str(bronze_table_path))
    )

    print("\nBronze schema:")
    bronze_df.printSchema()

    print("\nBronze sample rows:")
    bronze_df.show(5, truncate=False)

    print("\nBronze metadata sample:")
    bronze_df.select(
        "transaction_id",
        "source_table_name",
        "source_file_path",
        "pipeline_run_id",
        "ingestion_timestamp",
        "record_hash",
    ).show(5, truncate=False)

    row_count = bronze_df.count()

    print("\nBronze row count:")
    print(f"{table_name}: {row_count:,} rows")

    spark.stop()


if __name__ == "__main__":
    main()