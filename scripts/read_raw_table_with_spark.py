"""Read one raw MerchantLift BI table with Spark."""

from pandas import DataFrame

from datetime import datetime, timezone

from merchantlift.paths import RAW_DATA_DIR
from merchantlift.spark import create_spark_session

from pyspark.sql import functions as F

def add_bronze_metadata(df, table_name: str, source_file_path: str, pipeline_run_id: str) -> "DataFrame":

    row_as_text = F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(column_name).cast("string"), F.lit(""))
            for column_name in df.columns
        ],
    )

    df = (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_table_name", F.lit("fact_transactions"))
        .withColumn("source_file_path", F.lit(source_file_path))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("record_hash", F.sha2(row_as_text, 256))
    )     

    return df

def main() -> None:
    """Read one raw Parquet table and add Bronze metadata."""
    spark = create_spark_session("merchantlift-bronze-metadata-practice")

    table_name = "fact_transactions"
    raw_table_path = RAW_DATA_DIR / table_name

    pipeline_run_id = (
        "bronze_run_"
        + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    )

    print("=" * 80)
    print(f"Reading raw table: {table_name}")
    print(f"Raw table path: {raw_table_path}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print("=" * 80)

    raw_df = spark.read.parquet(str(raw_table_path))

    bronze_df = add_bronze_metadata(
        df=raw_df,
        table_name=table_name,
        source_file_path=str(raw_table_path),
        pipeline_run_id=pipeline_run_id,
    )

    print("\nBronze schema:")
    bronze_df.printSchema()

    print("\nBronze sample rows:")
    bronze_df.select(
        "transaction_id",
        "merchant_id",
        "transaction_amount",
        "ingestion_timestamp",
        "source_table_name",
        "pipeline_run_id",
        "record_hash",
    ).show(5, truncate=False)

    print("\nRow count:")
    print(f"{table_name}: {bronze_df.count():,} rows")

    spark.stop()

if __name__ == "__main__":
    main()