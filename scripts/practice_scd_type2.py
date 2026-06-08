"""Practice SCD Type 2 concepts with a tiny Spark DataFrame."""

from pyspark.sql import functions as F

from merchantlift.spark import create_spark_session, create_spark_session_local


def main(spark_session=None) -> None:
    """Build a tiny SCD-style dimension snapshot."""
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-read-bronze-table")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    merchant_df = spark.createDataFrame(
        [
            ("merchant_001", "Coffee Hub", "coffee", 0.20),
            ("merchant_002", "Pizza Corner", "restaurant", 0.18),
        ],
        [
            "merchant_id",
            "merchant_name",
            "merchant_category",
            "merchant_margin_rate",
        ],
    )

    scd_df = (
        merchant_df
        .withColumn(
            "surrogate_scd_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("merchant_id"),
                    F.lit("2026-01-01"),
                ),
                256,
            ),
        )
        .withColumn("effective_start_date", F.to_date(F.lit("2026-01-01")))
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "scd_record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("merchant_name"),
                    F.col("merchant_category"),
                    F.col("merchant_margin_rate").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("scd_created_at", F.current_timestamp())
        .withColumn("scd_updated_at", F.current_timestamp())
        .withColumn("scd_rule_version", F.lit("scd_rules_v1"))
    )

    scd_df.show(truncate=False)
    scd_df.printSchema()

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()