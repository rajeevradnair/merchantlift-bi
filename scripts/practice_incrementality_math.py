"""Practice test/control incrementality math with tiny Spark DataFrames."""

from pyspark.sql import functions as F

from merchantlift.spark import create_spark_session_local


def safe_divide(numerator, denominator):
    """Return numerator / denominator, safely handling zero."""
    return F.when(denominator > 0, numerator / denominator).otherwise(F.lit(0.0))


def main() -> None:
    """Practice basic incrementality calculations."""
    spark = create_spark_session_local("merchantlift-incrementality-practice")

    test_spend = spark.createDataFrame(
        [
            ("offer_001", "campaign_001", "merchant_001", "2026-03-01", "card_001", 120.0),
            ("offer_001", "campaign_001", "merchant_001", "2026-03-01", "card_002", 80.0),
            ("offer_001", "campaign_001", "merchant_001", "2026-03-01", "card_003", 100.0),
        ],
        [
            "offer_id",
            "campaign_id",
            "merchant_id",
            "business_date",
            "tokenized_cardmember_id",
            "test_spend_amount",
        ],
    ).withColumn("business_date", F.to_date("business_date"))

    control_spend = spark.createDataFrame(
        [
            ("offer_001", "campaign_001", "merchant_001", "2026-03-01", "ctrl_001", 70.0),
            ("offer_001", "campaign_001", "merchant_001", "2026-03-01", "ctrl_002", 90.0),
            ("offer_001", "campaign_001", "merchant_001", "2026-03-01", "ctrl_003", 80.0),
        ],
        [
            "offer_id",
            "campaign_id",
            "merchant_id",
            "business_date",
            "tokenized_cardmember_id",
            "control_spend_amount",
        ],
    ).withColumn("business_date", F.to_date("business_date"))

    test_agg = (
        test_spend
        .groupBy("business_date", "offer_id", "campaign_id", "merchant_id")
        .agg(
            F.countDistinct("tokenized_cardmember_id").alias("test_cardmember_count"),
            F.sum("test_spend_amount").alias("total_test_spend_amount"),
        )
    )

    control_agg = (
        control_spend
        .groupBy("business_date", "offer_id", "campaign_id", "merchant_id")
        .agg(
            F.countDistinct("tokenized_cardmember_id").alias("control_cardmember_count"),
            F.sum("control_spend_amount").alias("total_control_spend_amount"),
        )
    )

    incrementality = (
        test_agg.alias("test")
        .join(
            control_agg.alias("control"),
            ["business_date", "offer_id", "campaign_id", "merchant_id"],
            "inner",
        )
        .withColumn(
            "avg_test_spend_per_cardmember",
            safe_divide(
                F.col("total_test_spend_amount"),
                F.col("test_cardmember_count"),
            ),
        )
        .withColumn(
            "avg_control_spend_per_cardmember",
            safe_divide(
                F.col("total_control_spend_amount"),
                F.col("control_cardmember_count"),
            ),
        )
        .withColumn(
            "lift_per_cardmember",
            F.col("avg_test_spend_per_cardmember")
            - F.col("avg_control_spend_per_cardmember"),
        )
        .withColumn(
            "incremental_revenue_amount",
            F.col("lift_per_cardmember") * F.col("test_cardmember_count"),
        )
    )

    incrementality.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()