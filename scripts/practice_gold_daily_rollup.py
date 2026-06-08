"""Practice daily Gold rollups with tiny Spark DataFrames."""

from pyspark.sql import functions as F

from merchantlift.spark import create_spark_session_local


def main(spark_session=None) -> None:
    """Practice merchant daily economics aggregation."""
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-try-gold-rollup")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    transactions = spark.createDataFrame(
        [
            ("tx_001", "merchant_001", "2026-03-01", 100.00),
            ("tx_002", "merchant_001", "2026-03-01", 50.00),
            ("tx_003", "merchant_002", "2026-03-01", 200.00),
        ],
        [
            "transaction_id",
            "merchant_id",
            "transaction_date",
            "transaction_amount",
        ],
    ).withColumn(
        "transaction_date",
        F.to_date("transaction_date"),
    )

    redemptions = spark.createDataFrame(
        [
            ("mred_001", "tx_001", "merchant_001", "offer_001", "2026-03-01", 10.00),
            ("mred_002", "tx_003", "merchant_002", "offer_002", "2026-03-01", 20.00),
        ],
        [
            "matched_redemption_id",
            "transaction_id",
            "merchant_id",
            "offer_id",
            "transaction_date",
            "calculated_reward_amount",
        ],
    ).withColumn(
        "transaction_date",
        F.to_date("transaction_date"),
    )

    merchant_scd = spark.createDataFrame(
        [
            ("merchant_001", 0.30, 0.03, True),
            ("merchant_002", 0.25, 0.04, True),
        ],
        [
            "merchant_id",
            "merchant_margin_rate",
            "platform_fee_rate",
            "is_current",
        ],
    )

    spend_daily = (
        transactions
        .groupBy(
            F.col("transaction_date").alias("business_date"),
            "merchant_id",
        )
        .agg(
            F.countDistinct("transaction_id").alias("transaction_count"),
            F.sum("transaction_amount").alias("gross_spend_amount"),
        )
    )
    spend_daily.show(5)

    reward_daily = (
        redemptions
        .groupBy(
            F.col("transaction_date").alias("business_date"),
            "merchant_id",
        )
        .agg(
            F.countDistinct("matched_redemption_id").alias("matched_redemption_count"),
            F.sum("calculated_reward_amount").alias("reward_cost_amount"),
        )
    )
    reward_daily.show(5)
    
    current_merchant = merchant_scd.filter(F.col("is_current") == True)

    merchant_daily = (
        spend_daily.alias("spend")
        .join(
            reward_daily.alias("reward"),
            ["business_date", "merchant_id"],
            "left",
        )
        .join(
            current_merchant.alias("merchant"),
            "merchant_id",
            "left",
        )
        .fillna(
            {
                "matched_redemption_count": 0,
                "reward_cost_amount": 0.0,
            }
        )
        .withColumn(
            "platform_fee_amount",
            F.col("gross_spend_amount") * F.col("platform_fee_rate"),
        )
        .withColumn(
            "estimated_merchant_margin_amount",
            F.col("gross_spend_amount") * F.col("merchant_margin_rate"),
        )
        .withColumn(
            "merchant_net_after_reward",
            F.col("estimated_merchant_margin_amount")
            - F.col("platform_fee_amount")
            - F.col("reward_cost_amount"),
        )
    )

    merchant_daily.show(truncate=False)

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()