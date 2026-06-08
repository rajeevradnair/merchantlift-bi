"""Practice net merchant profit calculations with a tiny Spark DataFrame."""

from pyspark.sql import functions as F

from merchantlift.spark import create_spark_session_local


def main() -> None:
    """Practice merchant profit logic."""
    spark = create_spark_session_local("merchantlift-net-profit-practice")

    df = spark.createDataFrame(
        [
            ("offer_001", 10000.0, 0.30, 500.0, 0.03),
            ("offer_002", 5000.0, 0.20, 1500.0, 0.04),
            ("offer_003", -2000.0, 0.25, 300.0, 0.03),
        ],
        [
            "offer_id",
            "incremental_revenue_amount",
            "merchant_margin_rate",
            "total_test_reward_amount",
            "platform_fee_rate",
        ],
    )

    profit_df = (
        df
        .withColumn(
            "estimated_incremental_margin_amount",
            F.col("incremental_revenue_amount") * F.col("merchant_margin_rate"),
        )
        .withColumn(
            "estimated_platform_fee_amount",
            F.col("incremental_revenue_amount") * F.col("platform_fee_rate"),
        )
        .withColumn(
            "net_merchant_profit_amount",
            F.col("estimated_incremental_margin_amount")
            - F.col("total_test_reward_amount")
            - F.col("estimated_platform_fee_amount"),
        )
        .withColumn(
            "profitability_status",
            F.when(
                F.col("net_merchant_profit_amount") > 0,
                F.lit("profitable"),
            )
            .when(
                F.col("net_merchant_profit_amount") < 0,
                F.lit("unprofitable"),
            )
            .otherwise(F.lit("break_even")),
        )
    )

    profit_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()