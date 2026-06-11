"""Practice cannibalization risk scoring with a tiny Spark DataFrame."""

from pyspark.sql import functions as F

from merchantlift.spark import create_spark_session_local


def main() -> None:
    """Practice deterministic cannibalization scoring."""
    spark = create_spark_session_local("merchantlift-cannibalization-practice")

    df = spark.createDataFrame(
        [
            ("offer_001", 10000.0, 2200.0, 12.5, 0.30, False),
            ("offer_002", 5000.0, -700.0, 1.5, 0.03, True),
            ("offer_003", -2000.0, -900.0, -3.0, -0.20, False),
        ],
        [
            "offer_id",
            "incremental_revenue_amount",
            "net_merchant_profit_amount",
            "total_cost_roas",
            "lift_percentage",
            "spend_lift_but_profit_loss_flag",
        ],
    )

    scored_df = (
        df
        .withColumn(
            "negative_profit_risk_points",
            F.when(F.col("net_merchant_profit_amount") < 0, F.lit(40)).otherwise(F.lit(0)),
        )
        .withColumn(
            "weak_lift_risk_points",
            F.when(F.col("lift_percentage") <= 0.05, F.lit(25)).otherwise(F.lit(0)),
        )
        .withColumn(
            "low_roas_risk_points",
            F.when(F.col("total_cost_roas") < 2, F.lit(20)).otherwise(F.lit(0)),
        )
        .withColumn(
            "spend_lift_profit_loss_risk_points",
            F.when(F.col("spend_lift_but_profit_loss_flag") == True, F.lit(15)).otherwise(F.lit(0)),
        )
        .withColumn(
            "cannibalization_risk_score",
            F.col("negative_profit_risk_points")
            + F.col("weak_lift_risk_points")
            + F.col("low_roas_risk_points")
            + F.col("spend_lift_profit_loss_risk_points"),
        )
        .withColumn(
            "cannibalization_risk_level",
            F.when(F.col("cannibalization_risk_score") >= 80, F.lit("critical"))
            .when(F.col("cannibalization_risk_score") >= 50, F.lit("high"))
            .when(F.col("cannibalization_risk_score") >= 25, F.lit("medium"))
            .otherwise(F.lit("low")),
        )
    )

    scored_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()