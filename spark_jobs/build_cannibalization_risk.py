"""Build Gold offer cannibalization risk features."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import GOLD_DIR
from merchantlift.spark import create_spark_session_local


CANNIBALIZATION_RULE_VERSION = "cannibalization_rules_v1"

GOLD_INCREMENTALITY_FEATURES_TABLE = "gold_incrementality_features"
GOLD_OFFER_CANNIBALIZATION_RISK_TABLE = "gold_offer_cannibalization_risk"

WEAK_LIFT_PERCENTAGE_THRESHOLD = 0.05
LOW_TOTAL_COST_ROAS_THRESHOLD = 2.0
HIGH_REWARD_COST_SHARE_THRESHOLD = 0.25
LOW_NET_PROFIT_MARGIN_THRESHOLD = 0.0
TEST_CONTROL_SPEND_PARITY_TOLERANCE = 0.05

NEGATIVE_NET_PROFIT_RISK_POINTS = 30
NEGATIVE_LIFT_RISK_POINTS = 25
WEAK_LIFT_RISK_POINTS = 15
LOW_TOTAL_COST_ROAS_RISK_POINTS = 15
HIGH_REWARD_COST_SHARE_RISK_POINTS = 10
POSITIVE_SPEND_LIFT_BUT_PROFIT_LOSS_RISK_POINTS = 20
REWARD_COST_EXCEEDS_INCREMENTAL_MARGIN_RISK_POINTS = 15
TEST_CONTROL_SPEND_NEAR_PARITY_RISK_POINTS = 10

CRITICAL_CANNIBALIZATION_RISK_THRESHOLD = 80
HIGH_CANNIBALIZATION_RISK_THRESHOLD = 50
MEDIUM_CANNIBALIZATION_RISK_THRESHOLD = 25

GOLD_INCREMENTALITY_FEATURES_REQUIRED_COLUMNS = (
    "business_date",
    "offer_id",
    "campaign_id",
    "merchant_id",
    "minimum_spend_amount",
    "reward_amount",
    "merchant_margin_rate",
    "platform_fee_rate",
    "test_cardmember_count",
    "test_transaction_count",
    "test_redemption_count",
    "total_test_spend_amount",
    "total_test_reward_amount",
    "control_cardmember_count",
    "total_control_spend_amount",
    "average_test_spend_per_cardmember",
    "average_control_spend_per_cardmember",
    "lift_per_cardmember",
    "lift_percentage",
    "incremental_revenue_amount",
    "incremental_revenue_direction",
    "estimated_incremental_margin_amount",
    "funded_reward_cost_amount",
    "estimated_incremental_platform_fee_amount",
    "net_merchant_profit_amount",
    "net_profit_per_test_cardmember",
    "net_profit_margin_on_incremental_revenue",
    "profitability_status",
    "profitable_incremental_offer_flag",
    "spend_lift_but_profit_loss_flag",
    "negative_lift_and_unprofitable_flag",
    "total_offer_cost_amount",
    "reward_roas",
    "total_cost_roas",
    "margin_roas",
    "net_profit_roas",
    "cost_per_incremental_revenue_dollar",
    "reward_cost_share_of_incremental_revenue",
    "platform_fee_share_of_incremental_revenue",
    "efficiency_status",
)

GOLD_OFFER_CANNIBALIZATION_RISK_REQUIRED_COLUMNS = (
    "business_date",
    "offer_id",
    "campaign_id",
    "merchant_id",
    "minimum_spend_amount",
    "reward_amount",
    "merchant_margin_rate",
    "platform_fee_rate",
    "test_cardmember_count",
    "test_transaction_count",
    "test_redemption_count",
    "total_test_spend_amount",
    "total_test_reward_amount",
    "control_cardmember_count",
    "total_control_spend_amount",
    "average_test_spend_per_cardmember",
    "average_control_spend_per_cardmember",
    "lift_per_cardmember",
    "lift_percentage",
    "incremental_revenue_amount",
    "incremental_revenue_direction",
    "estimated_incremental_margin_amount",
    "funded_reward_cost_amount",
    "estimated_incremental_platform_fee_amount",
    "net_merchant_profit_amount",
    "net_profit_per_test_cardmember",
    "net_profit_margin_on_incremental_revenue",
    "profitability_status",
    "profitable_incremental_offer_flag",
    "spend_lift_but_profit_loss_flag",
    "negative_lift_and_unprofitable_flag",
    "total_offer_cost_amount",
    "reward_roas",
    "total_cost_roas",
    "margin_roas",
    "net_profit_roas",
    "cost_per_incremental_revenue_dollar",
    "reward_cost_share_of_incremental_revenue",
    "platform_fee_share_of_incremental_revenue",
    "efficiency_status",
    "weak_lift_flag",
    "negative_lift_flag",
    "negative_net_profit_flag",
    "low_total_cost_roas_flag",
    "high_reward_cost_share_flag",
    "low_net_profit_margin_flag",
    "positive_spend_lift_but_profit_loss_flag",
    "reward_cost_exceeds_incremental_margin_flag",
    "test_control_spend_similarity_ratio",
    "test_control_spend_near_parity_flag",
    "negative_net_profit_risk_points",
    "negative_lift_risk_points",
    "weak_lift_risk_points",
    "low_total_cost_roas_risk_points",
    "high_reward_cost_share_risk_points",
    "positive_spend_lift_but_profit_loss_risk_points",
    "reward_cost_exceeds_incremental_margin_risk_points",
    "test_control_spend_near_parity_risk_points",
    "cannibalization_risk_score",
    "cannibalization_risk_level",
    "cannibalization_action_recommendation",
    "requires_merchant_review_flag",
    "requires_offer_redesign_flag",
    "safe_to_scale_flag",
    "primary_cannibalization_risk_driver",
    "cannibalization_risk_reason",
    "cannibalization_risk_detail",
    "cannibalization_pipeline_run_id",
    "cannibalization_rule_version",
    "cannibalization_created_at",
)

def build_pipeline_run_id() -> str:
    """Create a unique cannibalization pipeline run identifier.

    Returns:
        Pipeline run identifier.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"cannibalization_risk_run_{timestamp}"


def get_gold_table_path(table_name: str):
    """Return Gold Delta table path.

    Args:
        table_name: Gold table name.

    Returns:
        Gold table path.
    """
    return GOLD_DIR / table_name


def read_gold_table(
    spark,
    table_name: str,
) -> DataFrame:
    """Read one Gold Delta table.

    Args:
        spark: Active Spark session.
        table_name: Gold table name.

    Returns:
        Gold DataFrame.
    """
    return (
        spark.read
        .format("delta")
        .load(str(get_gold_table_path(table_name)))
    )


def write_gold_table(
    df: DataFrame,
    table_name: str,
    partition_column: str | None = None,
) -> None:
    """Write one Gold Delta table.

    Args:
        df: DataFrame to write.
        table_name: Gold output table name.
        partition_column: Optional partition column.
    """
    output_path = get_gold_table_path(table_name)

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_column is not None and partition_column in df.columns:
        writer = writer.partitionBy(partition_column)

    writer.save(str(output_path))

    print(f"Wrote Gold Delta table: {output_path}")


def safe_divide(
    numerator,
    denominator,
):
    """Safely divide two Spark columns.

    Args:
        numerator: Numerator expression.
        denominator: Denominator expression.

    Returns:
        Spark column expression.
    """
    return F.when(
        denominator != 0,
        numerator / denominator,
    ).otherwise(F.lit(0.0))


def require_columns(
    df: DataFrame,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    """Validate that required columns exist in a DataFrame.

    Args:
        df: DataFrame to validate.
        table_name: Human-readable table name.
        required_columns: Required column names.

    Raises:
        ValueError: If required columns are missing.
    """
    missing_columns = sorted(set(required_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing required columns: "
            f"{', '.join(missing_columns)}"
        )
    
def validate_incrementality_features_input_contract(
    features_df: DataFrame,
) -> None:
    """Validate required input columns for cannibalization scoring.

    Args:
        features_df: Gold incrementality features DataFrame.
    """
    require_columns(
        df=features_df,
        table_name=GOLD_INCREMENTALITY_FEATURES_TABLE,
        required_columns=GOLD_INCREMENTALITY_FEATURES_REQUIRED_COLUMNS,
    )

def inspect_input_table(
    table_name: str,
    df: DataFrame,
    sample_size: int = 5,
) -> int:
    """Print schema, row count, and sample rows for an input table.

    Args:
        table_name: Human-readable table name.
        df: DataFrame to inspect.
        sample_size: Number of sample rows to show.

    Returns:
        Row count.
    """
    print("\n" + "=" * 80)
    print(f"Inspecting input table: {table_name}")
    print("=" * 80)

    print("\nSchema:")
    df.printSchema()

    row_count = df.count()

    print("\nRow count:")
    print(f"{table_name}: {row_count:,} rows")

    print("\nSample rows:")
    df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "lift_percentage",
        "funded_reward_cost_amount",
        "net_merchant_profit_amount",
        "profitability_status",
        "total_cost_roas",
        "efficiency_status",
    ).show(sample_size, truncate=False)

    return row_count

def add_basic_cannibalization_flags(
    features_df: DataFrame,
) -> DataFrame:
    """Add basic cannibalization risk indicator flags.

    Args:
        features_df: Gold incrementality features DataFrame.

    Returns:
        DataFrame with basic cannibalization risk flags.
    """
    validate_incrementality_features_input_contract(
        features_df=features_df,
    )

    return (
        features_df
        .withColumn(
            "weak_lift_flag",
            F.col("lift_percentage") <= F.lit(WEAK_LIFT_PERCENTAGE_THRESHOLD),
        )
        .withColumn(
            "negative_lift_flag",
            F.col("incremental_revenue_amount") < 0,
        )
        .withColumn(
            "negative_net_profit_flag",
            F.col("net_merchant_profit_amount") < 0,
        )
        .withColumn(
            "low_total_cost_roas_flag",
            F.col("total_cost_roas") < F.lit(LOW_TOTAL_COST_ROAS_THRESHOLD),
        )
        .withColumn(
            "high_reward_cost_share_flag",
            F.col("reward_cost_share_of_incremental_revenue")
            >= F.lit(HIGH_REWARD_COST_SHARE_THRESHOLD),
        )
        .withColumn(
            "low_net_profit_margin_flag",
            F.col("net_profit_margin_on_incremental_revenue")
            <= F.lit(LOW_NET_PROFIT_MARGIN_THRESHOLD),
        )
        .withColumn(
            "positive_spend_lift_but_profit_loss_flag",
            (F.col("incremental_revenue_amount") > 0)
            & (F.col("net_merchant_profit_amount") < 0),
        )
        .withColumn(
            "reward_cost_exceeds_incremental_margin_flag",
            F.col("funded_reward_cost_amount")
            > F.col("estimated_incremental_margin_amount"),
        )
        .withColumn(
            "test_control_spend_similarity_ratio",
            safe_divide(
                F.col("average_test_spend_per_cardmember"),
                F.col("average_control_spend_per_cardmember"),
            ),
        )
        .withColumn(
            "test_control_spend_near_parity_flag",
            F.abs(F.col("test_control_spend_similarity_ratio") - F.lit(1.0))
            <= F.lit(0.05),
        )
    )

def add_weighted_cannibalization_score(
    cannibalization_flags_df: DataFrame,
) -> DataFrame:
    """Add weighted cannibalization risk score.

    Args:
        cannibalization_flags_df: DataFrame with cannibalization evidence flags.

    Returns:
        DataFrame with risk point columns and total risk score.
    """
    require_columns(
        df=cannibalization_flags_df,
        table_name="cannibalization_flags_df",
        required_columns=(
            "weak_lift_flag",
            "negative_lift_flag",
            "negative_net_profit_flag",
            "low_total_cost_roas_flag",
            "high_reward_cost_share_flag",
            "positive_spend_lift_but_profit_loss_flag",
            "reward_cost_exceeds_incremental_margin_flag",
            "test_control_spend_near_parity_flag",
        ),
    )

    return (
        cannibalization_flags_df
        .withColumn(
            "negative_net_profit_risk_points",
            F.when(
                F.col("negative_net_profit_flag"),
                F.lit(NEGATIVE_NET_PROFIT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "negative_lift_risk_points",
            F.when(
                F.col("negative_lift_flag"),
                F.lit(NEGATIVE_LIFT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "weak_lift_risk_points",
            F.when(
                F.col("weak_lift_flag"),
                F.lit(WEAK_LIFT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "low_total_cost_roas_risk_points",
            F.when(
                F.col("low_total_cost_roas_flag"),
                F.lit(LOW_TOTAL_COST_ROAS_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "high_reward_cost_share_risk_points",
            F.when(
                F.col("high_reward_cost_share_flag"),
                F.lit(HIGH_REWARD_COST_SHARE_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "positive_spend_lift_but_profit_loss_risk_points",
            F.when(
                F.col("positive_spend_lift_but_profit_loss_flag"),
                F.lit(POSITIVE_SPEND_LIFT_BUT_PROFIT_LOSS_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "reward_cost_exceeds_incremental_margin_risk_points",
            F.when(
                F.col("reward_cost_exceeds_incremental_margin_flag"),
                F.lit(REWARD_COST_EXCEEDS_INCREMENTAL_MARGIN_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "test_control_spend_near_parity_risk_points",
            F.when(
                F.col("test_control_spend_near_parity_flag"),
                F.lit(TEST_CONTROL_SPEND_NEAR_PARITY_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "cannibalization_risk_score",
            F.col("negative_net_profit_risk_points")
            + F.col("negative_lift_risk_points")
            + F.col("weak_lift_risk_points")
            + F.col("low_total_cost_roas_risk_points")
            + F.col("high_reward_cost_share_risk_points")
            + F.col("positive_spend_lift_but_profit_loss_risk_points")
            + F.col("reward_cost_exceeds_incremental_margin_risk_points")
            + F.col("test_control_spend_near_parity_risk_points"),
        )
    )


def classify_cannibalization_risk_level(
    cannibalization_score_df: DataFrame,
) -> DataFrame:
    """Classify cannibalization risk level from weighted score.

    Args:
        cannibalization_score_df: DataFrame with cannibalization risk score.

    Returns:
        DataFrame with cannibalization risk level and recommendation.
    """
    require_columns(
        df=cannibalization_score_df,
        table_name="cannibalization_score_df",
        required_columns=(
            "cannibalization_risk_score",
            "negative_net_profit_flag",
            "negative_lift_flag",
            "weak_lift_flag",
            "low_total_cost_roas_flag",
            "positive_spend_lift_but_profit_loss_flag",
            "reward_cost_exceeds_incremental_margin_flag",
        ),
    )

    return (
        cannibalization_score_df
        .withColumn(
            "cannibalization_risk_level",
            F.when(
                F.col("cannibalization_risk_score")
                >= F.lit(CRITICAL_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("critical"),
            )
            .when(
                F.col("cannibalization_risk_score")
                >= F.lit(HIGH_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("high"),
            )
            .when(
                F.col("cannibalization_risk_score")
                >= F.lit(MEDIUM_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("medium"),
            )
            .otherwise(F.lit("low")),
        )
        .withColumn(
            "cannibalization_action_recommendation",
            F.when(
                F.col("cannibalization_risk_level") == "critical",
                F.lit(
                    "Pause or redesign offer; high likelihood of subsidized spend or negative merchant economics."
                ),
            )
            .when(
                F.col("cannibalization_risk_level") == "high",
                F.lit(
                    "Review targeting, reward amount, and eligibility rules before scaling campaign."
                ),
            )
            .when(
                F.col("cannibalization_risk_level") == "medium",
                F.lit(
                    "Monitor campaign economics and consider tighter segmentation or lower reward cost."
                ),
            )
            .otherwise(
                F.lit(
                    "Low cannibalization concern; campaign appears economically acceptable under current rules."
                ),
            ),
        )
        .withColumn(
            "requires_merchant_review_flag",
            F.col("cannibalization_risk_level").isin("critical", "high"),
        )
        .withColumn(
            "requires_offer_redesign_flag",
            (F.col("cannibalization_risk_level") == "critical")
            | (
                F.col("positive_spend_lift_but_profit_loss_flag")
                & F.col("reward_cost_exceeds_incremental_margin_flag")
            ),
        )
        .withColumn(
            "safe_to_scale_flag",
            (F.col("cannibalization_risk_level") == "low")
            & (~F.col("negative_net_profit_flag"))
            & (~F.col("negative_lift_flag")),
        )
    )

def add_human_readable_risk_reasons(
    classified_risk_df: DataFrame,
) -> DataFrame:
    """Add human-readable cannibalization risk reasons.

    Args:
        classified_risk_df: DataFrame with cannibalization risk level.

    Returns:
        DataFrame with primary risk driver and risk explanation.
    """
    require_columns(
        df=classified_risk_df,
        table_name="classified_risk_df",
        required_columns=(
            "cannibalization_risk_score",
            "cannibalization_risk_level",
            "negative_net_profit_flag",
            "negative_lift_flag",
            "weak_lift_flag",
            "low_total_cost_roas_flag",
            "high_reward_cost_share_flag",
            "positive_spend_lift_but_profit_loss_flag",
            "reward_cost_exceeds_incremental_margin_flag",
            "test_control_spend_near_parity_flag",
            "requires_offer_redesign_flag",
            "safe_to_scale_flag",
        ),
    )

    return (
        classified_risk_df
        .withColumn(
            "primary_cannibalization_risk_driver",
            F.when(
                F.col("negative_lift_flag") & F.col("negative_net_profit_flag"),
                F.lit("negative_lift_and_negative_profit"),
            )
            .when(
                F.col("positive_spend_lift_but_profit_loss_flag"),
                F.lit("positive_spend_lift_but_profit_loss"),
            )
            .when(
                F.col("reward_cost_exceeds_incremental_margin_flag"),
                F.lit("reward_cost_exceeds_incremental_margin"),
            )
            .when(
                F.col("low_total_cost_roas_flag"),
                F.lit("low_total_cost_roas"),
            )
            .when(
                F.col("weak_lift_flag"),
                F.lit("weak_lift"),
            )
            .when(
                F.col("test_control_spend_near_parity_flag"),
                F.lit("test_control_spend_near_parity"),
            )
            .when(
                F.col("safe_to_scale_flag"),
                F.lit("low_risk_profitable_offer"),
            )
            .otherwise(F.lit("mixed_or_minor_risk_signals")),
        )
        .withColumn(
            "cannibalization_risk_reason",
            F.when(
                F.col("primary_cannibalization_risk_driver")
                == "negative_lift_and_negative_profit",
                F.lit(
                    "Offer underperformed the control group and produced negative merchant profit."
                ),
            )
            .when(
                F.col("primary_cannibalization_risk_driver")
                == "positive_spend_lift_but_profit_loss",
                F.lit(
                    "Offer produced incremental spend, but reward cost and platform fee exceeded merchant margin."
                ),
            )
            .when(
                F.col("primary_cannibalization_risk_driver")
                == "reward_cost_exceeds_incremental_margin",
                F.lit(
                    "Funded reward cost exceeded estimated incremental merchant margin."
                ),
            )
            .when(
                F.col("primary_cannibalization_risk_driver")
                == "low_total_cost_roas",
                F.lit(
                    "Offer generated weak incremental revenue relative to total offer cost."
                ),
            )
            .when(
                F.col("primary_cannibalization_risk_driver")
                == "weak_lift",
                F.lit(
                    "Test group spend was only slightly above control group spend."
                ),
            )
            .when(
                F.col("primary_cannibalization_risk_driver")
                == "test_control_spend_near_parity",
                F.lit(
                    "Test and control spend were very similar, suggesting possible subsidized organic spend."
                ),
            )
            .when(
                F.col("primary_cannibalization_risk_driver")
                == "low_risk_profitable_offer",
                F.lit(
                    "Offer shows low cannibalization risk and acceptable merchant economics."
                ),
            )
            .otherwise(
                F.lit(
                    "Offer has mixed or minor cannibalization signals and should be monitored."
                ),
            ),
        )
        .withColumn(
            "cannibalization_risk_detail",
            F.concat_ws(
                " ",
                F.lit("Risk level:"),
                F.col("cannibalization_risk_level"),
                F.lit("| Score:"),
                F.col("cannibalization_risk_score").cast("string"),
                F.lit("| Driver:"),
                F.col("primary_cannibalization_risk_driver"),
                F.lit("| Reason:"),
                F.col("cannibalization_risk_reason"),
            ),
        )
    )

def add_cannibalization_pipeline_metadata(
    risk_reasons_df: DataFrame,
    pipeline_run_id: str,
) -> DataFrame:
    """Add pipeline metadata to cannibalization risk features.

    Args:
        risk_reasons_df: DataFrame with cannibalization risk explanations.
        pipeline_run_id: Pipeline run identifier.

    Returns:
        DataFrame with pipeline metadata.
    """
    return (
        risk_reasons_df
        .withColumn(
            "cannibalization_pipeline_run_id",
            F.lit(pipeline_run_id),
        )
        .withColumn(
            "cannibalization_rule_version",
            F.lit(CANNIBALIZATION_RULE_VERSION),
        )
        .withColumn(
            "cannibalization_created_at",
            F.current_timestamp(),
        )
    )

def validate_gold_output_columns(
    df: DataFrame,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    """Validate required Gold output columns.

    Args:
        df: Gold DataFrame.
        table_name: Gold table name.
        required_columns: Required output columns.
    """
    require_columns(
        df=df,
        table_name=table_name,
        required_columns=required_columns,
    )

def validate_written_gold_table(
    spark,
    table_name: str,
    expected_row_count: int,
) -> None:
    """Validate written Gold table by reading it back.

    Args:
        spark: Active Spark session.
        table_name: Gold table name.
        expected_row_count: Expected row count.

    Raises:
        ValueError: If row count does not match.
    """
    written_df = read_gold_table(
        spark=spark,
        table_name=table_name,
    )

    actual_row_count = written_df.count()

    print("\nWritten Gold table validation")
    print("=" * 80)
    print(f"{'table':<45} {table_name}")
    print(f"{'expected rows':<45} {expected_row_count:>12,}")
    print(f"{'actual rows':<45} {actual_row_count:>12,}")
    print("=" * 80)

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"Gold row count mismatch for {table_name}: "
            f"expected {expected_row_count:,}, got {actual_row_count:,}"
        )

    print(f"Gold write validation passed: {table_name}")

def write_and_validate_gold_cannibalization_risk(
    spark,
    risk_df: DataFrame,
) -> None:
    """Write and validate Gold cannibalization risk table.

    Args:
        spark: Active Spark session.
        risk_df: Final cannibalization risk DataFrame.
    """
    validate_gold_output_columns(
        df=risk_df,
        table_name=GOLD_OFFER_CANNIBALIZATION_RISK_TABLE,
        required_columns=GOLD_OFFER_CANNIBALIZATION_RISK_REQUIRED_COLUMNS,
    )

    expected_row_count = risk_df.count()

    write_gold_table(
        df=risk_df,
        table_name=GOLD_OFFER_CANNIBALIZATION_RISK_TABLE,
        partition_column="business_date",
    )

    validate_written_gold_table(
        spark=spark,
        table_name=GOLD_OFFER_CANNIBALIZATION_RISK_TABLE,
        expected_row_count=expected_row_count,
    )


def validate_gold_cannibalization_risk_business_rules(
    spark,
) -> tuple[str, int]:
    """Validate business rules for Gold cannibalization risk output.

    Args:
        spark: Active Spark session.

    Returns:
        Table name and validation failure count.
    """
    df = read_gold_table(
        spark=spark,
        table_name=GOLD_OFFER_CANNIBALIZATION_RISK_TABLE,
    )

    require_columns(
        df=df,
        table_name=GOLD_OFFER_CANNIBALIZATION_RISK_TABLE,
        required_columns=GOLD_OFFER_CANNIBALIZATION_RISK_REQUIRED_COLUMNS,
    )

    duplicate_grain_count = (
        df
        .groupBy(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_required_count = (
        df
        .filter(
            F.col("business_date").isNull()
            | F.col("offer_id").isNull()
            | F.col("campaign_id").isNull()
            | F.col("merchant_id").isNull()
            | F.col("incremental_revenue_amount").isNull()
            | F.col("net_merchant_profit_amount").isNull()
            | F.col("cannibalization_risk_score").isNull()
            | F.col("cannibalization_risk_level").isNull()
            | F.col("primary_cannibalization_risk_driver").isNull()
            | F.col("cannibalization_risk_reason").isNull()
            | F.col("cannibalization_action_recommendation").isNull()
            | F.col("requires_merchant_review_flag").isNull()
            | F.col("requires_offer_redesign_flag").isNull()
            | F.col("safe_to_scale_flag").isNull()
            | F.col("cannibalization_pipeline_run_id").isNull()
            | F.col("cannibalization_rule_version").isNull()
            | F.col("cannibalization_created_at").isNull()
        )
        .count()
    )

    invalid_risk_level_count = (
        df
        .filter(
            ~F.col("cannibalization_risk_level").isin(
                "low",
                "medium",
                "high",
                "critical",
            )
        )
        .count()
    )

    invalid_primary_driver_count = (
        df
        .filter(
            ~F.col("primary_cannibalization_risk_driver").isin(
                "negative_lift_and_negative_profit",
                "positive_spend_lift_but_profit_loss",
                "reward_cost_exceeds_incremental_margin",
                "low_total_cost_roas",
                "weak_lift",
                "test_control_spend_near_parity",
                "low_risk_profitable_offer",
                "mixed_or_minor_risk_signals",
            )
        )
        .count()
    )

    negative_score_count = (
        df
        .filter(F.col("cannibalization_risk_score") < 0)
        .count()
    )

    risk_point_formula_mismatch_count = (
        df
        .withColumn(
            "expected_negative_net_profit_risk_points",
            F.when(
                F.col("negative_net_profit_flag"),
                F.lit(NEGATIVE_NET_PROFIT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_negative_lift_risk_points",
            F.when(
                F.col("negative_lift_flag"),
                F.lit(NEGATIVE_LIFT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_weak_lift_risk_points",
            F.when(
                F.col("weak_lift_flag"),
                F.lit(WEAK_LIFT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_low_total_cost_roas_risk_points",
            F.when(
                F.col("low_total_cost_roas_flag"),
                F.lit(LOW_TOTAL_COST_ROAS_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_high_reward_cost_share_risk_points",
            F.when(
                F.col("high_reward_cost_share_flag"),
                F.lit(HIGH_REWARD_COST_SHARE_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_positive_spend_lift_but_profit_loss_risk_points",
            F.when(
                F.col("positive_spend_lift_but_profit_loss_flag"),
                F.lit(POSITIVE_SPEND_LIFT_BUT_PROFIT_LOSS_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_reward_cost_exceeds_incremental_margin_risk_points",
            F.when(
                F.col("reward_cost_exceeds_incremental_margin_flag"),
                F.lit(REWARD_COST_EXCEEDS_INCREMENTAL_MARGIN_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_test_control_spend_near_parity_risk_points",
            F.when(
                F.col("test_control_spend_near_parity_flag"),
                F.lit(TEST_CONTROL_SPEND_NEAR_PARITY_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .filter(
            (F.col("negative_net_profit_risk_points")
             != F.col("expected_negative_net_profit_risk_points"))
            | (F.col("negative_lift_risk_points")
               != F.col("expected_negative_lift_risk_points"))
            | (F.col("weak_lift_risk_points")
               != F.col("expected_weak_lift_risk_points"))
            | (F.col("low_total_cost_roas_risk_points")
               != F.col("expected_low_total_cost_roas_risk_points"))
            | (F.col("high_reward_cost_share_risk_points")
               != F.col("expected_high_reward_cost_share_risk_points"))
            | (F.col("positive_spend_lift_but_profit_loss_risk_points")
               != F.col("expected_positive_spend_lift_but_profit_loss_risk_points"))
            | (F.col("reward_cost_exceeds_incremental_margin_risk_points")
               != F.col("expected_reward_cost_exceeds_incremental_margin_risk_points"))
            | (F.col("test_control_spend_near_parity_risk_points")
               != F.col("expected_test_control_spend_near_parity_risk_points"))
        )
        .count()
    )

    risk_score_formula_mismatch_count = (
        df
        .withColumn(
            "expected_cannibalization_risk_score",
            F.col("negative_net_profit_risk_points")
            + F.col("negative_lift_risk_points")
            + F.col("weak_lift_risk_points")
            + F.col("low_total_cost_roas_risk_points")
            + F.col("high_reward_cost_share_risk_points")
            + F.col("positive_spend_lift_but_profit_loss_risk_points")
            + F.col("reward_cost_exceeds_incremental_margin_risk_points")
            + F.col("test_control_spend_near_parity_risk_points"),
        )
        .filter(
            F.col("cannibalization_risk_score")
            != F.col("expected_cannibalization_risk_score")
        )
        .count()
    )

    risk_level_formula_mismatch_count = (
        df
        .withColumn(
            "expected_cannibalization_risk_level",
            F.when(
                F.col("cannibalization_risk_score")
                >= F.lit(CRITICAL_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("critical"),
            )
            .when(
                F.col("cannibalization_risk_score")
                >= F.lit(HIGH_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("high"),
            )
            .when(
                F.col("cannibalization_risk_score")
                >= F.lit(MEDIUM_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("medium"),
            )
            .otherwise(F.lit("low")),
        )
        .filter(
            F.col("cannibalization_risk_level")
            != F.col("expected_cannibalization_risk_level")
        )
        .count()
    )

    merchant_review_flag_mismatch_count = (
        df
        .withColumn(
            "expected_requires_merchant_review_flag",
            F.col("cannibalization_risk_level").isin("critical", "high"),
        )
        .filter(
            F.col("requires_merchant_review_flag")
            != F.col("expected_requires_merchant_review_flag")
        )
        .count()
    )

    offer_redesign_flag_mismatch_count = (
        df
        .withColumn(
            "expected_requires_offer_redesign_flag",
            (F.col("cannibalization_risk_level") == "critical")
            | (
                F.col("positive_spend_lift_but_profit_loss_flag")
                & F.col("reward_cost_exceeds_incremental_margin_flag")
            ),
        )
        .filter(
            F.col("requires_offer_redesign_flag")
            != F.col("expected_requires_offer_redesign_flag")
        )
        .count()
    )

    safe_to_scale_flag_mismatch_count = (
        df
        .withColumn(
            "expected_safe_to_scale_flag",
            (F.col("cannibalization_risk_level") == "low")
            & (~F.col("negative_net_profit_flag"))
            & (~F.col("negative_lift_flag")),
        )
        .filter(
            F.col("safe_to_scale_flag")
            != F.col("expected_safe_to_scale_flag")
        )
        .count()
    )

    primary_driver_mismatch_count = (
        df
        .withColumn(
            "expected_primary_cannibalization_risk_driver",
            F.when(
                F.col("negative_lift_flag") & F.col("negative_net_profit_flag"),
                F.lit("negative_lift_and_negative_profit"),
            )
            .when(
                F.col("positive_spend_lift_but_profit_loss_flag"),
                F.lit("positive_spend_lift_but_profit_loss"),
            )
            .when(
                F.col("reward_cost_exceeds_incremental_margin_flag"),
                F.lit("reward_cost_exceeds_incremental_margin"),
            )
            .when(
                F.col("low_total_cost_roas_flag"),
                F.lit("low_total_cost_roas"),
            )
            .when(
                F.col("weak_lift_flag"),
                F.lit("weak_lift"),
            )
            .when(
                F.col("test_control_spend_near_parity_flag"),
                F.lit("test_control_spend_near_parity"),
            )
            .when(
                F.col("safe_to_scale_flag"),
                F.lit("low_risk_profitable_offer"),
            )
            .otherwise(F.lit("mixed_or_minor_risk_signals")),
        )
        .filter(
            F.col("primary_cannibalization_risk_driver")
            != F.col("expected_primary_cannibalization_risk_driver")
        )
        .count()
    )

    metadata_rule_version_mismatch_count = (
        df
        .filter(
            F.col("cannibalization_rule_version")
            != F.lit(CANNIBALIZATION_RULE_VERSION)
        )
        .count()
    )

    failure_count = (
        duplicate_grain_count
        + null_required_count
        + invalid_risk_level_count
        + invalid_primary_driver_count
        + negative_score_count
        + risk_point_formula_mismatch_count
        + risk_score_formula_mismatch_count
        + risk_level_formula_mismatch_count
        + merchant_review_flag_mismatch_count
        + offer_redesign_flag_mismatch_count
        + safe_to_scale_flag_mismatch_count
        + primary_driver_mismatch_count
        + metadata_rule_version_mismatch_count
    )

    print("\nGold cannibalization risk business-rule validation")
    print("=" * 80)
    print(f"{'duplicate grain rows':<60} {duplicate_grain_count:>12,}")
    print(f"{'null required rows':<60} {null_required_count:>12,}")
    print(f"{'invalid risk level rows':<60} {invalid_risk_level_count:>12,}")
    print(f"{'invalid primary driver rows':<60} {invalid_primary_driver_count:>12,}")
    print(f"{'negative score rows':<60} {negative_score_count:>12,}")
    print(f"{'risk point formula mismatch rows':<60} {risk_point_formula_mismatch_count:>12,}")
    print(f"{'risk score formula mismatch rows':<60} {risk_score_formula_mismatch_count:>12,}")
    print(f"{'risk level formula mismatch rows':<60} {risk_level_formula_mismatch_count:>12,}")
    print(f"{'merchant review flag mismatch rows':<60} {merchant_review_flag_mismatch_count:>12,}")
    print(f"{'offer redesign flag mismatch rows':<60} {offer_redesign_flag_mismatch_count:>12,}")
    print(f"{'safe to scale flag mismatch rows':<60} {safe_to_scale_flag_mismatch_count:>12,}")
    print(f"{'primary driver mismatch rows':<60} {primary_driver_mismatch_count:>12,}")
    print(f"{'metadata rule version mismatch rows':<60} {metadata_rule_version_mismatch_count:>12,}")
    print(f"{'total validation failures':<60} {failure_count:>12,}")
    print("=" * 80)

    return GOLD_OFFER_CANNIBALIZATION_RISK_TABLE, failure_count

def validate_gold_cannibalization_risk_business_rules(
    spark,
) -> tuple[str, int]:
    """Validate business rules for Gold cannibalization risk output.

    Args:
        spark: Active Spark session.

    Returns:
        Table name and validation failure count.
    """
    df = read_gold_table(
        spark=spark,
        table_name=GOLD_OFFER_CANNIBALIZATION_RISK_TABLE,
    )

    require_columns(
        df=df,
        table_name=GOLD_OFFER_CANNIBALIZATION_RISK_TABLE,
        required_columns=GOLD_OFFER_CANNIBALIZATION_RISK_REQUIRED_COLUMNS,
    )

    duplicate_grain_count = (
        df
        .groupBy(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_required_count = (
        df
        .filter(
            F.col("business_date").isNull()
            | F.col("offer_id").isNull()
            | F.col("campaign_id").isNull()
            | F.col("merchant_id").isNull()
            | F.col("incremental_revenue_amount").isNull()
            | F.col("net_merchant_profit_amount").isNull()
            | F.col("cannibalization_risk_score").isNull()
            | F.col("cannibalization_risk_level").isNull()
            | F.col("primary_cannibalization_risk_driver").isNull()
            | F.col("cannibalization_risk_reason").isNull()
            | F.col("cannibalization_action_recommendation").isNull()
            | F.col("requires_merchant_review_flag").isNull()
            | F.col("requires_offer_redesign_flag").isNull()
            | F.col("safe_to_scale_flag").isNull()
            | F.col("cannibalization_pipeline_run_id").isNull()
            | F.col("cannibalization_rule_version").isNull()
            | F.col("cannibalization_created_at").isNull()
        )
        .count()
    )

    invalid_risk_level_count = (
        df
        .filter(
            ~F.col("cannibalization_risk_level").isin(
                "low",
                "medium",
                "high",
                "critical",
            )
        )
        .count()
    )

    invalid_primary_driver_count = (
        df
        .filter(
            ~F.col("primary_cannibalization_risk_driver").isin(
                "negative_lift_and_negative_profit",
                "positive_spend_lift_but_profit_loss",
                "reward_cost_exceeds_incremental_margin",
                "low_total_cost_roas",
                "weak_lift",
                "test_control_spend_near_parity",
                "low_risk_profitable_offer",
                "mixed_or_minor_risk_signals",
            )
        )
        .count()
    )

    negative_score_count = (
        df
        .filter(F.col("cannibalization_risk_score") < 0)
        .count()
    )

    risk_point_formula_mismatch_count = (
        df
        .withColumn(
            "expected_negative_net_profit_risk_points",
            F.when(
                F.col("negative_net_profit_flag"),
                F.lit(NEGATIVE_NET_PROFIT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_negative_lift_risk_points",
            F.when(
                F.col("negative_lift_flag"),
                F.lit(NEGATIVE_LIFT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_weak_lift_risk_points",
            F.when(
                F.col("weak_lift_flag"),
                F.lit(WEAK_LIFT_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_low_total_cost_roas_risk_points",
            F.when(
                F.col("low_total_cost_roas_flag"),
                F.lit(LOW_TOTAL_COST_ROAS_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_high_reward_cost_share_risk_points",
            F.when(
                F.col("high_reward_cost_share_flag"),
                F.lit(HIGH_REWARD_COST_SHARE_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_positive_spend_lift_but_profit_loss_risk_points",
            F.when(
                F.col("positive_spend_lift_but_profit_loss_flag"),
                F.lit(POSITIVE_SPEND_LIFT_BUT_PROFIT_LOSS_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_reward_cost_exceeds_incremental_margin_risk_points",
            F.when(
                F.col("reward_cost_exceeds_incremental_margin_flag"),
                F.lit(REWARD_COST_EXCEEDS_INCREMENTAL_MARGIN_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "expected_test_control_spend_near_parity_risk_points",
            F.when(
                F.col("test_control_spend_near_parity_flag"),
                F.lit(TEST_CONTROL_SPEND_NEAR_PARITY_RISK_POINTS),
            ).otherwise(F.lit(0)),
        )
        .filter(
            (F.col("negative_net_profit_risk_points")
             != F.col("expected_negative_net_profit_risk_points"))
            | (F.col("negative_lift_risk_points")
               != F.col("expected_negative_lift_risk_points"))
            | (F.col("weak_lift_risk_points")
               != F.col("expected_weak_lift_risk_points"))
            | (F.col("low_total_cost_roas_risk_points")
               != F.col("expected_low_total_cost_roas_risk_points"))
            | (F.col("high_reward_cost_share_risk_points")
               != F.col("expected_high_reward_cost_share_risk_points"))
            | (F.col("positive_spend_lift_but_profit_loss_risk_points")
               != F.col("expected_positive_spend_lift_but_profit_loss_risk_points"))
            | (F.col("reward_cost_exceeds_incremental_margin_risk_points")
               != F.col("expected_reward_cost_exceeds_incremental_margin_risk_points"))
            | (F.col("test_control_spend_near_parity_risk_points")
               != F.col("expected_test_control_spend_near_parity_risk_points"))
        )
        .count()
    )

    risk_score_formula_mismatch_count = (
        df
        .withColumn(
            "expected_cannibalization_risk_score",
            F.col("negative_net_profit_risk_points")
            + F.col("negative_lift_risk_points")
            + F.col("weak_lift_risk_points")
            + F.col("low_total_cost_roas_risk_points")
            + F.col("high_reward_cost_share_risk_points")
            + F.col("positive_spend_lift_but_profit_loss_risk_points")
            + F.col("reward_cost_exceeds_incremental_margin_risk_points")
            + F.col("test_control_spend_near_parity_risk_points"),
        )
        .filter(
            F.col("cannibalization_risk_score")
            != F.col("expected_cannibalization_risk_score")
        )
        .count()
    )

    risk_level_formula_mismatch_count = (
        df
        .withColumn(
            "expected_cannibalization_risk_level",
            F.when(
                F.col("cannibalization_risk_score")
                >= F.lit(CRITICAL_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("critical"),
            )
            .when(
                F.col("cannibalization_risk_score")
                >= F.lit(HIGH_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("high"),
            )
            .when(
                F.col("cannibalization_risk_score")
                >= F.lit(MEDIUM_CANNIBALIZATION_RISK_THRESHOLD),
                F.lit("medium"),
            )
            .otherwise(F.lit("low")),
        )
        .filter(
            F.col("cannibalization_risk_level")
            != F.col("expected_cannibalization_risk_level")
        )
        .count()
    )

    merchant_review_flag_mismatch_count = (
        df
        .withColumn(
            "expected_requires_merchant_review_flag",
            F.col("cannibalization_risk_level").isin("critical", "high"),
        )
        .filter(
            F.col("requires_merchant_review_flag")
            != F.col("expected_requires_merchant_review_flag")
        )
        .count()
    )

    offer_redesign_flag_mismatch_count = (
        df
        .withColumn(
            "expected_requires_offer_redesign_flag",
            (F.col("cannibalization_risk_level") == "critical")
            | (
                F.col("positive_spend_lift_but_profit_loss_flag")
                & F.col("reward_cost_exceeds_incremental_margin_flag")
            ),
        )
        .filter(
            F.col("requires_offer_redesign_flag")
            != F.col("expected_requires_offer_redesign_flag")
        )
        .count()
    )

    safe_to_scale_flag_mismatch_count = (
        df
        .withColumn(
            "expected_safe_to_scale_flag",
            (F.col("cannibalization_risk_level") == "low")
            & (~F.col("negative_net_profit_flag"))
            & (~F.col("negative_lift_flag")),
        )
        .filter(
            F.col("safe_to_scale_flag")
            != F.col("expected_safe_to_scale_flag")
        )
        .count()
    )

    primary_driver_mismatch_count = (
        df
        .withColumn(
            "expected_primary_cannibalization_risk_driver",
            F.when(
                F.col("negative_lift_flag") & F.col("negative_net_profit_flag"),
                F.lit("negative_lift_and_negative_profit"),
            )
            .when(
                F.col("positive_spend_lift_but_profit_loss_flag"),
                F.lit("positive_spend_lift_but_profit_loss"),
            )
            .when(
                F.col("reward_cost_exceeds_incremental_margin_flag"),
                F.lit("reward_cost_exceeds_incremental_margin"),
            )
            .when(
                F.col("low_total_cost_roas_flag"),
                F.lit("low_total_cost_roas"),
            )
            .when(
                F.col("weak_lift_flag"),
                F.lit("weak_lift"),
            )
            .when(
                F.col("test_control_spend_near_parity_flag"),
                F.lit("test_control_spend_near_parity"),
            )
            .when(
                F.col("safe_to_scale_flag"),
                F.lit("low_risk_profitable_offer"),
            )
            .otherwise(F.lit("mixed_or_minor_risk_signals")),
        )
        .filter(
            F.col("primary_cannibalization_risk_driver")
            != F.col("expected_primary_cannibalization_risk_driver")
        )
        .count()
    )

    metadata_rule_version_mismatch_count = (
        df
        .filter(
            F.col("cannibalization_rule_version")
            != F.lit(CANNIBALIZATION_RULE_VERSION)
        )
        .count()
    )

    failure_count = (
        duplicate_grain_count
        + null_required_count
        + invalid_risk_level_count
        + invalid_primary_driver_count
        + negative_score_count
        + risk_point_formula_mismatch_count
        + risk_score_formula_mismatch_count
        + risk_level_formula_mismatch_count
        + merchant_review_flag_mismatch_count
        + offer_redesign_flag_mismatch_count
        + safe_to_scale_flag_mismatch_count
        + primary_driver_mismatch_count
        + metadata_rule_version_mismatch_count
    )

    print("\nGold cannibalization risk business-rule validation")
    print("=" * 80)
    print(f"{'duplicate grain rows':<60} {duplicate_grain_count:>12,}")
    print(f"{'null required rows':<60} {null_required_count:>12,}")
    print(f"{'invalid risk level rows':<60} {invalid_risk_level_count:>12,}")
    print(f"{'invalid primary driver rows':<60} {invalid_primary_driver_count:>12,}")
    print(f"{'negative score rows':<60} {negative_score_count:>12,}")
    print(f"{'risk point formula mismatch rows':<60} {risk_point_formula_mismatch_count:>12,}")
    print(f"{'risk score formula mismatch rows':<60} {risk_score_formula_mismatch_count:>12,}")
    print(f"{'risk level formula mismatch rows':<60} {risk_level_formula_mismatch_count:>12,}")
    print(f"{'merchant review flag mismatch rows':<60} {merchant_review_flag_mismatch_count:>12,}")
    print(f"{'offer redesign flag mismatch rows':<60} {offer_redesign_flag_mismatch_count:>12,}")
    print(f"{'safe to scale flag mismatch rows':<60} {safe_to_scale_flag_mismatch_count:>12,}")
    print(f"{'primary driver mismatch rows':<60} {primary_driver_mismatch_count:>12,}")
    print(f"{'metadata rule version mismatch rows':<60} {metadata_rule_version_mismatch_count:>12,}")
    print(f"{'total validation failures':<60} {failure_count:>12,}")
    print("=" * 80)

    return GOLD_OFFER_CANNIBALIZATION_RISK_TABLE, failure_count

def validate_all_cannibalization_outputs(spark) -> None:
    """Validate all cannibalization risk outputs.

    Args:
        spark: Active Spark session.

    Raises:
        ValueError: If any cannibalization output fails validation.
    """
    print("\nValidating cannibalization risk outputs")
    print("=" * 80)

    validation_results = [
        validate_gold_cannibalization_risk_business_rules(spark=spark),
    ]

    print("\nCannibalization validation summary")
    print("=" * 80)
    print(f"{'table':<45} {'failures':>12} {'status':>12}")
    print("-" * 80)

    failed_tables = []

    for table_name, failure_count in validation_results:
        status = "PASSED" if failure_count == 0 else "FAILED"

        if failure_count > 0:
            failed_tables.append(table_name)

        print(f"{table_name:<45} {failure_count:>12,} {status:>12}")

    print("=" * 80)

    if failed_tables:
        raise ValueError(
            "Cannibalization output validation failed for: "
            + ", ".join(failed_tables)
        )

    print("All cannibalization risk output validations passed.")


def main(spark_session=None) -> None:
    """Run cannibalization risk feature build."""
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-cannibalization-risk")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    pipeline_run_id = build_pipeline_run_id()

    print("Starting Gold offer cannibalization risk build")
    print("=" * 80)
    print(f"Gold directory: {GOLD_DIR}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Cannibalization rule version: {CANNIBALIZATION_RULE_VERSION}")
    print(f"Input table: {GOLD_INCREMENTALITY_FEATURES_TABLE}")
    print(f"Output table: {GOLD_OFFER_CANNIBALIZATION_RISK_TABLE}")
    print("=" * 80)

    incrementality_features_df = read_gold_table(
        spark=spark,
        table_name=GOLD_INCREMENTALITY_FEATURES_TABLE,
    )

    validate_incrementality_features_input_contract(
        features_df=incrementality_features_df,
    )

    input_row_count = inspect_input_table(
        table_name=GOLD_INCREMENTALITY_FEATURES_TABLE,
        df=incrementality_features_df,
    )

    print("\nCannibalization input summary")
    print("=" * 80)
    print(
        f"{'input table':<45} "
        f"{GOLD_INCREMENTALITY_FEATURES_TABLE}"
    )
    print(
        f"{'input rows':<45} "
        f"{input_row_count:>12,}"
    )
    print("=" * 80)

    cannibalization_flags_df = add_basic_cannibalization_flags(
        features_df=incrementality_features_df,
    )

    cannibalization_flags_count = cannibalization_flags_df.count()

    print("\nBasic cannibalization risk flags")
    print("=" * 80)
    print(
        f"{'flagged rows':<45} "
        f"{cannibalization_flags_count:>12,}"
    )
    print("=" * 80)

    print("\nCannibalization flag sample")
    cannibalization_flags_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "lift_percentage",
        "net_merchant_profit_amount",
        "total_cost_roas",
        "reward_cost_share_of_incremental_revenue",
        "weak_lift_flag",
        "negative_lift_flag",
        "negative_net_profit_flag",
        "low_total_cost_roas_flag",
        "high_reward_cost_share_flag",
        "positive_spend_lift_but_profit_loss_flag",
        "reward_cost_exceeds_incremental_margin_flag",
        "test_control_spend_near_parity_flag",
    ).show(20, truncate=False)

    print("\nCannibalization flag summary")
    cannibalization_flags_df.select(
        F.sum(F.col("weak_lift_flag").cast("int")).alias(
            "weak_lift_count"
        ),
        F.sum(F.col("negative_lift_flag").cast("int")).alias(
            "negative_lift_count"
        ),
        F.sum(F.col("negative_net_profit_flag").cast("int")).alias(
            "negative_net_profit_count"
        ),
        F.sum(F.col("low_total_cost_roas_flag").cast("int")).alias(
            "low_total_cost_roas_count"
        ),
        F.sum(F.col("high_reward_cost_share_flag").cast("int")).alias(
            "high_reward_cost_share_count"
        ),
        F.sum(F.col("positive_spend_lift_but_profit_loss_flag").cast("int")).alias(
            "positive_spend_lift_but_profit_loss_count"
        ),
        F.sum(F.col("reward_cost_exceeds_incremental_margin_flag").cast("int")).alias(
            "reward_cost_exceeds_incremental_margin_count"
        ),
        F.sum(F.col("test_control_spend_near_parity_flag").cast("int")).alias(
            "test_control_spend_near_parity_count"
        ),
    ).show(truncate=False)

    print("\nCannibalization flag schema")
    cannibalization_flags_df.printSchema()

    cannibalization_score_df = add_weighted_cannibalization_score(
        cannibalization_flags_df=cannibalization_flags_df,
    )

    cannibalization_score_count = cannibalization_score_df.count()

    print("\nWeighted cannibalization risk score")
    print("=" * 80)
    print(
        f"{'scored rows':<45} "
        f"{cannibalization_score_count:>12,}"
    )
    print("=" * 80)

    print("\nCannibalization score sample")
    cannibalization_score_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "lift_percentage",
        "net_merchant_profit_amount",
        "total_cost_roas",
        "negative_net_profit_risk_points",
        "negative_lift_risk_points",
        "weak_lift_risk_points",
        "low_total_cost_roas_risk_points",
        "high_reward_cost_share_risk_points",
        "positive_spend_lift_but_profit_loss_risk_points",
        "reward_cost_exceeds_incremental_margin_risk_points",
        "test_control_spend_near_parity_risk_points",
        "cannibalization_risk_score",
    ).show(20, truncate=False)

    print("\nCannibalization score summary")
    cannibalization_score_df.select(
        F.min("cannibalization_risk_score").alias("min_risk_score"),
        F.avg("cannibalization_risk_score").alias("avg_risk_score"),
        F.max("cannibalization_risk_score").alias("max_risk_score"),
    ).show(truncate=False)

    print("\nTop cannibalization risk rows")
    cannibalization_score_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "lift_percentage",
        "funded_reward_cost_amount",
        "net_merchant_profit_amount",
        "total_cost_roas",
        "cannibalization_risk_score",
    ).orderBy(
        F.col("cannibalization_risk_score").desc(),
        F.col("net_merchant_profit_amount").asc(),
    ).show(20, truncate=False)

    print("\nCannibalization score schema")
    cannibalization_score_df.printSchema()

    classified_risk_df = classify_cannibalization_risk_level(
        cannibalization_score_df=cannibalization_score_df,
    )

    classified_risk_count = classified_risk_df.count()

    print("\nCannibalization risk classification")
    print("=" * 80)
    print(
        f"{'classified risk rows':<45} "
        f"{classified_risk_count:>12,}"
    )
    print("=" * 80)

    print("\nCannibalization risk classification sample")
    classified_risk_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "lift_percentage",
        "net_merchant_profit_amount",
        "total_cost_roas",
        "cannibalization_risk_score",
        "cannibalization_risk_level",
        "requires_merchant_review_flag",
        "requires_offer_redesign_flag",
        "safe_to_scale_flag",
        "cannibalization_action_recommendation",
    ).show(20, truncate=False)

    print("\nCannibalization risk level summary")
    (
        classified_risk_df
        .groupBy("cannibalization_risk_level")
        .count()
        .orderBy("cannibalization_risk_level")
        .show(truncate=False)
    )

    print("\nCannibalization action flag summary")
    classified_risk_df.select(
        F.sum(F.col("requires_merchant_review_flag").cast("int")).alias(
            "requires_merchant_review_count"
        ),
        F.sum(F.col("requires_offer_redesign_flag").cast("int")).alias(
            "requires_offer_redesign_count"
        ),
        F.sum(F.col("safe_to_scale_flag").cast("int")).alias(
            "safe_to_scale_count"
        ),
    ).show(truncate=False)

    print("\nCritical and high cannibalization risk rows")
    classified_risk_df.filter(
        F.col("cannibalization_risk_level").isin("critical", "high")
    ).select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "lift_percentage",
        "funded_reward_cost_amount",
        "net_merchant_profit_amount",
        "total_cost_roas",
        "cannibalization_risk_score",
        "cannibalization_risk_level",
        "cannibalization_action_recommendation",
    ).orderBy(
        F.col("cannibalization_risk_score").desc(),
        F.col("net_merchant_profit_amount").asc(),
    ).show(30, truncate=False)

    print("\nCannibalization risk classification schema")
    classified_risk_df.printSchema()

    risk_reasons_df = add_human_readable_risk_reasons(
        classified_risk_df=classified_risk_df,
    )

    risk_reasons_count = risk_reasons_df.count()

    print("\nCannibalization risk reasons")
    print("=" * 80)
    print(
        f"{'risk reason rows':<45} "
        f"{risk_reasons_count:>12,}"
    )
    print("=" * 80)

    print("\nCannibalization risk reason sample")
    risk_reasons_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "cannibalization_risk_score",
        "cannibalization_risk_level",
        "primary_cannibalization_risk_driver",
        "cannibalization_risk_reason",
        "cannibalization_action_recommendation",
    ).show(20, truncate=False)

    print("\nPrimary cannibalization risk driver summary")
    (
        risk_reasons_df
        .groupBy("primary_cannibalization_risk_driver")
        .count()
        .orderBy(F.col("count").desc())
        .show(truncate=False)
    )

    print("\nCritical cannibalization reason details")
    risk_reasons_df.filter(
        F.col("cannibalization_risk_level") == "critical"
    ).select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "lift_percentage",
        "funded_reward_cost_amount",
        "net_merchant_profit_amount",
        "total_cost_roas",
        "cannibalization_risk_score",
        "primary_cannibalization_risk_driver",
        "cannibalization_risk_detail",
    ).orderBy(
        F.col("cannibalization_risk_score").desc(),
        F.col("net_merchant_profit_amount").asc(),
    ).show(30, truncate=False)

    print("\nCannibalization risk reason schema")
    risk_reasons_df.printSchema()

    final_risk_df = add_cannibalization_pipeline_metadata(
        risk_reasons_df=risk_reasons_df,
        pipeline_run_id=pipeline_run_id,
    )

    final_risk_count = final_risk_df.count()

    print("\nFinal cannibalization risk features")
    print("=" * 80)
    print(
        f"{'final risk rows':<45} "
        f"{final_risk_count:>12,}"
    )
    print("=" * 80)

    print("\nFinal cannibalization risk sample")
    final_risk_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "net_merchant_profit_amount",
        "cannibalization_risk_score",
        "cannibalization_risk_level",
        "primary_cannibalization_risk_driver",
        "requires_merchant_review_flag",
        "requires_offer_redesign_flag",
        "safe_to_scale_flag",
        "cannibalization_pipeline_run_id",
        "cannibalization_rule_version",
        "cannibalization_created_at",
    ).show(20, truncate=False)

    print("\nWriting Gold offer cannibalization risk Delta table")
    print("=" * 80)

    write_and_validate_gold_cannibalization_risk(
        spark=spark,
        risk_df=final_risk_df,
    )

    print("\nGold offer cannibalization risk Delta table written and validated.")

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()