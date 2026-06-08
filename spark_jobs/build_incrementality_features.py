"""Build Gold incrementality profitability features."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import GOLD_DIR
from merchantlift.spark import create_spark_session_local


PROFITABILITY_RULE_VERSION = "incrementality_profitability_rules_v1"

GOLD_OFFER_INCREMENTALITY_TABLE = "gold_offer_incrementality"
GOLD_INCREMENTALITY_FEATURES_TABLE = "gold_incrementality_features"
GOLD_INCREMENTALITY_FEATURES_REQUIRED_COLUMNS = (
    "business_date",
    "offer_id",
    "campaign_id",
    "merchant_id",
    "minimum_spend_amount",
    "reward_amount",
    "merchant_margin_rate",
    "platform_fee_rate",
    "normalized_merchant_margin_rate",
    "normalized_platform_fee_rate",
    "test_cardmember_count",
    "test_transaction_count",
    "test_redemption_count",
    "total_test_spend_amount",
    "total_test_reward_amount",
    "control_cardmember_count",
    "control_transaction_count",
    "total_control_spend_amount",
    "average_test_spend_per_cardmember",
    "average_control_spend_per_cardmember",
    "lift_per_cardmember",
    "lift_direction",
    "lift_percentage",
    "incremental_revenue_amount",
    "incremental_revenue_direction",
    "estimated_incremental_margin_amount",
    "estimated_incremental_cogs_amount",
    "estimated_incremental_platform_fee_amount",
    "platform_fee_revenue_base_amount",
    "funded_reward_cost_amount",
    "net_merchant_profit_amount",
    "net_profit_per_test_cardmember",
    "net_profit_margin_on_incremental_revenue",
    "profitability_status",
    "incremental_revenue_positive_flag",
    "net_profit_positive_flag",
    "profitable_incremental_offer_flag",
    "spend_lift_but_profit_loss_flag",
    "negative_lift_and_unprofitable_flag",
    "profitability_explanation",
    "total_offer_cost_amount",
    "reward_roas",
    "total_cost_roas",
    "margin_roas",
    "net_profit_roas",
    "cost_per_incremental_revenue_dollar",
    "reward_cost_share_of_incremental_revenue",
    "platform_fee_share_of_incremental_revenue",
    "efficiency_status",
    "profitability_pipeline_run_id",
    "profitability_rule_version",
    "profitability_created_at",
)

def build_pipeline_run_id() -> str:
    """Create a unique profitability pipeline run identifier.

    Returns:
        Pipeline run identifier.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"incrementality_features_run_{timestamp}"


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
    df.show(sample_size, truncate=False)

    return row_count

def validate_incrementality_input_contract(
    incrementality_df: DataFrame,
) -> None:
    """Validate required columns for profitability feature input.

    Args:
        incrementality_df: Gold offer incrementality DataFrame.
    """
    require_columns(
        df=incrementality_df,
        table_name=GOLD_OFFER_INCREMENTALITY_TABLE,
        required_columns=(
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
            "estimated_incremental_platform_fee_amount",
            "estimated_incremental_value_after_reward",
        ),
    )

def calculate_profitability_base_metrics(
    incrementality_df: DataFrame,
) -> DataFrame:
    """Calculate margin, COGS proxy, and platform fee metrics.

    Args:
        incrementality_df: Gold offer incrementality DataFrame.

    Returns:
        DataFrame with profitability base metrics.
    """
    validate_incrementality_input_contract(
        incrementality_df=incrementality_df,
    )

    return (
        incrementality_df
        .withColumn(
            "normalized_merchant_margin_rate",
            F.coalesce(F.col("merchant_margin_rate"), F.lit(0.0)),
        )
        .withColumn(
            "normalized_platform_fee_rate",
            F.coalesce(F.col("platform_fee_rate"), F.lit(0.0)),
        )
        .withColumn(
            "estimated_incremental_margin_amount",
            F.col("incremental_revenue_amount")
            * F.col("normalized_merchant_margin_rate"),
        )
        .withColumn(
            "estimated_incremental_cogs_amount",
            F.col("incremental_revenue_amount")
            - F.col("estimated_incremental_margin_amount"),
        )
        .withColumn(
            "platform_fee_revenue_base_amount",
            F.when(
                F.col("incremental_revenue_amount") > 0,
                F.col("incremental_revenue_amount"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "estimated_incremental_platform_fee_amount",
            F.col("platform_fee_revenue_base_amount")
            * F.col("normalized_platform_fee_rate"),
        )
    )

def calculate_net_merchant_profit(
    profitability_base_df: DataFrame,
) -> DataFrame:
    """Calculate net merchant profit after reward cost and platform fee.

    Args:
        profitability_base_df: DataFrame with profitability base metrics.

    Returns:
        DataFrame with net merchant profit metrics.
    """
    require_columns(
        df=profitability_base_df,
        table_name="profitability_base_df",
        required_columns=(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "incremental_revenue_amount",
            "total_test_reward_amount",
            "estimated_incremental_margin_amount",
            "estimated_incremental_platform_fee_amount",
        ),
    )

    return (
        profitability_base_df
        .withColumn(
            "funded_reward_cost_amount",
            F.coalesce(F.col("total_test_reward_amount"), F.lit(0.0)),
        )
        .withColumn(
            "net_merchant_profit_amount",
            F.col("estimated_incremental_margin_amount")
            - F.col("funded_reward_cost_amount")
            - F.col("estimated_incremental_platform_fee_amount"),
        )
        .withColumn(
            "net_profit_per_test_cardmember",
            safe_divide(
                F.col("net_merchant_profit_amount"),
                F.col("test_cardmember_count"),
            ),
        )
        .withColumn(
            "net_profit_margin_on_incremental_revenue",
            safe_divide(
                F.col("net_merchant_profit_amount"),
                F.col("incremental_revenue_amount"),
            ),
        )
    )

def classify_profitability(
    net_profit_df: DataFrame,
) -> DataFrame:
    """Classify offer profitability using net merchant profit.

    Args:
        net_profit_df: DataFrame with net merchant profit metrics.

    Returns:
        DataFrame with profitability classifications.
    """
    require_columns(
        df=net_profit_df,
        table_name="net_profit_df",
        required_columns=(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "incremental_revenue_amount",
            "incremental_revenue_direction",
            "lift_direction",
            "total_test_reward_amount",
            "funded_reward_cost_amount",
            "estimated_incremental_margin_amount",
            "estimated_incremental_platform_fee_amount",
            "net_merchant_profit_amount",
            "net_profit_per_test_cardmember",
            "net_profit_margin_on_incremental_revenue",
        ),
    )

    return (
        net_profit_df
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
        .withColumn(
            "incremental_revenue_positive_flag",
            F.col("incremental_revenue_amount") > 0,
        )
        .withColumn(
            "net_profit_positive_flag",
            F.col("net_merchant_profit_amount") > 0,
        )
        .withColumn(
            "profitable_incremental_offer_flag",
            (F.col("incremental_revenue_amount") > 0)
            & (F.col("net_merchant_profit_amount") > 0),
        )
        .withColumn(
            "spend_lift_but_profit_loss_flag",
            (F.col("incremental_revenue_amount") > 0)
            & (F.col("net_merchant_profit_amount") < 0),
        )
        .withColumn(
            "negative_lift_and_unprofitable_flag",
            (F.col("incremental_revenue_amount") < 0)
            & (F.col("net_merchant_profit_amount") < 0),
        )
        .withColumn(
            "profitability_explanation",
            F.when(
                F.col("profitable_incremental_offer_flag"),
                F.lit(
                    "Offer produced positive incremental revenue and positive net merchant profit."
                ),
            )
            .when(
                F.col("spend_lift_but_profit_loss_flag"),
                F.lit(
                    "Offer produced incremental revenue, but reward cost and platform fee exceeded merchant margin."
                ),
            )
            .when(
                F.col("negative_lift_and_unprofitable_flag"),
                F.lit(
                    "Offer underperformed control group and produced negative net merchant profit."
                ),
            )
            .when(
                F.col("profitability_status") == "break_even",
                F.lit(
                    "Offer approximately broke even after reward cost and platform fee."
                ),
            )
            .otherwise(
                F.lit(
                    "Offer profitability requires review."
                ),
            ),
        )
    )

def add_roas_and_efficiency_metrics(
    classified_profitability_df: DataFrame,
) -> DataFrame:
    """Add ROAS and offer efficiency metrics.

    Args:
        classified_profitability_df: DataFrame with profitability classifications.

    Returns:
        DataFrame with ROAS and efficiency metrics.
    """
    require_columns(
        df=classified_profitability_df,
        table_name="classified_profitability_df",
        required_columns=(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "incremental_revenue_amount",
            "funded_reward_cost_amount",
            "estimated_incremental_platform_fee_amount",
            "estimated_incremental_margin_amount",
            "net_merchant_profit_amount",
            "profitability_status",
        ),
    )

    return (
        classified_profitability_df
        .withColumn(
            "total_offer_cost_amount",
            F.col("funded_reward_cost_amount")
            + F.col("estimated_incremental_platform_fee_amount"),
        )
        .withColumn(
            "reward_roas",
            safe_divide(
                F.col("incremental_revenue_amount"),
                F.col("funded_reward_cost_amount"),
            ),
        )
        .withColumn(
            "total_cost_roas",
            safe_divide(
                F.col("incremental_revenue_amount"),
                F.col("total_offer_cost_amount"),
            ),
        )
        .withColumn(
            "margin_roas",
            safe_divide(
                F.col("estimated_incremental_margin_amount"),
                F.col("total_offer_cost_amount"),
            ),
        )
        .withColumn(
            "net_profit_roas",
            safe_divide(
                F.col("net_merchant_profit_amount"),
                F.col("total_offer_cost_amount"),
            ),
        )
        .withColumn(
            "cost_per_incremental_revenue_dollar",
            safe_divide(
                F.col("total_offer_cost_amount"),
                F.col("incremental_revenue_amount"),
            ),
        )
        .withColumn(
            "reward_cost_share_of_incremental_revenue",
            safe_divide(
                F.col("funded_reward_cost_amount"),
                F.col("incremental_revenue_amount"),
            ),
        )
        .withColumn(
            "platform_fee_share_of_incremental_revenue",
            safe_divide(
                F.col("estimated_incremental_platform_fee_amount"),
                F.col("incremental_revenue_amount"),
            ),
        )
        .withColumn(
            "efficiency_status",
            F.when(
                (F.col("net_merchant_profit_amount") > 0)
                & (F.col("total_cost_roas") >= 5),
                F.lit("highly_efficient"),
            )
            .when(
                (F.col("net_merchant_profit_amount") > 0)
                & (F.col("total_cost_roas") >= 2),
                F.lit("efficient"),
            )
            .when(
                F.col("net_merchant_profit_amount") > 0,
                F.lit("profitable_but_low_efficiency"),
            )
            .when(
                (F.col("incremental_revenue_amount") > 0)
                & (F.col("net_merchant_profit_amount") < 0),
                F.lit("revenue_positive_but_profit_negative"),
            )
            .when(
                F.col("incremental_revenue_amount") < 0,
                F.lit("negative_incrementality"),
            )
            .otherwise(F.lit("needs_review")),
        )
    )

def add_profitability_pipeline_metadata(
    efficiency_metrics_df: DataFrame,
    pipeline_run_id: str,
) -> DataFrame:
    """Add pipeline metadata to profitability features.

    Args:
        efficiency_metrics_df: DataFrame with profitability and efficiency metrics.
        pipeline_run_id: Profitability pipeline run identifier.

    Returns:
        DataFrame with pipeline metadata.
    """
    return (
        efficiency_metrics_df
        .withColumn(
            "profitability_pipeline_run_id",
            F.lit(pipeline_run_id),
        )
        .withColumn(
            "profitability_rule_version",
            F.lit(PROFITABILITY_RULE_VERSION),
        )
        .withColumn(
            "profitability_created_at",
            F.current_timestamp(),
        )
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

def write_and_validate_gold_incrementality_features(
    spark,
    features_df: DataFrame,
) -> None:
    """Write and validate Gold incrementality feature table.

    Args:
        spark: Active Spark session.
        features_df: Final incrementality feature DataFrame.
    """
    validate_gold_output_columns(
        df=features_df,
        table_name=GOLD_INCREMENTALITY_FEATURES_TABLE,
        required_columns=GOLD_INCREMENTALITY_FEATURES_REQUIRED_COLUMNS,
    )

    expected_row_count = features_df.count()

    write_gold_table(
        df=features_df,
        table_name=GOLD_INCREMENTALITY_FEATURES_TABLE,
        partition_column="business_date",
    )

    validate_written_gold_table(
        spark=spark,
        table_name=GOLD_INCREMENTALITY_FEATURES_TABLE,
        expected_row_count=expected_row_count,
    )

def validate_gold_incrementality_features_business_rules(
    spark,
) -> tuple[str, int]:
    """Validate business rules for Gold incrementality features.

    Args:
        spark: Active Spark session.

    Returns:
        Table name and validation failure count.
    """
    df = read_gold_table(
        spark=spark,
        table_name=GOLD_INCREMENTALITY_FEATURES_TABLE,
    )

    require_columns(
        df=df,
        table_name=GOLD_INCREMENTALITY_FEATURES_TABLE,
        required_columns=GOLD_INCREMENTALITY_FEATURES_REQUIRED_COLUMNS,
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
            | F.col("funded_reward_cost_amount").isNull()
            | F.col("estimated_incremental_margin_amount").isNull()
            | F.col("estimated_incremental_platform_fee_amount").isNull()
            | F.col("net_merchant_profit_amount").isNull()
            | F.col("profitability_status").isNull()
            | F.col("efficiency_status").isNull()
            | F.col("profitability_pipeline_run_id").isNull()
            | F.col("profitability_rule_version").isNull()
            | F.col("profitability_created_at").isNull()
        )
        .count()
    )

    invalid_rate_count = (
        df
        .filter(
            (F.col("normalized_merchant_margin_rate") < 0)
            | (F.col("normalized_merchant_margin_rate") > 1)
            | (F.col("normalized_platform_fee_rate") < 0)
            | (F.col("normalized_platform_fee_rate") > 1)
            | (F.col("merchant_margin_rate") < 0)
            | (F.col("merchant_margin_rate") > 1)
            | (F.col("platform_fee_rate") < 0)
            | (F.col("platform_fee_rate") > 1)
        )
        .count()
    )

    negative_cost_count = (
        df
        .filter(
            (F.col("funded_reward_cost_amount") < 0)
            | (F.col("total_test_reward_amount") < 0)
            | (F.col("total_offer_cost_amount") < 0)
            | (F.col("minimum_spend_amount") < 0)
            | (F.col("reward_amount") < 0)
        )
        .count()
    )

    invalid_profitability_status_count = (
        df
        .filter(
            ~F.col("profitability_status").isin(
                "profitable",
                "unprofitable",
                "break_even",
            )
        )
        .count()
    )

    invalid_efficiency_status_count = (
        df
        .filter(
            ~F.col("efficiency_status").isin(
                "highly_efficient",
                "efficient",
                "profitable_but_low_efficiency",
                "revenue_positive_but_profit_negative",
                "negative_incrementality",
                "needs_review",
            )
        )
        .count()
    )

    margin_formula_mismatch_count = (
        df
        .withColumn(
            "expected_incremental_margin_amount",
            F.col("incremental_revenue_amount")
            * F.col("normalized_merchant_margin_rate"),
        )
        .filter(
            F.abs(
                F.col("estimated_incremental_margin_amount")
                - F.col("expected_incremental_margin_amount")
            ) > 0.01
        )
        .count()
    )

    cogs_formula_mismatch_count = (
        df
        .withColumn(
            "expected_incremental_cogs_amount",
            F.col("incremental_revenue_amount")
            - F.col("estimated_incremental_margin_amount"),
        )
        .filter(
            F.abs(
                F.col("estimated_incremental_cogs_amount")
                - F.col("expected_incremental_cogs_amount")
            ) > 0.01
        )
        .count()
    )

    platform_fee_formula_mismatch_count = (
        df
        .withColumn(
            "expected_platform_fee_revenue_base_amount",
            F.when(
                F.col("incremental_revenue_amount") > 0,
                F.col("incremental_revenue_amount"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "expected_platform_fee_amount",
            F.col("expected_platform_fee_revenue_base_amount")
            * F.col("normalized_platform_fee_rate"),
        )
        .filter(
            (
                F.abs(
                    F.col("platform_fee_revenue_base_amount")
                    - F.col("expected_platform_fee_revenue_base_amount")
                ) > 0.01
            )
            | (
                F.abs(
                    F.col("estimated_incremental_platform_fee_amount")
                    - F.col("expected_platform_fee_amount")
                ) > 0.01
            )
        )
        .count()
    )

    net_profit_formula_mismatch_count = (
        df
        .withColumn(
            "expected_net_merchant_profit_amount",
            F.col("estimated_incremental_margin_amount")
            - F.col("funded_reward_cost_amount")
            - F.col("estimated_incremental_platform_fee_amount"),
        )
        .filter(
            F.abs(
                F.col("net_merchant_profit_amount")
                - F.col("expected_net_merchant_profit_amount")
            ) > 0.01
        )
        .count()
    )

    total_offer_cost_formula_mismatch_count = (
        df
        .withColumn(
            "expected_total_offer_cost_amount",
            F.col("funded_reward_cost_amount")
            + F.col("estimated_incremental_platform_fee_amount"),
        )
        .filter(
            F.abs(
                F.col("total_offer_cost_amount")
                - F.col("expected_total_offer_cost_amount")
            ) > 0.01
        )
        .count()
    )

    roas_formula_mismatch_count = (
        df
        .withColumn(
            "expected_reward_roas",
            safe_divide(
                F.col("incremental_revenue_amount"),
                F.col("funded_reward_cost_amount"),
            ),
        )
        .withColumn(
            "expected_total_cost_roas",
            safe_divide(
                F.col("incremental_revenue_amount"),
                F.col("total_offer_cost_amount"),
            ),
        )
        .withColumn(
            "expected_margin_roas",
            safe_divide(
                F.col("estimated_incremental_margin_amount"),
                F.col("total_offer_cost_amount"),
            ),
        )
        .withColumn(
            "expected_net_profit_roas",
            safe_divide(
                F.col("net_merchant_profit_amount"),
                F.col("total_offer_cost_amount"),
            ),
        )
        .filter(
            (F.abs(F.col("reward_roas") - F.col("expected_reward_roas")) > 0.0001)
            | (F.abs(F.col("total_cost_roas") - F.col("expected_total_cost_roas")) > 0.0001)
            | (F.abs(F.col("margin_roas") - F.col("expected_margin_roas")) > 0.0001)
            | (F.abs(F.col("net_profit_roas") - F.col("expected_net_profit_roas")) > 0.0001)
        )
        .count()
    )

    profitability_label_mismatch_count = (
        df
        .withColumn(
            "expected_profitability_status",
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
        .filter(
            F.col("profitability_status")
            != F.col("expected_profitability_status")
        )
        .count()
    )

    decision_flag_mismatch_count = (
        df
        .withColumn(
            "expected_profitable_incremental_offer_flag",
            (F.col("incremental_revenue_amount") > 0)
            & (F.col("net_merchant_profit_amount") > 0),
        )
        .withColumn(
            "expected_spend_lift_but_profit_loss_flag",
            (F.col("incremental_revenue_amount") > 0)
            & (F.col("net_merchant_profit_amount") < 0),
        )
        .withColumn(
            "expected_negative_lift_and_unprofitable_flag",
            (F.col("incremental_revenue_amount") < 0)
            & (F.col("net_merchant_profit_amount") < 0),
        )
        .filter(
            (F.col("profitable_incremental_offer_flag")
             != F.col("expected_profitable_incremental_offer_flag"))
            | (F.col("spend_lift_but_profit_loss_flag")
               != F.col("expected_spend_lift_but_profit_loss_flag"))
            | (F.col("negative_lift_and_unprofitable_flag")
               != F.col("expected_negative_lift_and_unprofitable_flag"))
        )
        .count()
    )

    failure_count = (
        duplicate_grain_count
        + null_required_count
        + invalid_rate_count
        + negative_cost_count
        + invalid_profitability_status_count
        + invalid_efficiency_status_count
        + margin_formula_mismatch_count
        + cogs_formula_mismatch_count
        + platform_fee_formula_mismatch_count
        + net_profit_formula_mismatch_count
        + total_offer_cost_formula_mismatch_count
        + roas_formula_mismatch_count
        + profitability_label_mismatch_count
        + decision_flag_mismatch_count
    )

    print("\nGold incrementality features business-rule validation")
    print("=" * 80)
    print(f"{'duplicate grain rows':<55} {duplicate_grain_count:>12,}")
    print(f"{'null required rows':<55} {null_required_count:>12,}")
    print(f"{'invalid rate rows':<55} {invalid_rate_count:>12,}")
    print(f"{'negative cost rows':<55} {negative_cost_count:>12,}")
    print(f"{'invalid profitability status rows':<55} {invalid_profitability_status_count:>12,}")
    print(f"{'invalid efficiency status rows':<55} {invalid_efficiency_status_count:>12,}")
    print(f"{'margin formula mismatch rows':<55} {margin_formula_mismatch_count:>12,}")
    print(f"{'COGS formula mismatch rows':<55} {cogs_formula_mismatch_count:>12,}")
    print(f"{'platform fee formula mismatch rows':<55} {platform_fee_formula_mismatch_count:>12,}")
    print(f"{'net profit formula mismatch rows':<55} {net_profit_formula_mismatch_count:>12,}")
    print(f"{'total offer cost formula mismatch rows':<55} {total_offer_cost_formula_mismatch_count:>12,}")
    print(f"{'ROAS formula mismatch rows':<55} {roas_formula_mismatch_count:>12,}")
    print(f"{'profitability label mismatch rows':<55} {profitability_label_mismatch_count:>12,}")
    print(f"{'decision flag mismatch rows':<55} {decision_flag_mismatch_count:>12,}")
    print(f"{'total validation failures':<55} {failure_count:>12,}")
    print("=" * 80)

    return GOLD_INCREMENTALITY_FEATURES_TABLE, failure_count


def validate_all_profitability_outputs(spark) -> None:
    """Validate all profitability feature outputs.

    Args:
        spark: Active Spark session.

    Raises:
        ValueError: If any profitability output fails validation.
    """
    print("\nValidating profitability outputs")
    print("=" * 80)

    validation_results = [
        validate_gold_incrementality_features_business_rules(spark=spark),
    ]

    print("\nProfitability validation summary")
    print("=" * 80)
    print(f"{'table':<40} {'failures':>12} {'status':>12}")
    print("-" * 80)

    failed_tables = []

    for table_name, failure_count in validation_results:
        status = "PASSED" if failure_count == 0 else "FAILED"

        if failure_count > 0:
            failed_tables.append(table_name)

        print(f"{table_name:<40} {failure_count:>12,} {status:>12}")

    print("=" * 80)

    if failed_tables:
        raise ValueError(
            "Profitability output validation failed for: "
            + ", ".join(failed_tables)
        )

    print("All profitability output validations passed.")



def main(spark_session=None) -> None:
    """Run incrementality profitability feature build."""
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-merchant-economic")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    pipeline_run_id = build_pipeline_run_id()

    print("Starting Gold incrementality profitability feature build")
    print("=" * 80)
    print(f"Gold directory: {GOLD_DIR}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Profitability rule version: {PROFITABILITY_RULE_VERSION}")
    print(f"Input table: {GOLD_OFFER_INCREMENTALITY_TABLE}")
    print(f"Output table: {GOLD_INCREMENTALITY_FEATURES_TABLE}")
    print("=" * 80)

    incrementality_df = read_gold_table(
        spark=spark,
        table_name=GOLD_OFFER_INCREMENTALITY_TABLE,
    )

    validate_incrementality_input_contract(
        incrementality_df=incrementality_df,
    )

    incrementality_row_count = inspect_input_table(
        table_name=GOLD_OFFER_INCREMENTALITY_TABLE,
        df=incrementality_df,
    )

    print("\nProfitability input summary")
    print("=" * 80)
    print(
        f"{'input table':<45} "
        f"{GOLD_OFFER_INCREMENTALITY_TABLE}"
    )
    print(
        f"{'input rows':<45} "
        f"{incrementality_row_count:>12,}"
    )
    print("=" * 80)

    profitability_base_df = calculate_profitability_base_metrics(
        incrementality_df=incrementality_df,
    )

    profitability_base_count = profitability_base_df.count()

    print("\nProfitability base metrics")
    print("=" * 80)
    print(
        f"{'profitability base rows':<45} "
        f"{profitability_base_count:>12,}"
    )
    print("=" * 80)

    print("\nProfitability base sample")
    profitability_base_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "merchant_margin_rate",
        "platform_fee_rate",
        "normalized_merchant_margin_rate",
        "normalized_platform_fee_rate",
        "estimated_incremental_margin_amount",
        "estimated_incremental_cogs_amount",
        "estimated_incremental_platform_fee_amount",
        "total_test_reward_amount",
    ).show(20, truncate=False)

    print("\nProfitability base schema")
    profitability_base_df.printSchema()

    net_profit_df = calculate_net_merchant_profit(
        profitability_base_df=profitability_base_df,
    )

    net_profit_count = net_profit_df.count()

    print("\nNet merchant profit metrics")
    print("=" * 80)
    print(
        f"{'net profit rows':<45} "
        f"{net_profit_count:>12,}"
    )
    print("=" * 80)

    print("\nNet merchant profit sample")
    net_profit_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "estimated_incremental_margin_amount",
        "funded_reward_cost_amount",
        "estimated_incremental_platform_fee_amount",
        "net_merchant_profit_amount",
        "net_profit_per_test_cardmember",
        "net_profit_margin_on_incremental_revenue",
    ).show(20, truncate=False)

    print("\nNet merchant profit schema")
    net_profit_df.printSchema()

    classified_profitability_df = classify_profitability(
        net_profit_df=net_profit_df,
    )

    classified_profitability_count = classified_profitability_df.count()

    print("\nProfitability classification")
    print("=" * 80)
    print(
        f"{'classified profitability rows':<45} "
        f"{classified_profitability_count:>12,}"
    )
    print("=" * 80)

    print("\nProfitability classification sample")
    classified_profitability_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "net_merchant_profit_amount",
        "profitability_status",
        "profitable_incremental_offer_flag",
        "spend_lift_but_profit_loss_flag",
        "negative_lift_and_unprofitable_flag",
        "profitability_explanation",
    ).show(20, truncate=False)

    print("\nProfitability status summary")
    (
        classified_profitability_df
        .groupBy("profitability_status")
        .count()
        .show(truncate=False)
    )

    print("\nProfitability decision flag summary")
    classified_profitability_df.select(
        F.sum(F.col("profitable_incremental_offer_flag").cast("int")).alias(
            "profitable_incremental_offer_count"
        ),
        F.sum(F.col("spend_lift_but_profit_loss_flag").cast("int")).alias(
            "spend_lift_but_profit_loss_count"
        ),
        F.sum(F.col("negative_lift_and_unprofitable_flag").cast("int")).alias(
            "negative_lift_and_unprofitable_count"
        ),
    ).show(truncate=False)

    print("\nProfitability classification schema")
    classified_profitability_df.printSchema()

    efficiency_metrics_df = add_roas_and_efficiency_metrics(
        classified_profitability_df=classified_profitability_df,
    )

    efficiency_metrics_count = efficiency_metrics_df.count()

    print("\nROAS and efficiency metrics")
    print("=" * 80)
    print(
        f"{'efficiency metric rows':<45} "
        f"{efficiency_metrics_count:>12,}"
    )
    print("=" * 80)

    print("\nROAS and efficiency sample")
    efficiency_metrics_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "funded_reward_cost_amount",
        "estimated_incremental_platform_fee_amount",
        "total_offer_cost_amount",
        "net_merchant_profit_amount",
        "reward_roas",
        "total_cost_roas",
        "margin_roas",
        "net_profit_roas",
        "cost_per_incremental_revenue_dollar",
        "efficiency_status",
    ).show(20, truncate=False)

    print("\nEfficiency status summary")
    (
        efficiency_metrics_df
        .groupBy("efficiency_status")
        .count()
        .show(truncate=False)
    )

    print("\nROAS and efficiency schema")
    efficiency_metrics_df.printSchema()

    final_features_df = add_profitability_pipeline_metadata(
        efficiency_metrics_df=efficiency_metrics_df,
        pipeline_run_id=pipeline_run_id,
    )

    final_features_count = final_features_df.count()

    print("\nFinal incrementality profitability features")
    print("=" * 80)
    print(
        f"{'final feature rows':<45} "
        f"{final_features_count:>12,}"
    )
    print("=" * 80)

    print("\nFinal feature sample")
    final_features_df.select(
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
        "incremental_revenue_amount",
        "net_merchant_profit_amount",
        "profitability_status",
        "total_cost_roas",
        "net_profit_roas",
        "efficiency_status",
        "profitability_pipeline_run_id",
        "profitability_rule_version",
        "profitability_created_at",
    ).show(20, truncate=False)

    print("\nWriting Gold incrementality features Delta table")
    print("=" * 80)

    write_and_validate_gold_incrementality_features(
        spark=spark,
        features_df=final_features_df,
    )

    print("\nGold incrementality features Delta table written and validated.")

    validate_all_profitability_outputs(spark=spark)

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()