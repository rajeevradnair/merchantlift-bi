"""Build Gold offer incrementality features from test and control groups."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import GOLD_DIR, SILVER_DIR
from merchantlift.spark import create_spark_session_local


INCREMENTALITY_RULE_VERSION = "incrementality_rules_v1"

MATCHED_REDEMPTIONS_TABLE = "fact_matched_offer_redemptions_clean"
CONTROL_TRANSACTIONS_TABLE = "fact_control_group_transactions_clean"
OFFER_SCD_TABLE = "dim_offer_scd"
MERCHANT_SCD_TABLE = "dim_merchant_scd"

GOLD_OFFER_INCREMENTALITY_TABLE = "gold_offer_incrementality"
GOLD_OFFER_INCREMENTALITY_REQUIRED_COLUMNS = (
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
    "average_test_spend_per_cardmember",
    "average_test_reward_per_redemption",
    "control_cardmember_count",
    "control_transaction_count",
    "total_control_spend_amount",
    "average_control_spend_per_cardmember",
    "lift_per_cardmember",
    "lift_direction",
    "test_to_control_spend_ratio",
    "lift_percentage",
    "incremental_revenue_amount",
    "absolute_incremental_revenue_amount",
    "incremental_revenue_direction",
    "estimated_incremental_margin_amount",
    "estimated_incremental_platform_fee_amount",
    "estimated_incremental_value_after_reward",
    "incrementality_pipeline_run_id",
    "incrementality_rule_version",
    "incrementality_created_at",
)

def build_pipeline_run_id() -> str:
    """Create a unique incrementality pipeline run identifier.

    Returns:
        Pipeline run identifier.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"incrementality_run_{timestamp}"


def get_silver_table_path(table_name: str):
    """Return Silver Delta table path.

    Args:
        table_name: Silver table name.

    Returns:
        Silver table path.
    """
    return SILVER_DIR / table_name


def get_gold_table_path(table_name: str):
    """Return Gold Delta table path.

    Args:
        table_name: Gold table name.

    Returns:
        Gold table path.
    """
    return GOLD_DIR / table_name


def read_silver_table(
    spark,
    table_name: str,
) -> DataFrame:
    """Read one Silver Delta table.

    Args:
        spark: Active Spark session.
        table_name: Silver table name.

    Returns:
        Silver DataFrame.
    """
    return (
        spark.read
        .format("delta")
        .load(str(get_silver_table_path(table_name)))
    )

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
        ValueError: If read-back row count does not match expected count.
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

def write_and_validate_gold_incrementality(
    spark,
    incrementality_df: DataFrame,
) -> None:
    """Write and validate the Gold offer incrementality table.

    Args:
        spark: Active Spark session.
        incrementality_df: Enriched incrementality DataFrame.
    """
    validate_gold_output_columns(
        df=incrementality_df,
        table_name=GOLD_OFFER_INCREMENTALITY_TABLE,
        required_columns=GOLD_OFFER_INCREMENTALITY_REQUIRED_COLUMNS,
    )

    expected_row_count = incrementality_df.count()

    write_gold_table(
        df=incrementality_df,
        table_name=GOLD_OFFER_INCREMENTALITY_TABLE,
        partition_column="business_date",
    )

    validate_written_gold_table(
        spark=spark,
        table_name=GOLD_OFFER_INCREMENTALITY_TABLE,
        expected_row_count=expected_row_count,
    )


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
        denominator > 0,
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

def read_incrementality_input_tables(spark) -> dict[str, DataFrame]:
    """Read all Silver inputs required for incrementality.

    Args:
        spark: Active Spark session.

    Returns:
        Dictionary of table name to DataFrame.
    """
    return {
        MATCHED_REDEMPTIONS_TABLE: read_silver_table(
            spark=spark,
            table_name=MATCHED_REDEMPTIONS_TABLE,
        ),
        CONTROL_TRANSACTIONS_TABLE: read_silver_table(
            spark=spark,
            table_name=CONTROL_TRANSACTIONS_TABLE,
        ),
        OFFER_SCD_TABLE: read_silver_table(
            spark=spark,
            table_name=OFFER_SCD_TABLE,
        ),
        MERCHANT_SCD_TABLE: read_silver_table(
            spark=spark,
            table_name=MERCHANT_SCD_TABLE,
        ),
    }

def validate_incrementality_input_contracts(
    input_tables: dict[str, DataFrame],
) -> None:
    """Validate required columns for incrementality inputs.

    Args:
        input_tables: Dictionary of input table name to DataFrame.
    """
    require_columns(
        df=input_tables[MATCHED_REDEMPTIONS_TABLE],
        table_name=MATCHED_REDEMPTIONS_TABLE,
        required_columns=(
            "matched_redemption_id",
            "transaction_id",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "tokenized_cardmember_id",
            "transaction_date",
            "transaction_amount",
            "calculated_reward_amount",
        ),
    )

    require_columns(
        df=input_tables[CONTROL_TRANSACTIONS_TABLE],
        table_name=CONTROL_TRANSACTIONS_TABLE,
        required_columns=(
            "control_transaction_id",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "tokenized_cardmember_id",
            "transaction_date",
            "transaction_amount",
        ),
    )

    require_columns(
        df=input_tables[OFFER_SCD_TABLE],
        table_name=OFFER_SCD_TABLE,
        required_columns=(
            "offer_id",
            "campaign_id",
            "merchant_id",
            "minimum_spend_amount",
            "reward_amount",
            "is_current",
        ),
    )

    require_columns(
        df=input_tables[MERCHANT_SCD_TABLE],
        table_name=MERCHANT_SCD_TABLE,
        required_columns=(
            "merchant_id",
            "merchant_margin_rate",
            "platform_fee_rate",
            "is_current",
        ),
    )

def get_current_scd_rows(
    scd_df: DataFrame,
) -> DataFrame:
    """Return current rows from an SCD table.

    Args:
        scd_df: SCD DataFrame.

    Returns:
        Current SCD rows.
    """
    return scd_df.filter(F.col("is_current") == True)


def build_test_group_spend(
    matched_redemptions_df: DataFrame,
) -> DataFrame:
    """Build test group spend aggregation from matched redemptions.

    Args:
        matched_redemptions_df: Silver matched redemption DataFrame.

    Returns:
        Test group spend aggregation.
    """
    require_columns(
        df=matched_redemptions_df,
        table_name=MATCHED_REDEMPTIONS_TABLE,
        required_columns=(
            "matched_redemption_id",
            "transaction_id",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "tokenized_cardmember_id",
            "transaction_date",
            "transaction_amount",
            "calculated_reward_amount",
        ),
    )

    filtered_test_df = (
        matched_redemptions_df
        .filter(F.col("matched_redemption_id").isNotNull())
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("offer_id").isNotNull())
        .filter(F.col("campaign_id").isNotNull())
        .filter(F.col("merchant_id").isNotNull())
        .filter(F.col("tokenized_cardmember_id").isNotNull())
        .filter(F.col("transaction_date").isNotNull())
        .filter(F.col("transaction_amount").isNotNull())
        .filter(F.col("calculated_reward_amount").isNotNull())
        .filter(F.col("transaction_amount") >= 0)
        .filter(F.col("calculated_reward_amount") >= 0)
    )

    aggregated_df = (
        filtered_test_df
        .groupBy(
            F.col("transaction_date").alias("business_date"),
            "offer_id",
            "campaign_id",
            "merchant_id",
        )
        .agg(
            F.countDistinct("tokenized_cardmember_id").alias(
                "test_cardmember_count"
            ),
            F.countDistinct("transaction_id").alias(
                "test_transaction_count"
            ),
            F.countDistinct("matched_redemption_id").alias(
                "test_redemption_count"
            ),
            F.sum("transaction_amount").alias(
                "total_test_spend_amount"
            ),
            F.sum("calculated_reward_amount").alias(
                "total_test_reward_amount"
            ),
        )
    )

    return (
        aggregated_df
        .withColumn(
            "average_test_spend_per_cardmember",
            safe_divide(
                F.col("total_test_spend_amount"),
                F.col("test_cardmember_count"),
            ),
        )
        .withColumn(
            "average_test_reward_per_redemption",
            safe_divide(
                F.col("total_test_reward_amount"),
                F.col("test_redemption_count"),
            ),
        )
    )

def build_control_group_spend(
    control_transactions_df: DataFrame,
) -> DataFrame:
    """Build control group spend aggregation.

    Args:
        control_transactions_df: Silver control group transactions DataFrame.

    Returns:
        Control group spend aggregation.
    """
    require_columns(
        df=control_transactions_df,
        table_name=CONTROL_TRANSACTIONS_TABLE,
        required_columns=(
            "control_transaction_id",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "tokenized_cardmember_id",
            "transaction_date",
            "transaction_amount",
        ),
    )

    filtered_control_df = (
        control_transactions_df
        .filter(F.col("control_transaction_id").isNotNull())
        .filter(F.col("offer_id").isNotNull())
        .filter(F.col("campaign_id").isNotNull())
        .filter(F.col("merchant_id").isNotNull())
        .filter(F.col("tokenized_cardmember_id").isNotNull())
        .filter(F.col("transaction_date").isNotNull())
        .filter(F.col("transaction_amount").isNotNull())
        .filter(F.col("transaction_amount") >= 0)
    )

    aggregated_df = (
        filtered_control_df
        .groupBy(
            F.col("transaction_date").alias("business_date"),
            "offer_id",
            "campaign_id",
            "merchant_id",
        )
        .agg(
            F.countDistinct("tokenized_cardmember_id").alias(
                "control_cardmember_count"
            ),
            F.countDistinct("control_transaction_id").alias(
                "control_transaction_count"
            ),
            F.sum("transaction_amount").alias(
                "total_control_spend_amount"
            ),
        )
    )

    return aggregated_df.withColumn(
        "average_control_spend_per_cardmember",
        safe_divide(
            F.col("total_control_spend_amount"),
            F.col("control_cardmember_count"),
        ),
    )

def build_test_control_lift(
    test_group_spend_df: DataFrame,
    control_group_spend_df: DataFrame,
) -> DataFrame:
    """Join test/control aggregates and calculate lift per cardmember.

    Args:
        test_group_spend_df: Test group spend aggregation.
        control_group_spend_df: Control group spend aggregation.

    Returns:
        Test/control lift DataFrame.
    """
    join_keys = (
        "business_date",
        "offer_id",
        "campaign_id",
        "merchant_id",
    )

    require_columns(
        df=test_group_spend_df,
        table_name="test_group_spend_df",
        required_columns=(
            *join_keys,
            "test_cardmember_count",
            "test_transaction_count",
            "test_redemption_count",
            "total_test_spend_amount",
            "total_test_reward_amount",
            "average_test_spend_per_cardmember",
            "average_test_reward_per_redemption",
        ),
    )

    require_columns(
        df=control_group_spend_df,
        table_name="control_group_spend_df",
        required_columns=(
            *join_keys,
            "control_cardmember_count",
            "control_transaction_count",
            "total_control_spend_amount",
            "average_control_spend_per_cardmember",
        ),
    )

    return (
        test_group_spend_df.alias("test")
        .join(
            control_group_spend_df.alias("control"),
            list(join_keys),
            "inner",
        )
        .withColumn(
            "lift_per_cardmember",
            F.col("average_test_spend_per_cardmember")
            - F.col("average_control_spend_per_cardmember"),
        )
        .withColumn(
            "lift_direction",
            F.when(
                F.col("lift_per_cardmember") > 0,
                F.lit("positive_lift"),
            )
            .when(
                F.col("lift_per_cardmember") < 0,
                F.lit("negative_lift"),
            )
            .otherwise(F.lit("no_lift")),
        )
        .withColumn(
            "test_to_control_spend_ratio",
            safe_divide(
                F.col("average_test_spend_per_cardmember"),
                F.col("average_control_spend_per_cardmember"),
            ),
        )
        .select(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "test_cardmember_count",
            "test_transaction_count",
            "test_redemption_count",
            "total_test_spend_amount",
            "total_test_reward_amount",
            "average_test_spend_per_cardmember",
            "average_test_reward_per_redemption",
            "control_cardmember_count",
            "control_transaction_count",
            "total_control_spend_amount",
            "average_control_spend_per_cardmember",
            "lift_per_cardmember",
            "lift_direction",
            "test_to_control_spend_ratio",
        )
    )

def calculate_scaled_incremental_revenue(
    test_control_lift_df: DataFrame,
) -> DataFrame:
    """Calculate scaled incremental revenue from test/control lift.

    Args:
        test_control_lift_df: Test/control lift DataFrame.

    Returns:
        DataFrame with scaled incremental revenue metrics.
    """
    require_columns(
        df=test_control_lift_df,
        table_name="test_control_lift_df",
        required_columns=(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "test_cardmember_count",
            "control_cardmember_count",
            "total_test_spend_amount",
            "total_control_spend_amount",
            "average_test_spend_per_cardmember",
            "average_control_spend_per_cardmember",
            "lift_per_cardmember",
            "lift_direction",
            "test_to_control_spend_ratio",
        ),
    )

    return (
        test_control_lift_df
        .withColumn(
            "incremental_revenue_amount",
            F.col("lift_per_cardmember") * F.col("test_cardmember_count"),
        )
        .withColumn(
            "incremental_revenue_direction",
            F.when(
                F.col("incremental_revenue_amount") > 0,
                F.lit("positive_incremental_revenue"),
            )
            .when(
                F.col("incremental_revenue_amount") < 0,
                F.lit("negative_incremental_revenue"),
            )
            .otherwise(F.lit("no_incremental_revenue")),
        )
        .withColumn(
            "lift_percentage",
            safe_divide(
                F.col("lift_per_cardmember"),
                F.col("average_control_spend_per_cardmember"),
            ),
        )
        .withColumn(
            "absolute_incremental_revenue_amount",
            F.abs(F.col("incremental_revenue_amount")),
        )
    )

def enrich_incrementality_with_context(
    scaled_incrementality_df: DataFrame,
    current_offer_scd_df: DataFrame,
    current_merchant_scd_df: DataFrame,
    pipeline_run_id: str,
) -> DataFrame:
    """Enrich incrementality metrics with offer and merchant context.

    Args:
        scaled_incrementality_df: Incrementality metrics with scaled revenue.
        current_offer_scd_df: Current offer SCD rows.
        current_merchant_scd_df: Current merchant SCD rows.
        pipeline_run_id: Incrementality pipeline run identifier.

    Returns:
        Enriched incrementality DataFrame.
    """
    require_columns(
        df=scaled_incrementality_df,
        table_name="scaled_incrementality_df",
        required_columns=(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "test_cardmember_count",
            "test_transaction_count",
            "test_redemption_count",
            "total_test_spend_amount",
            "total_test_reward_amount",
            "average_test_spend_per_cardmember",
            "average_test_reward_per_redemption",
            "control_cardmember_count",
            "control_transaction_count",
            "total_control_spend_amount",
            "average_control_spend_per_cardmember",
            "lift_per_cardmember",
            "lift_direction",
            "test_to_control_spend_ratio",
            "incremental_revenue_amount",
            "incremental_revenue_direction",
            "lift_percentage",
            "absolute_incremental_revenue_amount",
        ),
    )

    require_columns(
        df=current_offer_scd_df,
        table_name=OFFER_SCD_TABLE,
        required_columns=(
            "offer_id",
            "campaign_id",
            "merchant_id",
            "minimum_spend_amount",
            "reward_amount",
            "is_current",
        ),
    )

    require_columns(
        df=current_merchant_scd_df,
        table_name=MERCHANT_SCD_TABLE,
        required_columns=(
            "merchant_id",
            "merchant_margin_rate",
            "platform_fee_rate",
            "is_current",
        ),
    )

    offer_context_df = (
        current_offer_scd_df
        .select(
            "offer_id",
            "campaign_id",
            "merchant_id",
            "minimum_spend_amount",
            "reward_amount",
        )
        .dropDuplicates(["offer_id", "campaign_id", "merchant_id"])
    )

    merchant_context_df = (
        current_merchant_scd_df
        .select(
            "merchant_id",
            "merchant_margin_rate",
            "platform_fee_rate",
        )
        .dropDuplicates(["merchant_id"])
    )

    return (
        scaled_incrementality_df.alias("inc")
        .join(
            offer_context_df.alias("offer"),
            ["offer_id", "campaign_id", "merchant_id"],
            "left",
        )
        .join(
            merchant_context_df.alias("merchant"),
            "merchant_id",
            "left",
        )
        .withColumn(
            "merchant_margin_rate",
            F.coalesce(F.col("merchant_margin_rate"), F.lit(0.0)),
        )
        .withColumn(
            "platform_fee_rate",
            F.coalesce(F.col("platform_fee_rate"), F.lit(0.0)),
        )
        .withColumn(
            "minimum_spend_amount",
            F.coalesce(F.col("minimum_spend_amount"), F.lit(0.0)),
        )
        .withColumn(
            "reward_amount",
            F.coalesce(F.col("reward_amount"), F.lit(0.0)),
        )
        .withColumn(
            "estimated_incremental_margin_amount",
            F.col("incremental_revenue_amount") * F.col("merchant_margin_rate"),
        )
        .withColumn(
            "estimated_incremental_platform_fee_amount",
            F.col("incremental_revenue_amount") * F.col("platform_fee_rate"),
        )
        .withColumn(
            "estimated_incremental_value_after_reward",
            F.col("estimated_incremental_margin_amount")
            - F.col("total_test_reward_amount")
            - F.col("estimated_incremental_platform_fee_amount"),
        )
        .withColumn(
            "incrementality_pipeline_run_id",
            F.lit(pipeline_run_id),
        )
        .withColumn(
            "incrementality_rule_version",
            F.lit(INCREMENTALITY_RULE_VERSION),
        )
        .withColumn(
            "incrementality_created_at",
            F.current_timestamp(),
        )
        .select(
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
            "average_test_spend_per_cardmember",
            "average_test_reward_per_redemption",
            "control_cardmember_count",
            "control_transaction_count",
            "total_control_spend_amount",
            "average_control_spend_per_cardmember",
            "lift_per_cardmember",
            "lift_direction",
            "test_to_control_spend_ratio",
            "lift_percentage",
            "incremental_revenue_amount",
            "absolute_incremental_revenue_amount",
            "incremental_revenue_direction",
            "estimated_incremental_margin_amount",
            "estimated_incremental_platform_fee_amount",
            "estimated_incremental_value_after_reward",
            "incrementality_pipeline_run_id",
            "incrementality_rule_version",
            "incrementality_created_at",
        )
    )

def validate_gold_incrementality_business_rules(
    spark,
) -> tuple[str, int]:
    """Validate business rules for the Gold offer incrementality table.

    Args:
        spark: Active Spark session.

    Returns:
        Table name and validation failure count.
    """
    df = read_gold_table(
        spark=spark,
        table_name=GOLD_OFFER_INCREMENTALITY_TABLE,
    )

    require_columns(
        df=df,
        table_name=GOLD_OFFER_INCREMENTALITY_TABLE,
        required_columns=GOLD_OFFER_INCREMENTALITY_REQUIRED_COLUMNS,
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
            | F.col("test_cardmember_count").isNull()
            | F.col("control_cardmember_count").isNull()
            | F.col("total_test_spend_amount").isNull()
            | F.col("total_control_spend_amount").isNull()
            | F.col("average_test_spend_per_cardmember").isNull()
            | F.col("average_control_spend_per_cardmember").isNull()
            | F.col("lift_per_cardmember").isNull()
            | F.col("incremental_revenue_amount").isNull()
            | F.col("incrementality_pipeline_run_id").isNull()
            | F.col("incrementality_rule_version").isNull()
            | F.col("incrementality_created_at").isNull()
        )
        .count()
    )

    invalid_count_metric_count = (
        df
        .filter(
            (F.col("test_cardmember_count") <= 0)
            | (F.col("control_cardmember_count") <= 0)
            | (F.col("test_transaction_count") < 0)
            | (F.col("test_redemption_count") < 0)
            | (F.col("control_transaction_count") < 0)
        )
        .count()
    )

    negative_spend_or_reward_count = (
        df
        .filter(
            (F.col("total_test_spend_amount") < 0)
            | (F.col("total_test_reward_amount") < 0)
            | (F.col("total_control_spend_amount") < 0)
            | (F.col("average_test_spend_per_cardmember") < 0)
            | (F.col("average_test_reward_per_redemption") < 0)
            | (F.col("average_control_spend_per_cardmember") < 0)
            | (F.col("minimum_spend_amount") < 0)
            | (F.col("reward_amount") < 0)
        )
        .count()
    )

    invalid_rate_count = (
        df
        .filter(
            (F.col("merchant_margin_rate") < 0)
            | (F.col("merchant_margin_rate") > 1)
            | (F.col("platform_fee_rate") < 0)
            | (F.col("platform_fee_rate") > 1)
            | (F.col("test_to_control_spend_ratio") < 0)
        )
        .count()
    )

    invalid_direction_count = (
        df
        .filter(
            ~F.col("lift_direction").isin(
                "positive_lift",
                "negative_lift",
                "no_lift",
            )
            | ~F.col("incremental_revenue_direction").isin(
                "positive_incremental_revenue",
                "negative_incremental_revenue",
                "no_incremental_revenue",
            )
        )
        .count()
    )

    lift_formula_mismatch_count = (
        df
        .withColumn(
            "expected_lift_per_cardmember",
            F.col("average_test_spend_per_cardmember")
            - F.col("average_control_spend_per_cardmember"),
        )
        .filter(
            F.abs(
                F.col("lift_per_cardmember")
                - F.col("expected_lift_per_cardmember")
            ) > 0.01
        )
        .count()
    )

    incremental_revenue_formula_mismatch_count = (
        df
        .withColumn(
            "expected_incremental_revenue_amount",
            F.col("lift_per_cardmember") * F.col("test_cardmember_count"),
        )
        .filter(
            F.abs(
                F.col("incremental_revenue_amount")
                - F.col("expected_incremental_revenue_amount")
            ) > 0.01
        )
        .count()
    )

    lift_percentage_formula_mismatch_count = (
        df
        .withColumn(
            "expected_lift_percentage",
            safe_divide(
                F.col("lift_per_cardmember"),
                F.col("average_control_spend_per_cardmember"),
            ),
        )
        .filter(
            F.abs(
                F.col("lift_percentage")
                - F.col("expected_lift_percentage")
            ) > 0.0001
        )
        .count()
    )

    estimated_value_formula_mismatch_count = (
        df
        .withColumn(
            "expected_incremental_margin_amount",
            F.col("incremental_revenue_amount")
            * F.col("merchant_margin_rate"),
        )
        .withColumn(
            "expected_incremental_platform_fee_amount",
            F.col("incremental_revenue_amount")
            * F.col("platform_fee_rate"),
        )
        .withColumn(
            "expected_incremental_value_after_reward",
            F.col("expected_incremental_margin_amount")
            - F.col("total_test_reward_amount")
            - F.col("expected_incremental_platform_fee_amount"),
        )
        .filter(
            (
                F.abs(
                    F.col("estimated_incremental_margin_amount")
                    - F.col("expected_incremental_margin_amount")
                ) > 0.01
            )
            | (
                F.abs(
                    F.col("estimated_incremental_platform_fee_amount")
                    - F.col("expected_incremental_platform_fee_amount")
                ) > 0.01
            )
            | (
                F.abs(
                    F.col("estimated_incremental_value_after_reward")
                    - F.col("expected_incremental_value_after_reward")
                ) > 0.01
            )
        )
        .count()
    )

    failure_count = (
        duplicate_grain_count
        + null_required_count
        + invalid_count_metric_count
        + negative_spend_or_reward_count
        + invalid_rate_count
        + invalid_direction_count
        + lift_formula_mismatch_count
        + incremental_revenue_formula_mismatch_count
        + lift_percentage_formula_mismatch_count
        + estimated_value_formula_mismatch_count
    )

    print("\nGold offer incrementality business-rule validation")
    print("=" * 80)
    print(f"{'duplicate grain rows':<55} {duplicate_grain_count:>12,}")
    print(f"{'null required rows':<55} {null_required_count:>12,}")
    print(f"{'invalid count metric rows':<55} {invalid_count_metric_count:>12,}")
    print(f"{'negative spend/reward rows':<55} {negative_spend_or_reward_count:>12,}")
    print(f"{'invalid rate rows':<55} {invalid_rate_count:>12,}")
    print(f"{'invalid direction rows':<55} {invalid_direction_count:>12,}")
    print(f"{'lift formula mismatch rows':<55} {lift_formula_mismatch_count:>12,}")
    print(
        f"{'incremental revenue formula mismatch rows':<55} "
        f"{incremental_revenue_formula_mismatch_count:>12,}"
    )
    print(
        f"{'lift percentage formula mismatch rows':<55} "
        f"{lift_percentage_formula_mismatch_count:>12,}"
    )
    print(
        f"{'estimated value formula mismatch rows':<55} "
        f"{estimated_value_formula_mismatch_count:>12,}"
    )
    print(f"{'total validation failures':<55} {failure_count:>12,}")
    print("=" * 80)

    return GOLD_OFFER_INCREMENTALITY_TABLE, failure_count

def validate_all_incrementality_outputs(spark) -> None:
    """Validate all incrementality outputs.

    Args:
        spark: Active Spark session.

    Raises:
        ValueError: If any incrementality output fails validation.
    """
    print("\nValidating incrementality outputs")
    print("=" * 80)

    validation_results = [
        validate_gold_incrementality_business_rules(spark=spark),
    ]

    print("\nIncrementality validation summary")
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
            "Incrementality output validation failed for: "
            + ", ".join(failed_tables)
        )

    print("All incrementality output validations passed.")


def main(spark_session=None) -> None:
    """Run offer incrementality build."""
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-incrementality")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    pipeline_run_id = build_pipeline_run_id()

    print("Starting Gold offer incrementality build")
    print("=" * 80)
    print(f"Silver directory: {SILVER_DIR}")
    print(f"Gold directory: {GOLD_DIR}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Incrementality rule version: {INCREMENTALITY_RULE_VERSION}")
    print(f"Output table: {GOLD_OFFER_INCREMENTALITY_TABLE}")
    print("=" * 80)

    input_tables = read_incrementality_input_tables(spark=spark)

    validate_incrementality_input_contracts(
        input_tables=input_tables,
    )

    input_counts: list[tuple[str, int]] = []

    for table_name, df in input_tables.items():
        row_count = inspect_input_table(
            table_name=table_name,
            df=df,
        )

        input_counts.append(
            (
                table_name,
                row_count,
            )
        )

    print("\nIncrementality input table summary")
    print("=" * 80)
    print(f"{'table':<45} {'rows':>12}")
    print("-" * 80)

    total_input_rows = 0

    for table_name, row_count in input_counts:
        total_input_rows += row_count
        print(f"{table_name:<45} {row_count:>12,}")

    print("-" * 80)
    print(f"{'TOTAL':<45} {total_input_rows:>12,}")
    print("=" * 80)

    current_offer_scd_df = get_current_scd_rows(
        scd_df=input_tables[OFFER_SCD_TABLE],
    )

    current_merchant_scd_df = get_current_scd_rows(
        scd_df=input_tables[MERCHANT_SCD_TABLE],
    )

    print("\nCurrent SCD row counts")
    print("=" * 80)
    print(
        f"{'current offer rows':<45} "
        f"{current_offer_scd_df.count():>12,}"
    )
    print(
        f"{'current merchant rows':<45} "
        f"{current_merchant_scd_df.count():>12,}"
    )
    print("=" * 80)

    test_group_spend_df = build_test_group_spend(
        matched_redemptions_df=input_tables[MATCHED_REDEMPTIONS_TABLE],
    )

    test_group_spend_count = test_group_spend_df.count()

    print("\nTest group spend aggregation")
    print("=" * 80)
    print(
        f"{'test group spend rows':<45} "
        f"{test_group_spend_count:>12,}"
    )
    print("=" * 80)

    print("\nTest group spend sample")
    test_group_spend_df.show(20, truncate=False)

    print("\nTest group spend schema")
    test_group_spend_df.printSchema()

    control_group_spend_df = build_control_group_spend(
        control_transactions_df=input_tables[CONTROL_TRANSACTIONS_TABLE],
    )

    control_group_spend_count = control_group_spend_df.count()

    print("\nControl group spend aggregation")
    print("=" * 80)
    print(
        f"{'control group spend rows':<45} "
        f"{control_group_spend_count:>12,}"
    )
    print("=" * 80)

    print("\nControl group spend sample")
    control_group_spend_df.show(20, truncate=False)

    print("\nControl group spend schema")
    control_group_spend_df.printSchema()

    test_control_lift_df = build_test_control_lift(
        test_group_spend_df=test_group_spend_df,
        control_group_spend_df=control_group_spend_df,
    )

    test_control_lift_count = test_control_lift_df.count()

    print("\nTest/control lift calculation")
    print("=" * 80)
    print(
        f"{'test/control lift rows':<45} "
        f"{test_control_lift_count:>12,}"
    )
    print("=" * 80)

    print("\nTest/control lift sample")
    test_control_lift_df.show(20, truncate=False)

    print("\nTest/control lift schema")
    test_control_lift_df.printSchema()

    scaled_incrementality_df = calculate_scaled_incremental_revenue(
        test_control_lift_df=test_control_lift_df,
    )

    scaled_incrementality_count = scaled_incrementality_df.count()

    print("\nScaled incremental revenue calculation")
    print("=" * 80)
    print(
        f"{'scaled incrementality rows':<45} "
        f"{scaled_incrementality_count:>12,}"
    )
    print("=" * 80)

    print("\nScaled incrementality sample")
    scaled_incrementality_df.show(20, truncate=False)

    print("\nScaled incrementality schema")
    scaled_incrementality_df.printSchema()

    print("\nIncremental revenue direction summary")
    (
        scaled_incrementality_df
        .groupBy("incremental_revenue_direction")
        .count()
        .show(truncate=False)
    )

    enriched_incrementality_df = enrich_incrementality_with_context(
        scaled_incrementality_df=scaled_incrementality_df,
        current_offer_scd_df=current_offer_scd_df,
        current_merchant_scd_df=current_merchant_scd_df,
        pipeline_run_id=pipeline_run_id,
    )

    enriched_incrementality_count = enriched_incrementality_df.count()

    print("\nEnriched incrementality metrics")
    print("=" * 80)
    print(
        f"{'enriched incrementality rows':<45} "
        f"{enriched_incrementality_count:>12,}"
    )
    print("=" * 80)

    print("\nEnriched incrementality sample")
    enriched_incrementality_df.show(20, truncate=False)

    print("\nEnriched incrementality schema")
    enriched_incrementality_df.printSchema()

    print("\nEstimated incremental value direction summary")
    (
        enriched_incrementality_df
        .withColumn(
            "estimated_value_direction",
            F.when(
                F.col("estimated_incremental_value_after_reward") > 0,
                F.lit("positive_estimated_value"),
            )
            .when(
                F.col("estimated_incremental_value_after_reward") < 0,
                F.lit("negative_estimated_value"),
            )
            .otherwise(F.lit("neutral_estimated_value")),
        )
        .groupBy("estimated_value_direction")
        .count()
        .show(truncate=False)
    )

    print("\nWriting Gold incrementality Delta table")
    print("=" * 80)

    write_and_validate_gold_incrementality(
        spark=spark,
        incrementality_df=enriched_incrementality_df,
    )

    print("\nGold incrementality Delta table written and validated.")

    validate_all_incrementality_outputs(spark=spark)

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()