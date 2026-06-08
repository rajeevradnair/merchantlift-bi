"""Build Gold merchant and offer daily economics marts."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import GOLD_DIR, SILVER_DIR
from merchantlift.spark import create_spark_session_local


GOLD_RULE_VERSION = "merchant_economics_rules_v1"

TRANSACTIONS_TABLE = "fact_transactions_clean"
MATCHED_REDEMPTIONS_TABLE = "fact_matched_offer_redemptions_clean"
MERCHANT_SCD_TABLE = "dim_merchant_scd"
OFFER_SCD_TABLE = "dim_offer_scd"
CAMPAIGN_SCD_TABLE = "dim_campaign_scd"

GOLD_MERCHANT_DAILY_TABLE = "gold_merchant_daily"
GOLD_OFFER_DAILY_TABLE = "gold_offer_daily"

GOLD_MERCHANT_DAILY_REQUIRED_COLUMNS = (
    "business_date",
    "merchant_id",
    "transaction_count",
    "gross_spend_amount",
    "average_transaction_amount",
    "matched_redemption_count",
    "redeemed_transaction_count",
    "redemption_rate",
    "reward_cost_amount",
    "average_reward_amount",
    "merchant_margin_rate",
    "platform_fee_rate",
    "platform_fee_amount",
    "estimated_merchant_margin_amount",
    "merchant_net_after_reward",
    "gold_pipeline_run_id",
    "gold_rule_version",
    "gold_created_at",
)

GOLD_OFFER_DAILY_REQUIRED_COLUMNS = (
    "business_date",
    "offer_id",
    "campaign_id",
    "merchant_id",
    "campaign_name",
    "campaign_start_date",
    "campaign_end_date",
    "minimum_spend_amount",
    "reward_amount",
    "matched_redemption_count",
    "redeemed_transaction_count",
    "gross_redeemed_spend_amount",
    "reward_cost_amount",
    "average_redeemed_transaction_amount",
    "average_reward_amount",
    "reward_to_redeemed_spend_ratio",
    "gold_pipeline_run_id",
    "gold_rule_version",
    "gold_created_at",
)




def build_pipeline_run_id() -> str:
    """Create a unique Gold pipeline run identifier.

    Returns:
        Pipeline run identifier.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"merchant_economics_run_{timestamp}"


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

def require_columns(
    df: DataFrame,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    """Validate required columns exist in a DataFrame.

    Args:
        df: DataFrame to validate.
        table_name: Human-readable table name.
        required_columns: Required column names.

    Raises:
        ValueError: If any required columns are missing.
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
    """Print schema, row count, and sample rows for one input table.

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

def read_gold_input_tables(spark) -> dict[str, DataFrame]:
    """Read all Silver/SCD inputs required for Gold economics marts.

    Args:
        spark: Active Spark session.

    Returns:
        Dictionary of table name to DataFrame.
    """
    return {
        TRANSACTIONS_TABLE: read_silver_table(
            spark=spark,
            table_name=TRANSACTIONS_TABLE,
        ),
        MATCHED_REDEMPTIONS_TABLE: read_silver_table(
            spark=spark,
            table_name=MATCHED_REDEMPTIONS_TABLE,
        ),
        MERCHANT_SCD_TABLE: read_silver_table(
            spark=spark,
            table_name=MERCHANT_SCD_TABLE,
        ),
        OFFER_SCD_TABLE: read_silver_table(
            spark=spark,
            table_name=OFFER_SCD_TABLE,
        ),
        CAMPAIGN_SCD_TABLE: read_silver_table(
            spark=spark,
            table_name=CAMPAIGN_SCD_TABLE,
        ),
    }

def validate_gold_input_contracts(
    input_tables: dict[str, DataFrame],
) -> None:
    """Validate that Gold input tables contain required columns.

    Args:
        input_tables: Dictionary of input table name to DataFrame.
    """
    require_columns(
        df=input_tables[TRANSACTIONS_TABLE],
        table_name=TRANSACTIONS_TABLE,
        required_columns=(
            "transaction_id",
            "merchant_id",
            "transaction_date",
            "transaction_amount",
        ),
    )

    require_columns(
        df=input_tables[MATCHED_REDEMPTIONS_TABLE],
        table_name=MATCHED_REDEMPTIONS_TABLE,
        required_columns=(
            "matched_redemption_id",
            "transaction_id",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "transaction_date",
            "calculated_reward_amount",
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
        df=input_tables[CAMPAIGN_SCD_TABLE],
        table_name=CAMPAIGN_SCD_TABLE,
        required_columns=(
            "campaign_id",
            "campaign_name",
            "campaign_start_date",
            "campaign_end_date",
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


def build_merchant_daily_spend(
    transactions_df: DataFrame,
) -> DataFrame:
    """Build daily merchant spend aggregation.

    Args:
        transactions_df: Silver transactions DataFrame.

    Returns:
        Merchant daily spend DataFrame.
    """
    require_columns(
        df=transactions_df,
        table_name=TRANSACTIONS_TABLE,
        required_columns=(
            "transaction_id",
            "merchant_id",
            "transaction_date",
            "transaction_amount",
        ),
    )

    filtered_transactions_df = (
        transactions_df
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("merchant_id").isNotNull())
        .filter(F.col("transaction_date").isNotNull())
        .filter(F.col("transaction_amount").isNotNull())
        .filter(F.col("transaction_amount") >= 0)
    )

    return (
        filtered_transactions_df
        .groupBy(
            F.col("transaction_date").alias("business_date"),
            F.col("merchant_id"),
        )
        .agg(
            F.countDistinct("transaction_id").alias("transaction_count"),
            F.sum("transaction_amount").alias("gross_spend_amount"),
            F.avg("transaction_amount").alias("average_transaction_amount"),
        )
    )

def build_merchant_daily_rewards(
    matched_redemptions_df: DataFrame,
) -> DataFrame:
    """Build daily merchant reward and redemption aggregation.

    Args:
        matched_redemptions_df: Silver matched redemption DataFrame.

    Returns:
        Merchant daily reward aggregation DataFrame.
    """
    require_columns(
        df=matched_redemptions_df,
        table_name=MATCHED_REDEMPTIONS_TABLE,
        required_columns=(
            "matched_redemption_id",
            "transaction_id",
            "merchant_id",
            "transaction_date",
            "calculated_reward_amount",
        ),
    )

    filtered_redemptions_df = (
        matched_redemptions_df
        .filter(F.col("matched_redemption_id").isNotNull())
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("merchant_id").isNotNull())
        .filter(F.col("transaction_date").isNotNull())
        .filter(F.col("calculated_reward_amount").isNotNull())
        .filter(F.col("calculated_reward_amount") >= 0)
    )

    return (
        filtered_redemptions_df
        .groupBy(
            F.col("transaction_date").alias("business_date"),
            F.col("merchant_id"),
        )
        .agg(
            F.countDistinct("matched_redemption_id").alias(
                "matched_redemption_count"
            ),
            F.countDistinct("transaction_id").alias(
                "redeemed_transaction_count"
            ),
            F.sum("calculated_reward_amount").alias(
                "reward_cost_amount"
            ),
            F.avg("calculated_reward_amount").alias(
                "average_reward_amount"
            ),
        )
    )


def write_gold_table(
    df: DataFrame,
    table_name: str,
    partition_column: str | None = None,
) -> None:
    """Write one Gold Delta table.

    Args:
        df: DataFrame to write.
        table_name: Gold table name.
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


def build_merchant_daily_economics(
    merchant_daily_spend_df: DataFrame,
    merchant_daily_rewards_df: DataFrame,
    current_merchant_scd_df: DataFrame,
    pipeline_run_id: str,
) -> DataFrame:
    """Build Gold merchant daily economics mart.

    Args:
        merchant_daily_spend_df: Daily merchant spend aggregation.
        merchant_daily_rewards_df: Daily merchant reward aggregation.
        current_merchant_scd_df: Current merchant SCD rows.
        pipeline_run_id: Gold pipeline run identifier.

    Returns:
        Gold merchant daily economics DataFrame.
    """
    require_columns(
        df=merchant_daily_spend_df,
        table_name="merchant_daily_spend_df",
        required_columns=(
            "business_date",
            "merchant_id",
            "transaction_count",
            "gross_spend_amount",
            "average_transaction_amount",
        ),
    )

    require_columns(
        df=merchant_daily_rewards_df,
        table_name="merchant_daily_rewards_df",
        required_columns=(
            "business_date",
            "merchant_id",
            "matched_redemption_count",
            "redeemed_transaction_count",
            "reward_cost_amount",
            "average_reward_amount",
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

    merchant_economics_df = current_merchant_scd_df.select(
        "merchant_id",
        "merchant_margin_rate",
        "platform_fee_rate",
    )

    return (
        merchant_daily_spend_df.alias("spend")
        .join(
            merchant_daily_rewards_df.alias("reward"),
            ["business_date", "merchant_id"],
            "left",
        )
        .join(
            merchant_economics_df.alias("merchant"),
            "merchant_id",
            "left",
        )
        .fillna(
            {
                "matched_redemption_count": 0,
                "redeemed_transaction_count": 0,
                "reward_cost_amount": 0.0,
                "average_reward_amount": 0.0,
            }
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
        .withColumn(
            "redemption_rate",
            F.when(
                F.col("transaction_count") > 0,
                F.col("redeemed_transaction_count") / F.col("transaction_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "gold_pipeline_run_id",
            F.lit(pipeline_run_id),
        )
        .withColumn(
            "gold_rule_version",
            F.lit(GOLD_RULE_VERSION),
        )
        .withColumn(
            "gold_created_at",
            F.current_timestamp(),
        )
        .select(
            "business_date",
            "merchant_id",
            "transaction_count",
            "gross_spend_amount",
            "average_transaction_amount",
            "matched_redemption_count",
            "redeemed_transaction_count",
            "redemption_rate",
            "reward_cost_amount",
            "average_reward_amount",
            "merchant_margin_rate",
            "platform_fee_rate",
            "platform_fee_amount",
            "estimated_merchant_margin_amount",
            "merchant_net_after_reward",
            "gold_pipeline_run_id",
            "gold_rule_version",
            "gold_created_at",
        )
    )

def build_offer_daily_economics(
    matched_redemptions_df: DataFrame,
    current_offer_scd_df: DataFrame,
    current_campaign_scd_df: DataFrame,
    pipeline_run_id: str,
) -> DataFrame:
    """Build Gold offer daily economics mart.

    Args:
        matched_redemptions_df: Silver matched redemption DataFrame.
        current_offer_scd_df: Current offer SCD rows.
        current_campaign_scd_df: Current campaign SCD rows.
        pipeline_run_id: Gold pipeline run identifier.

    Returns:
        Gold offer daily economics DataFrame.
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
            "transaction_date",
            "transaction_amount",
            "calculated_reward_amount",
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
        df=current_campaign_scd_df,
        table_name=CAMPAIGN_SCD_TABLE,
        required_columns=(
            "campaign_id",
            "campaign_name",
            "campaign_start_date",
            "campaign_end_date",
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
        .dropDuplicates(["offer_id"])
    )

    campaign_context_df = (
        current_campaign_scd_df
        .select(
            "campaign_id",
            "campaign_name",
            "campaign_start_date",
            "campaign_end_date",
        )
        .dropDuplicates(["campaign_id"])
    )

    offer_daily_base_df = (
        matched_redemptions_df
        .filter(F.col("matched_redemption_id").isNotNull())
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("offer_id").isNotNull())
        .filter(F.col("campaign_id").isNotNull())
        .filter(F.col("merchant_id").isNotNull())
        .filter(F.col("transaction_date").isNotNull())
        .filter(F.col("transaction_amount").isNotNull())
        .filter(F.col("calculated_reward_amount").isNotNull())
        .filter(F.col("transaction_amount") >= 0)
        .filter(F.col("calculated_reward_amount") >= 0)
        .groupBy(
            F.col("transaction_date").alias("business_date"),
            "offer_id",
            "campaign_id",
            "merchant_id",
        )
        .agg(
            F.countDistinct("matched_redemption_id").alias(
                "matched_redemption_count"
            ),
            F.countDistinct("transaction_id").alias(
                "redeemed_transaction_count"
            ),
            F.sum("transaction_amount").alias(
                "gross_redeemed_spend_amount"
            ),
            F.sum("calculated_reward_amount").alias(
                "reward_cost_amount"
            ),
            F.avg("transaction_amount").alias(
                "average_redeemed_transaction_amount"
            ),
            F.avg("calculated_reward_amount").alias(
                "average_reward_amount"
            ),
        )
    )

    return (
        offer_daily_base_df.alias("base")
        .join(
            offer_context_df.alias("offer"),
            ["offer_id", "campaign_id", "merchant_id"],
            "left",
        )
        .join(
            campaign_context_df.alias("campaign"),
            "campaign_id",
            "left",
        )
        .withColumn(
            "reward_to_redeemed_spend_ratio",
            F.when(
                F.col("gross_redeemed_spend_amount") > 0,
                F.col("reward_cost_amount")
                / F.col("gross_redeemed_spend_amount"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "gold_pipeline_run_id",
            F.lit(pipeline_run_id),
        )
        .withColumn(
            "gold_rule_version",
            F.lit(GOLD_RULE_VERSION),
        )
        .withColumn(
            "gold_created_at",
            F.current_timestamp(),
        )
        .select(
            "business_date",
            "offer_id",
            "campaign_id",
            "merchant_id",
            "campaign_name",
            "campaign_start_date",
            "campaign_end_date",
            "minimum_spend_amount",
            "reward_amount",
            "matched_redemption_count",
            "redeemed_transaction_count",
            "gross_redeemed_spend_amount",
            "reward_cost_amount",
            "average_redeemed_transaction_amount",
            "average_reward_amount",
            "reward_to_redeemed_spend_ratio",
            "gold_pipeline_run_id",
            "gold_rule_version",
            "gold_created_at",
        )
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

def validate_written_gold_table(
    spark,
    table_name: str,
    expected_row_count: int,
) -> None:
    """Validate a written Gold Delta table by row count.

    Args:
        spark: Active Spark session.
        table_name: Gold table name.
        expected_row_count: Expected number of rows.

    Raises:
        ValueError: If the written row count does not match.
    """
    written_df = read_gold_table(
        spark=spark,
        table_name=table_name,
    )

    actual_row_count = written_df.count()

    print("\nWritten Gold table validation")
    print("=" * 80)
    print(f"{'table':<40} {table_name}")
    print(f"{'expected rows':<40} {expected_row_count:>12,}")
    print(f"{'actual rows':<40} {actual_row_count:>12,}")
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

def write_and_validate_gold_table(
    spark,
    df: DataFrame,
    table_name: str,
    required_columns: tuple[str, ...],
    partition_column: str,
) -> None:
    """Write and validate one Gold Delta table.

    Args:
        spark: Active Spark session.
        df: Gold DataFrame to write.
        table_name: Gold output table name.
        required_columns: Required output columns.
        partition_column: Partition column for Delta write.
    """
    validate_gold_output_columns(
        df=df,
        table_name=table_name,
        required_columns=required_columns,
    )

    expected_row_count = df.count()

    write_gold_table(
        df=df,
        table_name=table_name,
        partition_column=partition_column,
    )

    validate_written_gold_table(
        spark=spark,
        table_name=table_name,
        expected_row_count=expected_row_count,
    )

def validate_gold_merchant_daily_business_rules(
    spark,
) -> tuple[str, int]:
    """Validate business rules for gold_merchant_daily.

    Args:
        spark: Active Spark session.

    Returns:
        Table name and validation failure count.
    """
    df = read_gold_table(
        spark=spark,
        table_name=GOLD_MERCHANT_DAILY_TABLE,
    )

    require_columns(
        df=df,
        table_name=GOLD_MERCHANT_DAILY_TABLE,
        required_columns=GOLD_MERCHANT_DAILY_REQUIRED_COLUMNS,
    )

    duplicate_grain_count = (
        df
        .groupBy("business_date", "merchant_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_required_count = (
        df
        .filter(
            F.col("business_date").isNull()
            | F.col("merchant_id").isNull()
            | F.col("transaction_count").isNull()
            | F.col("gross_spend_amount").isNull()
            | F.col("reward_cost_amount").isNull()
            | F.col("platform_fee_amount").isNull()
            | F.col("estimated_merchant_margin_amount").isNull()
            | F.col("merchant_net_after_reward").isNull()
            | F.col("gold_pipeline_run_id").isNull()
            | F.col("gold_rule_version").isNull()
            | F.col("gold_created_at").isNull()
        )
        .count()
    )

    negative_metric_count = (
        df
        .filter(
            (F.col("transaction_count") < 0)
            | (F.col("gross_spend_amount") < 0)
            | (F.col("average_transaction_amount") < 0)
            | (F.col("matched_redemption_count") < 0)
            | (F.col("redeemed_transaction_count") < 0)
            | (F.col("reward_cost_amount") < 0)
            | (F.col("average_reward_amount") < 0)
            | (F.col("platform_fee_amount") < 0)
            | (F.col("estimated_merchant_margin_amount") < 0)
        )
        .count()
    )

    invalid_rate_count = (
        df
        .filter(
            (F.col("redemption_rate") < 0)
            | (F.col("redemption_rate") > 1)
            | (F.col("merchant_margin_rate") < 0)
            | (F.col("merchant_margin_rate") > 1)
            | (F.col("platform_fee_rate") < 0)
            | (F.col("platform_fee_rate") > 1)
        )
        .count()
    )

    redemption_count_mismatch_count = (
        df
        .filter(
            F.col("redeemed_transaction_count")
            > F.col("transaction_count")
        )
        .count()
    )

    formula_mismatch_count = (
        df
        .withColumn(
            "expected_platform_fee_amount",
            F.col("gross_spend_amount") * F.col("platform_fee_rate"),
        )
        .withColumn(
            "expected_margin_amount",
            F.col("gross_spend_amount") * F.col("merchant_margin_rate"),
        )
        .withColumn(
            "expected_net_after_reward",
            F.col("expected_margin_amount")
            - F.col("expected_platform_fee_amount")
            - F.col("reward_cost_amount"),
        )
        .filter(
            (F.abs(F.col("platform_fee_amount") - F.col("expected_platform_fee_amount")) > 0.01)
            | (F.abs(F.col("estimated_merchant_margin_amount") - F.col("expected_margin_amount")) > 0.01)
            | (F.abs(F.col("merchant_net_after_reward") - F.col("expected_net_after_reward")) > 0.01)
        )
        .count()
    )

    failure_count = (
        duplicate_grain_count
        + null_required_count
        + negative_metric_count
        + invalid_rate_count
        + redemption_count_mismatch_count
        + formula_mismatch_count
    )

    print("\nGold merchant daily business-rule validation")
    print("=" * 80)
    print(f"{'duplicate grain rows':<50} {duplicate_grain_count:>12,}")
    print(f"{'null required rows':<50} {null_required_count:>12,}")
    print(f"{'negative metric rows':<50} {negative_metric_count:>12,}")
    print(f"{'invalid rate rows':<50} {invalid_rate_count:>12,}")
    print(f"{'redeemed count > transaction count rows':<50} {redemption_count_mismatch_count:>12,}")
    print(f"{'formula mismatch rows':<50} {formula_mismatch_count:>12,}")
    print(f"{'total validation failures':<50} {failure_count:>12,}")
    print("=" * 80)

    return GOLD_MERCHANT_DAILY_TABLE, failure_count

def validate_gold_offer_daily_business_rules(
    spark,
) -> tuple[str, int]:
    """Validate business rules for gold_offer_daily.

    Args:
        spark: Active Spark session.

    Returns:
        Table name and validation failure count.
    """
    df = read_gold_table(
        spark=spark,
        table_name=GOLD_OFFER_DAILY_TABLE,
    )

    require_columns(
        df=df,
        table_name=GOLD_OFFER_DAILY_TABLE,
        required_columns=GOLD_OFFER_DAILY_REQUIRED_COLUMNS,
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
            | F.col("matched_redemption_count").isNull()
            | F.col("redeemed_transaction_count").isNull()
            | F.col("gross_redeemed_spend_amount").isNull()
            | F.col("reward_cost_amount").isNull()
            | F.col("reward_to_redeemed_spend_ratio").isNull()
            | F.col("gold_pipeline_run_id").isNull()
            | F.col("gold_rule_version").isNull()
            | F.col("gold_created_at").isNull()
        )
        .count()
    )

    negative_metric_count = (
        df
        .filter(
            (F.col("matched_redemption_count") < 0)
            | (F.col("redeemed_transaction_count") < 0)
            | (F.col("gross_redeemed_spend_amount") < 0)
            | (F.col("reward_cost_amount") < 0)
            | (F.col("average_redeemed_transaction_amount") < 0)
            | (F.col("average_reward_amount") < 0)
            | (F.col("minimum_spend_amount") < 0)
            | (F.col("reward_amount") < 0)
        )
        .count()
    )

    invalid_ratio_count = (
        df
        .filter(
            (F.col("reward_to_redeemed_spend_ratio") < 0)
            | (F.col("reward_to_redeemed_spend_ratio") > 1)
        )
        .count()
    )

    redemption_count_mismatch_count = (
        df
        .filter(
            F.col("redeemed_transaction_count")
            > F.col("matched_redemption_count")
        )
        .count()
    )

    formula_mismatch_count = (
        df
        .withColumn(
            "expected_reward_to_spend_ratio",
            F.when(
                F.col("gross_redeemed_spend_amount") > 0,
                F.col("reward_cost_amount")
                / F.col("gross_redeemed_spend_amount"),
            ).otherwise(F.lit(0.0)),
        )
        .filter(
            F.abs(
                F.col("reward_to_redeemed_spend_ratio")
                - F.col("expected_reward_to_spend_ratio")
            ) > 0.0001
        )
        .count()
    )

    failure_count = (
        duplicate_grain_count
        + null_required_count
        + negative_metric_count
        + invalid_ratio_count
        + redemption_count_mismatch_count
        + formula_mismatch_count
    )

    print("\nGold offer daily business-rule validation")
    print("=" * 80)
    print(f"{'duplicate grain rows':<50} {duplicate_grain_count:>12,}")
    print(f"{'null required rows':<50} {null_required_count:>12,}")
    print(f"{'negative metric rows':<50} {negative_metric_count:>12,}")
    print(f"{'invalid reward-to-spend ratio rows':<50} {invalid_ratio_count:>12,}")
    print(f"{'redeemed count > matched count rows':<50} {redemption_count_mismatch_count:>12,}")
    print(f"{'formula mismatch rows':<50} {formula_mismatch_count:>12,}")
    print(f"{'total validation failures':<50} {failure_count:>12,}")
    print("=" * 80)

    return GOLD_OFFER_DAILY_TABLE, failure_count

def validate_all_gold_outputs(spark) -> None:
    """Validate all Gold merchant economics outputs.

    Args:
        spark: Active Spark session.

    Raises:
        ValueError: If any Gold table fails validation.
    """
    print("\nValidating Gold outputs")
    print("=" * 80)

    validation_results = [
        validate_gold_merchant_daily_business_rules(spark=spark),
        validate_gold_offer_daily_business_rules(spark=spark),
    ]

    print("\nGold validation summary")
    print("=" * 80)
    print(f"{'table':<35} {'failures':>12} {'status':>12}")
    print("-" * 80)

    failed_tables = []

    for table_name, failure_count in validation_results:
        status = "PASSED" if failure_count == 0 else "FAILED"

        if failure_count > 0:
            failed_tables.append(table_name)

        print(f"{table_name:<35} {failure_count:>12,} {status:>12}")

    print("=" * 80)

    if failed_tables:
        raise ValueError(
            "Gold output validation failed for: "
            + ", ".join(failed_tables)
        )

    print("All Gold output validations passed.")


def main(spark_session=None) -> None:
    """Run Gold merchant economics build."""

    if spark_session is None:
        spark = create_spark_session_local("merchantlift-merchant-economic")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    pipeline_run_id = build_pipeline_run_id()

    print("Starting Gold merchant economics build")
    print("=" * 80)
    print(f"Silver directory: {SILVER_DIR}")
    print(f"Gold directory: {GOLD_DIR}")
    print(f"Pipeline run ID: {pipeline_run_id}")
    print(f"Gold rule version: {GOLD_RULE_VERSION}")
    print(f"Output table: {GOLD_MERCHANT_DAILY_TABLE}")
    print(f"Output table: {GOLD_OFFER_DAILY_TABLE}")
    print("=" * 80)

    input_tables = read_gold_input_tables(spark=spark)

    validate_gold_input_contracts(
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

    print("\nGold input table summary")
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

    current_merchant_scd_df = get_current_scd_rows(
        scd_df=input_tables[MERCHANT_SCD_TABLE],
    )

    current_offer_scd_df = get_current_scd_rows(
        scd_df=input_tables[OFFER_SCD_TABLE],
    )

    current_campaign_scd_df = get_current_scd_rows(
        scd_df=input_tables[CAMPAIGN_SCD_TABLE],
    )

    print("\nCurrent SCD row counts")
    print("=" * 80)
    print(
        f"{'current merchant rows':<45} "
        f"{current_merchant_scd_df.count():>12,}"
    )
    print(
        f"{'current offer rows':<45} "
        f"{current_offer_scd_df.count():>12,}"
    )
    print(
        f"{'current campaign rows':<45} "
        f"{current_campaign_scd_df.count():>12,}"
    )
    print("=" * 80)

    merchant_daily_spend_df = build_merchant_daily_spend(
        transactions_df=input_tables[TRANSACTIONS_TABLE],
    )

    merchant_daily_spend_count = merchant_daily_spend_df.count()

    print("\nMerchant daily spend aggregation")
    print("=" * 80)
    print(
        f"{'merchant daily spend rows':<45} "
        f"{merchant_daily_spend_count:>12,}"
    )
    print("=" * 80)

    print("\nMerchant daily spend sample")
    merchant_daily_spend_df.show(20, truncate=False)

    print("\nMerchant daily spend schema")
    merchant_daily_spend_df.printSchema()

    merchant_daily_rewards_df = build_merchant_daily_rewards(
        matched_redemptions_df=input_tables[MATCHED_REDEMPTIONS_TABLE],
    )

    merchant_daily_rewards_count = merchant_daily_rewards_df.count()

    print("\nMerchant daily reward aggregation")
    print("=" * 80)
    print(
        f"{'merchant daily reward rows':<45} "
        f"{merchant_daily_rewards_count:>12,}"
    )
    print("=" * 80)

    print("\nMerchant daily reward sample")
    merchant_daily_rewards_df.show(20, truncate=False)

    print("\nMerchant daily reward schema")
    merchant_daily_rewards_df.printSchema()

    gold_merchant_daily_df = build_merchant_daily_economics(
        merchant_daily_spend_df=merchant_daily_spend_df,
        merchant_daily_rewards_df=merchant_daily_rewards_df,
        current_merchant_scd_df=current_merchant_scd_df,
        pipeline_run_id=pipeline_run_id,
    )

    gold_merchant_daily_count = gold_merchant_daily_df.count()

    print("\nGold merchant daily economics")
    print("=" * 80)
    print(
        f"{'gold merchant daily rows':<45} "
        f"{gold_merchant_daily_count:>12,}"
    )
    print("=" * 80)

    print("\nGold merchant daily sample")
    gold_merchant_daily_df.show(20, truncate=False)

    print("\nGold merchant daily schema")
    gold_merchant_daily_df.printSchema()

    gold_offer_daily_df = build_offer_daily_economics(
        matched_redemptions_df=input_tables[MATCHED_REDEMPTIONS_TABLE],
        current_offer_scd_df=current_offer_scd_df,
        current_campaign_scd_df=current_campaign_scd_df,
        pipeline_run_id=pipeline_run_id,
    )

    gold_offer_daily_count = gold_offer_daily_df.count()

    print("\nGold offer daily economics")
    print("=" * 80)
    print(
        f"{'gold offer daily rows':<45} "
        f"{gold_offer_daily_count:>12,}"
    )
    print("=" * 80)

    print("\nGold offer daily sample")
    gold_offer_daily_df.show(20, truncate=False)

    print("\nGold offer daily schema")
    gold_offer_daily_df.printSchema()

    print("\nWriting Gold Delta tables")
    print("=" * 80)

    write_and_validate_gold_table(
        spark=spark,
        df=gold_merchant_daily_df,
        table_name=GOLD_MERCHANT_DAILY_TABLE,
        required_columns=GOLD_MERCHANT_DAILY_REQUIRED_COLUMNS,
        partition_column="business_date",
    )

    write_and_validate_gold_table(
        spark=spark,
        df=gold_offer_daily_df,
        table_name=GOLD_OFFER_DAILY_TABLE,
        required_columns=GOLD_OFFER_DAILY_REQUIRED_COLUMNS,
        partition_column="business_date",
    )

    print("\nAll Gold Delta tables written and validated.")

    validate_all_gold_outputs(spark=spark)

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()