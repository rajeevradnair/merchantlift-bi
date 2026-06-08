"""Profile Spark join strategy for transaction-to-offer matching."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from merchantlift.paths import SILVER_DIR
from merchantlift.spark import create_spark_session_local


def read_silver_table(spark, table_name: str) -> DataFrame:
    """Read a Silver Delta table.

    Args:
        spark: Active Spark session.
        table_name: Silver table name.

    Returns:
        Silver table DataFrame.
    """
    table_path = SILVER_DIR / table_name

    return (
        spark.read
        .format("delta")
        .load(str(table_path))
    )


def print_count(label: str, df: DataFrame) -> int:
    """Print and return a DataFrame row count.

    Args:
        label: Human-readable DataFrame label.
        df: DataFrame to count.

    Returns:
        Row count.
    """
    row_count = df.count()
    print(f"{label:<45} {row_count:>12,} rows")
    return row_count


def show_top_keys(
    df: DataFrame,
    key_column: str,
    label: str,
    limit: int = 10,
) -> None:
    """Show the most frequent values for a key column.

    Args:
        df: Input DataFrame.
        key_column: Column to profile.
        label: Human-readable profile label.
        limit: Number of rows to show.
    """
    if key_column not in df.columns:
        print(f"\nSkipping {label}: missing column {key_column}")
        return

    print(f"\nTop {limit} keys for {label}:")
    (
        df
        .groupBy(key_column)
        .count()
        .orderBy(F.desc("count"))
        .show(limit, truncate=False)
    )


def build_activation_offer_candidates(
    activations_df: DataFrame,
    offers_df: DataFrame,
) -> DataFrame:
    """Attach offer rules to activation rows.

    Args:
        activations_df: Silver offer activations.
        offers_df: Silver offers.

    Returns:
        Activation rows enriched with offer rules.
    """
    activation_columns = [
        "activation_id",
        "tokenized_cardmember_id",
        "offer_id",
        "activation_timestamp",
        "offer_expiry_timestamp",
    ]

    offer_columns = [
        "offer_id",
        "campaign_id",
        "merchant_id",
        "minimum_spend_amount",
    ]

    available_activation_columns = [
        column_name
        for column_name in activation_columns
        if column_name in activations_df.columns
    ]

    available_offer_columns = [
        column_name
        for column_name in offer_columns
        if column_name in offers_df.columns
    ]

    filtered_activations = activations_df.select(
        *available_activation_columns
    )

    filtered_offers = offers_df.select(
        *available_offer_columns
    )

    return (
        filtered_activations.alias("act")
        .join(
            F.broadcast(filtered_offers).alias("offer"),
            F.col("act.offer_id") == F.col("offer.offer_id"),
            "inner",
        )
        .select(
            F.col("act.activation_id"),
            F.col("act.tokenized_cardmember_id"),
            F.col("act.offer_id"),
            F.col("offer.campaign_id"),
            F.col("offer.merchant_id"),
            F.col("act.activation_timestamp"),
            F.col("act.offer_expiry_timestamp"),
            F.col("offer.minimum_spend_amount"),
        )
    )


def build_safe_candidate_join(
    transactions_df: DataFrame,
    activation_offer_df: DataFrame,
) -> DataFrame:
    """Build safe transaction-to-offer candidate join.

    Args:
        transactions_df: Silver transactions.
        activation_offer_df: Activation rows enriched with offer rules.

    Returns:
        Candidate matches DataFrame.
    """
    tx = transactions_df.select(
        "transaction_id",
        "tokenized_cardmember_id",
        "merchant_id",
        "transaction_timestamp",
        "transaction_date",
        "transaction_amount",
    )

    act = activation_offer_df.select(
        "activation_id",
        "offer_id",
        "campaign_id",
        "tokenized_cardmember_id",
        "merchant_id",
        "activation_timestamp",
        "offer_expiry_timestamp",
        "minimum_spend_amount",
    )

    return (
        tx.alias("tx")
        .join(
            act.alias("act"),
            (
                (F.col("tx.tokenized_cardmember_id") == F.col("act.tokenized_cardmember_id"))
                & (F.col("tx.merchant_id") == F.col("act.merchant_id"))
                & (F.col("tx.transaction_timestamp") >= F.col("act.activation_timestamp"))
                & (F.col("tx.transaction_timestamp") <= F.col("act.offer_expiry_timestamp"))
                & (F.col("tx.transaction_amount") >= F.col("act.minimum_spend_amount"))
            ),
            "inner",
        )
        .select(
            F.col("tx.transaction_id"),
            F.col("tx.tokenized_cardmember_id"),
            F.col("tx.merchant_id"),
            F.col("tx.transaction_timestamp"),
            F.col("tx.transaction_date"),
            F.col("tx.transaction_amount"),
            F.col("act.activation_id"),
            F.col("act.offer_id"),
            F.col("act.campaign_id"),
            F.col("act.minimum_spend_amount"),
        )
    )


def main(spark_session=None) -> None:
    """Profile join safety over Silver tables."""
    if spark_session is None:
        spark = create_spark_session_local("merchantlift-read-bronze-table")
        should_stop_spark = True
    else:
        spark = spark_session
        should_stop_spark = False

    print("Spark join profiling over Silver tables")
    print("=" * 80)
    print(f"Silver directory: {SILVER_DIR}")
    print("=" * 80)

    transactions_df = read_silver_table(
        spark=spark,
        table_name="fact_transactions_clean",
    )

    activations_df = read_silver_table(
        spark=spark,
        table_name="fact_offer_activations_clean",
    )

    offers_df = read_silver_table(
        spark=spark,
        table_name="dim_offer_clean",
    )

    print("\nInput row counts")
    print("=" * 80)
    transaction_count = print_count(
        label="silver.fact_transactions_clean",
        df=transactions_df,
    )
    activation_count = print_count(
        label="silver.fact_offer_activations_clean",
        df=activations_df,
    )
    offer_count = print_count(
        label="silver.dim_offer_clean",
        df=offers_df,
    )

    show_top_keys(
        df=transactions_df,
        key_column="merchant_id",
        label="transactions by merchant_id",
    )

    show_top_keys(
        df=transactions_df,
        key_column="tokenized_cardmember_id",
        label="transactions by tokenized_cardmember_id",
    )

    show_top_keys(
        df=activations_df,
        key_column="tokenized_cardmember_id",
        label="activations by tokenized_cardmember_id",
    )

    activation_offer_df = build_activation_offer_candidates(
        activations_df=activations_df,
        offers_df=offers_df,
    )

    print("\nCandidate preparation counts")
    print("=" * 80)
    activation_offer_count = print_count(
        label="activation-offer candidates",
        df=activation_offer_df,
    )

    matched_candidates_df = build_safe_candidate_join(
        transactions_df=transactions_df,
        activation_offer_df=activation_offer_df,
    )

    matched_candidate_count = print_count(
        label="safe matched candidates",
        df=matched_candidates_df,
    )

    print("\nJoin expansion ratios")
    print("=" * 80)

    if transaction_count > 0:
        print(
            "matched_candidates / transactions = "
            f"{matched_candidate_count / transaction_count:.6f}"
        )

    if activation_count > 0:
        print(
            "matched_candidates / activations = "
            f"{matched_candidate_count / activation_count:.6f}"
        )

    if activation_offer_count > 0:
        print(
            "matched_candidates / activation_offer_candidates = "
            f"{matched_candidate_count / activation_offer_count:.6f}"
        )

    print("\nMatched candidate sample")
    matched_candidates_df.show(20, truncate=False)

    print("\nPhysical plan for safe candidate join")
    matched_candidates_df.explain(mode="formatted")

    if should_stop_spark:
        spark.stop()


if __name__ == "__main__":
    main()