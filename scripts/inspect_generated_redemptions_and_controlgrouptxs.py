"""Inspect redemption and control-group outputs."""

import polars as pl

from merchantlift.paths import RAW_DATA_DIR


def read_table(table_name: str) -> pl.DataFrame:
    """Read one raw generated table."""
    path = RAW_DATA_DIR / table_name / "part-00000.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing table: {path}")

    return pl.read_parquet(path)


def print_basic_quality(
    table_name: str,
    df: pl.DataFrame,
    id_column: str,
) -> None:
    """Print basic row-count and duplicate-ID checks."""
    duplicate_count = df.height - df[id_column].n_unique()

    print(f"\nQuality checks for {table_name}:")
    print(f"Rows: {df.height:,}")
    print(f"Duplicate {id_column}: {duplicate_count}")


def main() -> None:
    
    fact_transactions = read_table("fact_transactions")
    redemptions = read_table("fact_offer_redemptions")
    control_transactions = read_table("fact_control_group_transactions")

    print("=" * 80)
    print("fact_offer_redemptions")
    print("=" * 80)
    print(redemptions.head(5))

    print_basic_quality(
        table_name="fact_offer_redemptions",
        df=redemptions,
        id_column="redemption_id",
    )

    print("\nRedemption status counts:")
    print(redemptions.group_by("redemption_status").len().sort("redemption_status"))

    print("\nReward amount checks:")
    negative_rewards = redemptions.filter(
        pl.col("calculated_reward_amount") < 0
    ).height
    null_rewards = redemptions.filter(
        pl.col("calculated_reward_amount").is_null()
    ).height

    print(f"Negative reward amounts: {negative_rewards}")
    print(f"Null reward amounts: {null_rewards}")

    print("\nRedemption transaction lineage check:")
    redemption_transaction_ids = redemptions.select("transaction_id").unique()
    transaction_ids = fact_transactions.select("transaction_id").unique()

    orphan_redemptions = redemption_transaction_ids.join(
        transaction_ids,
        on="transaction_id",
        how="anti",
    )

    print(f"Orphan redemption transaction_ids: {orphan_redemptions.height:,}")

    if orphan_redemptions.height > 0:
        print("Sample orphan transaction IDs:")
        print(orphan_redemptions.head(10))

    


    print("\n" + "=" * 80)
    print("fact_control_group_transactions")
    print("=" * 80)
    print(control_transactions.head(5))

    print_basic_quality(
        table_name="fact_control_group_transactions",
        df=control_transactions,
        id_column="control_transaction_id",
    )

    print("\nControl amount checks:")
    negative_control_amounts = control_transactions.filter(
        pl.col("transaction_amount") < 0
    ).height
    null_control_amounts = control_transactions.filter(
        pl.col("transaction_amount").is_null()
    ).height

    print(f"Negative control amounts: {negative_control_amounts}")
    print(f"Null control amounts: {null_control_amounts}")

    print("\nControl match-quality checks:")
    low_match_scores = control_transactions.filter(
        pl.col("match_quality_score") < 0.75
    ).height
    high_match_scores = control_transactions.filter(
        pl.col("match_quality_score") > 0.98
    ).height

    print(f"Match scores below 0.75: {low_match_scores}")
    print(f"Match scores above 0.98: {high_match_scores}")

    print("\nControl shopper behavior counts:")
    print(
        control_transactions.group_by("shopper_behavior_type")
        .len()
        .sort("shopper_behavior_type")
    )

    print("\nControl transaction lineage check:")
    control_transaction_ids = control_transactions.select("transaction_id").unique()

    orphan_control_transactions = control_transaction_ids.join(
        transaction_ids,
        on="transaction_id",
        how="anti",
    )

    print(
        "\nOrphan control transaction_ids: "
        f"{orphan_control_transactions.height:,}"
    )

    if orphan_control_transactions.height > 0:
        print("Sample orphan control transaction IDs:")
        print(orphan_control_transactions.head(10))

    print("\n" + "=" * 80)
    print("Funnel summary")
    print("=" * 80)
    print(f"fact_transactions rows: {fact_transactions.height:,}")
    print(f"fact_offer_redemptions rows: {redemptions.height:,}")
    print(f"fact_control_group_transactions rows: {control_transactions.height:,}")

    print("\nExpected good values:")
    print("- Orphan redemption transaction_ids should be 0")
    print("- Orphan control transaction_ids should be 0")
    print("- Negative reward amounts should be 0")
    print("- Negative control amounts should be 0")
    print("- Duplicate redemption_id should be 0")
    print("- Duplicate control_transaction_id should be 0")


if __name__ == "__main__":
    main()