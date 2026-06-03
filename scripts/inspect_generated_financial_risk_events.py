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
    """Print row-count and duplicate-ID checks."""
    duplicate_count = df.height - df[id_column].n_unique()

    print(f"\nQuality checks for {table_name}:")
    print(f"Rows: {df.height:,}")
    print(f"Duplicate {id_column}: {duplicate_count}")


def main() -> None:
    """Inspect Day 19 outputs."""
    reward_liability = read_table("fact_reward_liability")
    merchant_settlements = read_table("fact_merchant_settlements")
    fraud_risk_events = read_table("fact_fraud_risk_events")
    reconciliation = read_table("fact_data_quality_reconciliation")

    print("=" * 80)
    print("fact_reward_liability")
    print("=" * 80)
    print(reward_liability.head(5))

    print_basic_quality(
        table_name="fact_reward_liability",
        df=reward_liability,
        id_column="reward_liability_id",
    )

    print("\nLiability owner counts:")
    print(reward_liability.group_by("liability_owner").len().sort("liability_owner"))

    print("\nReward amount checks:")
    negative_rewards = reward_liability.filter(pl.col("reward_amount") < 0).height
    null_rewards = reward_liability.filter(pl.col("reward_amount").is_null()).height

    funding_mismatch = reward_liability.filter(
        (
            pl.col("merchant_funded_amount")
            + pl.col("platform_funded_amount")
            - pl.col("reward_amount")
        ).abs()
        > 0.01
    ).height

    print(f"Negative reward amounts: {negative_rewards}")
    print(f"Null reward amounts: {null_rewards}")
    print(f"Funding split mismatches: {funding_mismatch}")

    print("\n" + "=" * 80)
    print("fact_merchant_settlements")
    print("=" * 80)
    print(merchant_settlements.head(5))

    print_basic_quality(
        table_name="fact_merchant_settlements",
        df=merchant_settlements,
        id_column="settlement_id",
    )

    print("\nSettlement formula checks:")

    settlement_amount_mismatches = merchant_settlements.filter(
        (
            pl.col("gross_transaction_amount")
            - pl.col("platform_fee_amount")
            - pl.col("merchant_settlement_amount")
        ).abs()
        > 0.01
    ).height

    merchant_net_mismatches = merchant_settlements.filter(
        (
            pl.col("merchant_settlement_amount")
            - pl.col("merchant_funded_amount")
            - pl.col("merchant_net_after_reward")
        ).abs()
        > 0.01
    ).height

    negative_settlements = merchant_settlements.filter(
        pl.col("merchant_settlement_amount") < 0
    ).height

    negative_net_after_reward = merchant_settlements.filter(
        pl.col("merchant_net_after_reward") < 0
    ).height

    print(f"Settlement amount formula mismatches: {settlement_amount_mismatches}")
    print(f"Merchant net formula mismatches: {merchant_net_mismatches}")
    print(f"Negative merchant settlements: {negative_settlements}")
    print(f"Negative merchant net after reward: {negative_net_after_reward}")

    print("\nLiability owner counts in settlements:")
    print(
        merchant_settlements.group_by("liability_owner")
        .len()
        .sort("liability_owner")
    )

    print("\n" + "=" * 80)
    print("fact_fraud_risk_events")
    print("=" * 80)
    print(fraud_risk_events.head(5))

    print_basic_quality(
        table_name="fact_fraud_risk_events",
        df=fraud_risk_events,
        id_column="fraud_event_id",
    )

    print("\nRisk rule counts:")
    print(fraud_risk_events.group_by("risk_rule_name").len().sort("risk_rule_name"))

    print("\nRisk severity counts:")
    print(fraud_risk_events.group_by("risk_severity").len().sort("risk_severity"))

    print("\nRisk score checks:")
    low_risk_scores = fraud_risk_events.filter(pl.col("risk_score") < 0.60).height
    high_risk_scores = fraud_risk_events.filter(pl.col("risk_score") > 0.99).height
    null_transaction_ids = fraud_risk_events.filter(
        pl.col("transaction_id").is_null()
    ).height

    print(f"Risk scores below 0.60: {low_risk_scores}")
    print(f"Risk scores above 0.99: {high_risk_scores}")
    print(f"Null transaction IDs: {null_transaction_ids}")

    print("\n" + "=" * 80)
    print("fact_data_quality_reconciliation")
    print("=" * 80)
    print(reconciliation.head(5))

    print_basic_quality(
        table_name="fact_data_quality_reconciliation",
        df=reconciliation,
        id_column="reconciliation_id",
    )

    print("\nReconciliation status counts:")
    print(
        reconciliation.group_by("reconciliation_status")
        .len()
        .sort("reconciliation_status")
    )

    print("\nSettlement delta checks:")
    large_settlement_deltas = reconciliation.filter(
        pl.col("settlement_delta").abs() > 0.01
    ).height

    null_reconciliation_transaction_ids = reconciliation.filter(
        pl.col("transaction_id").is_null()
    ).height

    print(f"Rows with settlement_delta > 0.01: {large_settlement_deltas}")
    print(f"Null reconciliation transaction IDs: {null_reconciliation_transaction_ids}")

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Reward liability rows: {reward_liability.height:,}")
    print(f"Merchant settlement rows: {merchant_settlements.height:,}")
    print(f"Fraud risk event rows: {fraud_risk_events.height:,}")
    print(f"Reconciliation rows: {reconciliation.height:,}")

    print("\nExpected good values:")
    print("- Duplicate reward_liability_id should be 0")
    print("- Duplicate settlement_id should be 0")
    print("- Duplicate fraud_event_id should be 0")
    print("- Duplicate reconciliation_id should be 0")
    print("- Funding split mismatches should be 0")
    print("- Settlement amount formula mismatches should be 0")
    print("- Merchant net formula mismatches should be 0")
    print("- Risk scores below 0.60 should be 0")
    print("- Risk scores above 0.99 should be 0")
    print("- Null transaction IDs should be 0")


if __name__ == "__main__":
    main()