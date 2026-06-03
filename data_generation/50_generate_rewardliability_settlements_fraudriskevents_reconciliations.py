"""Generate financial, fraud, and reconciliation events for MerchantLift BI.

This script creates four fact tables:

1. fact_reward_liability
2. fact_merchant_settlements
3. fact_fraud_risk_events
4. fact_data_quality_reconciliation

Business meaning:

- Reward liability records the reward cost created by qualified redemptions.
- Merchant settlements record merchant payout and platform fee amounts.
- Fraud risk events flag suspicious reward or transaction behavior.
- Reconciliation records prove whether transaction, reward, and settlement amounts tie out.

Outputs later:
    data/raw/fact_reward_liability/part-00000.parquet
    data/raw/fact_merchant_settlements/part-00000.parquet
    data/raw/fact_fraud_risk_events/part-00000.parquet
    data/raw/fact_data_quality_reconciliation/part-00000.parquet
"""

from __future__ import annotations

from typing import Any
from pathlib import Path

import random

import polars as pl

from datetime import datetime

from merchantlift.config import load_yaml_config
from merchantlift.paths import CONFIG_DIR, RAW_DATA_DIR


def get_config() -> dict[str, Any]:
    """Load project settings from config/project_settings.yaml."""
    return load_yaml_config(CONFIG_DIR / "project_settings.yaml")


def make_id(prefix: str, number: int, width: int = 6) -> str:
    return f"{prefix}_{number:0{width}d}"
    

def write_parquet_table(df:pl.DataFrame, table_name: str) -> None:
    output_dir_path:Path = RAW_DATA_DIR / table_name
    output_dir_path.mkdir(parents=True, exist_ok=True)
    output_file_path = output_dir_path / "part-00000.parquet"
    df.write_parquet(output_file_path)
    print(f"Wrote {df.height:,} rows to {output_file_path}")


def read_raw_table(table_name: str) -> pl.DataFrame:
    """Read a raw Parquet table from data/raw/<table_name>/.

    Args:
        table_name: Folder name under data/raw.

    Returns:
        A Polars DataFrame.

    Raises:
        FileNotFoundError: If the expected Parquet file is missing.
    """
    parquet_path = RAW_DATA_DIR / table_name / "part-00000.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Missing required table: {table_name}. "
            f"Expected file at: {parquet_path}"
        )

    return pl.read_parquet(parquet_path)

def load_financial_risk_inputs() -> tuple[
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
]:
    """Load raw tables required for financial, fraud, and reconciliation events.

    Returns:
        A tuple containing:
        - fact_transactions
        - fact_offer_redemptions
        - fact_control_group_transactions
        - dim_merchant
        - dim_risk_rule
    """
    fact_transactions = read_raw_table("fact_transactions")
    fact_offer_redemptions = read_raw_table("fact_offer_redemptions")
    fact_control_group_transactions = read_raw_table(
        "fact_control_group_transactions"
    )
    dim_merchant = read_raw_table("dim_merchant")
    dim_risk_rule = read_raw_table("dim_risk_rule")

    return (
        fact_transactions,
        fact_offer_redemptions,
        fact_control_group_transactions,
        dim_merchant,
        dim_risk_rule,
    )

def determine_liability_owner(redemption: dict[str, Any]) -> str:
    """Determine who owns the reward liability.

    For this project, I am thinking most offers are merchant-funded.

    Args:
        redemption: One redemption row.

    Returns:
        Liability owner value.
    """
    return "merchant"


def calculate_funding_split(
    reward_amount: float,
    liability_owner: str,
) -> tuple[float, float]:
    """Split reward amount between merchant and platform.

    Args:
        reward_amount: Total calculated reward amount.
        liability_owner: merchant, platform, or shared.

    Returns:
        A tuple of merchant_funded_amount and platform_funded_amount.
    """
    if liability_owner == "merchant":
        return round(reward_amount, 2), 0.0

    if liability_owner == "platform":
        return 0.0, round(reward_amount, 2)

    if liability_owner == "shared":
        merchant_share = round(reward_amount * 0.75, 2)
        platform_share = round(reward_amount - merchant_share, 2)
        return merchant_share, platform_share

    raise ValueError(f"Unsupported liability_owner: {liability_owner}")


def build_reward_liability_lookup(
    reward_liability: pl.DataFrame,
) -> dict[str, dict[str, Any]]:
    """Build transaction_id -> reward liability lookup.

    Args:
        reward_liability: fact_reward_liability DataFrame.

    Returns:
        Dictionary keyed by transaction_id.
    """
    return {
        row["transaction_id"]: row
        for row in reward_liability.to_dicts()
    }


def build_merchant_lookup(
    dim_merchant: pl.DataFrame,
) -> dict[str, dict[str, Any]]:
    """Build merchant_id -> merchant lookup.

    Args:
        dim_merchant: Merchant dimension table.

    Returns:
        Dictionary keyed by merchant_id.
    """
    return {
        row["merchant_id"]: row
        for row in dim_merchant.to_dicts()
    }


def generate_reward_liability_row(
    liability_number: int,
    redemption: dict[str, Any],
    ) -> dict[str, Any]:
    """Generate one fact_reward_liability row from one redemption.

    Args:
        liability_number: Sequential number for reward_liability_id.
        redemption: One fact_offer_redemptions row.

    Returns:
        One reward liability event row.
    """
    reward_amount = float(redemption["calculated_reward_amount"])

    liability_owner = determine_liability_owner(redemption)

    merchant_funded_amount, platform_funded_amount = calculate_funding_split(
        reward_amount=reward_amount,
        liability_owner=liability_owner,
    )

    return {
        "reward_liability_id": make_id("rew", liability_number),
        "redemption_id": redemption["redemption_id"],
        "transaction_id": redemption["transaction_id"],
        "activation_id": redemption["activation_id"],
        "tokenized_cardmember_id": redemption["tokenized_cardmember_id"],
        "offer_id": redemption["offer_id"],
        "campaign_id": redemption["campaign_id"],
        "merchant_id": redemption["merchant_id"],
        "liability_date": redemption["redemption_date"],
        "liability_timestamp": redemption["redemption_timestamp"],
        "reward_amount": reward_amount,
        "liability_owner": liability_owner,
        "merchant_funded_amount": merchant_funded_amount,
        "platform_funded_amount": platform_funded_amount,
        "liability_status": "accrued",
        "created_at": datetime.utcnow(),
    }

def generate_reward_liability(
    row_count: int,
) -> pl.DataFrame:
    """Generate fact_reward_liability rows from redemption rows.

    Args:
        row_count: Target number of reward liability rows.

    Returns:
        A Polars DataFrame containing reward liability records.
    """
    (
        _,
        fact_offer_redemptions,
        _,
        _,
        _,
    ) = load_financial_risk_inputs()

    if fact_offer_redemptions.height == 0:
        raise ValueError("No redemption rows found.")

    redemption_rows = fact_offer_redemptions.to_dicts()

    rows = []

    for liability_number, redemption in enumerate(
        redemption_rows[:row_count],
        start=1,
    ):
        rows.append(
            generate_reward_liability_row(
                liability_number=liability_number,
                redemption=redemption,
            )
        )

    return pl.DataFrame(rows)


def generate_merchant_settlement_row(
    settlement_number: int,
    transaction: dict[str, Any],
    merchant: dict[str, Any],
    reward_liability_by_transaction_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Generate one fact_merchant_settlements row.

    Args:
        settlement_number: Sequential number for settlement_id.
        transaction: One fact_transactions row.
        merchant: Matching merchant dimension row.
        reward_liability_by_transaction_id: Lookup of reward liability by transaction_id.

    Returns:
        One merchant settlement row.
    """
    transaction_id = transaction["transaction_id"]
    gross_transaction_amount = float(transaction["transaction_amount"])

    platform_fee_rate = float(merchant["platform_fee_rate"])
    platform_fee_amount = round(gross_transaction_amount * platform_fee_rate, 2)

    merchant_settlement_amount = round(
        gross_transaction_amount - platform_fee_amount,
        2,
    )

    liability = reward_liability_by_transaction_id.get(transaction_id)

    # There is not necessarily a reward liability for every transaction, so we need to handle the case where there is none.
    if liability is None:
        redemption_id = None
        reward_liability_id = None
        reward_amount = 0.0
        liability_owner = "none"
        merchant_funded_amount = 0.0
        platform_funded_amount = 0.0
    else:
        # We assume one reward liability per transaction for simplicity, but this could be adjusted if needed.
        redemption_id = liability["redemption_id"]
        reward_liability_id = liability["reward_liability_id"]
        reward_amount = float(liability["reward_amount"])
        liability_owner = liability["liability_owner"]
        merchant_funded_amount = float(liability["merchant_funded_amount"])
        platform_funded_amount = float(liability["platform_funded_amount"])

    merchant_net_after_reward = round(
        merchant_settlement_amount - merchant_funded_amount,
        2,
    )

    return {
        "settlement_id": make_id("settle", settlement_number),
        "transaction_id": transaction_id,
        "redemption_id": redemption_id,
        "reward_liability_id": reward_liability_id,
        "merchant_id": transaction["merchant_id"],
        "settlement_date": transaction["transaction_date"],
        "gross_transaction_amount": gross_transaction_amount,
        "platform_fee_rate": platform_fee_rate,
        "platform_fee_amount": platform_fee_amount,
        "reward_amount": reward_amount,
        "liability_owner": liability_owner,
        "merchant_funded_amount": merchant_funded_amount,
        "platform_funded_amount": platform_funded_amount,
        "merchant_settlement_amount": merchant_settlement_amount,
        "merchant_net_after_reward": merchant_net_after_reward,
        "settlement_status": "pending",
        "created_at": datetime.utcnow(),
    }


def generate_merchant_settlements(
    row_count: int,
    reward_liability: pl.DataFrame,
) -> pl.DataFrame:
    """Generate fact_merchant_settlements rows.

    Args:
        row_count: Target number of settlement rows.
        reward_liability: Reward liability table used to attach funding impact.

    Returns:
        A Polars DataFrame containing merchant settlement records.
    """
    (
        fact_transactions,
        _,
        _,
        dim_merchant,
        _,
    ) = load_financial_risk_inputs()

    if fact_transactions.height == 0:
        raise ValueError("No transaction rows found.")

    reward_liability_lookup = build_reward_liability_lookup(
        reward_liability=reward_liability,
    )

    merchant_lookup = build_merchant_lookup(dim_merchant)

    transaction_rows = fact_transactions.to_dicts()

    rows = []

    # Iterate through all transactions to generate settlements, even those without reward liability, to reflect the full universe of transactions that could have settlements. 
    # We can attach reward liability info where it exists and leave it blank where it doesn't.
    for settlement_number, transaction in enumerate(
        transaction_rows[:row_count],
        start=1,
    ):
        merchant_id = transaction["merchant_id"]

        if merchant_id not in merchant_lookup:
            continue

        rows.append(
            generate_merchant_settlement_row(
                settlement_number=settlement_number,
                transaction=transaction,
                merchant=merchant_lookup[merchant_id],
                reward_liability_by_transaction_id=reward_liability_lookup,
            )
        )

    return pl.DataFrame(rows)


# Risk rules and fraud event generation would follow a similar pattern, 
# where we define the logic for determining risk events based on transaction and 
# redemption patterns, and then generate rows accordingly.

def build_risk_rule_lookup(
    dim_risk_rule: pl.DataFrame,
) -> list[dict[str, Any]]:
    """Convert risk rule dimension into a list of rule dictionaries.

    Args:
        dim_risk_rule: Risk rule dimension table.

    Returns:
        List of active risk rule records.
    """
    active_rules = dim_risk_rule.filter(pl.col("is_active") == True)

    return active_rules.to_dicts()

def generate_fraud_risk_event_row(
    fraud_event_number: int,
    transaction: dict[str, Any],
    risk_rule: dict[str, Any],
    redemption: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate one fact_fraud_risk_events row.

    Args:
        fraud_event_number: Sequential number for fraud_event_id.
        transaction: Transaction row associated with the risk event.
        risk_rule: Risk rule dimension row.
        redemption: Optional redemption row associated with the event.

    Returns:
        One fraud-risk event row.
    """
    risk_score = round(random.uniform(0.60, 0.99), 4)

    # Calculate risk event status based on risk score thresholds. 
    # These thresholds are arbitrary .
    if risk_score >= 0.90:
        event_status = "open"
    elif risk_score >= 0.75:
        event_status = "reviewed"
    else:
        event_status = "confirmed_false_positive"

    return {
        "fraud_event_id": make_id("fraud", fraud_event_number),
        "transaction_id": transaction["transaction_id"],
        "redemption_id": redemption["redemption_id"] if redemption else None,
        "tokenized_cardmember_id": transaction["tokenized_cardmember_id"],
        "merchant_id": transaction["merchant_id"],
        "risk_rule_id": risk_rule["risk_rule_id"],
        "risk_rule_name": risk_rule["risk_rule_name"],
        "risk_category": risk_rule["risk_category"],
        "risk_severity": risk_rule["severity"],
        "risk_score": risk_score,
        "event_timestamp": transaction["transaction_timestamp"],
        "event_date": transaction["transaction_date"],
        "risk_event_status": event_status,
        "risk_event_description": risk_rule["risk_rule_description"],
        "created_at": datetime.utcnow(),
    }

def build_redemption_lookup(
    fact_offer_redemptions: pl.DataFrame,
) -> dict[str, dict[str, Any]]:
    """Build transaction_id -> redemption lookup.

    Args:
        fact_offer_redemptions: Redemption fact table.

    Returns:
        Dictionary keyed by transaction_id.
    """
    return {
        row["transaction_id"]: row
        for row in fact_offer_redemptions.to_dicts()
    }

def generate_fraud_risk_events(
    row_count: int,
) -> pl.DataFrame:
    """Generate fact_fraud_risk_events rows.

    Args:
        row_count: Target number of fraud-risk events.

    Returns:
        A Polars DataFrame containing fraud-risk event records.
    """
    (
        fact_transactions,
        fact_offer_redemptions,
        _,
        _,
        dim_risk_rule,
    ) = load_financial_risk_inputs()

    if fact_transactions.height == 0:
        raise ValueError("No transaction rows found.")

    risk_rules = build_risk_rule_lookup(dim_risk_rule)

    if not risk_rules:
        raise ValueError("No active risk rules found.")

    redemption_lookup = build_redemption_lookup(
        fact_offer_redemptions=fact_offer_redemptions,
    )

    transaction_rows = fact_transactions.to_dicts()

    rows = []

    for fraud_event_number in range(1, row_count + 1):
        transaction = random.choice(transaction_rows)

        redemption = redemption_lookup.get(transaction["transaction_id"])

        risk_rule = random.choice(risk_rules)

        rows.append(
            generate_fraud_risk_event_row(
                fraud_event_number=fraud_event_number,
                transaction=transaction,
                risk_rule=risk_rule,
                redemption=redemption,
            )
        )

    return pl.DataFrame(rows)



def build_settlement_lookup(
    merchant_settlements: pl.DataFrame,
) -> dict[str, dict[str, Any]]:
    """Build transaction_id -> merchant settlement lookup.

    Args:
        merchant_settlements: fact_merchant_settlements DataFrame.

    Returns:
        Dictionary keyed by transaction_id.
    """
    return {
        row["transaction_id"]: row
        for row in merchant_settlements.to_dicts()
    }

def generate_reconciliation_row(
    reconciliation_number: int,
    transaction: dict[str, Any],
    settlement: dict[str, Any] | None,
) -> dict[str, Any]:
    """Generate one fact_data_quality_reconciliation row.

    Args:
        reconciliation_number: Sequential number for reconciliation_id.
        transaction: One fact_transactions row.
        settlement: Matching settlement row, if available.

    Returns:
        One reconciliation event row.
    """
    transaction_amount = float(transaction["transaction_amount"])

    if settlement is None:
        merchant_settlement_amount = 0.0
        platform_fee_amount = 0.0
        reward_amount = 0.0
        merchant_funded_amount = 0.0
        platform_funded_amount = 0.0
        settlement_delta = transaction_amount
        reconciliation_status = "mismatched"
        reconciliation_reason = "Missing merchant settlement record"
    else:
        merchant_settlement_amount = float(settlement["merchant_settlement_amount"])
        platform_fee_amount = float(settlement["platform_fee_amount"])
        reward_amount = float(settlement["reward_amount"])
        merchant_funded_amount = float(settlement["merchant_funded_amount"])
        platform_funded_amount = float(settlement["platform_funded_amount"])

        settlement_delta = round(
            transaction_amount
            - merchant_settlement_amount
            - platform_fee_amount,
            2,
        )

        if abs(settlement_delta) <= 0.01:
            reconciliation_status = "matched"
            reconciliation_reason = "Transaction amount ties to settlement and platform fee"
        else:
            reconciliation_status = "mismatched"
            reconciliation_reason = "Settlement delta exceeds tolerance"

    return {
        "reconciliation_id": make_id("recon", reconciliation_number),
        "transaction_id": transaction["transaction_id"],
        "merchant_id": transaction["merchant_id"],
        "reconciliation_date": transaction["transaction_date"],
        "transaction_amount": transaction_amount,
        "merchant_settlement_amount": merchant_settlement_amount,
        "platform_fee_amount": platform_fee_amount,
        "reward_amount": reward_amount,
        "merchant_funded_amount": merchant_funded_amount,
        "platform_funded_amount": platform_funded_amount,
        "settlement_delta": settlement_delta,
        "reconciliation_status": reconciliation_status,
        "reconciliation_reason": reconciliation_reason,
        "created_at": datetime.utcnow(),
    }


def generate_data_quality_reconciliation(
    row_count: int,
    merchant_settlements: pl.DataFrame,
) -> pl.DataFrame:
    """Generate fact_data_quality_reconciliation rows.

    Args:
        row_count: Target number of reconciliation rows.
        merchant_settlements: Settlement table used for tie-out checks.

    Returns:
        A Polars DataFrame containing reconciliation records.
    """
    (
        fact_transactions,
        _,
        _,
        _,
        _,
    ) = load_financial_risk_inputs()

    if fact_transactions.height == 0:
        raise ValueError("No transaction rows found.")

    settlement_lookup = build_settlement_lookup(
        merchant_settlements=merchant_settlements,
    )

    transaction_rows = fact_transactions.to_dicts()

    rows = []

    for reconciliation_number, transaction in enumerate(
        transaction_rows[:row_count],
        start=1,
    ):
        settlement = settlement_lookup.get(transaction["transaction_id"])

        rows.append(
            generate_reconciliation_row(
                reconciliation_number=reconciliation_number,
                transaction=transaction,
                settlement=settlement,
            )
        )

    return pl.DataFrame(rows)


def main() -> None:
    """Generate and write financial, risk, and reconciliation outputs."""
    config = get_config()

    local_scale = config["synthetic_generation"]["local_sample_scale"]

    reward_rows = int(local_scale["fact_reward_liability"])
    settlement_rows = int(local_scale["fact_merchant_settlements"])
    fraud_rows = int(local_scale["fact_fraud_risk_events"])
    reconciliation_rows = int(local_scale["fact_data_quality_reconciliation"])

    print("Generating 19 financial and risk outputs...")
    print(f"Reward liability target rows: {reward_rows:,}")
    print(f"Merchant settlement target rows: {settlement_rows:,}")
    print(f"Fraud risk event target rows: {fraud_rows:,}")
    print(f"Reconciliation target rows: {reconciliation_rows:,}")

    reward_liability = generate_reward_liability(
        row_count=reward_rows,
    )

    merchant_settlements = generate_merchant_settlements(
        row_count=settlement_rows,
        reward_liability=reward_liability,
    )

    fraud_risk_events = generate_fraud_risk_events(
        row_count=fraud_rows,
    )

    data_quality_reconciliation = generate_data_quality_reconciliation(
        row_count=reconciliation_rows,
        merchant_settlements=merchant_settlements,
    )

    write_parquet_table(
        df=reward_liability,
        table_name="fact_reward_liability",
    )

    write_parquet_table(
        df=merchant_settlements,
        table_name="fact_merchant_settlements",
    )

    write_parquet_table(
        df=fraud_risk_events,
        table_name="fact_fraud_risk_events",
    )

    write_parquet_table(
        df=data_quality_reconciliation,
        table_name="fact_data_quality_reconciliation",
    )

    print()
    print(f"Reward liability rows written: {reward_liability.height:,}")
    print(f"Merchant settlement rows written: {merchant_settlements.height:,}")
    print(f"Fraud risk event rows written: {fraud_risk_events.height:,}")
    print(
        "Data quality reconciliation rows written: "
        f"{data_quality_reconciliation.height:,}"
    )


if __name__ == "__main__":
    main()