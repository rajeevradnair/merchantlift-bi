"""Generate redemption and control-group events for MerchantLift BI.

This script creates two Day 18 fact tables:

1. fact_offer_redemptions
2. fact_control_group_transactions

Business meaning:

- Redemptions show transactions that qualified for activated offers.
- Control-group transactions show baseline spend from similar users who did not see the offer.

Outputs later:
    data/raw/fact_offer_redemptions/part-00000.parquet
    data/raw/fact_control_group_transactions/part-00000.parquet
"""

from __future__ import annotations

from datetime import datetime, timedelta
import random
from typing import Any

import polars as pl

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


def load_required_inputs() -> tuple[
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
]:
    """Load raw tables required for Day 18 generation.

    Returns:
        A tuple containing:
        - fact_transactions
        - fact_offer_activations
        - fact_offer_customer_assignment
        - dim_offer
    """
    fact_transactions = read_raw_table("fact_transactions")
    fact_offer_activations = read_raw_table("fact_offer_activations")
    fact_offer_customer_assignment = read_raw_table(
        "fact_offer_customer_assignment"
    )
    dim_offer = read_raw_table("dim_offer")
    dim_merchant = read_raw_table("dim_merchant")

    return (
        fact_transactions,
        fact_offer_activations,
        fact_offer_customer_assignment,
        dim_offer,
        dim_merchant,
    )

################################################
## Helper functions for redemption generation
################################################

def build_activation_offer_lookup(
    fact_offer_activations: pl.DataFrame,
    dim_offer: pl.DataFrame,
) -> pl.DataFrame:
    """Join activations with offer rules.

    This gives each activation the offer rules needed to evaluate
    whether a transaction qualifies for redemption.

    Args:
        fact_offer_activations: Raw activation fact table.
        dim_offer: Offer dimension table with reward and minimum spend rules.

    Returns:
        A Polars DataFrame containing enriched activation rows.
    """
    enriched_activations = fact_offer_activations.join(
        dim_offer.select(
            [
                "offer_id",
                "offer_type",
                "minimum_spend_amount",
                "reward_amount",
                "reward_multiplier",
                "max_reward_amount",
                "offer_status",
            ]
        ),
        on="offer_id",
        how="left",
        coalesce=True,
    )

    return enriched_activations


def calculate_reward_amount(
    transaction_amount: float,
    activation: dict[str, Any],
) -> float:
    """Calculate reward amount for a qualifying redemption.

    Args:
        transaction_amount: Amount of the qualifying transaction.
        activation: Enriched activation row containing offer reward rules.

    Returns:
        Calculated reward amount rounded to 2 decimals.
    """
    offer_type = activation["offer_type"]

    if offer_type == "fixed_cashback":
        return round(float(activation["reward_amount"]), 2)

    if offer_type == "percent_cashback":
        reward_multiplier = float(activation["reward_multiplier"])
        max_reward_amount = float(activation["max_reward_amount"])

        calculated_reward = transaction_amount * reward_multiplier

        return round(min(calculated_reward, max_reward_amount), 2)

    raise ValueError(f"Unsupported offer_type: {offer_type}")


def find_matching_transaction_for_specific_activation(
    activation: dict[str, Any],
    fact_transactions: pl.DataFrame,
) -> dict[str, Any] | None:
    """Find one transaction that qualifies for one activation.

    A transaction qualifies if:
    - same tokenized_cardmember_id
    - same merchant_id
    - transaction timestamp is after activation
    - transaction timestamp is before offer expiry
    - transaction amount meets minimum spend
    - transaction status is settled

    Args:
        activation: One enriched activation row.
        fact_transactions: Raw transaction table.

    Returns:
        One matching transaction row as a dictionary, or None if no match exists.
    """
    matching_transactions = fact_transactions.filter(
        (pl.col("tokenized_cardmember_id") == activation["tokenized_cardmember_id"])
        & (pl.col("merchant_id") == activation["merchant_id"])
        & (pl.col("transaction_timestamp") >= activation["activation_timestamp"])
        & (pl.col("transaction_timestamp") <= activation["offer_expiry_timestamp"])
        & (pl.col("transaction_amount") >= float(activation["minimum_spend_amount"]))
        & (pl.col("transaction_status") == "settled")
    )

    if matching_transactions.height == 0:
        return None

    # Return one random matching transaction as a dictionary
    return matching_transactions.sample(n=1).to_dicts()[0]


def generate_redemption_row(
    redemption_number: int,
    activation: dict[str, Any],
    transaction: dict[str, Any],
) -> dict[str, Any]:
    """Build one fact_offer_redemptions row.

    Args:
        redemption_number: Sequential number for redemption_id.
        activation: Enriched activation row.
        transaction: Matching transaction row.

    Returns:
        One redemption event row.
    """
    transaction_amount = float(transaction["transaction_amount"])

    calculated_reward_amount = calculate_reward_amount(
        transaction_amount=transaction_amount,
        activation=activation,
    )

    return {
        "redemption_id": make_id("red", redemption_number),
        "transaction_id": transaction["transaction_id"],
        "activation_id": activation["activation_id"],
        "assignment_id": activation["assignment_id"],
        "impression_id": activation["impression_id"],
        "tokenized_cardmember_id": activation["tokenized_cardmember_id"],
        "offer_id": activation["offer_id"],
        "campaign_id": activation["campaign_id"],
        "merchant_id": activation["merchant_id"],
        "redemption_timestamp": transaction["transaction_timestamp"],
        "redemption_date": transaction["transaction_date"],
        "transaction_amount": transaction_amount,
        "calculated_reward_amount": calculated_reward_amount,
        "redemption_status": "qualified",
        "created_at": datetime.utcnow(),
    }

# Synthesize qualifying transactions if natural matches are insufficient in the local sample.
def synthesize_qualifying_transaction_for_activation(
    synthetic_transaction_number: int,
    activation: dict[str, Any],
) -> dict[str, Any]:
    """Create a synthetic qualifying transaction for an activation.

    This is used when the small local transaction sample does not naturally
    contain enough matching transactions.

    The synthetic transaction respects redemption rules:
    - same tokenized_cardmember_id
    - same merchant_id
    - after activation
    - before offer expiry
    - amount >= minimum_spend_amount
    - status = settled

    Args:
        synthetic_transaction_number: Number used for synthetic transaction ID.
        activation: Enriched activation row.

    Returns:
        A transaction-like dictionary that can qualify for redemption.
    """
    activation_timestamp = activation["activation_timestamp"]
    offer_expiry_timestamp = activation["offer_expiry_timestamp"]

    minimum_spend = float(activation["minimum_spend_amount"])

    redemption_timestamp = activation_timestamp + timedelta(
        days=random.randint(0, 7),
        hours=random.randint(1, 12),
        minutes=random.randint(0, 59),
    )

    if redemption_timestamp > offer_expiry_timestamp:
        redemption_timestamp = activation_timestamp + timedelta(hours=2)

    transaction_amount = round(
        minimum_spend + random.uniform(5.0, max(10.0, minimum_spend * 0.75)),
        2,
    )

    return {
        "transaction_id": make_id("tx_offer", synthetic_transaction_number),
        "tokenized_cardmember_id": activation["tokenized_cardmember_id"],
        "merchant_id": activation["merchant_id"],
        "transaction_timestamp": redemption_timestamp,
        "transaction_date": redemption_timestamp.date(),
        "transaction_amount": transaction_amount,
        "transaction_status": "settled",
    }

def generate_offer_redemptions(
    config: dict[str, Any],
    row_count: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Generate fact_offer_redemptions rows.

    This function first tries to use natural matches between transactions
    and activations. If the small local sample does not provide enough
    natural matches, it synthesizes qualifying transaction-like rows.

    Args:
        config: Project settings loaded from YAML.
        row_count: Target number of redemption rows.

    Returns:
        A Polars DataFrame of redemption events.
    """
    (
        fact_transactions,
        fact_offer_activations,
        _,
        dim_offer,
        _
    ) = load_required_inputs()

    enriched_activations = build_activation_offer_lookup(
        fact_offer_activations=fact_offer_activations,
        dim_offer=dim_offer,
    )

    activation_rows = enriched_activations.to_dicts()
    random.shuffle(activation_rows)

    rows = []
    supplemental_transactions = []
    synthetic_transaction_number = 1

    for activation in activation_rows:
        if len(rows) >= row_count:
            break

        matching_transaction = find_matching_transaction_for_specific_activation(
            activation=activation,
            fact_transactions=fact_transactions,
        )

        if matching_transaction is None:
            matching_transaction = synthesize_qualifying_transaction_for_activation(
                synthetic_transaction_number=synthetic_transaction_number,
                activation=activation,
            )
            supplemental_transactions.append(matching_transaction)
            synthetic_transaction_number += 1

        redemption_row = generate_redemption_row(
            redemption_number=len(rows) + 1,
            activation=activation,
            transaction=matching_transaction,
        )

        rows.append(redemption_row)

    return pl.DataFrame(rows), pl.DataFrame(supplemental_transactions)


################################################
## Helper functions for control group transaction generation
################################################

def generate_control_transaction_timestamp(assignment: dict[str, Any]) -> datetime:
    """Generate a control transaction timestamp during the campaign window.

    Args:
        assignment: One control assignment row.

    Returns:
        A timestamp during the synthetic campaign measurement window.
    """
    assignment_date = assignment["assignment_date"]

    selected_date = assignment_date + timedelta(
        days=random.randint(0, 30)
    )

    return datetime.combine(
        selected_date,
        datetime.min.time(),
    ) + timedelta(
        hours=random.randint(8, 22),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

def generate_control_transaction_amount(assignment: dict[str, Any]) -> float:
    """Generate a baseline control transaction amount.

    Args:
        assignment: One control assignment row.

    Returns:
        Synthetic baseline transaction amount.
    """
    shopper_behavior_type = assignment["shopper_behavior_type"]

    base_amount = random.uniform(25.0, 250.0)

    if shopper_behavior_type == "high_value_customer":
        base_amount *= random.uniform(1.5, 3.0)

    if shopper_behavior_type == "loyal_existing":
        base_amount *= random.uniform(1.2, 2.0)

    if shopper_behavior_type == "bargain_seeker":
        base_amount *= random.uniform(0.8, 1.3)

    if shopper_behavior_type == "lapsed_reactivated":
        base_amount *= random.uniform(0.4, 0.9)

    return round(base_amount, 2)


def build_fact_transactions_from_control_rows(
    control_transactions: pl.DataFrame,
    dim_merchant: pl.DataFrame,
) -> pl.DataFrame:
    """Convert control-group rows into fact_transactions-compatible rows.

    Args:
        control_transactions: fact_control_group_transactions DataFrame.
        dim_merchant: Merchant dimension table used to derive category_id and location_id.

    Returns:
        DataFrame shaped like fact_transactions rows.
    """
    merchant_lookup = dim_merchant.select(
        [
            "merchant_id",
            "category_id",
            "location_id",
        ]
    )

    control_fact_transactions = control_transactions.join(
        merchant_lookup,
        on="merchant_id",
        how="left",
    )

    return control_fact_transactions.select(
        [
            "transaction_id",
            "tokenized_cardmember_id",
            "merchant_id",
            "category_id",
            "location_id",
            "transaction_timestamp",
            "transaction_date",
            "transaction_amount",
            pl.lit("settled").alias("transaction_status"),
            "shopper_behavior_type",
            "segment_id",
            pl.lit(False).alias("is_test_group"),
            pl.lit(True).alias("is_control_group"),
            "created_at",
        ]
    )


def generate_control_transaction_row(
    control_transaction_number: int,
    assignment: dict[str, Any],
) -> dict[str, Any]:
    """Generate one fact_control_group_transactions row.

    Args:
        control_transaction_number: Sequential number for control transaction ID.
        assignment: One eligible control assignment row.

    Returns:
        One control-group transaction row.
    """
    transaction_timestamp = generate_control_transaction_timestamp(assignment)

    transaction_id = make_id("tx_ctrl", control_transaction_number)



    return {
        "control_transaction_id": make_id(
            "ctrl_tx",
            control_transaction_number,
        ),
        "transaction_id": transaction_id,
        "control_assignment_id": assignment["assignment_id"],
        "tokenized_cardmember_id": assignment["tokenized_cardmember_id"],
        "merchant_id": assignment["merchant_id"],
        "campaign_id": assignment["campaign_id"],
        "offer_id": assignment["offer_id"],
        "segment_id": assignment["segment_id"],
        "transaction_timestamp": transaction_timestamp,
        "transaction_date": transaction_timestamp.date(),
        "transaction_amount": generate_control_transaction_amount(assignment),
        "match_group_id": assignment["match_group_id"],
        "match_quality_score": round(random.uniform(0.75, 0.98), 4),
        "shopper_behavior_type": assignment["shopper_behavior_type"],
        "created_at": datetime.utcnow(),
    }


def append_supplemental_transactions(
    existing_transactions: pl.DataFrame,
    supplemental_transactions: pl.DataFrame,
) -> pl.DataFrame:
    """Append synthetic qualifying transactions to fact_transactions."""
    if supplemental_transactions.height == 0:
        return existing_transactions

    # Add missing columns expected in fact_transactions.
    supplemental_transactions = supplemental_transactions.with_columns(
        [
            pl.lit(None).alias("category_id"),
            pl.lit(None).alias("location_id"),
            pl.lit(None).alias("shopper_behavior_type"),
            pl.lit(None).alias("segment_id"),
            pl.lit(True).alias("is_test_group"),
            pl.lit(False).alias("is_control_group"),
            pl.lit(datetime.utcnow()).alias("created_at"),
        ]
    )

    supplemental_transactions = supplemental_transactions.select(
        existing_transactions.columns
    )

    return pl.concat(
        [existing_transactions, supplemental_transactions],
        how="vertical",
    )

def generate_control_group_transactions(
    row_count: int,
) -> pl.DataFrame:
    """Generate fact_control_group_transactions rows.

    Args:
        row_count: Target number of control-group transaction rows.

    Returns:
        A Polars DataFrame containing control-group transaction events.
    """
    
    (_, _, fact_offer_customer_assignment, _, _,)  = load_required_inputs()

    eligible_control_assignments = fact_offer_customer_assignment.filter(
        (pl.col("assignment_group") == "control")
        & (pl.col("assignment_status") == "eligible")
    )

    if eligible_control_assignments.height == 0:
        raise ValueError("No eligible control assignments found.")

    assignment_rows = eligible_control_assignments.to_dicts()

    rows = []

    for control_transaction_number in range(1, row_count + 1):
        assignment = random.choice(assignment_rows)

        rows.append(
            generate_control_transaction_row(
                control_transaction_number=control_transaction_number,
                assignment=assignment,
            )
        )

    return pl.DataFrame(rows)


def main() -> None:
    """Generate redemption and control-group outputs."""
    config = get_config()

    random_seed = int(config["synthetic_generation"]["random_seed"])
    random.seed(random_seed)

    local_scale = config["synthetic_generation"]["local_sample_scale"]

    redemption_rows = int(local_scale["fact_offer_redemptions"])
    control_rows = int(local_scale["fact_control_group_transactions"])

    print()
    print(f"Redemption target rows: {redemption_rows:,}")
    print(f"Control transaction target rows: {control_rows:,}")

    redemptions, supplemental_transactions = generate_offer_redemptions(
        config=config,
        row_count=redemption_rows,
    )

    control_transactions = generate_control_group_transactions(
        row_count=control_rows,
    )

    fact_transactions, _, _, _, dim_merchant, = load_required_inputs()

    control_fact_transactions = build_fact_transactions_from_control_rows(
        control_transactions=control_transactions,
        dim_merchant=dim_merchant,
    )

    updated_transactions = append_supplemental_transactions(
        existing_transactions=fact_transactions,
        supplemental_transactions=supplemental_transactions,
    )

    updated_transactions = pl.concat(
        [updated_transactions, control_fact_transactions],
        how="vertical",
    )

    write_parquet_table(updated_transactions, "fact_transactions")
    write_parquet_table(redemptions, "fact_offer_redemptions")
    write_parquet_table(control_transactions, "fact_control_group_transactions")

    print()
    print("Test data generation complete.")
    print(f"Redemptions written: {redemptions.height:,}")
    print(f"Supplemental transactions related to redemptions appended: {supplemental_transactions.height:,}")
    print(f"Updated fact_transactions rows: {updated_transactions.height:,}")

    print()
    print("Control group data generation complete.")
    print(f"Control transactions written: {control_transactions.height:,}")



    '''
    """Generate a tiny redemption sample."""
    config = get_config()

    random_seed = int(config["synthetic_generation"]["random_seed"])
    random.seed(random_seed)

    redemptions, supplemental_transactions  = generate_offer_redemptions(
        config=config,
        row_count=20,
    )

    fact_transactions, _, _, _ = load_required_inputs()
    # before = fact_transactions.height
    updated_fact_transactions = append_supplemental_transactions(
        existing_transactions=fact_transactions,
        supplemental_transactions=supplemental_transactions,
    )
    # after = updated_fact_transactions.height
    print("Generated 10 redemption rows.")
    print(redemptions)

    """Generate and print one control-group transaction row."""
    _, _, fact_offer_customer_assignment, _ = load_required_inputs()
    eligible_control_assignments = fact_offer_customer_assignment.filter(
        (pl.col("assignment_group") == "control")
        & (pl.col("assignment_status") == "eligible")
    )

    if eligible_control_assignments.height == 0:
        print("No eligible control assignments found.")
        return

    assignment = eligible_control_assignments.to_dicts()[0]

    control_row = generate_control_transaction_row(
        control_transaction_number=1,
        assignment=assignment,
    )

    print()
    print("Generated one control-group transaction row:")
    for key, value in control_row.items():
        print(f"{key}: {value}")


    
    """Generate a tiny control-group transaction sample."""
    config = get_config()

    random_seed = int(config["synthetic_generation"]["random_seed"])
    random.seed(random_seed)

    control_transactions = generate_control_group_transactions(
        row_count=10,
    )

    print("Generated 10 control-group transaction rows.")
    print(control_transactions)
    



    #print(f"Appended {after - before} synthetic transactions to fact_transactions.")

    ##
    # write_parquet_table(updated_transactions, "fact_transactions")
    # write_parquet_table(redemptions, "fact_offer_redemptions")
    ##

    
    config = get_config()

    local_scale = config["synthetic_generation"]["local_sample_scale"]

    redemption_rows = local_scale["fact_offer_redemptions"]
    control_rows = local_scale["fact_control_group_transactions"]

    print("Config loaded successfully.")
    print(f"Redemption target rows: {redemption_rows:,}")
    print(f"Control transaction target rows: {control_rows:,}")

    (
        fact_transactions,
        fact_offer_activations,
        fact_offer_customer_assignment,
        dim_offer,
    ) = load_required_inputs()

    print("Config loaded successfully.")
    print(f"Redemption target rows: {redemption_rows:,}")
    print(f"Control transaction target rows: {control_rows:,}")

    print("\nLoaded input tables.")
    print(f"Transactions: {fact_transactions.height:,}")
    print(f"Offer activations: {fact_offer_activations.height:,}")
    print(f"Offer-customer assignments: {fact_offer_customer_assignment.height:,}")
    print(f"Offers: {dim_offer.height:,}")

    enriched_activations = build_activation_offer_lookup(
        fact_offer_activations=fact_offer_activations,
        dim_offer=dim_offer,
    )

    print()
    print("Built activation-offer lookup.")
    print(f"Activation rows: {fact_offer_activations.height:,}")
    print(f"Enriched activation rows: {enriched_activations.height:,}")

    print("\nSample enriched activation:")
    sample = enriched_activations.head(1)
    print(sample)

    print()
    print("\nTargets:")
    print(f"Redemption target rows: {local_scale['fact_offer_redemptions']:,}")
    print(
        "Control transaction target rows: "
        f"{local_scale['fact_control_group_transactions']:,}"
    )

    activation_rows = enriched_activations.to_dicts()

    redemption_row = None

    # For each activation, try to find a matching transaction that qualifies for redemption.
    for activation in activation_rows:
        matching_transaction = find_matching_transaction_for_specific_activation(
            activation=activation,
            fact_transactions=fact_transactions,
        )

        if matching_transaction is None:
            continue

        redemption_row = generate_redemption_row(
            redemption_number=1,
            activation=activation,
            transaction=matching_transaction,
        )

        break

    if redemption_row is None:
        print("No qualifying transaction found for any activation.")
        print("This can happen with small local samples.")
        print("Later we may synthesize qualifying transactions to guarantee coverage.")
        return

    print("Generated one redemption row:")
    for key, value in redemption_row.items():
        print(f"{key}: {value}")

    '''


if __name__ == "__main__":
    main()