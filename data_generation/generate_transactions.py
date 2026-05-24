from __future__ import annotations
from generate_dimensions import make_id
from typing import Any
import polars as pl
from merchantlift.config import load_yaml_config
from merchantlift.paths import CONFIG_DIR, RAW_DATA_DIR
import random
from datetime import date, datetime, time, timedelta

def get_config() -> dict[str, Any]:
    return load_yaml_config(CONFIG_DIR / "project_settings.yaml")

def write_parquet_table(df: pl.DataFrame, table_name: str) -> None:
    output_dir = RAW_DATA_DIR / table_name
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "part-00000.parquet"
    df.write_parquet(output_path)
    print(f"Wrote {df.height:,} rows to {output_path}")

def read_raw_table(table_name: str) -> pl.DataFrame:
    parquet_path = RAW_DATA_DIR / table_name / "part-00000.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Missing required table: {table_name}. "
            f"Expected file at: {parquet_path}"
        )

    return pl.read_parquet(parquet_path)

def load_transaction_inputs() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    dim_cardmember_token = read_raw_table("dim_cardmember_token")
    dim_merchant = read_raw_table("dim_merchant")
    dim_category = read_raw_table("dim_category")

    return dim_cardmember_token, dim_merchant, dim_category

def build_merchant_lookup(
    dim_merchant: pl.DataFrame,
    dim_category: pl.DataFrame,
) -> list[dict[str, Any]]:
    merchant_with_category = dim_merchant.join(
        dim_category,
        on="category_id",
        how="left",
    )
    return merchant_with_category.to_dicts()


def generate_transaction_amount(merchant: dict[str, Any]) -> float:
    """Generate a transaction amount based on merchant category basket range.

    The merchant row already includes category behavior because we joined
    dim_merchant with dim_category.

    We use a triangular distribution so most amounts cluster near a realistic
    typical value instead of being evenly random across the full range.

    Args:
        merchant: Enriched merchant dictionary with basket_min and basket_max.

    Returns:
        A positive transaction amount rounded to 2 decimals.
    """
    basket_min = float(merchant["basket_min"])
    basket_max = float(merchant["basket_max"])

    typical_amount = basket_min + ((basket_max - basket_min) * 0.35)

    amount = random.triangular(
        low=basket_min,
        high=basket_max,
        mode=typical_amount,
    )

    return round(amount, 2)

def generate_transaction_timestamp() -> datetime:
    """Generate a random transaction timestamp in 2026.

    Returns:
        A datetime representing when the transaction happened.
    """
    start_date = date(2026, 1, 1)
    end_date = date(2026, 12, 31)

    total_days = (end_date - start_date).days
    selected_date = start_date + timedelta(days=random.randint(0, total_days))

    selected_time = time(
        hour=random.randint(7, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
    )

    return datetime.combine(selected_date, selected_time)


def apply_simple_seasonality(
    amount: float,
    merchant: dict[str, Any],
    transaction_timestamp: datetime,
) -> float:
    """Apply simple category-specific seasonality.

    Args:
        amount: Base transaction amount.
        merchant: Enriched merchant dictionary with category_name.
        transaction_timestamp: Generated transaction timestamp.

    Returns:
        Seasonality-adjusted transaction amount.
    """
    category_name = merchant["category_name"]
    is_weekend = transaction_timestamp.weekday() >= 5
    month = transaction_timestamp.month

    multiplier = 1.0

    if category_name == "dining" and is_weekend:
        multiplier = 1.15

    if category_name == "retail" and month in {11, 12}:
        multiplier = 1.25

    if category_name == "travel" and month in {6, 7, 11, 12}:
        multiplier = 1.20

    if category_name == "luxury" and month in {11, 12}:
        multiplier = 1.15

    print("Base multiplier = ", multiplier)

    return round(amount * multiplier, 2)

def generate_transaction_row(
    transaction_number: int,
    cardmember: dict[str, Any],
    merchant: dict[str, Any],
    transaction_status_values: list[str],
) -> dict[str, Any]:
    """Generate one complete transaction event row.

    Args:
        transaction_number: Sequential number used to create transaction_id.
        cardmember: One selected cardmember record from dim_cardmember_token.
        merchant: One selected enriched merchant record.
        transaction_status_values: Allowed transaction statuses from config.

    Returns:
        One transaction event as a dictionary.
    """
    transaction_timestamp = generate_transaction_timestamp()

    base_amount = generate_transaction_amount(merchant)

    adjusted_amount = apply_simple_seasonality(
        amount=base_amount,
        merchant=merchant,
        transaction_timestamp=transaction_timestamp,
    )

    transaction_status = random.choices(
        transaction_status_values,
        weights=[0.08, 0.86, 0.04, 0.02],
        k=1,
    )[0]

    return {
        "transaction_id": make_id("tx", transaction_number),
        "tokenized_cardmember_id": cardmember["tokenized_cardmember_id"],
        "merchant_id": merchant["merchant_id"],
        "category_id": merchant["category_id"],
        "location_id": merchant["location_id"],
        "transaction_timestamp": transaction_timestamp,
        "transaction_date": transaction_timestamp.date(),
        "transaction_amount": adjusted_amount,
        "transaction_status": transaction_status,
        "shopper_behavior_type": cardmember["shopper_behavior_type"],
        "segment_id": cardmember["segment_id"],
        "is_test_group": bool(cardmember["is_test_eligible"] and random.random() < 0.50),
        "is_control_group": bool(
            cardmember["is_control_eligible"] and random.random() < 0.30
        ),
        "created_at": datetime.utcnow(),
    }


def generate_fact_transactions(
    config: dict[str, Any],
    row_count: int,
) -> pl.DataFrame:
    """Generate fact_transactions rows.

    Args:
        config: Project settings loaded from YAML.
        row_count: Number of transaction rows to generate.

    Returns:
        A Polars DataFrame containing synthetic transaction events.
    """
    dim_cardmember_token, dim_merchant, dim_category = load_transaction_inputs()

    cardmembers = dim_cardmember_token.to_dicts()

    merchants = build_merchant_lookup(
        dim_merchant=dim_merchant,
        dim_category=dim_category,
    )

    transaction_status_values = config["facts"]["transaction_status_values"]

    rows = []

    for transaction_number in range(1, row_count + 1):
        transaction_row = generate_transaction_row(
            transaction_number=transaction_number,
            cardmember=random.choice(cardmembers),
            merchant=random.choice(merchants),
            transaction_status_values=transaction_status_values,
        )

        rows.append(transaction_row)

    return pl.DataFrame(rows)


def main() -> None:
    """Generate raw transaction events."""
    config = get_config()

    random_seed = config["synthetic_generation"]["random_seed"]
    random.seed(random_seed)

    row_count = config["synthetic_generation"]["local_sample_scale"][
        "fact_transactions"
    ]

    print("Generating MerchantLift BI fact_transactions...")
    print(f"Using random seed: {random_seed}")
    print(f"Local sample row count: {row_count:,}")

    fact_transactions = generate_fact_transactions(
        config=config,
        row_count=row_count,
    )

    write_parquet_table(
        df=fact_transactions,
        table_name="fact_transactions",
    )

    print("Finished generating fact_transactions.")


if __name__ == "__main__":
    main()