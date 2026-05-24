"""Inspect generated MerchantLift BI transaction events."""

import polars as pl

from merchantlift.paths import RAW_DATA_DIR


def main() -> None:
    """Inspect fact_transactions output."""
    parquet_path = RAW_DATA_DIR / "fact_transactions" / "part-00000.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing transactions file: {parquet_path}")

    df = pl.read_parquet(parquet_path)

    print("fact_transactions")
    print("=" * 80)
    print(f"Rows: {df.height:,}")
    print(f"Columns: {len(df.columns)}")

    print("\nSample rows:")
    print(df.head(5))

    print("\nTransaction status counts:")
    print(df.group_by("transaction_status").len().sort("transaction_status"))

    print("\nShopper behavior counts:")
    print(df.group_by("shopper_behavior_type").len().sort("shopper_behavior_type"))

    print("\nCategory spend summary:")
    print(
        df.group_by("category_id")
        .agg(
            [
                pl.len().alias("transaction_count"),
                pl.sum("transaction_amount").round(2).alias("total_spend"),
                pl.mean("transaction_amount").round(2).alias("avg_transaction_amount"),
                pl.min("transaction_amount").round(2).alias("min_transaction_amount"),
                pl.max("transaction_amount").round(2).alias("max_transaction_amount"),
            ]
        )
        .sort("category_id")
    )

    print("\nBasic quality checks:")
    duplicate_count = df.height - df["transaction_id"].n_unique()
    negative_amount_count = df.filter(pl.col("transaction_amount") < 0).height
    null_merchant_count = df.filter(pl.col("merchant_id").is_null()).height
    null_cardmember_count = df.filter(
        pl.col("tokenized_cardmember_id").is_null()
    ).height
    null_amount_count = df.filter(pl.col("transaction_amount").is_null()).height

    print(f"Duplicate transaction IDs: {duplicate_count}")
    print(f"Negative amounts: {negative_amount_count}")
    print(f"Null merchant IDs: {null_merchant_count}")
    print(f"Null tokenized cardmember IDs: {null_cardmember_count}")
    print(f"Null transaction amounts: {null_amount_count}")


if __name__ == "__main__":
    main()