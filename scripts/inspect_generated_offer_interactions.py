"""Inspect generated MerchantLift BI offer interaction events."""

import polars as pl

from merchantlift.paths import RAW_DATA_DIR


TABLES = [
    "fact_offer_customer_assignment",
    "fact_offer_impressions",
    "fact_offer_activations",
]


def read_table(table_name: str) -> pl.DataFrame:
    """Read one generated raw table."""
    path = RAW_DATA_DIR / table_name / "part-00000.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing table: {path}")

    return pl.read_parquet(path)


def print_quality_checks(table_name: str, df: pl.DataFrame, id_column: str) -> None:
    """Print basic quality checks."""
    duplicate_count = df.height - df[id_column].n_unique()

    print(f"\nQuality checks for {table_name}:")
    print(f"Rows: {df.height:,}")
    print(f"Duplicate {id_column}: {duplicate_count}")


def main() -> None:
    """Inspect offer assignment, impression, and activation outputs."""
    assignments = read_table("fact_offer_customer_assignment")
    impressions = read_table("fact_offer_impressions")
    activations = read_table("fact_offer_activations")

    print("=" * 80)
    print("fact_offer_customer_assignment")
    print("=" * 80)
    print(assignments.head(5))
    print(assignments.group_by("assignment_group").len().sort("assignment_group"))
    print(assignments.group_by("assignment_status").len().sort("assignment_status"))
    print_quality_checks(
        "fact_offer_customer_assignment",
        assignments,
        "assignment_id",
    )

    print("\n" + "=" * 80)
    print("fact_offer_impressions")
    print("=" * 80)
    print(impressions.head(5))
    print(impressions.group_by("channel").len().sort("channel"))
    print(impressions.group_by("was_clicked").len().sort("was_clicked"))
    print_quality_checks(
        "fact_offer_impressions",
        impressions,
        "impression_id",
    )

    print("\n" + "=" * 80)
    print("fact_offer_activations")
    print("=" * 80)
    print(activations.head(5))
    if activations.height > 0:
        print(activations.group_by("activation_status").len().sort("activation_status"))
    print_quality_checks(
        "fact_offer_activations",
        activations,
        "activation_id",
    )

    print("\nFunnel summary:")
    print(f"Assignments: {assignments.height:,}")
    print(f"Impressions: {impressions.height:,}")
    print(f"Activations: {activations.height:,}")
    print(f"Impression / assignment rate: {impressions.height / assignments.height:.2%}")
    print(f"Activation / impression rate: {activations.height / impressions.height:.2%}")


if __name__ == "__main__":
    main()