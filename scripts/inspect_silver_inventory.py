"""Inspect configured Silver transformation inventory."""

from merchantlift.silver_config import SILVER_TABLES


def main() -> None:
    """Print Silver table configuration."""
    print("Silver transformation inventory")
    print("=" * 80)

    for table in SILVER_TABLES:
        print(
            f"{table.bronze_table_name:<40} "
            f"-> {table.silver_table_name:<45} "
            f"primary_key={table.primary_key}"
        )

    print("=" * 80)
    print(f"Configured Silver tables: {len(SILVER_TABLES)}")


if __name__ == "__main__":
    main()