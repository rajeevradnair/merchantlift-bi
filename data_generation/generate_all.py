"""Run the MerchantLift BI raw data generation pipeline.

This script orchestrates existing data-generation scripts in dependency order.

It does not contain business-generation logic.
It only coordinates execution.
"""

from __future__ import annotations
import polars as pl
import time
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GENERATOR_SCRIPTS = [
    PROJECT_ROOT / "data_generation" / "10_generate_dimensions.py",
    PROJECT_ROOT / "data_generation" / "20_generate_transactions.py",
    PROJECT_ROOT / "data_generation" / "30_generate_offer_interactions.py",
    PROJECT_ROOT / "data_generation" / "40_generate_testgroupredemptions_and_controlgrouptxs.py",
    PROJECT_ROOT / "data_generation" / "50_generate_rewardliability_settlements_fraudriskevents_reconciliations.py",
]
EXPECTED_RAW_TABLES = [
    "dim_category",
    "dim_location",
    "dim_segment",
    "dim_risk_rule",
    "dim_merchant",
    "dim_campaign",
    "dim_offer",
    "dim_cardmember_token",
    "dim_privacy_consent",
    "dim_date",
    "fact_transactions",
    "fact_offer_customer_assignment",
    "fact_offer_impressions",
    "fact_offer_activations",
    "fact_offer_redemptions",
    "fact_control_group_transactions",
    "fact_reward_liability",
    "fact_merchant_settlements",
    "fact_fraud_risk_events",
    "fact_data_quality_reconciliation",
]

def get_raw_table_path(table_name: str) -> Path:
    """Return the expected Parquet path for one raw table.

    Args:
        table_name: Raw table folder name.

    Returns:
        Expected Parquet file path.
    """
    return PROJECT_ROOT / "data" / "raw" / table_name / "part-00000.parquet"

def run_generator(script_path: Path) -> None:
    """Run one generator script as a subprocess.

    Args:
        script_path: Path to the generator script.

    Raises:
        subprocess.CalledProcessError: If the generator exits with an error.
    """
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    relative_path = script_path.relative_to(PROJECT_ROOT)
    command = [sys.executable, str(script_path)]

    print("=" * 80)
    print(f"Running generator: {relative_path}")
    print(f"Command: {' '.join(command)}")
    print("=" * 80)

    start_time = time.perf_counter()

    try:
        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=env,
            check=True,
        )
    except subprocess.CalledProcessError as error:
        elapsed_seconds = time.perf_counter() - start_time

        print("\nGenerator failed.")
        print(f"Generator: {relative_path}")
        print(f"Exit code: {error.returncode}")
        print(f"Elapsed seconds: {elapsed_seconds:.2f}")
        print("Stopping pipeline to avoid creating inconsistent downstream data.")

        raise

    elapsed_seconds = time.perf_counter() - start_time

    print(f"Completed generator: {relative_path}")
    print(f"Elapsed seconds: {elapsed_seconds:.2f}")

def validate_expected_outputs() -> None:
    """Validate that all expected raw Parquet outputs exist."""
    missing_tables = []

    print("\nValidating expected raw outputs...")
    print("=" * 80)

    for table_name in EXPECTED_RAW_TABLES:
        parquet_path = get_raw_table_path(table_name)

        if parquet_path.exists():
            print(f"FOUND   {table_name}")
        else:
            print(f"MISSING {table_name}")
            missing_tables.append(table_name)

    if missing_tables:
        missing_table_list = ", ".join(missing_tables)
        raise FileNotFoundError(
            "Raw lake generation did not produce all expected tables. "
            f"Missing: {missing_table_list}"
        )

    print("=" * 80)
    print("All expected raw outputs are present.")

def get_table_row_count(table_name: str) -> int:
    """Return the row count for one generated raw table.

    Args:
        table_name: Raw table folder name.

    Returns:
        Number of rows in the table's Parquet file.
    """
    parquet_path = get_raw_table_path(table_name)

    if not parquet_path.exists():
        return 0

    return pl.read_parquet(parquet_path).height

def print_raw_table_summary() -> None:
    """Print row-count summary for all generated raw tables."""
    print("\nRaw table row-count summary:")
    print("=" * 80)

    total_rows = 0

    for table_name in EXPECTED_RAW_TABLES:
        row_count = get_table_row_count(table_name)
        total_rows += row_count
        print(f"{table_name:40} {row_count:12,} rows")

    print("=" * 80)
    print(f"{'TOTAL':<40} {total_rows:>12,} rows")

def main() -> None:
    """Run all raw data generators in dependency order."""
    pipeline_start_time = time.perf_counter()

    print("Starting raw lake generation pipeline...")
    print(f"Project root: {PROJECT_ROOT}")

    try:
        for sequence_number, script_path in enumerate(GENERATOR_SCRIPTS, start=1):
            relative_path = script_path.relative_to(PROJECT_ROOT)

            print("\n" + "#" * 80)
            print(f"Step {sequence_number} of {len(GENERATOR_SCRIPTS)}")
            print(f"Generator: {relative_path}")
            print("#" * 80)

            run_generator(script_path)

        validate_expected_outputs()

        print_raw_table_summary()

    except Exception:
        elapsed_seconds = time.perf_counter() - pipeline_start_time

        print("\nRaw lake generation pipeline failed.")
        print(f"Elapsed seconds before failure: {elapsed_seconds:.2f}")
        raise

    elapsed_seconds = time.perf_counter() - pipeline_start_time

    print("\nRaw lake generation pipeline complete.")
    print(f"Total elapsed seconds: {elapsed_seconds:.2f}")

if __name__ == "__main__":
    main()