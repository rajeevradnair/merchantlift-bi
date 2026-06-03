"""Validate the MerchantLift BI data foundation release.

This script checks whether the raw synthetic data foundation is complete,
measurable, and ready for Spark/Delta lakehouse ingestion.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl
from datetime import datetime

from merchantlift.paths import RAW_DATA_DIR

MONEY_TOLERANCE = 0.01
REPORT_PATH = Path("reports") / "release_1_data_foundation.md"

@dataclass(frozen=True)
class TableExpectation:
    """Expected table metadata for release validation."""

    table_name: str
    primary_key: str | None
    required_columns: tuple[str, ...]


EXPECTED_TABLES = [
    TableExpectation(
        table_name="dim_category",
        primary_key="category_id",
        required_columns=("category_id",),
    ),
    TableExpectation(
        table_name="dim_location",
        primary_key="location_id",
        required_columns=("location_id",),
    ),
    TableExpectation(
        table_name="dim_segment",
        primary_key="segment_id",
        required_columns=("segment_id",),
    ),
    TableExpectation(
        table_name="dim_risk_rule",
        primary_key="risk_rule_id",
        required_columns=("risk_rule_id",),
    ),
    TableExpectation(
        table_name="dim_merchant",
        primary_key="merchant_id",
        required_columns=("merchant_id", "category_id", "location_id"),
    ),
    TableExpectation(
        table_name="dim_campaign",
        primary_key="campaign_id",
        required_columns=("campaign_id",),
    ),
    TableExpectation(
        table_name="dim_offer",
        primary_key="offer_id",
        required_columns=("offer_id", "campaign_id", "merchant_id"),
    ),
    TableExpectation(
        table_name="dim_cardmember_token",
        primary_key="tokenized_cardmember_id",
        required_columns=("tokenized_cardmember_id",),
    ),
    TableExpectation(
        table_name="dim_privacy_consent",
        primary_key="consent_id",
        required_columns=("consent_id", "tokenized_cardmember_id"),
    ),
    TableExpectation(
        table_name="dim_date",
        primary_key="date_id",
        required_columns=("date_id",),
    ),
    TableExpectation(
        table_name="fact_transactions",
        primary_key="transaction_id",
        required_columns=(
            "transaction_id",
            "tokenized_cardmember_id",
            "merchant_id",
            "transaction_amount",
            "transaction_date",
        ),
    ),
    TableExpectation(
        table_name="fact_offer_customer_assignment",
        primary_key="assignment_id",
        required_columns=(
            "assignment_id",
            "tokenized_cardmember_id",
            "offer_id",
            "campaign_id",
            "assignment_group",
        ),
    ),
    TableExpectation(
        table_name="fact_offer_impressions",
        primary_key="impression_id",
        required_columns=("impression_id", "assignment_id", "offer_id"),
    ),
    TableExpectation(
        table_name="fact_offer_activations",
        primary_key="activation_id",
        required_columns=("activation_id", "impression_id", "offer_id"),
    ),
    TableExpectation(
        table_name="fact_offer_redemptions",
        primary_key="redemption_id",
        required_columns=(
            "redemption_id",
            "transaction_id",
            "activation_id",
            "offer_id",
            "calculated_reward_amount",
        ),
    ),
    TableExpectation(
        table_name="fact_control_group_transactions",
        primary_key="control_transaction_id",
        required_columns=(
            "control_transaction_id",
            "transaction_id",
            "control_assignment_id",
            "offer_id",
            "transaction_amount",
        ),
    ),
    TableExpectation(
        table_name="fact_reward_liability",
        primary_key="reward_liability_id",
        required_columns=(
            "reward_liability_id",
            "redemption_id",
            "transaction_id",
            "reward_amount",
            "liability_owner",
            "merchant_funded_amount",
            "platform_funded_amount",
        ),
    ),
    TableExpectation(
        table_name="fact_merchant_settlements",
        primary_key="settlement_id",
        required_columns=(
            "settlement_id",
            "transaction_id",
            "gross_transaction_amount",
            "platform_fee_amount",
            "merchant_settlement_amount",
            "merchant_net_after_reward",
        ),
    ),
    TableExpectation(
        table_name="fact_fraud_risk_events",
        primary_key="fraud_event_id",
        required_columns=(
            "fraud_event_id",
            "transaction_id",
            "risk_rule_id",
            "risk_score",
        ),
    ),
    TableExpectation(
        table_name="fact_data_quality_reconciliation",
        primary_key="reconciliation_id",
        required_columns=(
            "reconciliation_id",
            "transaction_id",
            "settlement_delta",
            "reconciliation_status",
        ),
    ),
]


@dataclass(frozen=True)
class ValidationResult:
    """Result of one release validation check."""

    table_name: str
    check_name: str
    status: str
    message: str

def validate_file_exists(table: TableExpectation) -> ValidationResult:
    """Validate that the expected raw Parquet file exists.

    Args:
        table: Expected table metadata.

    Returns:
        Validation result for file existence.
    """
    table_path = get_raw_table_path(table.table_name)

    if table_path.exists():
        return ValidationResult(
            table_name=table.table_name,
            check_name="file_exists",
            status="passed",
            message=f"Found {table_path}",
        )

    return ValidationResult(
        table_name=table.table_name,
        check_name="file_exists",
        status="failed",
        message=f"Missing expected file: {table_path}",
    )


def print_validation_results(results: list[ValidationResult]) -> None:
    """Print validation results in a readable format.

    Args:
        results: List of validation results.
    """
    print("\nValidation results:")
    print("=" * 80)

    for result in results:
        status_label = result.status.upper()
        print(
            f"{status_label:<8} "
            f"{result.table_name:<45} "
            f"{result.check_name:<20} "
            f"{result.message}"
        )

    print("=" * 80)


def fail_if_any_validation_failed(results: list[ValidationResult]) -> None:
    """Raise an error if any validation result failed.

    Args:
        results: List of validation results.

    Raises:
        ValueError: If any validation failed.
    """
    failed_results = [
        result
        for result in results
        if result.status == "failed"
    ]

    if not failed_results:
        return

    failed_checks = "\n".join(
        f"- {result.table_name}: {result.message}"
        for result in failed_results
    )

    raise ValueError(
        "Release validation failed.\n"
        f"{failed_checks}"
    )


@dataclass(frozen=True)
class TableProfile:
    """Basic physical profile for one raw table."""

    table_name: str
    row_count: int
    file_size_bytes: int
    file_size_mb: float

def profile_raw_table(table: TableExpectation) -> TableProfile:
    """Calculate row count and file size for one raw table.

    Args:
        table: Expected table metadata.

    Returns:
        Table profile with row count and file size.
    """
    table_path = get_raw_table_path(table.table_name)

    if not table_path.exists():
        return TableProfile(
            table_name=table.table_name,
            row_count=0,
            file_size_bytes=0,
            file_size_mb=0.0,
        )

    df = pl.read_parquet(table_path)

    file_size_bytes = table_path.stat().st_size
    file_size_mb = round(file_size_bytes / (1024 * 1024), 4)

    return TableProfile(
        table_name=table.table_name,
        row_count=df.height,
        file_size_bytes=file_size_bytes,
        file_size_mb=file_size_mb,
    )

def validate_positive_row_count(
    table: TableExpectation,
    profile: TableProfile,
) -> ValidationResult:
    """Validate that one raw table has at least one row.

    Args:
        table: Expected table metadata.
        profile: Table profile for the raw table.

    Returns:
        Validation result for positive row count.
    """
    if profile.row_count > 0:
        return ValidationResult(
            table_name=table.table_name,
            check_name="positive_row_count",
            status="passed",
            message=f"Row count = {profile.row_count:,}",
        )

    return ValidationResult(
        table_name=table.table_name,
        check_name="positive_row_count",
        status="failed",
        message="Row count is 0",
    )


def print_table_profiles(profiles: list[TableProfile]) -> None:
    """Print row-count and file-size summary.

    Args:
        profiles: Table profiles to print.
    """
    print("\nRaw table physical profile:")
    print("=" * 80)
    print(f"{'table_name':<45} {'rows':>12} {'size_mb':>12}")
    print("-" * 80)

    total_rows = 0
    total_size_mb = 0.0

    for profile in profiles:
        total_rows += profile.row_count
        total_size_mb += profile.file_size_mb

        print(
            f"{profile.table_name:<45} "
            f"{profile.row_count:>12,} "
            f"{profile.file_size_mb:>12,.4f}"
        )

    print("-" * 80)
    print(f"{'TOTAL':<45} {total_rows:>12,} {total_size_mb:>12,.4f}")
    print("=" * 80)


def validate_required_columns(
    table: TableExpectation,
) -> list[ValidationResult]:
    """Validate that required columns exist in one raw table.

    Args:
        table: Expected table metadata.

    Returns:
        Validation results for required-column presence.
    """
    table_path = get_raw_table_path(table.table_name)

    if not table_path.exists():
        return [
            ValidationResult(
                table_name=table.table_name,
                check_name="required_columns",
                status="failed",
                message="Cannot check columns because file is missing",
            )
        ]

    df = pl.read_parquet(table_path)

    actual_columns = set(df.columns)
    required_columns = set(table.required_columns)

    missing_columns = sorted(required_columns - actual_columns)

    if not missing_columns:
        return [
            ValidationResult(
                table_name=table.table_name,
                check_name="required_columns",
                status="passed",
                message=(
                    "All required columns present: "
                    f"{', '.join(table.required_columns)}"
                ),
            )
        ]

    return [
        ValidationResult(
            table_name=table.table_name,
            check_name="required_columns",
            status="failed",
            message=(
                "Missing required columns: "
                f"{', '.join(missing_columns)}"
            ),
        )
    ]

def print_schema_summary() -> None:
    """Print actual raw table columns for quick release inspection."""
    print("\nRaw table schema summary:")
    print("=" * 80)

    for table in EXPECTED_TABLES:
        table_path = get_raw_table_path(table.table_name)

        if not table_path.exists():
            print(f"\n{table.table_name}")
            print("  Missing file")
            continue

        df = pl.read_parquet(table_path)

        print(f"\n{table.table_name}")
        print(f"  Columns ({len(df.columns)}): {', '.join(df.columns)}")

    print("=" * 80)


def get_raw_table_path(table_name: str) -> Path:
    """Return the expected Parquet path for a raw table."""
    return RAW_DATA_DIR / table_name / "part-00000.parquet"


def validate_primary_key_uniqueness(
    table: TableExpectation,
) -> ValidationResult:
    """Validate that the configured primary key is unique and non-null.

    Args:
        table: Expected table metadata.

    Returns:
        Validation result for primary-key quality.
    """
    if table.primary_key is None:
        return ValidationResult(
            table_name=table.table_name,
            check_name="primary_key_uniqueness",
            status="passed",
            message="No primary key configured for this table",
        )

    table_path = get_raw_table_path(table.table_name)

    if not table_path.exists():
        return ValidationResult(
            table_name=table.table_name,
            check_name="primary_key_uniqueness",
            status="failed",
            message="Cannot check primary key because file is missing",
        )

    df = pl.read_parquet(table_path)

    if table.primary_key not in df.columns:
        return ValidationResult(
            table_name=table.table_name,
            check_name="primary_key_uniqueness",
            status="failed",
            message=f"Primary key column missing: {table.primary_key}",
        )

    null_count = df.filter(pl.col(table.primary_key).is_null()).height
    duplicate_count = df.height - df[table.primary_key].n_unique()

    if null_count == 0 and duplicate_count == 0:
        return ValidationResult(
            table_name=table.table_name,
            check_name="primary_key_uniqueness",
            status="passed",
            message=(
                f"{table.primary_key} is unique and non-null "
                f"across {df.height:,} rows"
            ),
        )

    return ValidationResult(
        table_name=table.table_name,
        check_name="primary_key_uniqueness",
        status="failed",
        message=(
            f"{table.primary_key} has "
            f"{null_count:,} nulls and {duplicate_count:,} duplicates"
        ),
    )

@dataclass(frozen=True)
class LineageExpectation:
    """Expected parent-child relationship between raw tables."""

    child_table: str
    child_column: str
    parent_table: str
    parent_column: str
    relationship_name: str

LINEAGE_EXPECTATIONS = [
    LineageExpectation(
        child_table="fact_offer_redemptions",
        child_column="transaction_id",
        parent_table="fact_transactions",
        parent_column="transaction_id",
        relationship_name="redemption_to_transaction",
    ),
    LineageExpectation(
        child_table="fact_control_group_transactions",
        child_column="transaction_id",
        parent_table="fact_transactions",
        parent_column="transaction_id",
        relationship_name="control_transaction_to_transaction",
    ),
    LineageExpectation(
        child_table="fact_reward_liability",
        child_column="redemption_id",
        parent_table="fact_offer_redemptions",
        parent_column="redemption_id",
        relationship_name="reward_liability_to_redemption",
    ),
    LineageExpectation(
        child_table="fact_reward_liability",
        child_column="transaction_id",
        parent_table="fact_transactions",
        parent_column="transaction_id",
        relationship_name="reward_liability_to_transaction",
    ),
    LineageExpectation(
        child_table="fact_merchant_settlements",
        child_column="transaction_id",
        parent_table="fact_transactions",
        parent_column="transaction_id",
        relationship_name="settlement_to_transaction",
    ),
    LineageExpectation(
        child_table="fact_fraud_risk_events",
        child_column="transaction_id",
        parent_table="fact_transactions",
        parent_column="transaction_id",
        relationship_name="fraud_event_to_transaction",
    ),
    LineageExpectation(
        child_table="fact_data_quality_reconciliation",
        child_column="transaction_id",
        parent_table="fact_transactions",
        parent_column="transaction_id",
        relationship_name="reconciliation_to_transaction",
    ),
]

def read_raw_table(table_name: str) -> pl.DataFrame:
    """Read one raw table as a Polars DataFrame.

    Args:
        table_name: Raw table folder name.

    Returns:
        Raw table DataFrame.

    Raises:
        FileNotFoundError: If the expected raw table file is missing.
    """
    table_path = get_raw_table_path(table_name)

    if not table_path.exists():
        raise FileNotFoundError(f"Missing raw table file: {table_path}")

    return pl.read_parquet(table_path)

def validate_lineage_relationship(
    expectation: LineageExpectation,
) -> ValidationResult:
    """Validate that child table keys exist in parent table keys.

    Args:
        expectation: Parent-child lineage expectation.

    Returns:
        Validation result for the lineage relationship.
    """
    child_path = get_raw_table_path(expectation.child_table)
    parent_path = get_raw_table_path(expectation.parent_table)

    if not child_path.exists():
        return ValidationResult(
            table_name=expectation.child_table,
            check_name=expectation.relationship_name,
            status="failed",
            message=f"Child table file missing: {child_path}",
        )

    if not parent_path.exists():
        return ValidationResult(
            table_name=expectation.child_table,
            check_name=expectation.relationship_name,
            status="failed",
            message=f"Parent table file missing: {parent_path}",
        )

    child_df = read_raw_table(expectation.child_table)
    parent_df = read_raw_table(expectation.parent_table)

    if expectation.child_column not in child_df.columns:
        return ValidationResult(
            table_name=expectation.child_table,
            check_name=expectation.relationship_name,
            status="failed",
            message=f"Child column missing: {expectation.child_column}",
        )

    if expectation.parent_column not in parent_df.columns:
        return ValidationResult(
            table_name=expectation.child_table,
            check_name=expectation.relationship_name,
            status="failed",
            message=f"Parent column missing: {expectation.parent_column}",
        )

    child_keys = child_df.select(expectation.child_column).unique()
    parent_keys = parent_df.select(expectation.parent_column).unique()

    orphan_keys = child_keys.join(
        parent_keys,
        left_on=expectation.child_column,
        right_on=expectation.parent_column,
        how="anti",
    )

    orphan_count = orphan_keys.height

    if orphan_count == 0:
        return ValidationResult(
            table_name=expectation.child_table,
            check_name=expectation.relationship_name,
            status="passed",
            message=(
                f"All {expectation.child_column} values exist in "
                f"{expectation.parent_table}.{expectation.parent_column}"
            ),
        )

    return ValidationResult(
        table_name=expectation.child_table,
        check_name=expectation.relationship_name,
        status="failed",
        message=(
            f"{orphan_count:,} orphan keys found for "
            f"{expectation.child_column} against "
            f"{expectation.parent_table}.{expectation.parent_column}"
        ),
    )

def validate_columns_available(
    table_name: str,
    required_columns: tuple[str, ...],
) -> ValidationResult | None:
    """Validate that columns required for a specialized check exist.

    Args:
        table_name: Raw table name.
        required_columns: Columns needed for the check.

    Returns:
        A failed ValidationResult if columns are missing, otherwise None.
    """
    table_path = get_raw_table_path(table_name)

    if not table_path.exists():
        return ValidationResult(
            table_name=table_name,
            check_name="financial_formula_columns",
            status="failed",
            message=f"Cannot check formulas because file is missing: {table_path}",
        )

    df = read_raw_table(table_name)
    missing_columns = sorted(set(required_columns) - set(df.columns))

    if missing_columns:
        return ValidationResult(
            table_name=table_name,
            check_name="financial_formula_columns",
            status="failed",
            message=(
                "Missing columns for financial formula check: "
                f"{', '.join(missing_columns)}"
            ),
        )

    return None

def validate_reward_funding_split() -> ValidationResult:
    """Validate reward funding split in fact_reward_liability.

    Formula:
        merchant_funded_amount + platform_funded_amount = reward_amount

    Returns:
        Validation result for reward funding split.
    """
    table_name = "fact_reward_liability"
    required_columns = (
        "reward_amount",
        "merchant_funded_amount",
        "platform_funded_amount",
    )

    column_result = validate_columns_available(
        table_name=table_name,
        required_columns=required_columns,
    )

    if column_result is not None:
        return column_result

    df = read_raw_table(table_name)

    mismatch_count = df.filter(
        (
            pl.col("merchant_funded_amount")
            + pl.col("platform_funded_amount")
            - pl.col("reward_amount")
        )
        .abs()
        > MONEY_TOLERANCE
    ).height

    if mismatch_count == 0:
        return ValidationResult(
            table_name=table_name,
            check_name="reward_funding_split",
            status="passed",
            message=(
                "merchant_funded_amount + platform_funded_amount "
                "matches reward_amount"
            ),
        )

    return ValidationResult(
        table_name=table_name,
        check_name="reward_funding_split",
        status="failed",
        message=f"{mismatch_count:,} rows have funding split mismatches",
    )

def validate_merchant_settlement_formulas() -> list[ValidationResult]:
    """Validate settlement formulas in fact_merchant_settlements.

    Formulas:
        merchant_settlement_amount =
            gross_transaction_amount - platform_fee_amount

        merchant_net_after_reward =
            merchant_settlement_amount - merchant_funded_amount

    Returns:
        Validation results for merchant settlement formulas.
    """
    table_name = "fact_merchant_settlements"
    required_columns = (
        "gross_transaction_amount",
        "platform_fee_amount",
        "merchant_settlement_amount",
        "merchant_funded_amount",
        "merchant_net_after_reward",
    )

    column_result = validate_columns_available(
        table_name=table_name,
        required_columns=required_columns,
    )

    if column_result is not None:
        return [column_result]

    df = read_raw_table(table_name)

    settlement_mismatch_count = df.filter(
        (
            pl.col("gross_transaction_amount")
            - pl.col("platform_fee_amount")
            - pl.col("merchant_settlement_amount")
        )
        .abs()
        > MONEY_TOLERANCE
    ).height

    net_after_reward_mismatch_count = df.filter(
        (
            pl.col("merchant_settlement_amount")
            - pl.col("merchant_funded_amount")
            - pl.col("merchant_net_after_reward")
        )
        .abs()
        > MONEY_TOLERANCE
    ).height

    results = []

    if settlement_mismatch_count == 0:
        results.append(
            ValidationResult(
                table_name=table_name,
                check_name="merchant_settlement_amount_formula",
                status="passed",
                message=(
                    "merchant_settlement_amount matches "
                    "gross_transaction_amount - platform_fee_amount"
                ),
            )
        )
    else:
        results.append(
            ValidationResult(
                table_name=table_name,
                check_name="merchant_settlement_amount_formula",
                status="failed",
                message=(
                    f"{settlement_mismatch_count:,} rows have "
                    "merchant settlement formula mismatches"
                ),
            )
        )

    if net_after_reward_mismatch_count == 0:
        results.append(
            ValidationResult(
                table_name=table_name,
                check_name="merchant_net_after_reward_formula",
                status="passed",
                message=(
                    "merchant_net_after_reward matches "
                    "merchant_settlement_amount - merchant_funded_amount"
                ),
            )
        )
    else:
        results.append(
            ValidationResult(
                table_name=table_name,
                check_name="merchant_net_after_reward_formula",
                status="failed",
                message=(
                    f"{net_after_reward_mismatch_count:,} rows have "
                    "merchant net-after-reward formula mismatches"
                ),
            )
        )

    return results

def validate_reconciliation_delta_formula() -> ValidationResult:
    """Validate settlement delta formula in reconciliation table.

    Formula:
        settlement_delta =
            transaction_amount
            - merchant_settlement_amount
            - platform_fee_amount

    Returns:
        Validation result for reconciliation delta calculation.
    """
    table_name = "fact_data_quality_reconciliation"
    required_columns = (
        "transaction_amount",
        "merchant_settlement_amount",
        "platform_fee_amount",
        "settlement_delta",
    )

    column_result = validate_columns_available(
        table_name=table_name,
        required_columns=required_columns,
    )

    if column_result is not None:
        return column_result

    df = read_raw_table(table_name)

    mismatch_count = df.filter(
        (
            pl.col("transaction_amount")
            - pl.col("merchant_settlement_amount")
            - pl.col("platform_fee_amount")
            - pl.col("settlement_delta")
        )
        .abs()
        > MONEY_TOLERANCE
    ).height

    if mismatch_count == 0:
        return ValidationResult(
            table_name=table_name,
            check_name="reconciliation_delta_formula",
            status="passed",
            message=(
                "settlement_delta matches transaction_amount "
                "- merchant_settlement_amount - platform_fee_amount"
            ),
        )

    return ValidationResult(
        table_name=table_name,
        check_name="reconciliation_delta_formula",
        status="failed",
        message=f"{mismatch_count:,} rows have reconciliation delta mismatches",
    )

def run_financial_formula_checks() -> list[ValidationResult]:
    """Run all financial formula validations.

    Returns:
        Financial formula validation results.
    """
    results = []

    results.append(validate_reward_funding_split())

    results.extend(validate_merchant_settlement_formulas())

    results.append(validate_reconciliation_delta_formula())

    return results


def summarize_validation_results(
    results: list[ValidationResult],
) -> dict[str, int]:
    """Summarize validation result counts.

    Args:
        results: Validation results.

    Returns:
        Dictionary with total, passed, and failed counts.
    """
    passed_count = sum(1 for result in results if result.status == "passed")
    failed_count = sum(1 for result in results if result.status == "failed")

    return {
        "total": len(results),
        "passed": passed_count,
        "failed": failed_count,
    }

def build_profile_markdown_table(profiles: list[TableProfile]) -> str:
    """Build a Markdown table for raw table profiles.

    Args:
        profiles: Raw table profiles.

    Returns:
        Markdown table as a string.
    """
    lines = [
        "| Table | Rows | File Size MB |",
        "|---|---:|---:|",
    ]

    total_rows = 0
    total_size_mb = 0.0

    for profile in profiles:
        total_rows += profile.row_count
        total_size_mb += profile.file_size_mb

        lines.append(
            "| "
            f"{profile.table_name} | "
            f"{profile.row_count:,} | "
            f"{profile.file_size_mb:,.4f} |"
        )

    lines.append(
        "| "
        "**TOTAL** | "
        f"**{total_rows:,}** | "
        f"**{total_size_mb:,.4f}** |"
    )

    return "\n".join(lines)

def build_validation_markdown_table(
    results: list[ValidationResult],
) -> str:
    """Build a Markdown table for validation results.

    Args:
        results: Validation results.

    Returns:
        Markdown table as a string.
    """
    lines = [
        "| Status | Table | Check | Message |",
        "|---|---|---|---|",
    ]

    for result in results:
        lines.append(
            "| "
            f"{result.status.upper()} | "
            f"{result.table_name} | "
            f"{result.check_name} | "
            f"{result.message.replace('|', '/')}"
            " |"
        )

    return "\n".join(lines)


def summarize_validation_results(
    results: list[ValidationResult],
) -> dict[str, int]:
    """Summarize validation result counts.

    Args:
        results: Validation results.

    Returns:
        Dictionary with total, passed, and failed counts.
    """
    passed_count = sum(
        1
        for result in results
        if result.status == "passed"
    )

    failed_count = sum(
        1
        for result in results
        if result.status == "failed"
    )

    return {
        "total": len(results),
        "passed": passed_count,
        "failed": failed_count,
    }


def build_profile_markdown_table(
    profiles: list[TableProfile],
) -> str:
    """Build a Markdown table for raw table profiles.

    Args:
        profiles: Raw table profiles.

    Returns:
        Markdown table as a string.
    """
    lines = [
        "| Table | Rows | File Size MB |",
        "|---|---:|---:|",
    ]

    total_rows = 0
    total_size_mb = 0.0

    for profile in profiles:
        total_rows += profile.row_count
        total_size_mb += profile.file_size_mb

        lines.append(
            "| "
            f"{profile.table_name} | "
            f"{profile.row_count:,} | "
            f"{profile.file_size_mb:,.4f} |"
        )

    lines.append(
        "| "
        "**TOTAL** | "
        f"**{total_rows:,}** | "
        f"**{total_size_mb:,.4f}** |"
    )

    return "\n".join(lines)


def clean_markdown_cell(value: str) -> str:
    """Clean text so it can safely appear inside a Markdown table cell.

    Args:
        value: Raw string value.

    Returns:
        Markdown-safe string.
    """
    return (
        value
        .replace("|", "/")
        .replace("\n", " ")
        .replace("\r", " ")
    )


def build_validation_markdown_table(
    results: list[ValidationResult],
) -> str:
    """Build a Markdown table for validation results.

    Args:
        results: Validation results.

    Returns:
        Markdown table as a string.
    """
    lines = [
        "| Status | Table | Check | Message |",
        "|---|---|---|---|",
    ]

    for result in results:
        lines.append(
            "| "
            f"{result.status.upper()} | "
            f"{result.table_name} | "
            f"{result.check_name} | "
            f"{clean_markdown_cell(result.message)} |"
        )

    return "\n".join(lines)


def write_release_report(
    profiles: list[TableProfile],
    results: list[ValidationResult],
) -> None:
    """Write the data foundation release report.

    Args:
        profiles: Raw table physical profiles.
        results: Validation results.
    """
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    summary = summarize_validation_results(results)

    if summary["failed"] == 0:
        release_status = "PASSED"
    else:
        release_status = "FAILED"

    profile_table = build_profile_markdown_table(profiles)
    validation_table = build_validation_markdown_table(results)

    generated_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    report_lines = [
        "# Data Foundation Release Report",
        "",
        "## Release Status",
        "",
        f"**Status:** {release_status}",
        "",
        f"**Generated At:** {generated_at}",
        "",
        "## Purpose",
        "",
        (
            "This report validates the MerchantLift BI raw synthetic data "
            "foundation."
        ),
        "",
        (
            "The goal of this release is to prove that the generated raw lake "
            "is complete, measurable, structurally usable, traceable, and "
            "financially coherent before Spark/Delta lakehouse ingestion begins."
        ),
        "",
        "## Validation Summary",
        "",
        "| Metric | Count |",
        "|---|---:|",
        f"| Total validation checks | {summary['total']:,} |",
        f"| Passed checks | {summary['passed']:,} |",
        f"| Failed checks | {summary['failed']:,} |",
        "",
        "## Raw Table Physical Profile",
        "",
        profile_table,
        "",
        "## Validation Scope",
        "",
        "The release validator checks:",
        "",
        "- raw table file existence",
        "- positive row counts",
        "- raw Parquet file size",
        "- required column presence",
        "- primary-key uniqueness and non-nullness",
        "- critical lineage relationships",
        "- reward funding split formula",
        "- merchant settlement formulas",
        "- reconciliation delta formula",
        "",
        "## Critical Lineage Relationships Validated",
        "",
        "The release validates that:",
        "",
        "- redemptions point to real transactions",
        "- control-group transactions point to real transactions",
        "- reward liabilities point to real redemptions",
        "- reward liabilities point to real transactions",
        "- merchant settlements point to real transactions",
        "- fraud-risk events point to real transactions",
        "- reconciliation checks point to real transactions",
        "",
        "## Financial Formula Checks Validated",
        "",
        "The release validates:",
        "",
        "```text",
        "merchant_funded_amount + platform_funded_amount = reward_amount",
        "```",
        "",
        "```text",
        "merchant_settlement_amount =",
        "gross_transaction_amount - platform_fee_amount",
        "```",
        "",
        "```text",
        "merchant_net_after_reward =",
        "merchant_settlement_amount - merchant_funded_amount",
        "```",
        "",
        "```text",
        "settlement_delta =",
        "transaction_amount - merchant_settlement_amount - platform_fee_amount",
        "```",
        "",
        "## Validation Results",
        "",
        validation_table,
        "",
        "## Release Readiness Statement",
        "",
        (
            "If the release status is PASSED, the raw synthetic data foundation "
            "is ready for Bronze ingestion into the Spark/Delta lakehouse layer."
        ),
        "",
        (
            "If the release status is FAILED, fix the failed checks before "
            "proceeding to Spark/Delta ingestion."
        ),
        "",
        "## Known Limitations",
        "",
        "This release validates the raw data foundation only.",
        "",
        "It does not yet validate:",
        "",
        "- Bronze Delta table creation",
        "- Silver cleaning rules",
        "- Gold business marts",
        "- dbt semantic models",
        "- BigQuery policy tags",
        "- Power BI dashboards",
        "",
        "Those are handled in later implementation phases.",
        "",
        "## Portfolio Evidence",
        "",
        (
            "This report demonstrates that MerchantLift BI has a measurable and "
            "testable data foundation, not only generated files."
        ),
        "",
        (
            "The foundation includes synthetic merchant-offer events, "
            "transaction activity, redemptions, control-group transactions, "
            "reward liability, merchant settlements, fraud-risk events, and "
            "reconciliation checks."
        ),
        "",
    ]

    REPORT_PATH.write_text("\n".join(report_lines))

    print(f"\nWrote release report to {REPORT_PATH}")


def main() -> None:
    """Run release validation checks."""
    print("MerchantLift BI data foundation validation")
    print("=" * 80)
    print(f"Raw data directory: {RAW_DATA_DIR}")
    print(f"Expected table count: {len(EXPECTED_TABLES)}")
    print("=" * 80)

    results: list[ValidationResult] = []
    profiles: list[TableProfile] = []

    for table in EXPECTED_TABLES:
        file_result = validate_file_exists(table)
        results.append(file_result)

        profile = profile_raw_table(table)
        profiles.append(profile)

        row_count_result = validate_positive_row_count(
            table=table,
            profile=profile,
        )
        results.append(row_count_result)
        column_results = validate_required_columns(table)
        results.extend(column_results)

        primary_key_result = validate_primary_key_uniqueness(table)
        results.append(primary_key_result)

    for lineage_expectation in LINEAGE_EXPECTATIONS:
        lineage_result = validate_lineage_relationship(lineage_expectation)
        results.append(lineage_result)

    financial_results = run_financial_formula_checks()
    results.extend(financial_results)

    print_validation_results(results)

    print_table_profiles(profiles)

    print_schema_summary()

    write_release_report(
        profiles=profiles,
        results=results,
    )

    fail_if_any_validation_failed(results)

    print("\nRelease row-count, file-size, and schema validation passed.")

if __name__ == "__main__":
    main()