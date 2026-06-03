"""Bronze ingestion table inventory for MerchantLift BI."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeTableConfig:
    """Configuration for one raw-to-bronze table ingestion."""

    table_name: str
    partition_column: str | None


BRONZE_TABLES = [
    BronzeTableConfig(
        table_name="dim_category",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_location",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_segment",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_risk_rule",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_merchant",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_campaign",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_offer",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_cardmember_token",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_privacy_consent",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="dim_date",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="fact_transactions",
        partition_column="transaction_date",
    ),
    BronzeTableConfig(
        table_name="fact_offer_customer_assignment",
        partition_column=None,
    ),
    BronzeTableConfig(
        table_name="fact_offer_impressions",
        partition_column="impression_date",
    ),
    BronzeTableConfig(
        table_name="fact_offer_activations",
        partition_column="activation_date",
    ),
    BronzeTableConfig(
        table_name="fact_offer_redemptions",
        partition_column="redemption_date",
    ),
    BronzeTableConfig(
        table_name="fact_control_group_transactions",
        partition_column="transaction_date",
    ),
    BronzeTableConfig(
        table_name="fact_reward_liability",
        partition_column="liability_date",
    ),
    BronzeTableConfig(
        table_name="fact_merchant_settlements",
        partition_column="settlement_date",
    ),
    BronzeTableConfig(
        table_name="fact_fraud_risk_events",
        partition_column="event_date",
    ),
    BronzeTableConfig(
        table_name="fact_data_quality_reconciliation",
        partition_column="reconciliation_date",
    ),
]