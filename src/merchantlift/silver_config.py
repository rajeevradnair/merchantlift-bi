"""Silver transformation table configuration for MerchantLift BI."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SilverTableConfig:
    """Configuration for one Bronze-to-Silver table transformation."""

    bronze_table_name: str
    silver_table_name: str
    primary_key: str
    timestamp_columns: tuple[str, ...]
    date_columns: tuple[str, ...]
    numeric_columns: tuple[str, ...]

@dataclass(frozen=True)
class SilverRelationshipConfig:
    """Configuration for one Silver referential integrity check."""

    child_table_name: str
    child_column: str
    parent_table_name: str
    parent_column: str
    relationship_name: str

SILVER_TABLES = [
    SilverTableConfig(
        bronze_table_name="dim_category",
        silver_table_name="dim_category_clean",
        primary_key="category_id",
        timestamp_columns=(),
        date_columns=(),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="dim_location",
        silver_table_name="dim_location_clean",
        primary_key="location_id",
        timestamp_columns=(),
        date_columns=(),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="dim_segment",
        silver_table_name="dim_segment_clean",
        primary_key="segment_id",
        timestamp_columns=(),
        date_columns=(),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="dim_risk_rule",
        silver_table_name="dim_risk_rule_clean",
        primary_key="risk_rule_id",
        timestamp_columns=(),
        date_columns=(),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="dim_merchant",
        silver_table_name="dim_merchant_clean",
        primary_key="merchant_id",
        timestamp_columns=(),
        date_columns=(),
        numeric_columns=(
            "merchant_margin_rate",
            "platform_fee_rate",
        ),
    ),
    SilverTableConfig(
        bronze_table_name="dim_campaign",
        silver_table_name="dim_campaign_clean",
        primary_key="campaign_id",
        timestamp_columns=(),
        date_columns=(
            "campaign_start_date",
            "campaign_end_date",
        ),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="dim_offer",
        silver_table_name="dim_offer_clean",
        primary_key="offer_id",
        timestamp_columns=(),
        date_columns=(
            "offer_start_date",
            "offer_end_date",
        ),
        numeric_columns=(
            "minimum_spend_amount",
            "reward_amount",
        ),
    ),
    SilverTableConfig(
        bronze_table_name="dim_cardmember_token",
        silver_table_name="dim_cardmember_token_clean",
        primary_key="tokenized_cardmember_id",
        timestamp_columns=(),
        date_columns=(),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="dim_privacy_consent",
        silver_table_name="dim_privacy_consent_clean",
        primary_key="tokenized_cardmember_id",
        timestamp_columns=(),
        date_columns=(),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="dim_date",
        silver_table_name="dim_date_clean",
        primary_key="date_id",
        timestamp_columns=(),
        date_columns=("calendar_date",),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="fact_transactions",
        silver_table_name="fact_transactions_clean",
        primary_key="transaction_id",
        timestamp_columns=("transaction_timestamp",),
        date_columns=("transaction_date",),
        numeric_columns=("transaction_amount",),
    ),
    SilverTableConfig(
        bronze_table_name="fact_offer_customer_assignment",
        silver_table_name="fact_offer_customer_assignment_clean",
        primary_key="assignment_id",
        timestamp_columns=(),
        date_columns=("assignment_date",),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="fact_offer_impressions",
        silver_table_name="fact_offer_impressions_clean",
        primary_key="impression_id",
        timestamp_columns=("impression_timestamp",),
        date_columns=("impression_date",),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="fact_offer_activations",
        silver_table_name="fact_offer_activations_clean",
        primary_key="activation_id",
        timestamp_columns=(
            "activation_timestamp",
            "offer_expiry_timestamp",
        ),
        date_columns=("activation_date",),
        numeric_columns=(),
    ),
    SilverTableConfig(
        bronze_table_name="fact_offer_redemptions",
        silver_table_name="fact_offer_redemptions_clean",
        primary_key="redemption_id",
        timestamp_columns=("redemption_timestamp",),
        date_columns=("redemption_date",),
        numeric_columns=(
            "transaction_amount",
            "calculated_reward_amount",
        ),
    ),
    SilverTableConfig(
        bronze_table_name="fact_control_group_transactions",
        silver_table_name="fact_control_group_transactions_clean",
        primary_key="control_transaction_id",
        timestamp_columns=("transaction_timestamp",),
        date_columns=("transaction_date",),
        numeric_columns=(
            "transaction_amount",
            "match_quality_score",
        ),
    ),
    SilverTableConfig(
        bronze_table_name="fact_reward_liability",
        silver_table_name="fact_reward_liability_clean",
        primary_key="reward_liability_id",
        timestamp_columns=("liability_timestamp",),
        date_columns=("liability_date",),
        numeric_columns=(
            "reward_amount",
            "merchant_funded_amount",
            "platform_funded_amount",
        ),
    ),
    SilverTableConfig(
        bronze_table_name="fact_merchant_settlements",
        silver_table_name="fact_merchant_settlements_clean",
        primary_key="settlement_id",
        timestamp_columns=(),
        date_columns=("settlement_date",),
        numeric_columns=(
            "gross_transaction_amount",
            "platform_fee_amount",
            "merchant_settlement_amount",
            "merchant_net_after_reward",
        ),
    ),
    SilverTableConfig(
        bronze_table_name="fact_fraud_risk_events",
        silver_table_name="fact_fraud_risk_events_clean",
        primary_key="fraud_event_id",
        timestamp_columns=("event_timestamp",),
        date_columns=("event_date",),
        numeric_columns=("risk_score",),
    ),
    SilverTableConfig(
        bronze_table_name="fact_data_quality_reconciliation",
        silver_table_name="fact_data_quality_reconciliation_clean",
        primary_key="reconciliation_id",
        timestamp_columns=(),
        date_columns=("reconciliation_date",),
        numeric_columns=(
            "transaction_amount",
            "merchant_settlement_amount",
            "platform_fee_amount",
            "settlement_delta",
        ),
    ),
]

SILVER_RELATIONSHIPS = [
    SilverRelationshipConfig(
        child_table_name="fact_offer_redemptions_clean",
        child_column="transaction_id",
        parent_table_name="fact_transactions_clean",
        parent_column="transaction_id",
        relationship_name="redemption_to_transaction",
    ),
    SilverRelationshipConfig(
        child_table_name="fact_control_group_transactions_clean",
        child_column="transaction_id",
        parent_table_name="fact_transactions_clean",
        parent_column="transaction_id",
        relationship_name="control_transaction_to_transaction",
    ),
    SilverRelationshipConfig(
        child_table_name="fact_reward_liability_clean",
        child_column="redemption_id",
        parent_table_name="fact_offer_redemptions_clean",
        parent_column="redemption_id",
        relationship_name="reward_liability_to_redemption",
    ),
    SilverRelationshipConfig(
        child_table_name="fact_reward_liability_clean",
        child_column="transaction_id",
        parent_table_name="fact_transactions_clean",
        parent_column="transaction_id",
        relationship_name="reward_liability_to_transaction",
    ),
    SilverRelationshipConfig(
        child_table_name="fact_merchant_settlements_clean",
        child_column="transaction_id",
        parent_table_name="fact_transactions_clean",
        parent_column="transaction_id",
        relationship_name="settlement_to_transaction",
    ),
    SilverRelationshipConfig(
        child_table_name="fact_fraud_risk_events_clean",
        child_column="transaction_id",
        parent_table_name="fact_transactions_clean",
        parent_column="transaction_id",
        relationship_name="fraud_event_to_transaction",
    ),
    SilverRelationshipConfig(
        child_table_name="fact_data_quality_reconciliation_clean",
        child_column="transaction_id",
        parent_table_name="fact_transactions_clean",
        parent_column="transaction_id",
        relationship_name="reconciliation_to_transaction",
    ),
]