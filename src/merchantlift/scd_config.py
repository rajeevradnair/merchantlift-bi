"""SCD dimension configuration for MerchantLift BI."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScdDimensionConfig:
    """Configuration for one slowly changing dimension."""

    source_table_name: str
    output_table_name: str
    business_key: str
    tracked_columns: tuple[str, ...]


SCD_DIMENSIONS = [
    ScdDimensionConfig(
        source_table_name="dim_merchant_clean",
        output_table_name="dim_merchant_scd",
        business_key="merchant_id",
        tracked_columns=(
            "merchant_name",
            "category_id",
            "location_id",
            "merchant_margin_rate",
            "platform_fee_rate",
        ),
    ),
    ScdDimensionConfig(
        source_table_name="dim_offer_clean",
        output_table_name="dim_offer_scd",
        business_key="offer_id",
        tracked_columns=(
            "campaign_id",
            "merchant_id",
            "minimum_spend_amount",
            "reward_amount",
            "offer_start_date",
            "offer_end_date",
        ),
    ),
    ScdDimensionConfig(
        source_table_name="dim_campaign_clean",
        output_table_name="dim_campaign_scd",
        business_key="campaign_id",
        tracked_columns=(
            "campaign_name",
            "campaign_start_date",
            "campaign_end_date",
        ),
    ),
]