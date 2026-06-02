"""Generate offer interaction events for MerchantLift BI.

This script creates the offer funnel before redemption:

1. fact_offer_customer_assignment
2. fact_offer_impressions
3. fact_offer_activations

The funnel is:

    assigned / eligible
        -> shown as impression
        -> activated by cardmember

Outputs:
    data/raw/fact_offer_customer_assignment/part-00000.parquet
    data/raw/fact_offer_impressions/part-00000.parquet
    data/raw/fact_offer_activations/part-00000.parquet
"""

from __future__ import annotations

import random
from datetime import datetime, time, timedelta
from typing import Any

import polars as pl

from merchantlift.config import load_yaml_config
from merchantlift.paths import CONFIG_DIR, RAW_DATA_DIR


def make_id(prefix: str, number: int, width: int = 9) -> str:
    """Create deterministic business event IDs."""
    return f"{prefix}_{number:0{width}d}"


def get_config() -> dict[str, Any]:
    """Load project settings."""
    return load_yaml_config(CONFIG_DIR / "project_settings.yaml")

def read_raw_table(table_name: str) -> pl.DataFrame:
    """Read a raw Parquet table from data/raw/<table_name>/."""
    parquet_path = RAW_DATA_DIR / table_name / "part-00000.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Missing required table: {table_name}. "
            f"Expected file at: {parquet_path}"
        )

    return pl.read_parquet(parquet_path)

def write_parquet_table(df: pl.DataFrame, table_name: str) -> None:
    """Write a DataFrame to data/raw/<table_name>/part-00000.parquet."""
    output_dir = RAW_DATA_DIR / table_name
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "part-00000.parquet"
    df.write_parquet(output_path)

    print(f"Wrote {df.height:,} rows to {output_path}")


def load_offer_interaction_inputs() -> tuple[
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
]:
    """Load dimension tables needed for offer interactions."""
    dim_cardmember_token = read_raw_table("dim_cardmember_token")
    dim_privacy_consent = read_raw_table("dim_privacy_consent")
    dim_offer = read_raw_table("dim_offer")
    dim_campaign = read_raw_table("dim_campaign")

    return dim_cardmember_token, dim_privacy_consent, dim_offer, dim_campaign

#######################################
## Assignment generation functions
#######################################
def build_eligible_cardmember_pool(
    dim_cardmember_token: pl.DataFrame,
    dim_privacy_consent: pl.DataFrame,
) -> list[dict[str, Any]]:
    """Build cardmember pool eligible for offer assignment.

    A cardmember is eligible if:
    - they exist in dim_cardmember_token
    - they have analytics consent
    - they have merchant reporting consent
    """
    eligible = dim_cardmember_token.join(
        dim_privacy_consent,
        on="tokenized_cardmember_id",
        how="inner", # Remember we are doing inner join
    ).filter(
        (pl.col("analytics_consent_flag") == True)
        & (pl.col("merchant_reporting_consent_flag") == True)
    )

    return eligible.to_dicts()

def build_active_offer_pool(
    dim_offer: pl.DataFrame,
    dim_campaign: pl.DataFrame,
) -> list[dict[str, Any]]:
    """Build offer pool with campaign context.

    We keep offers that are active or expired, because synthetic events
    can occur across historical campaign windows.
    """
    offer_with_campaign = dim_offer.join(
        dim_campaign,
        on="campaign_id",
        how="left",
        suffix="_campaign",
        coalesce=True,
    ).filter(
        pl.col("offer_status").is_in(["active", "expired"])
    )

    return offer_with_campaign.to_dicts()


def choose_assignment_group(config: dict[str, Any]) -> str:
    """Choose test/control/holdout assignment group."""
    group_mix = config["offer_interactions"]["assignment_group_mix"]

    groups = list(group_mix.keys())
    weights = list(group_mix.values())

    return random.choices(groups, weights=weights, k=1)[0]


def generate_assignment_row(
    assignment_number: int,
    cardmember: dict[str, Any],
    offer: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Generate one offer-customer assignment row."""
    assignment_group = choose_assignment_group(config)

    assignment_status = "eligible"

    if assignment_group == "test" and not bool(cardmember["is_test_eligible"]):
        assignment_status = "ineligible"

    if assignment_group == "control" and not bool(cardmember["is_control_eligible"]):
        assignment_status = "ineligible"

    return {
        "assignment_id": make_id("assign", assignment_number),
        "tokenized_cardmember_id": cardmember["tokenized_cardmember_id"],
        "offer_id": offer["offer_id"],
        "campaign_id": offer["campaign_id"],
        "merchant_id": offer["merchant_id"],
        "segment_id": cardmember["segment_id"],
        "assignment_group": assignment_group,
        "assignment_status": assignment_status,
        "match_group_id": make_id("match", assignment_number, width=6),
        "assignment_date": offer["offer_start_date"],
        "shopper_behavior_type": cardmember["shopper_behavior_type"],
        "merchant_reporting_consent_flag": cardmember[
            "merchant_reporting_consent_flag"
        ],
        "created_at": datetime.utcnow(),
    }


def generate_offer_customer_assignments(
    config: dict[str, Any],
    row_count: int,
) -> pl.DataFrame:
    """Generate fact_offer_customer_assignment rows."""
    (
        dim_cardmember_token,
        dim_privacy_consent,
        dim_offer,
        dim_campaign,
    ) = load_offer_interaction_inputs()

    eligible_cardmembers = build_eligible_cardmember_pool(
        dim_cardmember_token=dim_cardmember_token,
        dim_privacy_consent=dim_privacy_consent,
    )

    offers_with_campaign = build_active_offer_pool(
        dim_offer=dim_offer,
        dim_campaign=dim_campaign,
    )

    rows = []

    for assignment_number in range(1, row_count + 1):
        rows.append(
            generate_assignment_row(
                assignment_number=assignment_number,
                cardmember=random.choice(eligible_cardmembers),
                offer=random.choice(offers_with_campaign),
                config=config,
            )
        )

    return pl.DataFrame(rows)

#######################################
## Impression generation functions
#######################################

def choose_impression_channel(config: dict[str, Any]) -> str:
    """Choose app/email/web impression channel."""
    channel_mix = config["offer_interactions"]["impression_channel_mix"]

    channels = list(channel_mix.keys())
    weights = list(channel_mix.values())

    return random.choices(channels, weights=weights, k=1)[0]


def generate_timestamp_within_offer_window(
    start_date: Any,
    end_date: Any,
) -> datetime:
    """Generate timestamp inside an offer window."""
    total_days = max((end_date - start_date).days, 1)
    selected_date = start_date + timedelta(days=random.randint(0, total_days))

    return datetime.combine(
        selected_date,
        time(
            hour=random.randint(7, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        ),
    )


def generate_offer_impressions(
    assignments: pl.DataFrame,
    config: dict[str, Any],
    row_count: int,
) -> pl.DataFrame:
    """Generate fact_offer_impressions from eligible test assignments."""
    eligible_test_assignments = assignments.filter(
        (pl.col("assignment_group") == "test")
        & (pl.col("assignment_status") == "eligible")
    )

    if eligible_test_assignments.height == 0:
        raise ValueError("No eligible test assignments available for impressions.")

    assignment_rows = eligible_test_assignments.to_dicts()
    channel_click_rates = config["offer_interactions"]["click_rate_by_channel"]

    rows = []

    for impression_number in range(1, row_count + 1):
        assignment = random.choice(assignment_rows)
        channel = choose_impression_channel(config)

        impression_timestamp = generate_timestamp_within_offer_window(
            start_date=assignment["assignment_date"],
            end_date=assignment["assignment_date"] + timedelta(days=30),
        )

        was_clicked = random.random() < float(channel_click_rates[channel])

        rows.append(
            {
                "impression_id": make_id("imp", impression_number),
                "assignment_id": assignment["assignment_id"],
                "tokenized_cardmember_id": assignment["tokenized_cardmember_id"],
                "offer_id": assignment["offer_id"],
                "campaign_id": assignment["campaign_id"],
                "merchant_id": assignment["merchant_id"],
                "segment_id": assignment["segment_id"],
                "impression_timestamp": impression_timestamp,
                "impression_date": impression_timestamp.date(),
                "channel": channel,
                "was_clicked": was_clicked,
                "created_at": datetime.utcnow(),
            }
        )

    return pl.DataFrame(rows)


#######################################
## Activation generation functions
#######################################

def generate_offer_activations(
    impressions: pl.DataFrame,
    config: dict[str, Any],
    max_rows: int,
) -> pl.DataFrame:
    """Generate fact_offer_activations from impression rows."""
    activation_rates = config["offer_interactions"]["activation_rate_by_segment"]

    impression_rows = impressions.to_dicts()
    rows = []

    for impression in impression_rows:
        segment_id = impression["segment_id"]

        # Our dim_segment IDs are segment_001, segment_002, etc.
        # If exact segment names are unavailable here, use a safe default.
        activation_probability = 0.25

        # Clicked impressions should be more likely to activate.
        if bool(impression["was_clicked"]):
            activation_probability += 0.20

        if random.random() >= activation_probability:
            continue

        activation_timestamp = impression["impression_timestamp"] + timedelta(
            minutes=random.randint(1, 240)
        )

        rows.append(
            {
                "activation_id": make_id("act", len(rows) + 1),
                "impression_id": impression["impression_id"],
                "assignment_id": impression["assignment_id"],
                "tokenized_cardmember_id": impression["tokenized_cardmember_id"],
                "offer_id": impression["offer_id"],
                "campaign_id": impression["campaign_id"],
                "merchant_id": impression["merchant_id"],
                "segment_id": segment_id,
                "activation_timestamp": activation_timestamp,
                "activation_date": activation_timestamp.date(),
                "offer_expiry_timestamp": activation_timestamp + timedelta(days=30),
                "activation_status": "active",
                "created_at": datetime.utcnow(),
            }
        )

        if len(rows) >= max_rows:
            break

    return pl.DataFrame(rows)


def main() -> None:
    """Generate offer assignment, impression, and activation events."""
    config = get_config()

    random_seed = int(config["synthetic_generation"]["random_seed"])
    random.seed(random_seed)

    local_scale = config["synthetic_generation"]["local_sample_scale"]

    assignment_rows = int(local_scale["fact_offer_customer_assignment"])
    impression_rows = int(local_scale["fact_offer_impressions"])
    activation_rows = int(local_scale["fact_offer_activations"])

    print("Generating MerchantLift BI offer interaction events...")
    print()
    print(f"Assignments target: {assignment_rows:,}")
    print(f"Impressions target: {impression_rows:,}")
    print(f"Activations max target: {activation_rows:,}")

    assignments = generate_offer_customer_assignments(
        config=config,
        row_count=assignment_rows,
    )

    impressions = generate_offer_impressions(
        assignments=assignments,
        config=config,
        row_count=impression_rows,
    )

    activations = generate_offer_activations(
        impressions=impressions,
        config=config,
        max_rows=activation_rows,
    )

    print()
    write_parquet_table(assignments, "fact_offer_customer_assignment")
    write_parquet_table(impressions, "fact_offer_impressions")
    write_parquet_table(activations, "fact_offer_activations")

    print("Finished generating offer interaction events.")




    '''
    config = get_config()
    random.seed(int(config["synthetic_generation"]["random_seed"]))

    assignments = generate_offer_customer_assignments(
        config=config,
        row_count=500,
    )

    impressions = generate_offer_impressions(
        assignments=assignments,
        config=config,
        row_count=100,
    )

    activations = generate_offer_activations(
        impressions=impressions,
        config=config,
        max_rows=50,
    )

    print("Assignments:")
    print(assignments.head(3))

    print("\nImpressions:")
    print(impressions.head(3))

    print("\nActivations:")
    print(activations.head(3))
    print(f"\nActivation rows: {activations.height}")


    
    local_scale = config["synthetic_generation"]["local_sample_scale"]

    print("Config loaded successfully.")
    print(f"Local Sampling / Assignments: {local_scale['fact_offer_customer_assignment']:,}")
    print(f"Local Sampling / Impressions: {local_scale['fact_offer_impressions']:,}")
    print(f"Local Sampling / Activations: {local_scale['fact_offer_activations']:,}")


    random_seed = int(config["synthetic_generation"]["random_seed"])
    random.seed(random_seed)

    (
        dim_cardmember_token,
        dim_privacy_consent,
        dim_offer,
        dim_campaign,
    ) = load_offer_interaction_inputs()

    print()
    print("Loaded offer interaction inputs.")
    print(f"Cardmembers: {dim_cardmember_token.height:,}")
    print(f"Consent records: {dim_privacy_consent.height:,}")
    print(f"Offers: {dim_offer.height:,}")
    print(f"Campaigns: {dim_campaign.height:,}")

    eligible_cardmembers = build_eligible_cardmember_pool(
        dim_cardmember_token=dim_cardmember_token,
        dim_privacy_consent=dim_privacy_consent,
    )

    print()
    print("Built eligible cardmember pool.")
    print(f"Eligible cardmembers: {len(eligible_cardmembers):,}")
    #print("\nSample eligible cardmember:", eligible_cardmembers[0])

    offers_with_campaign = build_active_offer_pool(
        dim_offer=dim_offer,
        dim_campaign=dim_campaign,
    )

    print()
    print("Built active offer pool.")
    print(f"Offers available for interactions: {len(offers_with_campaign):,}")
    print("\nSample offer:")
    sample_offer = offers_with_campaign[0]
    print(f"offer_id: {sample_offer['offer_id']}")
    print(f"campaign_id: {sample_offer['campaign_id']}")
    print(f"merchant_id: {sample_offer['merchant_id']}")
    print(f"offer_start_date: {sample_offer['offer_start_date']}")
    print(f"offer_end_date: {sample_offer['offer_end_date']}")
    print(f"target_segment_id: {sample_offer['target_segment_id']}")


    row = generate_assignment_row(
        assignment_number=1,
        cardmember=random.choice(eligible_cardmembers),
        offer=random.choice(offers_with_campaign),
        config=config,
    )

    print("Generated one offer-customer assignment row:")
    for key, value in row.items():
        print(f"{key}: {value}")
    '''


if __name__ == "__main__":
    main()