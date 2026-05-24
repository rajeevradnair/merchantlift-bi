from pathlib import Path
from typing import Any
from datetime import datetime, date, timedelta
import polars as pl
import random
from faker import Faker
from merchantlift.paths import CONFIG_DIR, DATA_DIR, RAW_DATA_DIR
from merchantlift.config import load_yaml_config

DEFAULT_DIMENSION_COUNTS = {
    "num_locations": 25,
    "num_merchants": 200,
    "num_campaigns": 80,
    "num_offers": 300,
    "num_cardmembers": 5000,
}

def get_config() -> dict[str, Any]:
    return load_yaml_config(CONFIG_DIR/"project_settings.yaml")

def make_id(prefix: str, number: int, width: int = 6) -> str:
    return f"{prefix}_{number:0{width}d}"
    
def write_parquet_table(df:pl.DataFrame, table_name: str) -> None:
    output_dir_path:Path = RAW_DATA_DIR / table_name
    output_dir_path.mkdir(parents=True, exist_ok=True)
    output_file_path = output_dir_path / "part-00000.parquet"
    df.write_parquet(output_file_path)
    print(f"Wrote {df.height:,} rows to {output_file_path}")
    
def generate_dim_category(configs: dict[str, Any]) -> pl.DataFrame:
    rows = []
    category_rules = configs["synthetic_generation"]["category_rules"]
    for index, (category_name, rules) in enumerate(category_rules.items(), start=1):
        rows.append (
            {
                "category_id": make_id("category", index),
                "category_name": category_name,
                "parent_category": "merchant_spend",
                "basket_min": rules["basket_min"],
                "basket_max": rules["basket_max"],
                "default_margin_rate": rules["default_margin_rate"],
                "seasonality": rules["seasonality"],
                "created_at": datetime.utcnow(),
            }
        )
    #print(pl.DataFrame(rows))
    return pl.DataFrame(rows)

def generate_dim_location(fake: Faker, num_locations: int) -> pl.DataFrame:
    states = ["CA", "NY", "TX", "FL", "WA", "IL", "GA", "AZ", "CO", "MA"]
    timezones = {
        "CA": "America/Los_Angeles",
        "WA": "America/Los_Angeles",
        "AZ": "America/Phoenix",
        "CO": "America/Denver",
        "TX": "America/Chicago",
        "IL": "America/Chicago",
        "GA": "America/New_York",
        "FL": "America/New_York",
        "NY": "America/New_York",
        "MA": "America/New_York",
    }
    rows = []
    for index in range(1, num_locations + 1):
        state = random.choice(states)
        rows.append(
            {
                "location_id": make_id("location", index, width=4),
                "state": state,
                "market_area": fake.city(),
                "zip_prefix": str(random.randint(100, 999)),
                "timezone": timezones[state],
                "created_at": datetime.utcnow(),
            }
        )
    return pl.DataFrame(rows)

def generate_dim_segment(config: dict[str, Any]) -> pl.DataFrame:
    segment_rules = config["synthetic_generation"]["segment_rules"]

    rows = []

    for index, (segment_name, rules) in enumerate(segment_rules.items(), start=1):
        rows.append(
            {
                "segment_id": make_id("segment", index, width=3),
                "segment_name": segment_name,
                "segment_description": f"Synthetic segment for {segment_name}",
                "expected_response_rate": float(rules["response_rate"]),
                "expected_incrementality_rate": float(rules["incrementality_rate"]),
                "created_at": datetime.utcnow(),
            }
        )

    return pl.DataFrame(rows)

def generate_dim_risk_rule() -> pl.DataFrame:
    risk_rules = [
        {
            "risk_rule_name": "duplicate_redemption",
            "risk_category": "duplicate",
            "severity": "high",
            "description": "Same cardmember appears to redeem the same offer more than allowed.",
        },
        {
            "risk_rule_name": "refund_after_reward",
            "risk_category": "refund",
            "severity": "critical",
            "description": "Reward was created or paid, then transaction was refunded.",
        },
        {
            "risk_rule_name": "high_redemption_velocity",
            "risk_category": "velocity",
            "severity": "medium",
            "description": "Unusually high number of redemptions in a short time window.",
        },
        {
            "risk_rule_name": "merchant_location_anomaly",
            "risk_category": "location",
            "severity": "medium",
            "description": "Activity pattern appears unusual for merchant and location.",
        },
        {
            "risk_rule_name": "reward_gaming_pattern",
            "risk_category": "reward_gaming",
            "severity": "high",
            "description": "Behavior appears optimized mainly to harvest rewards.",
        },
    ]
    rows = []
    for index, rule in enumerate(risk_rules, start=1):
        rows.append(
            {
                "risk_rule_id": make_id("risk_rule", index, width=3),
                "risk_rule_name": rule["risk_rule_name"],
                "risk_category": rule["risk_category"],
                "severity": rule["severity"],
                "risk_rule_description": rule["description"],
                "is_active": True,
                "created_at": datetime.utcnow(),
            }
        )
    return pl.DataFrame(rows)

def generate_dim_merchant(
    fake: Faker,
    dim_category: pl.DataFrame,
    dim_location: pl.DataFrame,
    config: dict[str, Any],
    num_merchants: int,
) -> pl.DataFrame:

    merchant_status_values = config["dimensions"]["merchant_status_values"]

    category_ids = dim_category["category_id"].to_list()
    location_ids = dim_location["location_id"].to_list()

    #print(dim_category.to_dicts())

    category_margin_lookup = {
        row["category_id"]: row["default_margin_rate"]
        for row in dim_category.to_dicts()
    }
    rows = []

    for index in range(1, num_merchants + 1):
        category_id = random.choice(category_ids)
        location_id = random.choice(location_ids)
        default_margin_rate = float(category_margin_lookup[category_id])
        merchant_margin_rate = default_margin_rate + random.uniform(-0.05, 0.05)
        merchant_margin_rate = max(0.05, min(0.80, merchant_margin_rate))
        rows.append(
            {
                "merchant_id": make_id("merchant", index, width=6),
                "merchant_name": fake.company(),
                "category_id": category_id,
                "location_id": location_id,
                "merchant_status": random.choices(
                    merchant_status_values,
                    weights=[0.90, 0.07, 0.03],
                    k=1,
                )[0],
                "merchant_margin_rate": round(merchant_margin_rate, 4),
                "platform_fee_rate": round(random.uniform(0.01, 0.035), 4),
                "merchant_start_date": fake.date_between(
                    start_date="-5y",
                    end_date="-30d",
                ),
                "created_at": datetime.utcnow(),
            }
        )
    return pl.DataFrame(rows)

def generate_dim_campaign(
    fake: Faker,
    dim_segment: pl.DataFrame,
    config: dict[str, Any],
    num_campaigns: int,
) -> pl.DataFrame:
    objective_values = config["dimensions"]["campaign_objective_values"]
    status_values = config["dimensions"]["campaign_status_values"]
    segment_ids = dim_segment["segment_id"].to_list()
    base_start = date(2026, 1, 1)
    rows = []

    for index in range(1, num_campaigns + 1):
        campaign_start = base_start + timedelta(days=random.randint(0, 180))
        campaign_length_days = random.choice([14, 21, 30, 45, 60])
        campaign_end = campaign_start + timedelta(days=campaign_length_days)

        objective = random.choice(objective_values)

        rows.append(
            {
                "campaign_id": make_id("campaign", index, width=6),
                "campaign_name": f"{objective.replace('_', ' ').title()} Campaign {index}",
                "campaign_objective": objective,
                "campaign_start_date": campaign_start,
                "campaign_end_date": campaign_end,
                "campaign_status": random.choices(
                    status_values,
                    weights=[0.05, 0.55, 0.10, 0.25, 0.05],
                    k=1,
                )[0],
                "target_segment_id": random.choice(segment_ids),
                "budget_amount": round(random.uniform(25_000, 500_000), 2),
                "created_at": datetime.utcnow(),
            }
        )
    return pl.DataFrame(rows)

def generate_dim_offer(
    dim_campaign: pl.DataFrame,
    dim_merchant: pl.DataFrame,
    config: dict[str, Any],
    num_offers: int,
) -> pl.DataFrame:
    offer_type_values = config["dimensions"]["offer_type_values"]
    offer_status_values = config["dimensions"]["offer_status_values"]
    campaign_rows = dim_campaign.to_dicts()
    merchant_ids = dim_merchant["merchant_id"].to_list()
    rows = []
    for index in range(1, num_offers + 1):
        campaign = random.choice(campaign_rows)
        merchant_id = random.choice(merchant_ids)
        offer_type = random.choice(offer_type_values)

        if offer_type == "fixed_cashback":
            minimum_spend_amount = float(random.choice([50, 75, 100, 150, 250]))
            reward_amount = float(random.choice([5, 10, 15, 20, 50]))
            reward_multiplier = 0.0
            max_reward_amount = reward_amount
        else:
            minimum_spend_amount = float(random.choice([50, 100, 150, 250]))
            reward_amount = 0.0
            reward_multiplier = float(random.choice([0.05, 0.10, 0.15, 0.20]))
            max_reward_amount = float(random.choice([25, 50, 75, 100]))

        rows.append(
            {
                "offer_id": make_id("offer", index, width=6),
                "campaign_id": campaign["campaign_id"],
                "merchant_id": merchant_id,
                "offer_name": f"{offer_type.replace('_', ' ').title()} Offer {index}",
                "offer_type": offer_type,
                "minimum_spend_amount": minimum_spend_amount,
                "reward_amount": reward_amount,
                "reward_multiplier": reward_multiplier,
                "max_reward_amount": max_reward_amount,
                "offer_start_date": campaign["campaign_start_date"],
                "offer_end_date": campaign["campaign_end_date"],
                "offer_status": random.choices(
                    offer_status_values,
                    weights=[0.05, 0.65, 0.25, 0.05],
                    k=1,
                )[0],
                "created_at": datetime.utcnow(),
            }
        )
    return pl.DataFrame(rows)

def generate_dim_cardmember_token(
    dim_segment: pl.DataFrame,
    dim_location: pl.DataFrame,
    config: dict[str, Any],
    num_cardmembers: int,
) -> pl.DataFrame:

    segment_ids = dim_segment["segment_id"].to_list()
    location_ids = dim_location["location_id"].to_list()

    behavior_mix = config["synthetic_generation"]["behavior_mix"]
    behavior_types = list(behavior_mix.keys())
    behavior_weights = list(behavior_mix.values())

    rows = []

    for index in range(1, num_cardmembers + 1):
        historical_spend_90d = round(random.uniform(50, 5000), 2)
        historical_transaction_count_90d = random.randint(1, 40)
        average_ticket_size = round(
            historical_spend_90d / historical_transaction_count_90d,
            2,
        )

        rows.append(
            {
                "tokenized_cardmember_id": make_id("cm_tok", index, width=6),
                "segment_id": random.choice(segment_ids),
                "location_id": random.choice(location_ids),
                "account_open_date": date.today()
                - timedelta(days=random.randint(90, 3650)),
                "customer_tenure_months": random.randint(3, 120),
                "historical_spend_90d": historical_spend_90d,
                "historical_transaction_count_90d": historical_transaction_count_90d,
                "average_ticket_size": average_ticket_size,
                "merchant_affinity_score": round(random.uniform(0, 1), 4),
                "category_affinity_score": round(random.uniform(0, 1), 4),
                "shopper_behavior_type": random.choices(
                    behavior_types,
                    weights=behavior_weights,
                    k=1,
                )[0],
                "is_test_eligible": random.random() < 0.70,
                "is_control_eligible": random.random() < 0.70,
                "created_at": datetime.utcnow(),
            }
        )

    return pl.DataFrame(rows)

def generate_dim_privacy_consent(
    dim_cardmember_token: pl.DataFrame,
    config: dict[str, Any],
) -> pl.DataFrame:
    consent_status_values = config["dimensions"]["consent_status_values"]

    rows = []

    for index, cardmember in enumerate(dim_cardmember_token.to_dicts(), start=1):
        consent_status = random.choices(
            consent_status_values,
            weights=[0.88, 0.07, 0.05],
            k=1,
        )[0]

        analytics_consent_flag = consent_status == "granted"
        merchant_reporting_consent_flag = consent_status == "granted" and (
            random.random() < 0.90
        )

        rows.append(
            {
                "consent_id": make_id("consent", index, width=6),
                "tokenized_cardmember_id": cardmember["tokenized_cardmember_id"],
                "analytics_consent_flag": analytics_consent_flag,
                "merchant_reporting_consent_flag": merchant_reporting_consent_flag,
                "consent_effective_date": date.today()
                - timedelta(days=random.randint(1, 365)),
                "consent_status": consent_status,
                "created_at": datetime.utcnow(),
            }
        )
    return pl.DataFrame(rows)

def generate_dim_date(
    start_date: date = date(2026, 1, 1),
    end_date: date = date(2026, 12, 31),
) -> pl.DataFrame:
    rows = []
    current_date = start_date

    holidays = {
        "01-01": "New Year's Day",
        "07-04": "Independence Day",
        "12-25": "Christmas Day",
    }

    while current_date <= end_date:
        month_day = current_date.strftime("%m-%d")

        rows.append(
            {
                "date_id": current_date.strftime("%Y%m%d"),
                "calendar_date": current_date,
                "year": current_date.year,
                "quarter": ((current_date.month - 1) // 3) + 1,
                "month": current_date.month,
                "month_name": current_date.strftime("%B"),
                "week_of_year": int(current_date.strftime("%U")),
                "day_of_week": current_date.strftime("%A"),
                "day_of_month": current_date.day,
                "is_weekend": current_date.weekday() >= 5,
                "is_holiday": month_day in holidays,
                "holiday_name": holidays.get(month_day),
                "created_at": datetime.utcnow(),
            }
        )
        current_date += timedelta(days=1)
    return pl.DataFrame(rows)

if __name__ == "__main__":
    config = get_config()

    random_seed = int(config["synthetic_generation"]["random_seed"])
    random.seed(random_seed)

    fake = Faker()
    Faker.seed(random_seed)

    counts = DEFAULT_DIMENSION_COUNTS

    print("Generating independent dimensions...")

    dim_category = generate_dim_category(config)
    write_parquet_table(dim_category, "dim_category")

    #print(dim_category["category_id"].to_list())

    dim_location = generate_dim_location(
        fake=fake,
        num_locations=counts["num_locations"],
    )
    write_parquet_table(dim_location, "dim_location")

    dim_segment = generate_dim_segment(config)
    write_parquet_table(dim_segment, "dim_segment")

    dim_risk_rule = generate_dim_risk_rule()
    write_parquet_table(dim_risk_rule, "dim_risk_rule")

    print("Finished independent dimensions.")

    dim_merchant = generate_dim_merchant(
        fake=fake,
        dim_category=dim_category,
        dim_location=dim_location,
        config=config,
        num_merchants=counts["num_merchants"],
    )
    write_parquet_table(dim_merchant, "dim_merchant")

    dim_campaign = generate_dim_campaign(
        fake=fake,
        dim_segment=dim_segment,
        config=config,
        num_campaigns=counts["num_campaigns"],
    )
    write_parquet_table(dim_campaign, "dim_campaign")

    dim_offer = generate_dim_offer(
        dim_campaign=dim_campaign,
        dim_merchant=dim_merchant,
        config=config,
        num_offers=counts["num_offers"],
    )
    write_parquet_table(dim_offer, "dim_offer")

    dim_cardmember_token = generate_dim_cardmember_token(
        dim_segment=dim_segment,
        dim_location=dim_location,
        config=config,
        num_cardmembers=counts["num_cardmembers"],
    )
    write_parquet_table(dim_cardmember_token, "dim_cardmember_token")


    dim_privacy_consent = generate_dim_privacy_consent(
        dim_cardmember_token=dim_cardmember_token,
        config=config,
    )
    write_parquet_table(dim_privacy_consent, "dim_privacy_consent")

    dim_date = generate_dim_date()
    write_parquet_table(dim_date, "dim_date")

    print("Finished generating all core dimensions.")