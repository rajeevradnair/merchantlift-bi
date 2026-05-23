# MerchantLift BI

## Privacy-Safe Merchant Offer Economics & Incrementality Intelligence Platform

MerchantLift BI is a production-grade data engineering and business intelligence platform for analyzing merchant-funded card-linked offers.

The platform answers one central business question:

> Did the merchant offer create incremental profitable spend, or did it simply subsidize customers who would have purchased anyway?

## Why This Project Exists

Merchant-funded offers can look successful when redemptions and total spend increase. However, high redemption volume does not automatically mean the offer created new value.

A merchant needs to know whether the offer caused new purchasing behavior or simply rewarded customers who were already planning to buy.

MerchantLift BI solves this by combining:

- Synthetic card-linked offer and transaction data
- Test-vs-control incrementality measurement
- Merchant economics modeling
- Reward liability tracking
- Fraud and offer-abuse detection
- Financial reconciliation
- Privacy-safe reporting
- Governed BI dashboards

## Target Scale

The platform is designed to process 100M+ synthetic events across (local implementation validated on 10M+ events):

- Transactions
- Offer impressions
- Offer activations
- Offer redemptions
- Control-group transactions
- Reward liability records
- Merchant settlements
- Fraud-risk events
- Data-quality reconciliation records

## High-Level Architecture

```text
Business domain rules
        ↓
Synthetic data generator
Python + Faker + Polars
        ↓
Partitioned Parquet files
        ↓
Raw data lake
        ↓
Great Expectations raw validation
        ↓
Spark / Databricks
        ↓
Bronze Delta tables
        ↓
Silver Delta tables
        ↓
Gold Delta tables
        ↓
BigQuery staging
        ↓
dbt staging models
        ↓
dbt intermediate models
        ↓
dbt marts
        ↓
BigQuery governed reporting datasets
        ↓
Policy tags / row-level security / DLP / KMS / audit logs
        ↓
Power BI dashboards
        ↓
Executive, merchant, finance, risk, compliance insights

MerchantLift BI models the full lifecycle of a merchant-funded card-linked offer:
```

## Core Merchant Offer Lifecycle

```text
campaign → offer → impression → activation → transaction → redemption → reward liability → settlement → fraud checks → reconciliation → privacy-safe reporting
```

## Stakeholder Use Cases

MerchantLift BI is designed for multiple stakeholder groups:

- Executives monitoring profitable incremental growth
- Merchant teams optimizing campaigns and offers
- Finance teams validating reward liability and settlement reconciliation
- Risk teams detecting fraud and offer abuse
- Compliance teams enforcing privacy-safe reporting
- Data engineering teams maintaining reliable pipelines
- Analytics engineering teams building trusted marts and dashboards

The stakeholder map is documented in:

```text
docs/stakeholder_use_cases.md
```

## Incrementality Problem

MerchantLift BI does not treat redemptions as automatic proof of campaign success.

A redemption proves that a customer qualified for an offer, but it does not prove that the offer caused the purchase.

The incrementality design explains why simple before/after analysis is weak and why matched test/control groups are needed.

The incrementality problem framing is documented in:

```text
docs/incrementality_problem.md
```

## Privacy and Compliance Design

MerchantLift BI is synthetic, but it is designed with financial-services-grade privacy principles.

The platform uses:

- no raw PAN/card-number policy
- synthetic data only
- tokenized cardmember identifiers
- least-privilege access design
- aggregated reporting
- cohort suppression
- BigQuery policy tag design
- BigQuery row-level security design
- Google Cloud DLP inspection configuration
- Cloud KMS design notes
- IAM service account separation
- Cloud Audit Logs design

The privacy and compliance design is documented in:

```text
docs/privacy_compliance_design.md
```

The architecture is documented in:

```text
docs/incrementality_problem.md
```

The reusable data contract template is documented in:

```text
docs/incrementality_problem.md
```

The initial ERD is documented in:

```text
docs/initial_erd.md
docs/initial_erd.png
```

## Dimension Design

MerchantLift BI uses dimension tables to describe the business context around fact events.

Core dimensions include:

- `dim_cardmember_token`
- `dim_merchant`
- `dim_merchant_scd`
- `dim_offer`
- `dim_offer_scd`
- `dim_campaign`
- `dim_campaign_scd`
- `dim_location`
- `dim_category`
- `dim_segment`
- `dim_date`
- `dim_privacy_consent`
- `dim_risk_rule`

Dimension design and contracts are documented in:

```text
docs/dimension_design.md
docs/dimension_contracts.md
```

## Fact Design

MerchantLift BI uses fact tables to record business events and measurable activity.

Core fact tables include:

- `fact_transactions`
- `fact_offer_impressions`
- `fact_offer_activations`
- `fact_offer_redemptions`
- `fact_control_group_transactions`
- `fact_reward_liability`
- `fact_merchant_settlements`
- `fact_fraud_risk_events`
- `fact_data_quality_reconciliation`

Fact table design and contracts are documented in:

```text
docs/fact_table_design.md
docs/fact_contracts.md
```

## Synthetic Data Design

MerchantLift BI uses synthetic data that is fake but behaviorally realistic.

The synthetic data design includes:

- 10M+ full-scale target rows
- smaller local sample generation
- merchant category behavior
- customer segments
- shopper behavior types
- test/control design
- subsidized shopper simulation
- reward rules
- fraud and offer-abuse patterns
- reconciliation mismatches
- privacy-safe cohort behavior

Synthetic data design is documented in:

```text
docs/synthetic_data_design.md
docs/synthetic_generation_rules.md
```