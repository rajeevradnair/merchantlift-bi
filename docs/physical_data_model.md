# Physical Data Model

## Purpose

This document explains how MerchantLift BI tables will be physically organized across local lakehouse folders, Delta Lake layers, BigQuery staging, and dbt marts.

---

## Storage Flow

```text
Raw Parquet
→ Bronze Delta
→ Silver Delta
→ Gold Delta
→ BigQuery staging
→ dbt staging/intermediate/marts
→ BigQuery governed reporting marts
→ Power BI
```

---

## Local Lakehouse Zones

| Zone | Folder | Purpose |
|---|---|---|
| Raw | `data/raw/` | Immutable generated Parquet files |
| Bronze | `data/bronze/` | Lightly structured ingested tables |
| Silver | `data/silver/` | Cleaned, validated, deduplicated tables |
| Gold | `data/gold/` | Business-ready feature and rollup tables |
| Samples | `data/samples/` | Small demo/test datasets |

---

## Raw Folder Layout

```text
data/raw/
  fact_transactions/
  fact_offer_impressions/
  fact_offer_activations/
  fact_offer_redemptions/
  fact_control_group_transactions/
  fact_reward_liability/
  fact_merchant_settlements/
  fact_fraud_risk_events/
  fact_data_quality_reconciliation/
  dim_cardmember_token/
  dim_merchant/
  dim_offer/
  dim_campaign/
  dim_location/
  dim_category/
  dim_segment/
  dim_privacy_consent/
  dim_risk_rule/
```

---

## Partitioning Strategy

| Table | Partition Column | Reason |
|---|---|---|
| `fact_transactions` | `transaction_date` | Most queries filter by transaction date |
| `fact_offer_impressions` | `impression_date` | Impression volume is high |
| `fact_offer_activations` | `activation_date` | Activation analysis is time-based |
| `fact_offer_redemptions` | `redemption_date` | Redemptions drive rewards |
| `fact_control_group_transactions` | `transaction_date` | Baseline spend by campaign window |
| `fact_reward_liability` | `liability_date` | Finance and pacing reporting |
| `fact_merchant_settlements` | `settlement_date` | Finance reconciliation |
| `fact_fraud_risk_events` | `event_date` | Risk monitoring |
| `fact_data_quality_reconciliation` | `reconciliation_date` | Data quality reporting |

---

## Clustering Strategy

| Table | Cluster Columns | Reason |
|---|---|---|
| `fact_transactions` | `merchant_id`, `tokenized_cardmember_id` | Redemption matching and merchant analytics |
| `fact_offer_activations` | `offer_id`, `tokenized_cardmember_id` | Activation-to-transaction matching |
| `fact_offer_redemptions` | `merchant_id`, `offer_id` | Offer reporting |
| `fact_control_group_transactions` | `merchant_id`, `campaign_id`, `segment_id` | Incrementality comparison |
| `fact_reward_liability` | `merchant_id`, `offer_id` | Liability rollups |
| `fact_fraud_risk_events` | `merchant_id`, `risk_rule_id` | Risk analysis |
| `mart_privacy_safe_offer_lift` | `merchant_id`, `campaign_id` | BI filtering |

---

## BigQuery Dataset Design

| Dataset | Purpose |
|---|---|
| `merchantlift_staging` | Curated outputs landed from gold Delta |
| `merchantlift_intermediate` | dbt intermediate models |
| `merchantlift_marts` | Business reporting marts |
| `merchantlift_privacy_safe` | Suppressed/aggregated privacy-safe marts |
| `merchantlift_audit` | Audit and data quality reporting |

---

## Reporting Safety

Raw, bronze, and silver tables may contain tokenized IDs.

Power BI should not connect to raw, bronze, silver, or low-level staging tables.

Power BI should connect to:

```text
merchantlift_marts
merchantlift_privacy_safe
```

---

## Summary

The physical model separates large-scale lakehouse processing from warehouse analytics.

Spark/Delta handles heavy transformation.

BigQuery/dbt handles governed analytics modeling.

Power BI consumes only reporting-safe marts.
```