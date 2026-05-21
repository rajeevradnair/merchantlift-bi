
---

# 9. File 3: `docs/table_grain_registry.md`

This file gives you a first registry of all important future tables.

Paste this:

```markdown
# Table Grain Registry

## Purpose

This document defines the expected grain for the main MerchantLift BI tables.

Grain means:

> What does one row represent?

Grain must be defined before building data generators, Spark transformations, dbt models, and dashboards.

---

## Fact Tables

| Table | Grain | Why It Exists |
|---|---|---|
| `fact_transactions` | One row per card-linked transaction | Raw purchase behavior used for spend, redemption matching, fraud, and reconciliation |
| `fact_offer_impressions` | One row per offer shown to one cardmember | Measures offer exposure |
| `fact_offer_activations` | One row per offer activated by one cardmember | Measures customer intent |
| `fact_offer_redemptions` | One row per qualified transaction-offer redemption | Connects transaction behavior to offer rules |
| `fact_control_group_transactions` | One row per control-group transaction | Estimates baseline behavior without offer exposure |
| `fact_reward_liability` | One row per reward liability event | Tracks reward cost created by redemptions |
| `fact_merchant_settlements` | One row per merchant settlement event | Tracks financial settlement to merchants |
| `fact_fraud_risk_events` | One row per detected fraud or abuse signal | Tracks suspicious offer behavior |
| `fact_data_quality_reconciliation` | One row per reconciliation check event | Tracks mismatches between transaction, reward, fee, and settlement records |

---

## Dimension Tables

| Table | Grain | Why It Exists |
|---|---|---|
| `dim_cardmember_token` | One row per tokenized cardmember | Allows synthetic identity joins without real PII |
| `dim_merchant` | One row per merchant | Stores merchant attributes |
| `dim_merchant_scd` | One row per merchant version | Tracks historical merchant changes |
| `dim_offer` | One row per offer | Stores offer rules and eligibility |
| `dim_offer_scd` | One row per offer version | Tracks historical offer changes |
| `dim_campaign` | One row per campaign | Groups related offers |
| `dim_campaign_scd` | One row per campaign version | Tracks historical campaign changes |
| `dim_location` | One row per synthetic location | Supports geography analysis and privacy design |
| `dim_category` | One row per merchant category | Supports category-level economics and margin assumptions |
| `dim_segment` | One row per customer segment | Supports test/control matching and dashboard segmentation |
| `dim_date` | One row per calendar date | Supports time-series reporting |
| `dim_privacy_consent` | One row per tokenized cardmember consent state | Supports privacy eligibility |
| `dim_risk_rule` | One row per risk rule | Explains fraud/abuse detection logic |

---

## dbt Marts : TODO

| Mart | Grain | Why It Exists |
|---|---|---|
| `mart_offer_performance` | One row per reporting date, merchant, campaign, and offer | Activation, redemption, breakage, and campaign performance |
| `mart_merchant_economics` | One row per reporting date, merchant, campaign, and offer | Gross spend, reward cost, margin, ROAS, net merchant profit |
| `mart_offer_incrementality` | One row per reporting date, merchant, campaign, offer, and segment | Test/control lift and incremental revenue |
| `mart_reward_liability` | One row per reporting date, merchant, campaign, and offer | Expected/actual liability and pacing |
| `mart_offer_abuse_risk` | One row per reporting date, merchant, campaign, offer, and risk rule | Fraud and offer-abuse monitoring |
| `mart_data_quality_reconciliation` | One row per reporting date, merchant, and reconciliation category | Finance and trust layer for mismatches |
| `mart_privacy_safe_offer_lift` | One row per reporting date, merchant, campaign, and segment cohort | Governed privacy-safe incrementality reporting |

---

## Grain Mistakes to Avoid

| Mistake | Why to avoid |
|---|---|
| Joining impression-level data directly to transaction-level data | Can multiply transaction spend |
| Joining activation-level data without offer/date conditions | Can create false redemptions |
| Aggregating before deduplication | Can overstate spend and rewards |
| Reporting customer-level rows to merchants | Violates privacy-safe reporting design |
| Ignoring table grain in dbt marts | Produces misleading KPIs |

---

## Summary

Table grain is the foundation of trustworthy analytics.

Every future Spark job, dbt model, and dashboard should preserve or intentionally change grain.