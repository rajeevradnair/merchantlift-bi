# Data Model

## Purpose

This document defines the core data model for MerchantLift BI.

MerchantLift BI analyzes merchant-funded card-linked offers, transactions, redemptions, reward liability, merchant settlements, fraud/abuse, reconciliation, and privacy-safe reporting.

The central business question is:

> Did the merchant offer create incremental profitable spend, or did it simply subsidize customers who would have purchased anyway?

---

## 1. Modeling Principles

| Principle | Meaning |
|---|---|
| Grain first | Every table must define what one row means |
| Facts store events | Transaction, impression, activation, redemption, settlement, and fraud events |
| Dimensions describe entities | Merchant, offer, campaign, segment, category, location, consent, risk rules |
| Keys must be stable | IDs should support joins across tables |
| Privacy by design | Tokenized IDs are allowed in engineering layers but not merchant-facing marts |
| SCD where history matters | Merchant, offer, and campaign changes should be tracked historically |
| Reporting marts aggregate | Dashboards should use marts, not raw fact tables |

---

## 2. Fact Tables

| Table | Grain | Primary Key | Purpose |
|---|---|---|---|
| `fact_transactions` | One row per card-linked transaction | `transaction_id` | Stores synthetic transaction behavior |
| `fact_offer_impressions` | One row per offer shown to one cardmember | `impression_id` | Tracks offer exposure |
| `fact_offer_activations` | One row per cardmember-offer activation | `activation_id` | Tracks intent to use offer |
| `fact_offer_redemptions` | One row per qualifying transaction-offer match | `redemption_id` | Tracks qualified offer redemption |
| `fact_control_group_transactions` | One row per control-group transaction | `control_transaction_id` | Supports baseline spend estimation |
| `fact_reward_liability` | One row per reward liability event | `reward_liability_id` | Tracks reward cost |
| `fact_merchant_settlements` | One row per settlement event | `settlement_id` | Tracks merchant settlement and platform fees |
| `fact_fraud_risk_events` | One row per fraud/abuse signal | `fraud_event_id` | Tracks suspicious behavior |
| `fact_data_quality_reconciliation` | One row per reconciliation check | `reconciliation_id` | Tracks transaction/reward/settlement mismatches |

---

## 3. Dimension Tables

| Table | Grain | Primary Key | Purpose |
|---|---|---|---|
| `dim_cardmember_token` | One row per tokenized cardmember | `tokenized_cardmember_id` | Synthetic identity join layer |
| `dim_merchant` | One row per merchant | `merchant_id` | Merchant attributes |
| `dim_merchant_scd` | One row per merchant version | `merchant_sk` | Historical merchant attributes |
| `dim_offer` | One row per offer | `offer_id` | Offer rules |
| `dim_offer_scd` | One row per offer version | `offer_sk` | Historical offer rules |
| `dim_campaign` | One row per campaign | `campaign_id` | Campaign metadata |
| `dim_campaign_scd` | One row per campaign version | `campaign_sk` | Historical campaign metadata |
| `dim_location` | One row per synthetic location | `location_id` | Geography analysis |
| `dim_category` | One row per merchant category | `category_id` | Category hierarchy and margin assumptions |
| `dim_segment` | One row per customer segment | `segment_id` | Segmentation and test/control matching |
| `dim_date` | One row per calendar date | `date_id` | Calendar reporting |
| `dim_privacy_consent` | One row per tokenized cardmember consent state | `consent_id` | Privacy eligibility |
| `dim_risk_rule` | One row per risk rule | `risk_rule_id` | Fraud/abuse rule definitions |

---

## 4. Core Relationship Paths

### Transaction Path

```text
dim_cardmember_token
    → fact_transactions
    → dim_merchant

dim_campaign
    → dim_offer
    → dim_merchant

fact_offer_activations
    + fact_transactions
    + dim_offer
    → fact_offer_redemptions

fact_offer_redemptions
    → fact_reward_liability

fact_offer_redemptions
    + fact_control_group_transactions
    → mart_offer_incrementality

fact_transactions
    + fact_reward_liability
    + fact_merchant_settlements
    → fact_data_quality_reconciliation
```

### SCDs
dim_merchant_scd
dim_offer_scd
dim_campaign_scd


