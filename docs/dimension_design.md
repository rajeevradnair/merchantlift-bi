# Dimension Design

## Purpose

This document defines the dimension tables for MerchantLift BI.

Dimension tables describe the business entities that surround fact events.

Fact tables answer:

> What happened?

Dimension tables answer:

> What was the business context around what happened?

---

## 1. Dimension Design Principles

| Principle | Meaning |
|---|---|
| Grain first | Every dimension must define what one row represents |
| Stable identifiers | Dimensions provide stable join keys |
| Business context | Dimensions contain descriptive attributes |
| Privacy classification | Sensitive fields must be labeled |
| Historical accuracy | Important changes are tracked with SCD tables |
| Reusability | Dimensions should support multiple facts and marts |

---

## 2. Dimension Inventory

| Dimension | Grain | Primary Key | Purpose |
|---|---|---|---|
| `dim_cardmember_token` | One row per tokenized cardmember | `tokenized_cardmember_id` | Synthetic identity join layer |
| `dim_merchant` | One row per merchant | `merchant_id` | Current merchant attributes |
| `dim_merchant_scd` | One row per merchant historical version | `merchant_sk` | Historical merchant attributes |
| `dim_offer` | One row per offer | `offer_id` | Current offer rules |
| `dim_offer_scd` | One row per offer historical version | `offer_sk` | Historical offer rules |
| `dim_campaign` | One row per campaign | `campaign_id` | Current campaign metadata |
| `dim_campaign_scd` | One row per campaign historical version | `campaign_sk` | Historical campaign metadata |
| `dim_location` | One row per synthetic location | `location_id` | Geography and privacy-safe location grouping |
| `dim_category` | One row per merchant category | `category_id` | Category hierarchy and margin assumptions |
| `dim_segment` | One row per customer segment | `segment_id` | Customer classification and test/control matching |
| `dim_date` | One row per date | `date_id` | Calendar reporting |
| `dim_privacy_consent` | One row per consent state per tokenized cardmember | `consent_id` | Privacy and reporting eligibility |
| `dim_risk_rule` | One row per fraud/abuse rule | `risk_rule_id` | Fraud rule definitions |

---

## 3. `dim_cardmember_token`

### Grain

One row per synthetic tokenized cardmember.

### Purpose

This dimension allows lifecycle events to connect without using raw customer identity.

It supports joins across:

- impressions
- activations
- transactions
- redemptions
- control-group transactions
- consent records
- segment assignment

### Example Columns

| Column | Type | Description | Privacy Classification |
|---|---|---|---|
| `tokenized_cardmember_id` | string | Synthetic tokenized customer ID | sensitive_identifier |
| `segment_id` | string | Customer segment | internal |
| `location_id` | string | Synthetic location | sensitive_location |
| `account_open_date` | date | Synthetic account open date | internal |
| `customer_tenure_months` | integer | Tenure measure | internal |
| `historical_spend_90d` | numeric | Pre-campaign spend | financial_sensitive |
| `historical_transaction_count_90d` | integer | Pre-campaign activity | internal |
| `merchant_affinity_score` | numeric | Likelihood to shop merchant | internal |
| `category_affinity_score` | numeric | Category preference | internal |
| `is_test_eligible` | boolean | Can be assigned to test group | internal |
| `is_control_eligible` | boolean | Can be assigned to control group | internal |

### Notes

This table should not be exposed directly to merchant-facing BI.

---

## 4. `dim_merchant`

### Grain

One row per merchant.

### Purpose

Stores current merchant attributes.

### Example Columns

| Column | Type | Description | Privacy Classification |
|---|---|---|---|
| `merchant_id` | string | Merchant identifier | internal |
| `merchant_name` | string | Synthetic merchant name | public_reporting |
| `category_id` | string | Merchant category | internal |
| `location_id` | string | Merchant location | sensitive_location |
| `merchant_status` | string | active/inactive/suspended | internal |
| `merchant_margin_rate` | numeric | Margin assumption | financial_sensitive |
| `platform_fee_rate` | numeric | Platform fee rate | financial_sensitive |
| `merchant_start_date` | date | Merchant onboarding date | internal |

### Allowed Values

```text
merchant_status = active, inactive, suspended