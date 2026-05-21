# BigQuery Policy Tag Design

## Purpose

This document defines planned BigQuery policy tags for MerchantLift BI.

Policy tags support column-level security by labeling sensitive columns.

---

## Policy Tags

| Policy Tag | Sensitivity | Example Columns | Intended Access |
|---|---|---|---|
| `sensitive_identifier` | High | `tokenized_cardmember_id` | Data engineering only |
| `sensitive_location` | Medium | `zip_code`, `zip_prefix` | Restricted analytics |
| `sensitive_consent` | High | `consent_status`, `privacy_eligibility_flag` | Compliance and data engineering |
| `sensitive_risk_signal` | High | `risk_score`, `abuse_score`, `risk_rule_id` | Risk and compliance |
| `financial_sensitive` | Medium | `settlement_amount`, `platform_fee`, `reward_liability` | Finance and authorized analytics |
| `public_reporting` | Low | Aggregated KPI columns | BI and executive reporting |

---

## Column Classification Examples

| Table | Column | Policy Tag |
|---|---|---|
| `fact_transactions` | `tokenized_cardmember_id` | `sensitive_identifier` |
| `fact_transactions` | `zip_code` | `sensitive_location` |
| `dim_privacy_consent` | `consent_status` | `sensitive_consent` |
| `fact_fraud_risk_events` | `risk_score` | `sensitive_risk_signal` |
| `fact_reward_liability` | `reward_liability_amount` | `financial_sensitive` |
| `mart_privacy_safe_offer_lift` | `visible_test_spend` | `public_reporting` |

---

## Reporting Rule

Merchant-facing reporting marts should not expose columns tagged as:

```text
sensitive_identifier
sensitive_consent
sensitive_risk_signal