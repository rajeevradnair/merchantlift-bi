# Data Foundation Release Report

## Release Status

**Status:** PASSED

**Generated At:** 2026-06-03T18:27:59Z

## Purpose

This report validates the MerchantLift BI raw synthetic data foundation.

The goal of this release is to prove that the generated raw lake is complete, measurable, structurally usable, traceable, and financially coherent before Spark/Delta lakehouse ingestion begins.

## Validation Summary

| Metric | Count |
|---|---:|
| Total validation checks | 91 |
| Passed checks | 91 |
| Failed checks | 0 |

## Raw Table Physical Profile

| Table | Rows | File Size MB |
|---|---:|---:|
| dim_category | 6 | 0.0034 |
| dim_location | 25 | 0.0028 |
| dim_segment | 6 | 0.0029 |
| dim_risk_rule | 5 | 0.0035 |
| dim_merchant | 200 | 0.0091 |
| dim_campaign | 80 | 0.0052 |
| dim_offer | 300 | 0.0094 |
| dim_cardmember_token | 5,000 | 0.1091 |
| dim_privacy_consent | 5,000 | 0.0211 |
| dim_date | 365 | 0.0071 |
| fact_transactions | 12,266 | 0.2198 |
| fact_offer_customer_assignment | 5,000 | 0.0542 |
| fact_offer_impressions | 3,000 | 0.0578 |
| fact_offer_activations | 766 | 0.0250 |
| fact_offer_redemptions | 766 | 0.0287 |
| fact_control_group_transactions | 1,500 | 0.0463 |
| fact_reward_liability | 766 | 0.0239 |
| fact_merchant_settlements | 1,000 | 0.0274 |
| fact_fraud_risk_events | 200 | 0.0123 |
| fact_data_quality_reconciliation | 200 | 0.0096 |
| **TOTAL** | **36,451** | **0.6786** |

## Validation Scope

The release validator checks:

- raw table file existence
- positive row counts
- raw Parquet file size
- required column presence
- primary-key uniqueness and non-nullness
- critical lineage relationships
- reward funding split formula
- merchant settlement formulas
- reconciliation delta formula

## Critical Lineage Relationships Validated

The release validates that:

- redemptions point to real transactions
- control-group transactions point to real transactions
- reward liabilities point to real redemptions
- reward liabilities point to real transactions
- merchant settlements point to real transactions
- fraud-risk events point to real transactions
- reconciliation checks point to real transactions

## Financial Formula Checks Validated

The release validates:

```text
merchant_funded_amount + platform_funded_amount = reward_amount
```

```text
merchant_settlement_amount =
gross_transaction_amount - platform_fee_amount
```

```text
merchant_net_after_reward =
merchant_settlement_amount - merchant_funded_amount
```

```text
settlement_delta =
transaction_amount - merchant_settlement_amount - platform_fee_amount
```

## Validation Results

| Status | Table | Check | Message |
|---|---|---|---|
| PASSED | dim_category | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_category/part-00000.parquet |
| PASSED | dim_category | positive_row_count | Row count = 6 |
| PASSED | dim_category | required_columns | All required columns present: category_id |
| PASSED | dim_category | primary_key_uniqueness | category_id is unique and non-null across 6 rows |
| PASSED | dim_location | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_location/part-00000.parquet |
| PASSED | dim_location | positive_row_count | Row count = 25 |
| PASSED | dim_location | required_columns | All required columns present: location_id |
| PASSED | dim_location | primary_key_uniqueness | location_id is unique and non-null across 25 rows |
| PASSED | dim_segment | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_segment/part-00000.parquet |
| PASSED | dim_segment | positive_row_count | Row count = 6 |
| PASSED | dim_segment | required_columns | All required columns present: segment_id |
| PASSED | dim_segment | primary_key_uniqueness | segment_id is unique and non-null across 6 rows |
| PASSED | dim_risk_rule | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_risk_rule/part-00000.parquet |
| PASSED | dim_risk_rule | positive_row_count | Row count = 5 |
| PASSED | dim_risk_rule | required_columns | All required columns present: risk_rule_id |
| PASSED | dim_risk_rule | primary_key_uniqueness | risk_rule_id is unique and non-null across 5 rows |
| PASSED | dim_merchant | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_merchant/part-00000.parquet |
| PASSED | dim_merchant | positive_row_count | Row count = 200 |
| PASSED | dim_merchant | required_columns | All required columns present: merchant_id, category_id, location_id |
| PASSED | dim_merchant | primary_key_uniqueness | merchant_id is unique and non-null across 200 rows |
| PASSED | dim_campaign | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_campaign/part-00000.parquet |
| PASSED | dim_campaign | positive_row_count | Row count = 80 |
| PASSED | dim_campaign | required_columns | All required columns present: campaign_id |
| PASSED | dim_campaign | primary_key_uniqueness | campaign_id is unique and non-null across 80 rows |
| PASSED | dim_offer | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_offer/part-00000.parquet |
| PASSED | dim_offer | positive_row_count | Row count = 300 |
| PASSED | dim_offer | required_columns | All required columns present: offer_id, campaign_id, merchant_id |
| PASSED | dim_offer | primary_key_uniqueness | offer_id is unique and non-null across 300 rows |
| PASSED | dim_cardmember_token | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_cardmember_token/part-00000.parquet |
| PASSED | dim_cardmember_token | positive_row_count | Row count = 5,000 |
| PASSED | dim_cardmember_token | required_columns | All required columns present: tokenized_cardmember_id |
| PASSED | dim_cardmember_token | primary_key_uniqueness | tokenized_cardmember_id is unique and non-null across 5,000 rows |
| PASSED | dim_privacy_consent | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_privacy_consent/part-00000.parquet |
| PASSED | dim_privacy_consent | positive_row_count | Row count = 5,000 |
| PASSED | dim_privacy_consent | required_columns | All required columns present: consent_id, tokenized_cardmember_id |
| PASSED | dim_privacy_consent | primary_key_uniqueness | consent_id is unique and non-null across 5,000 rows |
| PASSED | dim_date | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/dim_date/part-00000.parquet |
| PASSED | dim_date | positive_row_count | Row count = 365 |
| PASSED | dim_date | required_columns | All required columns present: date_id |
| PASSED | dim_date | primary_key_uniqueness | date_id is unique and non-null across 365 rows |
| PASSED | fact_transactions | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_transactions/part-00000.parquet |
| PASSED | fact_transactions | positive_row_count | Row count = 12,266 |
| PASSED | fact_transactions | required_columns | All required columns present: transaction_id, tokenized_cardmember_id, merchant_id, transaction_amount, transaction_date |
| PASSED | fact_transactions | primary_key_uniqueness | transaction_id is unique and non-null across 12,266 rows |
| PASSED | fact_offer_customer_assignment | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_offer_customer_assignment/part-00000.parquet |
| PASSED | fact_offer_customer_assignment | positive_row_count | Row count = 5,000 |
| PASSED | fact_offer_customer_assignment | required_columns | All required columns present: assignment_id, tokenized_cardmember_id, offer_id, campaign_id, assignment_group |
| PASSED | fact_offer_customer_assignment | primary_key_uniqueness | assignment_id is unique and non-null across 5,000 rows |
| PASSED | fact_offer_impressions | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_offer_impressions/part-00000.parquet |
| PASSED | fact_offer_impressions | positive_row_count | Row count = 3,000 |
| PASSED | fact_offer_impressions | required_columns | All required columns present: impression_id, assignment_id, offer_id |
| PASSED | fact_offer_impressions | primary_key_uniqueness | impression_id is unique and non-null across 3,000 rows |
| PASSED | fact_offer_activations | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_offer_activations/part-00000.parquet |
| PASSED | fact_offer_activations | positive_row_count | Row count = 766 |
| PASSED | fact_offer_activations | required_columns | All required columns present: activation_id, impression_id, offer_id |
| PASSED | fact_offer_activations | primary_key_uniqueness | activation_id is unique and non-null across 766 rows |
| PASSED | fact_offer_redemptions | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_offer_redemptions/part-00000.parquet |
| PASSED | fact_offer_redemptions | positive_row_count | Row count = 766 |
| PASSED | fact_offer_redemptions | required_columns | All required columns present: redemption_id, transaction_id, activation_id, offer_id, calculated_reward_amount |
| PASSED | fact_offer_redemptions | primary_key_uniqueness | redemption_id is unique and non-null across 766 rows |
| PASSED | fact_control_group_transactions | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_control_group_transactions/part-00000.parquet |
| PASSED | fact_control_group_transactions | positive_row_count | Row count = 1,500 |
| PASSED | fact_control_group_transactions | required_columns | All required columns present: control_transaction_id, transaction_id, control_assignment_id, offer_id, transaction_amount |
| PASSED | fact_control_group_transactions | primary_key_uniqueness | control_transaction_id is unique and non-null across 1,500 rows |
| PASSED | fact_reward_liability | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_reward_liability/part-00000.parquet |
| PASSED | fact_reward_liability | positive_row_count | Row count = 766 |
| PASSED | fact_reward_liability | required_columns | All required columns present: reward_liability_id, redemption_id, transaction_id, reward_amount, liability_owner, merchant_funded_amount, platform_funded_amount |
| PASSED | fact_reward_liability | primary_key_uniqueness | reward_liability_id is unique and non-null across 766 rows |
| PASSED | fact_merchant_settlements | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_merchant_settlements/part-00000.parquet |
| PASSED | fact_merchant_settlements | positive_row_count | Row count = 1,000 |
| PASSED | fact_merchant_settlements | required_columns | All required columns present: settlement_id, transaction_id, gross_transaction_amount, platform_fee_amount, merchant_settlement_amount, merchant_net_after_reward |
| PASSED | fact_merchant_settlements | primary_key_uniqueness | settlement_id is unique and non-null across 1,000 rows |
| PASSED | fact_fraud_risk_events | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_fraud_risk_events/part-00000.parquet |
| PASSED | fact_fraud_risk_events | positive_row_count | Row count = 200 |
| PASSED | fact_fraud_risk_events | required_columns | All required columns present: fraud_event_id, transaction_id, risk_rule_id, risk_score |
| PASSED | fact_fraud_risk_events | primary_key_uniqueness | fraud_event_id is unique and non-null across 200 rows |
| PASSED | fact_data_quality_reconciliation | file_exists | Found /Users/rnair/projects/merchantlift-bi/data/raw/fact_data_quality_reconciliation/part-00000.parquet |
| PASSED | fact_data_quality_reconciliation | positive_row_count | Row count = 200 |
| PASSED | fact_data_quality_reconciliation | required_columns | All required columns present: reconciliation_id, transaction_id, settlement_delta, reconciliation_status |
| PASSED | fact_data_quality_reconciliation | primary_key_uniqueness | reconciliation_id is unique and non-null across 200 rows |
| PASSED | fact_offer_redemptions | redemption_to_transaction | All transaction_id values exist in fact_transactions.transaction_id |
| PASSED | fact_control_group_transactions | control_transaction_to_transaction | All transaction_id values exist in fact_transactions.transaction_id |
| PASSED | fact_reward_liability | reward_liability_to_redemption | All redemption_id values exist in fact_offer_redemptions.redemption_id |
| PASSED | fact_reward_liability | reward_liability_to_transaction | All transaction_id values exist in fact_transactions.transaction_id |
| PASSED | fact_merchant_settlements | settlement_to_transaction | All transaction_id values exist in fact_transactions.transaction_id |
| PASSED | fact_fraud_risk_events | fraud_event_to_transaction | All transaction_id values exist in fact_transactions.transaction_id |
| PASSED | fact_data_quality_reconciliation | reconciliation_to_transaction | All transaction_id values exist in fact_transactions.transaction_id |
| PASSED | fact_reward_liability | reward_funding_split | merchant_funded_amount + platform_funded_amount matches reward_amount |
| PASSED | fact_merchant_settlements | merchant_settlement_amount_formula | merchant_settlement_amount matches gross_transaction_amount - platform_fee_amount |
| PASSED | fact_merchant_settlements | merchant_net_after_reward_formula | merchant_net_after_reward matches merchant_settlement_amount - merchant_funded_amount |
| PASSED | fact_data_quality_reconciliation | reconciliation_delta_formula | settlement_delta matches transaction_amount - merchant_settlement_amount - platform_fee_amount |

## Release Readiness Statement

If the release status is PASSED, the raw synthetic data foundation is ready for Bronze ingestion into the Spark/Delta lakehouse layer.

If the release status is FAILED, fix the failed checks before proceeding to Spark/Delta ingestion.

## Known Limitations

This release validates the raw data foundation only.

It does not yet validate:

- Bronze Delta table creation
- Silver cleaning rules
- Gold business marts
- dbt semantic models
- BigQuery policy tags
- Power BI dashboards

Those are handled in later implementation phases.

## Portfolio Evidence

This report demonstrates that MerchantLift BI has a measurable and testable data foundation, not only generated files.

The foundation includes synthetic merchant-offer events, transaction activity, redemptions, control-group transactions, reward liability, merchant settlements, fraud-risk events, and reconciliation checks.
