
# Financial, Fraud, and Reconciliation Event Generation

## Purpose

This document explains how MerchantLift BI generates the financial, fraud, and reconciliation fact tables.

The  generator creates four raw fact tables:

- fact_reward_liability
- fact_merchant_settlements
- fact_fraud_risk_events
- fact_data_quality_reconciliation

These tables form the money, risk, and trust layer of MerchantLift BI.

The project's central business question is:

Did the merchant offer create incremental profitable spend,
or did it simply subsidize customers who would have purchased anyway?

To answer that question credibly, the platform needs more than redemptions.

It must also track:

- reward cost
- reward funding owner
- merchant payout
- platform fee
- fraud or abuse risk
- settlement tie-out
- data quality reconciliation

## Conceptual Summary

The  flow is:

redemption
    -> reward liability
    -> merchant settlement
    -> fraud risk event
    -> reconciliation check

Plain-English version:

A redemption creates a reward cost.
The reward cost must be funded by someone.
The merchant transaction must be settled.
Suspicious behavior must be flagged.
Finance needs to prove the numbers tie out.

Short memory hook:

Redeemed -> Owe reward -> Settle merchant -> Check fraud -> Reconcile money

## Source Tables

 reads these existing raw tables:

- fact_transactions
- fact_offer_redemptions
- fact_control_group_transactions
- dim_merchant
- dim_risk_rule

### fact_transactions

This is the broad transaction universe.

It contains:

- general card-linked transactions
- supplemental qualifying redemption transactions
- control-group baseline transactions

### fact_offer_redemptions

This table contains transactions that qualified for an activated offer.

Each redemption points back to a real transaction:

fact_transactions.transaction_id
    -> fact_offer_redemptions.transaction_id

### fact_control_group_transactions

This table contains baseline no-offer spend from similar control users.

Each control row also points back to a real transaction:

fact_transactions.transaction_id
    -> fact_control_group_transactions.transaction_id

### dim_merchant

This table provides merchant economics fields such as:

- merchant_id
- merchant_margin_rate
- platform_fee_rate
- category_id
- location_id

### dim_risk_rule

This table provides governed fraud/offer-abuse rule definitions, such as:

- duplicate_redemption
- refund_after_reward
- high_redemption_velocity
- merchant_location_anomaly
- reward_gaming_pattern

## Output Tables

## 1. fact_reward_liability

### Grain

One row per redemption-created reward liability.

### Business Meaning

A reward liability record means:

A cardmember qualified for a reward, and the platform must track who funds that reward.

### Important Columns

- reward_liability_id
- redemption_id
- transaction_id
- activation_id
- tokenized_cardmember_id
- offer_id
- campaign_id
- merchant_id
- liability_date
- liability_timestamp
- reward_amount
- liability_owner
- merchant_funded_amount
- platform_funded_amount
- liability_status
- created_at

### Liability Ownership

The generator includes:

liability_owner = merchant | platform | shared

For this project version, merchant-funded offers default to:

liability_owner = merchant

### Funding Split

For merchant-funded reward:

merchant_funded_amount = reward_amount
platform_funded_amount = 0

For platform-funded reward:

merchant_funded_amount = 0
platform_funded_amount = reward_amount

For shared-funded reward:

merchant_funded_amount = reward_amount * 0.75
platform_funded_amount = reward_amount * 0.25

### Why This Matters

Reward liability is necessary for:

- merchant profitability
- reward cost tracking
- finance reporting
- campaign economics
- reconciliation

## 2. fact_merchant_settlements

### Grain

One row per transaction settlement.

### Business Meaning

Merchant settlement records how much the merchant receives after platform fees and how reward funding impacts merchant economics.

### Important Columns

- settlement_id
- transaction_id
- redemption_id
- reward_liability_id
- merchant_id
- settlement_date
- gross_transaction_amount
- platform_fee_rate
- platform_fee_amount
- reward_amount
- liability_owner
- merchant_funded_amount
- platform_funded_amount
- merchant_settlement_amount
- merchant_net_after_reward
- settlement_status
- created_at

### Key Formula

platform_fee_amount =
    gross_transaction_amount * platform_fee_rate

merchant_settlement_amount =
    gross_transaction_amount - platform_fee_amount

merchant_net_after_reward =
    merchant_settlement_amount - merchant_funded_amount

### Separate the Concepts

platform_fee = platform revenue
reward_funding = who pays the cardmember reward
merchant_settlement = merchant payout after platform fee
merchant_net_after_reward = merchant payout after merchant-funded reward cost

### Merchant-Funded Example

gross_transaction_amount = 120.00
platform_fee_rate = 0.02
platform_fee_amount = 2.40
merchant_settlement_amount = 117.60
merchant_funded_amount = 20.00
platform_funded_amount = 0.00
merchant_net_after_reward = 97.60

### Why This Matters

Merchant settlement supports:

- finance trust
- merchant payout reporting
- platform revenue analysis
- merchant net-profit analysis
- reward funding analysis

## 3. fact_fraud_risk_events

### Grain

One row per fraud or offer-abuse risk event.

### Business Meaning

A fraud-risk event annotates suspicious behavior around transactions and redemptions.

It does not replace the transaction.

It flags the transaction for risk review.

### Important Columns

- fraud_event_id
- transaction_id
- redemption_id
- tokenized_cardmember_id
- merchant_id
- risk_rule_id
- risk_rule_name
- risk_category
- risk_severity
- risk_score
- event_timestamp
- event_date
- risk_event_status
- risk_event_description
- created_at

### Risk Rule Examples

- duplicate_redemption
- refund_after_reward
- high_redemption_velocity
- merchant_location_anomaly
- reward_gaming_pattern

### Event Status Examples

- open
- reviewed
- confirmed_false_positive
- confirmed_abuse

### Why This Matters

Fraud-risk events support:

- fraud dashboard
- offer-abuse monitoring
- refund-after-reward analysis
- merchant risk review
- reward leakage detection

## 4. fact_data_quality_reconciliation

### Grain

One row per transaction reconciliation check.

### Business Meaning

Reconciliation proves whether the transaction amount ties correctly to merchant settlement and platform fee.

### Important Columns

- reconciliation_id
- transaction_id
- merchant_id
- reconciliation_date
- transaction_amount
- merchant_settlement_amount
- platform_fee_amount
- reward_amount
- merchant_funded_amount
- platform_funded_amount
- settlement_delta
- reconciliation_status
- reconciliation_reason
- created_at

### Core Reconciliation Formula

settlement_delta =
    transaction_amount
    - merchant_settlement_amount
    - platform_fee_amount

For a clean settlement, expected value is:

settlement_delta = 0.00

### Example Matched Row

transaction_amount = 120.00
merchant_settlement_amount = 117.60
platform_fee_amount = 2.40
settlement_delta = 0.00
reconciliation_status = matched

### Example Mismatched Row

transaction_amount = 120.00
merchant_settlement_amount = 110.00
platform_fee_amount = 2.40
settlement_delta = 7.60
reconciliation_status = mismatched

### Why This Matters

Reconciliation supports:

- finance trust
- data quality validation
- merchant settlement audit
- late-arriving transaction detection
- refund-after-reward review
- dashboard confidence

## Data Lineage

 preserves strong lineage.

### Reward Liability Lineage

fact_offer_redemptions.redemption_id
    -> fact_reward_liability.redemption_id

### Settlement Lineage

fact_transactions.transaction_id
    -> fact_merchant_settlements.transaction_id

### Fraud Lineage

fact_transactions.transaction_id
    -> fact_fraud_risk_events.transaction_id

fact_offer_redemptions.redemption_id
    -> fact_fraud_risk_events.redemption_id

### Reconciliation Lineage

fact_transactions.transaction_id
    -> fact_data_quality_reconciliation.transaction_id

## Generator Script

The  generator is:

data_generation/generate_financial_risk_events.py

Run it with:

PYTHONPATH=src python data_generation/generate_financial_risk_events.py

Expected outputs:

data/raw/fact_reward_liability/part-00000.parquet
data/raw/fact_merchant_settlements/part-00000.parquet
data/raw/fact_fraud_risk_events/part-00000.parquet
data/raw/fact_data_quality_reconciliation/part-00000.parquet

## Inspection Script

The  inspection script is:

scripts/inspect_generated_financial_risk_events.py

Run it with:

PYTHONPATH=src python scripts/inspect_generated_financial_risk_events.py

The inspection script validates:

- row counts
- duplicate IDs
- reward amount validity
- funding split correctness
- settlement formula correctness
- merchant net formula correctness
- risk score range
- null transaction IDs
- reconciliation status counts
- settlement delta checks

## Expected Good Validation Values

Expected good values include:

Duplicate reward_liability_id: 0
Duplicate settlement_id: 0
Duplicate fraud_event_id: 0
Duplicate reconciliation_id: 0
Funding split mismatches: 0
Settlement amount formula mismatches: 0
Merchant net formula mismatches: 0
Risk scores below 0.60: 0
Risk scores above 0.99: 0
Null transaction IDs: 0

For reconciliation:

Rows with settlement_delta > 0.01

should ideally be low or zero.

If it is high in local mode, it usually means reconciliation is checking transaction rows that do not yet have settlement records because the local settlement target is smaller than the total transaction universe.