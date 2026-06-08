# Incrementality Math

## Purpose

Incrementality measures whether a merchant-funded offer created spend that would not have happened otherwise.

A redemption proves a customer used an offer.

Incrementality estimates whether the offer changed customer behavior.

## Core Question

Did the offer create incremental profitable spend, or did it subsidize customers who would have purchased anyway?

## Why Redemption Alone Is Not Enough

A customer can redeem an offer on a purchase they already planned to make.

That means redemption count alone can overstate business value.

The correct comparison is:

```text
test group behavior
versus
similar control group behavior

Basic Test/Control Formula

Average test spend:

avg_test_spend = total_test_spend / test_cardmember_count

Average control spend:

avg_control_spend = total_control_spend / control_cardmember_count

Lift per cardmember:

lift_per_cardmember = avg_test_spend - avg_control_spend

Scaled incremental revenue:

incremental_revenue = lift_per_cardmember * test_cardmember_count


# Incrementality Math

## Purpose

Incrementality measures whether a merchant-funded offer created spend that would not have happened otherwise.

A redemption proves that a customer used an offer.

Incrementality estimates whether the offer changed customer behavior.

The central business question is:

```text
Did the merchant offer create incremental profitable spend,
or did it subsidize customers who would have purchased anyway?
```

---

## Why Redemption Alone Is Not Enough

A customer can redeem an offer on a purchase they already planned to make.

Example:

```text
Customer receives: Spend $100, get $10 back.
Customer already planned to spend $100.
Customer redeems the offer.
```

In this case, the offer created reward cost but may not have created new spend.

That is subsidization.

Therefore, the platform should not only count redemptions.

It should compare the behavior of customers who used the offer against a similar baseline group.

---

## Test Group and Control Group

### Test Group

The test group represents observed offer-influenced behavior.

In the initial implementation, the test group is derived from:

```text
silver.fact_matched_offer_redemptions_clean
```

This means the first version uses a redeemer-based test group.

That is practical for the first implementation, but it can overstate lift because redeemers are often more engaged than non-redeemers.

Future versions can improve this by using:

```text
all exposed users
all activated users
all eligible assigned test users
```

### Control Group

The control group represents baseline behavior.

It is derived from:

```text
silver.fact_control_group_transactions_clean
```

The control group should represent similar customers who did not receive, activate, or redeem the offer.

The control group gives the counterfactual estimate:

```text
What would similar customers have spent without the offer?
```

---

## Basic Incrementality Formula

Average test spend:

```text
average_test_spend_per_cardmember =
    total_test_spend_amount / test_cardmember_count
```

Average control spend:

```text
average_control_spend_per_cardmember =
    total_control_spend_amount / control_cardmember_count
```

Lift per cardmember:

```text
lift_per_cardmember =
    average_test_spend_per_cardmember
    - average_control_spend_per_cardmember
```

Scaled incremental revenue:

```text
incremental_revenue_amount =
    lift_per_cardmember * test_cardmember_count
```

Lift percentage:

```text
lift_percentage =
    lift_per_cardmember / average_control_spend_per_cardmember
```

If the denominator is zero, the implementation safely returns zero.

---

## Example

Test group:

```text
100 cardmembers spend $12,000
average_test_spend_per_cardmember = 12,000 / 100 = 120
```

Control group:

```text
100 cardmembers spend $9,000
average_control_spend_per_cardmember = 9,000 / 100 = 90
```

Lift:

```text
lift_per_cardmember = 120 - 90 = 30
```

Scaled incremental revenue:

```text
incremental_revenue_amount = 30 * 100 = 3,000
```

Interpretation:

```text
The offer is estimated to have created $3,000 of incremental spend.
```

---

## Lift Direction

The job labels lift as:

```text
positive_lift
negative_lift
no_lift
```

Rules:

```text
lift_per_cardmember > 0  -> positive_lift
lift_per_cardmember < 0  -> negative_lift
lift_per_cardmember = 0  -> no_lift
```

A negative lift is not a technical failure.

It means the test group spent less than the control group.

That may indicate:

```text
weak offer
poor targeting
synthetic data imbalance
seasonality mismatch
control group quality issue
```

---

## Incremental Revenue Direction

The job labels incremental revenue as:

```text
positive_incremental_revenue
negative_incremental_revenue
no_incremental_revenue
```

Rules:

```text
incremental_revenue_amount > 0  -> positive_incremental_revenue
incremental_revenue_amount < 0  -> negative_incremental_revenue
incremental_revenue_amount = 0  -> no_incremental_revenue
```

---

## Economic Enrichment

After calculating lift and incremental revenue, the job enriches the result with offer and merchant context.

From `dim_offer_scd`, it adds:

```text
minimum_spend_amount
reward_amount
```

From `dim_merchant_scd`, it adds:

```text
merchant_margin_rate
platform_fee_rate
```

Then it calculates early estimated economics.

Estimated incremental margin:

```text
estimated_incremental_margin_amount =
    incremental_revenue_amount * merchant_margin_rate
```

Estimated incremental platform fee:

```text
estimated_incremental_platform_fee_amount =
    incremental_revenue_amount * platform_fee_rate
```

Estimated incremental value after reward:

```text
estimated_incremental_value_after_reward =
    estimated_incremental_margin_amount
    - total_test_reward_amount
    - estimated_incremental_platform_fee_amount
```

This is a first-pass economics estimate.

It is not the final merchant profit model yet.

---

## Important Interpretation

This implementation answers:

```text
Did the test group spend more than the control group?
How much lift per cardmember was observed?
How much incremental revenue was estimated?
What is the estimated value after reward cost and platform fee?
```

It does not yet provide research-grade causal inference.

Future improvements should add:

```text
pre-period matching quality checks
confidence intervals
statistical significance
matched cohort diagnostics
privacy-safe cohort suppression
outlier handling
cannibalization risk scoring
synthetic experiment calibration
```

---

## Input Tables

The incrementality job reads:

```text
silver.fact_matched_offer_redemptions_clean
silver.fact_control_group_transactions_clean
silver.dim_offer_scd
silver.dim_merchant_scd
```

### fact_matched_offer_redemptions_clean

Used as the initial test group source.

Required columns:

```text
matched_redemption_id
transaction_id
offer_id
campaign_id
merchant_id
tokenized_cardmember_id
transaction_date
transaction_amount
calculated_reward_amount
```

### fact_control_group_transactions_clean

Used as the control group source.

Required columns:

```text
control_transaction_id
offer_id
campaign_id
merchant_id
tokenized_cardmember_id
transaction_date
transaction_amount
```

If the real schema uses `transaction_id` instead of `control_transaction_id`, the Spark job should be adjusted to use the actual column name.

### dim_offer_scd

Used for offer rule context.

Required columns:

```text
offer_id
campaign_id
merchant_id
minimum_spend_amount
reward_amount
is_current
```

### dim_merchant_scd

Used for merchant economics context.

Required columns:

```text
merchant_id
merchant_margin_rate
platform_fee_rate
is_current
```

---

## Output Table: gold_offer_incrementality

### Purpose

`gold_offer_incrementality` is the first Gold incrementality table.

It compares test and control spend at the offer/campaign/merchant/day grain.

It answers:

```text
Did this offer appear to create incremental spend?
```

### Output Location

```text
data/lakehouse/gold/gold_offer_incrementality/
```

### Grain

One row represents:

```text
one offer
one campaign
one merchant
one business date
```

Grain columns:

```text
business_date
offer_id
campaign_id
merchant_id
```

### Core Columns

```text
business_date
offer_id
campaign_id
merchant_id
minimum_spend_amount
reward_amount
merchant_margin_rate
platform_fee_rate
test_cardmember_count
test_transaction_count
test_redemption_count
total_test_spend_amount
total_test_reward_amount
average_test_spend_per_cardmember
average_test_reward_per_redemption
control_cardmember_count
control_transaction_count
total_control_spend_amount
average_control_spend_per_cardmember
lift_per_cardmember
lift_direction
test_to_control_spend_ratio
lift_percentage
incremental_revenue_amount
absolute_incremental_revenue_amount
incremental_revenue_direction
estimated_incremental_margin_amount
estimated_incremental_platform_fee_amount
estimated_incremental_value_after_reward
incrementality_pipeline_run_id
incrementality_rule_version
incrementality_created_at
```

---

## Processing Flow

The Spark job follows this flow:

```text
1. Read matched redemptions as test group input.
2. Read control group transactions as control group input.
3. Read offer SCD context.
4. Read merchant SCD context.
5. Validate input contracts.
6. Build test group spend aggregation.
7. Build control group spend aggregation.
8. Join test and control groups.
9. Calculate lift per cardmember.
10. Calculate scaled incremental revenue.
11. Enrich with offer and merchant context.
12. Calculate estimated incremental value after reward.
13. Write Gold Delta table.
14. Read back written table.
15. Validate incrementality business rules.
```

---

## Test Group Aggregation

Source:

```text
fact_matched_offer_redemptions_clean
```

Grouping:

```text
transaction_date as business_date
offer_id
campaign_id
merchant_id
```

Metrics:

```text
test_cardmember_count
test_transaction_count
test_redemption_count
total_test_spend_amount
total_test_reward_amount
average_test_spend_per_cardmember
average_test_reward_per_redemption
```

---

## Control Group Aggregation

Source:

```text
fact_control_group_transactions_clean
```

Grouping:

```text
transaction_date as business_date
offer_id
campaign_id
merchant_id
```

Metrics:

```text
control_cardmember_count
control_transaction_count
total_control_spend_amount
average_control_spend_per_cardmember
```

---

## Test/Control Join

The job joins test and control groups using:

```text
business_date
offer_id
campaign_id
merchant_id
```

The first implementation uses an inner join.

Why?

Lift requires both:

```text
test group exists
control group exists
```

If either side is missing, the job cannot estimate lift for that grain.

Future improvement:

```text
write unmatched test/control diagnostics table
```

---

## Output Validation

The job validates:

```text
unique grain
required values are not null
test and control counts are valid
spend and reward metrics are non-negative
rate fields are sane
direction labels are valid
lift formula is correct
incremental revenue formula is correct
lift percentage formula is correct
estimated value formula is correct
pipeline metadata exists
```

### Grain Validation

Expected unique grain:

```text
business_date + offer_id + campaign_id + merchant_id
```

### Formula Validation

Lift:

```text
lift_per_cardmember =
    average_test_spend_per_cardmember
    - average_control_spend_per_cardmember
```

Incremental revenue:

```text
incremental_revenue_amount =
    lift_per_cardmember * test_cardmember_count
```

Lift percentage:

```text
lift_percentage =
    lift_per_cardmember / average_control_spend_per_cardmember
```

Estimated incremental value after reward:

```text
estimated_incremental_value_after_reward =
    estimated_incremental_margin_amount
    - total_test_reward_amount
    - estimated_incremental_platform_fee_amount
```

---

## Local Execution

Run only after Bronze, Silver, matched redemptions, SCD, and Gold merchant economics outputs exist.

Recommended local run order:

```bash
PYTHONPATH=src python spark_jobs/bronze_ingestion.py
PYTHONPATH=src python spark_jobs/silver_transformations.py
PYTHONPATH=src python spark_jobs/build_redemption_matching.py
PYTHONPATH=src python spark_jobs/build_scd_dimensions.py
PYTHONPATH=src python spark_jobs/build_merchant_economics.py
PYTHONPATH=src python spark_jobs/build_incrementality.py
```

If upstream tables already exist, run only:

```bash
PYTHONPATH=src python spark_jobs/build_incrementality.py
```

Expected successful output:

```text
Gold write validation passed: gold_offer_incrementality
All incrementality output validations passed.
```

Check output folder:

```bash
ls data/lakehouse/gold/gold_offer_incrementality
```

Expected:

```text
_delta_log
business_date=...
```

The `_delta_log` folder proves the output is a Delta table.

---

## Databricks Execution Notes

The incrementality job should run both locally and in Databricks.

Local execution proves the Spark logic works.

Databricks execution proves the same logic works in the lakehouse runtime against Delta tables.

### Required Environment Variables

Before running in Databricks, set:

```bash
export MERCHANTLIFT_LAKEHOUSE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse
export MERCHANTLIFT_BRONZE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/bronze
export MERCHANTLIFT_SILVER_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/silver
export MERCHANTLIFT_GOLD_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/gold
```

If raw ingestion is needed:

```bash
export MERCHANTLIFT_RAW_DATA_DIR=/dbfs/FileStore/merchantlift/data/raw
```

### Databricks Run Command

From a Databricks notebook shell cell:

```bash
%sh
cd /Workspace/Repos/<your-folder>/merchantlift-bi

export MERCHANTLIFT_LAKEHOUSE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse
export MERCHANTLIFT_BRONZE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/bronze
export MERCHANTLIFT_SILVER_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/silver
export MERCHANTLIFT_GOLD_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/gold

PYTHONPATH=src python spark_jobs/build_incrementality.py
```
