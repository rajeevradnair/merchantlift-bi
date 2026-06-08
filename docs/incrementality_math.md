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
ity.py'>
Starting Gold offer incrementality build
================================================================================
Silver directory: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/silver
Gold directory: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/gold
Pipeline run ID: incrementality_run_20260608_145237
Incrementality rule version: incrementality_rules_v1
Output table: gold_offer_incrementality
================================================================================

================================================================================
Inspecting input table: fact_matched_offer_redemptions_clean
================================================================================

Schema:
root
 |-- matched_redemption_id: string (nullable = true)
 |-- transaction_id: string (nullable = true)
 |-- activation_id: string (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- tokenized_cardmember_id: string (nullable = true)
 |-- transaction_timestamp: timestamp (nullable = true)
 |-- transaction_date: date (nullable = true)
 |-- transaction_amount: double (nullable = true)
 |-- activation_timestamp: timestamp (nullable = true)
 |-- offer_expiry_timestamp: timestamp (nullable = true)
 |-- minimum_spend_amount: double (nullable = true)
 |-- reward_amount: double (nullable = true)
 |-- calculated_reward_amount: double (nullable = true)
 |-- match_rule_version: string (nullable = true)
 |-- match_pipeline_run_id: string (nullable = true)
 |-- matched_at: timestamp (nullable = true)
 |-- match_deduplication_reason: string (nullable = true)


Row count:
fact_matched_offer_redemptions_clean: 766 rows

Sample rows:
+---------------------------------------------------------------------+---------------+-------------+------------+---------------+---------------+-----------------------+---------------------+----------------+------------------+--------------------+----------------------+--------------------+-------------+------------------------+-------------------------+------------------------------------+--------------------------+--------------------------------------------------------------+
|matched_redemption_id                                                |transaction_id |activation_id|offer_id    |campaign_id    |merchant_id    |tokenized_cardmember_id|transaction_timestamp|transaction_date|transaction_amount|activation_timestamp|offer_expiry_timestamp|minimum_spend_amount|reward_amount|calculated_reward_amount|match_rule_version       |match_pipeline_run_id               |matched_at                |match_deduplication_reason                                    |
+---------------------------------------------------------------------+---------------+-------------+------------+---------------+---------------+-----------------------+---------------------+----------------+------------------+--------------------+----------------------+--------------------+-------------+------------------------+-------------------------+------------------------------------+--------------------------+--------------------------------------------------------------+
|mred_5e44c82834182ffc391e720936282a4a03882c7655789fa6c9e0bfb3d397da86|tx_offer_000085|act_000000186|offer_000259|campaign_000034|merchant_000081|cm_tok_001895          |2026-03-28 16:42:35  |2026-03-28      |168.57            |2026-03-26 08:40:35 |2026-04-25 08:40:35   |150.0               |0.0          |0.0                     |redemption_match_rules_v1|redemption_match_run_20260608_141332|2026-06-08 14:14:21.794769|selected_highest_reward_then_earliest_activation_then_offer_id|
|mred_54d4eaa71f3ed4f2e32e30b82b0c9647e923781088d46420f8412cf8ab3df5fb|tx_offer_000170|act_000000392|offer_000282|campaign_000008|merchant_000060|cm_tok_003519          |2026-03-28 22:51:16  |2026-03-28      |69.04             |2026-03-26 16:36:32 |2026-04-25 16:36:32   |50.0                |50.0         |50.0                    |redemption_match_rules_v1|redemption_match_run_20260608_141332|2026-06-08 14:14:21.794769|selected_highest_reward_then_earliest_activation_then_offer_id|
|mred_5a777223ae02baed098b8d77b5b953f766f9ed01737e5d3792dd1cb351ec422a|tx_offer_000374|act_000000325|offer_000003|campaign_000063|merchant_000050|cm_tok_003176          |2026-03-28 22:54:28  |2026-03-28      |101.03            |2026-03-22 10:41:28 |2026-04-21 10:41:28   |75.0                |10.0         |10.0                    |redemption_match_rules_v1|redemption_match_run_20260608_141332|2026-06-08 14:14:21.794769|selected_highest_reward_then_earliest_activation_then_offer_id|
|mred_7c74e23a096ed38897a609cc401d4f96d57200658d8a34cf7f5d4299d516edc8|tx_offer_000467|act_000000300|offer_000146|campaign_000037|merchant_000005|cm_tok_001746          |2026-03-28 22:22:51  |2026-03-28      |106.07            |2026-03-23 16:24:51 |2026-04-22 16:24:51   |100.0               |0.0          |0.0                     |redemption_match_rules_v1|redemption_match_run_20260608_141332|2026-06-08 14:14:21.794769|selected_highest_reward_then_earliest_activation_then_offer_id|
|mred_cba6f172d2f8e2b6aab6db25aac4ec68ba2fbae0834dc1edf3e5d318310b5a6f|tx_offer_000591|act_000000247|offer_000045|campaign_000060|merchant_000142|cm_tok_001869          |2026-03-28 23:05:15  |2026-03-28      |354.12            |2026-03-27 17:16:15 |2026-04-26 17:16:15   |250.0               |5.0          |5.0                     |redemption_match_rules_v1|redemption_match_run_20260608_141332|2026-06-08 14:14:21.794769|selected_highest_reward_then_earliest_activation_then_offer_id|
+---------------------------------------------------------------------+---------------+-------------+------------+---------------+---------------+-----------------------+---------------------+----------------+------------------+--------------------+----------------------+--------------------+-------------+------------------------+-------------------------+------------------------------------+--------------------------+--------------------------------------------------------------+
only showing top 5 rows

================================================================================
Inspecting input table: fact_control_group_transactions_clean
================================================================================

Schema:
root
 |-- control_transaction_id: string (nullable = true)
 |-- transaction_id: string (nullable = true)
 |-- control_assignment_id: string (nullable = true)
 |-- tokenized_cardmember_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- segment_id: string (nullable = true)
 |-- transaction_timestamp: timestamp (nullable = true)
 |-- transaction_date: date (nullable = true)
 |-- transaction_amount: double (nullable = true)
 |-- match_group_id: string (nullable = true)
 |-- match_quality_score: double (nullable = true)
 |-- shopper_behavior_type: string (nullable = true)
 |-- created_at: timestamp_ntz (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_table_name: string (nullable = true)
 |-- source_file_path: string (nullable = true)
 |-- pipeline_run_id: string (nullable = true)
 |-- record_hash: string (nullable = true)
 |-- silver_transformed_at: timestamp (nullable = true)
 |-- silver_pipeline_run_id: string (nullable = true)
 |-- quality_status: string (nullable = true)
 |-- validation_rule_version: string (nullable = true)


Row count:
fact_control_group_transactions_clean: 1,500 rows

Sample rows:
+----------------------+--------------+---------------------+-----------------------+---------------+---------------+------------+-----------+---------------------+----------------+------------------+--------------+-------------------+---------------------+--------------------------+--------------------------+-------------------------------+-------------------------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|control_transaction_id|transaction_id|control_assignment_id|tokenized_cardmember_id|merchant_id    |campaign_id    |offer_id    |segment_id |transaction_timestamp|transaction_date|transaction_amount|match_group_id|match_quality_score|shopper_behavior_type|created_at                |ingestion_timestamp       |source_table_name              |source_file_path                                                                                 |pipeline_run_id           |record_hash                                                     |silver_transformed_at     |silver_pipeline_run_id    |quality_status|validation_rule_version|
+----------------------+--------------+---------------------+-----------------------+---------------+---------------+------------+-----------+---------------------+----------------+------------------+--------------+-------------------+---------------------+--------------------------+--------------------------+-------------------------------+-------------------------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|ctrl_tx_000645        |tx_ctrl_000645|assign_000004767     |cm_tok_000731          |merchant_000181|campaign_000039|offer_000263|segment_004|2026-02-22 20:02:17  |2026-02-22      |408.79            |match_004767  |0.9663             |loyal_existing       |2026-06-03 00:58:17.173902|2026-06-07 19:48:07.789256|fact_control_group_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_control_group_transactions|bronze_run_20260607_194640|bc43d073ea9a297924731e7123d67c1dcf3104554c69f093b8815c897fb19419|2026-06-07 23:54:28.870862|silver_run_20260607_235211|passed        |silver_rules_v1        |
|ctrl_tx_000808        |tx_ctrl_000808|assign_000002439     |cm_tok_004518          |merchant_000062|campaign_000039|offer_000176|segment_002|2026-02-22 21:26:17  |2026-02-22      |52.39             |match_002439  |0.8692             |incremental_shopper  |2026-06-03 00:58:17.17441 |2026-06-07 19:48:07.789256|fact_control_group_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_control_group_transactions|bronze_run_20260607_194640|d5b682d73e688a3ac694db82c01a31ee18b63d17ab4dfc444a95f483d93fcde4|2026-06-07 23:54:28.870862|silver_run_20260607_235211|passed        |silver_rules_v1        |
|ctrl_tx_000539        |tx_ctrl_000539|assign_000001248     |cm_tok_004348          |merchant_000052|campaign_000055|offer_000217|segment_004|2026-02-22 19:34:32  |2026-02-22      |99.54             |match_001248  |0.9123             |subsidized_shopper   |2026-06-03 00:58:17.173575|2026-06-07 19:48:07.789256|fact_control_group_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_control_group_transactions|bronze_run_20260607_194640|5c5df458b73f4809e4429412a6a57345b69e5bfadd249be7019334ca49c7e1d0|2026-06-07 23:54:28.870862|silver_run_20260607_235211|passed        |silver_rules_v1        |
|ctrl_tx_001046        |tx_ctrl_001046|assign_000003685     |cm_tok_004788          |merchant_000178|campaign_000055|offer_000222|segment_002|2026-02-22 20:43:15  |2026-02-22      |68.42             |match_003685  |0.8488             |loyal_existing       |2026-06-03 00:58:17.175168|2026-06-07 19:48:07.789256|fact_control_group_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_control_group_transactions|bronze_run_20260607_194640|d56ffed655f99f2a76dea97857e2fc402ce912393559755570f0d917a9c56d01|2026-06-07 23:54:28.870862|silver_run_20260607_235211|passed        |silver_rules_v1        |
|ctrl_tx_001211        |tx_ctrl_001211|assign_000004251     |cm_tok_001810          |merchant_000178|campaign_000055|offer_000222|segment_002|2026-02-22 12:01:41  |2026-02-22      |201.24            |match_004251  |0.8824             |incremental_shopper  |2026-06-03 00:58:17.17569 |2026-06-07 19:48:07.789256|fact_control_group_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_control_group_transactions|bronze_run_20260607_194640|0bc76efa38893462704c2fda280a3832caf8f180dc8382cffe180fe4d09ea012|2026-06-07 23:54:28.870862|silver_run_20260607_235211|passed        |silver_rules_v1        |
+----------------------+--------------+---------------------+-----------------------+---------------+---------------+------------+-----------+---------------------+----------------+------------------+--------------+-------------------+---------------------+--------------------------+--------------------------+-------------------------------+-------------------------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
only showing top 5 rows

================================================================================
Inspecting input table: dim_offer_scd
================================================================================

Schema:
root
 |-- surrogate_scd_id: string (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- minimum_spend_amount: double (nullable = true)
 |-- reward_amount: double (nullable = true)
 |-- offer_start_date: date (nullable = true)
 |-- offer_end_date: date (nullable = true)
 |-- effective_start_date: date (nullable = true)
 |-- effective_end_date: date (nullable = true)
 |-- is_current: boolean (nullable = true)
 |-- scd_record_hash: string (nullable = true)
 |-- scd_created_at: timestamp (nullable = true)
 |-- scd_updated_at: timestamp (nullable = true)
 |-- scd_pipeline_run_id: string (nullable = true)
 |-- scd_rule_version: string (nullable = true)


Row count:
dim_offer_scd: 300 rows

Sample rows:
+--------------------------------------------------------------------+------------+---------------+---------------+--------------------+-------------+----------------+--------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|surrogate_scd_id                                                    |offer_id    |campaign_id    |merchant_id    |minimum_spend_amount|reward_amount|offer_start_date|offer_end_date|effective_start_date|effective_end_date|is_current|scd_record_hash                                                 |scd_created_at            |scd_updated_at            |scd_pipeline_run_id    |scd_rule_version|
+--------------------------------------------------------------------+------------+---------------+---------------+--------------------+-------------+----------------+--------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|scd_f9644844a30a76dc0ccec7a38ad4ac5e01270089a4c07c37f56fd8c0d2bd00d9|offer_000069|campaign_000051|merchant_000096|150.0               |0.0          |2026-06-08      |2026-06-22    |2026-01-01          |NULL              |true      |55afb1a3d43b847f538d651ff21e50158f4f2030aec546cdb17af46e596351c5|2026-06-08 05:42:20.076502|2026-06-08 05:42:20.076502|scd_run_20260608_054159|scd_rules_v1    |
|scd_5d76f6b8cc7164138551d9f7b596c1d3dd86ae51898a42af310b344be173deb9|offer_000269|campaign_000012|merchant_000085|50.0                |20.0         |2026-04-12      |2026-04-26    |2026-01-01          |NULL              |true      |dc0f0697d9ce9674ae20333cd922300bc7eb79453e97cfa97024961565921729|2026-06-08 05:42:20.076502|2026-06-08 05:42:20.076502|scd_run_20260608_054159|scd_rules_v1    |
|scd_13fce9f9569d2d0d6d986433c25492ec0fc3d133cf528a2a10d95f1da3d9272a|offer_000042|campaign_000041|merchant_000171|50.0                |0.0          |2026-05-24      |2026-06-23    |2026-01-01          |NULL              |true      |16b2def88ff8fb3d13d93a500b79494fcadb09af1b83bcece0032b9ae6356c42|2026-06-08 05:42:20.076502|2026-06-08 05:42:20.076502|scd_run_20260608_054159|scd_rules_v1    |
|scd_604e3a7ab7e666176481b69ffcc585c3dc780fecb8e6a1c8fc17b1077967e505|offer_000268|campaign_000010|merchant_000006|75.0                |20.0         |2026-05-04      |2026-06-03    |2026-01-01          |NULL              |true      |b9136f6ebe792a135ae5dea51aeb88c24a1bdb7e8b2fc084f8cf1960f60c10df|2026-06-08 05:42:20.076502|2026-06-08 05:42:20.076502|scd_run_20260608_054159|scd_rules_v1    |
|scd_1eaea75594f6361c1771619c3e318a717794e396928c8357ab256e4c0715ca86|offer_000250|campaign_000040|merchant_000084|250.0               |20.0         |2026-01-25      |2026-02-24    |2026-01-01          |NULL              |true      |0b07fb7fa7461914018b125ca1b183efadab801d10979a6cf46dceaa2ea94118|2026-06-08 05:42:20.076502|2026-06-08 05:42:20.076502|scd_run_20260608_054159|scd_rules_v1    |
+--------------------------------------------------------------------+------------+---------------+---------------+--------------------+-------------+----------------+--------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
only showing top 5 rows

================================================================================
Inspecting input table: dim_merchant_scd
================================================================================

Schema:
root
 |-- surrogate_scd_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- merchant_name: string (nullable = true)
 |-- category_id: string (nullable = true)
 |-- location_id: string (nullable = true)
 |-- merchant_margin_rate: double (nullable = true)
 |-- platform_fee_rate: double (nullable = true)
 |-- effective_start_date: date (nullable = true)
 |-- effective_end_date: date (nullable = true)
 |-- is_current: boolean (nullable = true)
 |-- scd_record_hash: string (nullable = true)
 |-- scd_created_at: timestamp (nullable = true)
 |-- scd_updated_at: timestamp (nullable = true)
 |-- scd_pipeline_run_id: string (nullable = true)
 |-- scd_rule_version: string (nullable = true)


Row count:
dim_merchant_scd: 200 rows

Sample rows:
+--------------------------------------------------------------------+---------------+------------------------+---------------+-------------+--------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|surrogate_scd_id                                                    |merchant_id    |merchant_name           |category_id    |location_id  |merchant_margin_rate|platform_fee_rate|effective_start_date|effective_end_date|is_current|scd_record_hash                                                 |scd_created_at            |scd_updated_at            |scd_pipeline_run_id    |scd_rule_version|
+--------------------------------------------------------------------+---------------+------------------------+---------------+-------------+--------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|scd_51f611478722083c7167cd3240691d47c77fb2da5472f30a26bfb64e112cabb9|merchant_000027|Davis Group             |category_000005|location_0020|0.5423              |0.0235           |2026-01-01          |NULL              |true      |eeaf0af7b6fab83c34fd623542f7ad88d4ebf65a8dee28c887f3e4db735354d1|2026-06-08 05:42:15.115581|2026-06-08 05:42:15.115581|scd_run_20260608_054159|scd_rules_v1    |
|scd_9fa4a44aeb36e94aab88bff19a6d4af3a37e85f9dde133eed215b91fd84eb718|merchant_000033|Kidd, Huff and Novak    |category_000002|location_0018|0.1132              |0.0243           |2026-01-01          |NULL              |true      |f3d45a905300a3ecc5bccb443a4db4e55e80d43f3289fa852d94bb149963178f|2026-06-08 05:42:15.115581|2026-06-08 05:42:15.115581|scd_run_20260608_054159|scd_rules_v1    |
|scd_9802cf5d207396460fa83469e72a2fbcdd12620abcb19c35ecc416b54a9ee65f|merchant_000054|Campbell-Clark          |category_000003|location_0005|0.4933              |0.032            |2026-01-01          |NULL              |true      |8aa1ec958bf17289ec1d721d6352978ebc43d90c3910f0070ff184e693347cb8|2026-06-08 05:42:15.115581|2026-06-08 05:42:15.115581|scd_run_20260608_054159|scd_rules_v1    |
|scd_72e0259a6e5b37eb4cd5f967c29568ee5cdec46880117344b805209ad3ee5d74|merchant_000056|Davis Group             |category_000005|location_0021|0.5529              |0.0239           |2026-01-01          |NULL              |true      |d207987b007ab17e9ee85970f32ecd514208e33a6585ed07dd9cb3cd7fac42d3|2026-06-08 05:42:15.115581|2026-06-08 05:42:15.115581|scd_run_20260608_054159|scd_rules_v1    |
|scd_ddd3e4536be7a4a8715192703573b788b3d1956865a213efe752061db37ec059|merchant_000076|Lee, Williams and Graham|category_000002|location_0019|0.1963              |0.0127           |2026-01-01          |NULL              |true      |59b3c67ad7ab333222f52452fea44c4f889569b7c9b9ea9dcc8559cddacbafa4|2026-06-08 05:42:15.115581|2026-06-08 05:42:15.115581|scd_run_20260608_054159|scd_rules_v1    |
+--------------------------------------------------------------------+---------------+------------------------+---------------+-------------+--------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
only showing top 5 rows

Incrementality input table summary
================================================================================
table                                                 rows
--------------------------------------------------------------------------------
fact_matched_offer_redemptions_clean                   766
fact_control_group_transactions_clean                1,500
dim_offer_scd                                          300
dim_merchant_scd                                       200
--------------------------------------------------------------------------------
TOTAL                                                2,766
================================================================================

Current SCD row counts
================================================================================
current offer rows                                     300
current merchant rows                                  200
================================================================================

Test group spend aggregation
================================================================================
test group spend rows                                  737
================================================================================

Test group spend sample
+-------------+------------+---------------+---------------+---------------------+----------------------+---------------------+-----------------------+------------------------+---------------------------------+----------------------------------+
|business_date|offer_id    |campaign_id    |merchant_id    |test_cardmember_count|test_transaction_count|test_redemption_count|total_test_spend_amount|total_test_reward_amount|average_test_spend_per_cardmember|average_test_reward_per_redemption|
+-------------+------------+---------------+---------------+---------------------+----------------------+---------------------+-----------------------+------------------------+---------------------------------+----------------------------------+
|2026-06-13   |offer_000140|campaign_000046|merchant_000044|1                    |1                     |1                    |268.32                 |20.0                    |268.32                           |20.0                              |
|2026-06-13   |offer_000008|campaign_000061|merchant_000050|1                    |1                     |1                    |341.55                 |0.0                     |341.55                           |0.0                               |
|2026-06-13   |offer_000170|campaign_000051|merchant_0

... [*** WARNING: max output size exceeded, skipping output. ***] ...

(nullable = false)
 |-- test_redemption_count: long (nullable = false)
 |-- total_test_spend_amount: double (nullable = true)
 |-- total_test_reward_amount: double (nullable = true)
 |-- average_test_spend_per_cardmember: double (nullable = true)
 |-- average_test_reward_per_redemption: double (nullable = true)
 |-- control_cardmember_count: long (nullable = false)
 |-- control_transaction_count: long (nullable = false)
 |-- total_control_spend_amount: double (nullable = true)
 |-- average_control_spend_per_cardmember: double (nullable = true)
 |-- lift_per_cardmember: double (nullable = true)
 |-- lift_direction: string (nullable = false)
 |-- test_to_control_spend_ratio: double (nullable = true)
 |-- incremental_revenue_amount: double (nullable = true)
 |-- incremental_revenue_direction: string (nullable = false)
 |-- lift_percentage: double (nullable = true)
 |-- absolute_incremental_revenue_amount: double (nullable = true)


Incremental revenue direction summary
+-----------------------------+-----+
|incremental_revenue_direction|count|
+-----------------------------+-----+
|positive_incremental_revenue |52   |
|negative_incremental_revenue |35   |
+-----------------------------+-----+


Enriched incrementality metrics
================================================================================
enriched incrementality rows                            87
================================================================================

Enriched incrementality sample
+-------------+------------+---------------+---------------+--------------------+-------------+--------------------+-----------------+---------------------+----------------------+---------------------+-----------------------+------------------------+---------------------------------+----------------------------------+------------------------+-------------------------+--------------------------+------------------------------------+-------------------+--------------+---------------------------+--------------------+--------------------------+-----------------------------------+-----------------------------+-----------------------------------+-----------------------------------------+----------------------------------------+----------------------------------+---------------------------+--------------------------+
|business_date|offer_id    |campaign_id    |merchant_id    |minimum_spend_amount|reward_amount|merchant_margin_rate|platform_fee_rate|test_cardmember_count|test_transaction_count|test_redemption_count|total_test_spend_amount|total_test_reward_amount|average_test_spend_per_cardmember|average_test_reward_per_redemption|control_cardmember_count|control_transaction_count|total_control_spend_amount|average_control_spend_per_cardmember|lift_per_cardmember|lift_direction|test_to_control_spend_ratio|lift_percentage     |incremental_revenue_amount|absolute_incremental_revenue_amount|incremental_revenue_direction|estimated_incremental_margin_amount|estimated_incremental_platform_fee_amount|estimated_incremental_value_after_reward|incrementality_pipeline_run_id    |incrementality_rule_version|incrementality_created_at |
+-------------+------------+---------------+---------------+--------------------+-------------+--------------------+-----------------+---------------------+----------------------+---------------------+-----------------------+------------------------+---------------------------------+----------------------------------+------------------------+-------------------------+--------------------------+------------------------------------+-------------------+--------------+---------------------------+--------------------+--------------------------+-----------------------------------+-----------------------------+-----------------------------------+-----------------------------------------+----------------------------------------+----------------------------------+---------------------------+--------------------------+
|2026-06-15   |offer_000148|campaign_000052|merchant_000118|75.0                |15.0         |0.1699              |0.0142           |1                    |1                     |1                    |112.69                 |15.0                    |112.69                           |15.0                              |1                       |1                        |106.75                    |106.75                              |5.939999999999998  |positive_lift |1.0556440281030446         |0.055644028103044474|5.939999999999998         |5.939999999999998                  |positive_incremental_revenue |1.0092059999999996                 |0.08434799999999998                      |-14.075142000000001                     |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-05-22   |offer_000298|campaign_000010|merchant_000088|150.0               |10.0         |0.6784              |0.0255           |1                    |1                     |1                    |222.31                 |10.0                    |222.31                           |10.0                              |1                       |1                        |25.57                     |25.57                               |196.74             |positive_lift |8.694172858818929          |7.694172858818929   |196.74                    |196.74                             |positive_incremental_revenue |133.46841600000002                 |5.01687                                  |118.45154600000002                      |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-06-26   |offer_000235|campaign_000069|merchant_000138|100.0               |5.0          |0.4121              |0.013            |1                    |1                     |1                    |140.32                 |5.0                     |140.32                           |5.0                               |1                       |1                        |191.17                    |191.17                              |-50.849999999999994|negative_lift |0.7340063817544594         |-0.26599361824554063|-50.849999999999994       |50.849999999999994                 |negative_incremental_revenue |-20.955285                         |-0.6610499999999999                      |-25.294235                              |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-03-27   |offer_000049|campaign_000008|merchant_000032|100.0               |10.0         |0.3071              |0.0267           |1                    |1                     |1                    |107.29                 |10.0                    |107.29                           |10.0                              |2                       |2                        |213.35000000000002        |106.67500000000001                  |0.6149999999999949 |positive_lift |1.0057651745957346         |0.00576517459573466 |0.6149999999999949        |0.6149999999999949                 |positive_incremental_revenue |0.1888664999999984                 |0.016420499999999866                     |-9.827554000000003                      |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-06-25   |offer_000070|campaign_000052|merchant_000129|50.0                |5.0          |0.1279              |0.0288           |1                    |1                     |1                    |73.93                  |5.0                     |73.93                            |5.0                               |1                       |1                        |70.38                     |70.38                               |3.5500000000000114 |positive_lift |1.0504404660414892         |0.050440466041489225|3.5500000000000114        |3.5500000000000114                 |positive_incremental_revenue |0.4540450000000015                 |0.10224000000000033                      |-4.6481949999999985                     |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-06-25   |offer_000086|campaign_000069|merchant_000070|100.0               |0.0          |0.7311              |0.0297           |1                    |1                     |1                    |130.23                 |0.0                     |130.23                           |0.0                               |1                       |1                        |92.76                     |92.76                               |37.469999999999985 |positive_lift |1.4039456662354461         |0.40394566623544614 |37.469999999999985        |37.469999999999985                 |positive_incremental_revenue |27.394316999999987                 |1.1128589999999996                       |26.281457999999986                      |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-06-15   |offer_000097|campaign_000052|merchant_000110|50.0                |0.0          |0.1534              |0.0248           |1                    |1                     |1                    |83.26                  |0.0                     |83.26                            |0.0                               |1                       |1                        |325.62                    |325.62                              |-242.36            |negative_lift |0.2556968245193784         |-0.7443031754806216 |-242.36                   |242.36                             |negative_incremental_revenue |-37.178024                         |-6.010528                                |-31.167496                              |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-05-22   |offer_000244|campaign_000033|merchant_000198|250.0               |5.0          |0.5232              |0.0215           |1                    |1                     |1                    |340.13                 |5.0                     |340.13                           |5.0                               |1                       |1                        |111.76                    |111.76                              |228.37             |positive_lift |3.0433965640658553         |2.0433965640658553  |228.37                    |228.37                             |positive_incremental_revenue |119.48318400000001                 |4.909955                                 |109.57322900000001                      |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-04-11   |offer_000184|campaign_000044|merchant_000117|250.0               |20.0         |0.5239              |0.0129           |1                    |1                     |1                    |319.23                 |20.0                    |319.23                           |20.0                              |2                       |2                        |308.76                    |154.38                              |164.85000000000002 |positive_lift |2.0678196657598136         |1.0678196657598136  |164.85000000000002        |164.85000000000002                 |positive_incremental_revenue |86.36491500000001                  |2.1265650000000003                       |64.23835000000001                       |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-04-16   |offer_000139|campaign_000044|merchant_000066|250.0               |0.0          |0.321               |0.0126           |2                    |2                     |2                    |716.31                 |0.0                     |358.155                          |0.0                               |1                       |2                        |227.99                    |227.99                              |130.16499999999996 |positive_lift |1.5709241633404971         |0.5709241633404972  |260.3299999999999         |260.3299999999999                  |positive_incremental_revenue |83.56592999999998                  |3.2801579999999992                       |80.28577199999998                       |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-06-15   |offer_000275|campaign_000046|merchant_000123|150.0               |0.0          |0.5796              |0.011            |2                    |2                     |2                    |445.66999999999996     |0.0                     |222.83499999999998               |0.0                               |1                       |1                        |429.84                    |429.84                              |-207.005           |negative_lift |0.5184138284012656         |-0.4815861715987344 |-414.01                   |414.01                             |negative_incremental_revenue |-239.960196                        |-4.55411                                 |-235.406086                             |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-05-12   |offer_000078|campaign_000053|merchant_000177|150.0               |50.0         |0.2957              |0.0148           |1                    |1                     |1                    |200.13                 |50.0                    |200.13                           |50.0                              |1                       |1                        |188.47                    |188.47                              |11.659999999999997 |positive_lift |1.0618666100705683         |0.06186661007056824 |11.659999999999997        |11.659999999999997                 |positive_incremental_revenue |3.4478619999999993                 |0.17256799999999994                      |-46.724706                              |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-06-18   |offer_000135|campaign_000041|merchant_000053|150.0               |0.0          |0.5996              |0.0153           |1                    |1                     |1                    |175.17                 |0.0                     |175.17                           |0.0                               |1                       |1                        |26.78                     |26.78                               |148.39             |positive_lift |6.541075429424943          |5.541075429424943   |148.39                    |148.39                             |positive_incremental_revenue |88.974644                          |2.270367                                 |86.704277                               |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-04-16   |offer_000272|campaign_000020|merchant_000009|100.0               |20.0         |0.3822              |0.02             |1                    |1                     |1                    |136.84                 |20.0                    |136.84                           |20.0                              |2                       |2                        |311.31                    |155.655                             |-18.814999999999998|negative_lift |0.8791237030612573         |-0.12087629693874272|-18.814999999999998       |18.814999999999998                 |negative_incremental_revenue |-7.191092999999999                 |-0.37629999999999997                     |-26.814792999999998                     |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-06-26   |offer_000020|campaign_000079|merchant_000091|250.0               |20.0         |0.3414              |0.0149           |1                    |1                     |1                    |409.3                  |20.0                    |409.3                            |20.0                              |1                       |1                        |97.21                     |97.21                               |312.09000000000003 |positive_lift |4.210472173644687          |3.2104721736446873  |312.09000000000003        |312.09000000000003                 |positive_incremental_revenue |106.547526                         |4.6501410000000005                       |81.897385                               |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-05-12   |offer_000071|campaign_000043|merchant_000122|100.0               |0.0          |0.6602              |0.0313           |1                    |1                     |1                    |165.38                 |0.0                     |165.38                           |0.0                               |1                       |1                        |247.46                    |247.46                              |-82.08000000000001 |negative_lift |0.6683100299038228         |-0.3316899700961772 |-82.08000000000001        |82.08000000000001                  |negative_incremental_revenue |-54.18921600000001                 |-2.5691040000000007                      |-51.620112000000006                     |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-03-06   |offer_000083|campaign_000063|merchant_000082|150.0               |0.0          |0.4309              |0.0201           |1                    |1                     |1                    |206.66                 |0.0                     |206.66                           |0.0                               |1                       |1                        |199.19                    |199.19                              |7.469999999999999  |positive_lift |1.0375018826246298         |0.037501882624629744|7.469999999999999         |7.469999999999999                  |positive_incremental_revenue |3.2188229999999995                 |0.15014699999999997                      |3.0686759999999995                      |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-02-23   |offer_000201|campaign_000029|merchant_000075|100.0               |10.0         |0.5939              |0.0343           |1                    |1                     |1                    |159.09                 |10.0                    |159.09                           |10.0                              |1                       |1                        |46.26                     |46.26                               |112.83000000000001 |positive_lift |3.439040207522698          |2.439040207522698   |112.83000000000001        |112.83000000000001                 |positive_incremental_revenue |67.009737                          |3.870069                                 |53.139668                               |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-04-05   |offer_000279|campaign_000050|merchant_000091|75.0                |10.0         |0.3414              |0.0149           |1                    |1                     |1                    |107.86                 |10.0                    |107.86                           |10.0                              |2                       |2                        |139.53                    |69.765                              |38.095             |positive_lift |1.5460474449939081         |0.5460474449939081  |38.095                    |38.095                             |positive_incremental_revenue |13.005633                          |0.5676154999999999                       |2.4380174999999995                      |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
|2026-06-23   |offer_000022|campaign_000006|merchant_000163|150.0               |0.0          |0.404               |0.0218           |1                    |1                     |1                    |223.27                 |0.0                     |223.27                           |0.0                               |2                       |2                        |224.05                    |112.025                             |111.245            |positive_lift |1.99303726846686           |0.99303726846686    |111.245                   |111.245                            |positive_incremental_revenue |44.942980000000006                 |2.425141                                 |42.51783900000001                       |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:04.981826|
+-------------+------------+---------------+---------------+--------------------+-------------+--------------------+-----------------+---------------------+----------------------+---------------------+-----------------------+------------------------+---------------------------------+----------------------------------+------------------------+-------------------------+--------------------------+------------------------------------+-------------------+--------------+---------------------------+--------------------+--------------------------+-----------------------------------+-----------------------------+-----------------------------------+-----------------------------------------+----------------------------------------+----------------------------------+---------------------------+--------------------------+
only showing top 20 rows

Enriched incrementality schema
root
 |-- business_date: date (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- minimum_spend_amount: double (nullable = false)
 |-- reward_amount: double (nullable = false)
 |-- merchant_margin_rate: double (nullable = false)
 |-- platform_fee_rate: double (nullable = false)
 |-- test_cardmember_count: long (nullable = false)
 |-- test_transaction_count: long (nullable = false)
 |-- test_redemption_count: long (nullable = false)
 |-- total_test_spend_amount: double (nullable = true)
 |-- total_test_reward_amount: double (nullable = true)
 |-- average_test_spend_per_cardmember: double (nullable = true)
 |-- average_test_reward_per_redemption: double (nullable = true)
 |-- control_cardmember_count: long (nullable = false)
 |-- control_transaction_count: long (nullable = false)
 |-- total_control_spend_amount: double (nullable = true)
 |-- average_control_spend_per_cardmember: double (nullable = true)
 |-- lift_per_cardmember: double (nullable = true)
 |-- lift_direction: string (nullable = false)
 |-- test_to_control_spend_ratio: double (nullable = true)
 |-- lift_percentage: double (nullable = true)
 |-- incremental_revenue_amount: double (nullable = true)
 |-- absolute_incremental_revenue_amount: double (nullable = true)
 |-- incremental_revenue_direction: string (nullable = false)
 |-- estimated_incremental_margin_amount: double (nullable = true)
 |-- estimated_incremental_platform_fee_amount: double (nullable = true)
 |-- estimated_incremental_value_after_reward: double (nullable = true)
 |-- incrementality_pipeline_run_id: string (nullable = false)
 |-- incrementality_rule_version: string (nullable = false)
 |-- incrementality_created_at: timestamp (nullable = false)


Estimated incremental value direction summary
+-------------------------+-----+
|estimated_value_direction|count|
+-------------------------+-----+
|positive_estimated_value |44   |
|negative_estimated_value |43   |
+-------------------------+-----+


Writing Gold incrementality Delta table
================================================================================
Wrote Gold Delta table: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/gold/gold_offer_incrementality

Written Gold table validation
================================================================================
table                                         gold_offer_incrementality
expected rows                                           87
actual rows                                             87
================================================================================
Gold write validation passed: gold_offer_incrementality

Gold incrementality Delta table written and validated.

Validating incrementality outputs
================================================================================

Gold offer incrementality business-rule validation
================================================================================
duplicate grain rows                                               0
null required rows                                                 0
invalid count metric rows                                          0
negative spend/reward rows                                         0
invalid rate rows                                                  0
invalid direction rows                                             0
lift formula mismatch rows                                         0
incremental revenue formula mismatch rows                          0
lift percentage formula mismatch rows                              0
estimated value formula mismatch rows                              0
total validation failures                                          0
================================================================================

Incrementality validation summary
================================================================================
table                                        failures       status
--------------------------------------------------------------------------------
gold_offer_incrementality                           0       PASSED
================================================================================
All incrementality output validations passed.