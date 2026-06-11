
# Incrementality Profitability Features

## Purpose

The incrementality profitability feature job converts offer-level lift results into merchant profitability decision features.

The job reads the Gold offer incrementality table, calculates merchant margin, COGS proxy, platform fee, funded reward cost, net merchant profit, ROAS, and efficiency classifications, then writes a validated Gold Delta feature table.

Primary implementation file:

```text
spark_jobs/build_incrementality_features.py
```

Input table:

```text
gold_offer_incrementality
```

Output table:

```text
gold_incrementality_features
```

Output location:

```text
data/lakehouse/gold/gold_incrementality_features/
```

---

## Why This Layer Exists

The incrementality table tells us whether spend increased.

That is not enough.

A merchant-funded offer can create incremental revenue but still lose money if the reward cost and platform fee exceed the merchant margin.

Example:

```text
incremental revenue = $10,000
merchant margin rate = 20%
estimated incremental margin = $2,000
reward cost = $2,500
platform fee = $300
net merchant profit = $2,000 - $2,500 - $300 = -$800
```

The offer created spend, but the merchant lost money.

This is why the profitability feature layer exists.

It answers:

```text
Was the incremental spend economically worth it?
```

---

## Input Table

The job reads:

```text
gold_offer_incrementality
```

This input is produced by:

```text
spark_jobs/build_incrementality.py
```

Required input columns include:

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
control_cardmember_count
total_control_spend_amount
average_test_spend_per_cardmember
average_control_spend_per_cardmember
lift_per_cardmember
lift_percentage
incremental_revenue_amount
incremental_revenue_direction
estimated_incremental_margin_amount
estimated_incremental_platform_fee_amount
estimated_incremental_value_after_reward
```

---

## Output Table: gold_incrementality_features

### Purpose

`gold_incrementality_features` is the profitability decision feature table.

It builds on `gold_offer_incrementality` and adds:

```text
merchant margin features
COGS proxy
platform fee logic
funded reward cost
net merchant profit
profitability labels
ROAS metrics
efficiency metrics
decision flags
pipeline metadata
```

### Grain

One row represents:

```text
one business date
one offer
one campaign
one merchant
```

Grain columns:

```text
business_date
offer_id
campaign_id
merchant_id
```

---

## Core Business Formula

The main Day 30 formula is:

```text
net_merchant_profit_amount =
    estimated_incremental_margin_amount
    - funded_reward_cost_amount
    - estimated_incremental_platform_fee_amount
```

Where:

```text
estimated_incremental_margin_amount =
    incremental_revenue_amount * normalized_merchant_margin_rate
```

```text
funded_reward_cost_amount =
    total_test_reward_amount
```

```text
estimated_incremental_platform_fee_amount =
    platform_fee_revenue_base_amount * normalized_platform_fee_rate
```

---

## Platform Fee Revenue Base

The implementation uses:

```text
platform_fee_revenue_base_amount =
    incremental_revenue_amount if incremental_revenue_amount > 0
    else 0
```

This avoids charging a negative platform fee when incremental revenue is negative.

Example:

```text
incremental_revenue_amount = -2,000
platform_fee_revenue_base_amount = 0
platform_fee_rate = 0.03
estimated_incremental_platform_fee_amount = 0
```

This is more realistic than:

```text
-2,000 * 0.03 = -60
```

because a platform fee should not become a negative merchant credit simply because the campaign underperformed.

---

## Profitability Base Metrics

### Normalized Merchant Margin Rate

```text
normalized_merchant_margin_rate =
    merchant_margin_rate if present
    else 0
```

### Normalized Platform Fee Rate

```text
normalized_platform_fee_rate =
    platform_fee_rate if present
    else 0
```

### Estimated Incremental Margin

```text
estimated_incremental_margin_amount =
    incremental_revenue_amount * normalized_merchant_margin_rate
```

### Estimated Incremental COGS Proxy

```text
estimated_incremental_cogs_amount =
    incremental_revenue_amount - estimated_incremental_margin_amount
```

This is not a true accounting COGS value.

It is a proxy that represents the portion of incremental revenue not retained as merchant margin.

### Estimated Incremental Platform Fee

```text
estimated_incremental_platform_fee_amount =
    platform_fee_revenue_base_amount * normalized_platform_fee_rate
```

---

## Net Merchant Profit Metrics

### Funded Reward Cost

```text
funded_reward_cost_amount =
    total_test_reward_amount
```

This is the reward cost funded by the merchant for the test group redemptions.

### Net Merchant Profit

```text
net_merchant_profit_amount =
    estimated_incremental_margin_amount
    - funded_reward_cost_amount
    - estimated_incremental_platform_fee_amount
```

### Net Profit Per Test Cardmember

```text
net_profit_per_test_cardmember =
    net_merchant_profit_amount / test_cardmember_count
```

### Net Profit Margin on Incremental Revenue

```text
net_profit_margin_on_incremental_revenue =
    net_merchant_profit_amount / incremental_revenue_amount
```

If the denominator is zero, the job returns zero using safe division.

---

## Profitability Classification

The job classifies each offer/day row as:

```text
profitable
unprofitable
break_even
```

Rules:

```text
net_merchant_profit_amount > 0  -> profitable
net_merchant_profit_amount < 0  -> unprofitable
net_merchant_profit_amount = 0  -> break_even
```

---

## Profitability Decision Flags

### incremental_revenue_positive_flag

```text
incremental_revenue_amount > 0
```

### net_profit_positive_flag

```text
net_merchant_profit_amount > 0
```

### profitable_incremental_offer_flag

```text
incremental_revenue_amount > 0
AND net_merchant_profit_amount > 0
```

Interpretation:

```text
The offer created incremental spend and made money.
```

### spend_lift_but_profit_loss_flag

```text
incremental_revenue_amount > 0
AND net_merchant_profit_amount < 0
```

Interpretation:

```text
The offer created incremental spend, but reward and platform costs exceeded merchant margin.
```

This is one of the most important business-risk signals.

### negative_lift_and_unprofitable_flag

```text
incremental_revenue_amount < 0
AND net_merchant_profit_amount < 0
```

Interpretation:

```text
The offer underperformed control and lost money.
```

---

## Profitability Explanation

Each row includes a human-readable explanation.

Possible examples:

```text
Offer produced positive incremental revenue and positive net merchant profit.
```

```text
Offer produced incremental revenue, but reward cost and platform fee exceeded merchant margin.
```

```text
Offer underperformed control group and produced negative net merchant profit.
```

```text
Offer approximately broke even after reward cost and platform fee.
```

```text
Offer profitability requires review.
```

These explanations make the Gold table easier to inspect during demos and interviews.

---

## ROAS and Efficiency Metrics

The job adds merchant-friendly efficiency metrics.

### Total Offer Cost

```text
total_offer_cost_amount =
    funded_reward_cost_amount
    + estimated_incremental_platform_fee_amount
```

### Reward ROAS

```text
reward_roas =
    incremental_revenue_amount / funded_reward_cost_amount
```

This answers:

```text
How much incremental revenue did the merchant get per reward dollar?
```

### Total Cost ROAS

```text
total_cost_roas =
    incremental_revenue_amount / total_offer_cost_amount
```

This is stricter than reward ROAS because it includes platform fee.

### Margin ROAS

```text
margin_roas =
    estimated_incremental_margin_amount / total_offer_cost_amount
```

This is more merchant-realistic because revenue is not profit.

### Net Profit ROAS

```text
net_profit_roas =
    net_merchant_profit_amount / total_offer_cost_amount
```

This is the strictest efficiency metric.

### Cost Per Incremental Revenue Dollar

```text
cost_per_incremental_revenue_dollar =
    total_offer_cost_amount / incremental_revenue_amount
```

Example:

```text
0.08 means it cost $0.08 to generate $1.00 of incremental revenue.
```

### Reward Cost Share of Incremental Revenue

```text
reward_cost_share_of_incremental_revenue =
    funded_reward_cost_amount / incremental_revenue_amount
```

### Platform Fee Share of Incremental Revenue

```text
platform_fee_share_of_incremental_revenue =
    estimated_incremental_platform_fee_amount / incremental_revenue_amount
```

---

## Efficiency Status

The job labels each row with an efficiency status.

Allowed values:

```text
highly_efficient
efficient
profitable_but_low_efficiency
revenue_positive_but_profit_negative
negative_incrementality
needs_review
```

Rules:

```text
net_merchant_profit_amount > 0 AND total_cost_roas >= 5
    -> highly_efficient
```

```text
net_merchant_profit_amount > 0 AND total_cost_roas >= 2
    -> efficient
```

```text
net_merchant_profit_amount > 0
    -> profitable_but_low_efficiency
```

```text
incremental_revenue_amount > 0 AND net_merchant_profit_amount < 0
    -> revenue_positive_but_profit_negative
```

```text
incremental_revenue_amount < 0
    -> negative_incrementality
```

```text
otherwise
    -> needs_review
```

---

## Example

Input:

```text
incremental_revenue_amount = 10,000
merchant_margin_rate = 0.30
platform_fee_rate = 0.03
funded_reward_cost_amount = 500
```

Calculations:

```text
estimated_incremental_margin_amount = 10,000 * 0.30 = 3,000

platform_fee_revenue_base_amount = 10,000

estimated_incremental_platform_fee_amount = 10,000 * 0.03 = 300

net_merchant_profit_amount = 3,000 - 500 - 300 = 2,200

total_offer_cost_amount = 500 + 300 = 800

reward_roas = 10,000 / 500 = 20.0

total_cost_roas = 10,000 / 800 = 12.5

margin_roas = 3,000 / 800 = 3.75

net_profit_roas = 2,200 / 800 = 2.75

cost_per_incremental_revenue_dollar = 800 / 10,000 = 0.08
```

Interpretation:

```text
The offer created incremental revenue, generated positive net merchant profit, and was highly efficient.
```

---

## Processing Flow

The job follows this flow:

```text
1. Read gold_offer_incrementality.
2. Validate input schema.
3. Calculate normalized merchant margin rate.
4. Calculate normalized platform fee rate.
5. Calculate estimated incremental margin.
6. Calculate estimated incremental COGS proxy.
7. Calculate platform fee revenue base.
8. Calculate estimated platform fee.
9. Calculate funded reward cost.
10. Calculate net merchant profit.
11. Calculate net profit per test cardmember.
12. Calculate net profit margin on incremental revenue.
13. Add profitability status.
14. Add profitability decision flags.
15. Add profitability explanation.
16. Add ROAS metrics.
17. Add efficiency status.
18. Add pipeline metadata.
19. Write gold_incrementality_features.
20. Validate written table row count.
21. Validate business rules and formulas.
```

---

## Output Validation

The job validates:

```text
unique output grain
required values are not null
valid merchant margin and platform fee rates
non-negative direct costs
valid profitability labels
valid efficiency labels
margin formula correctness
COGS proxy formula correctness
platform fee formula correctness
net profit formula correctness
total offer cost formula correctness
ROAS formula correctness
profitability label correctness
decision flag correctness
```

### Unique Grain

Expected grain:

```text
business_date + offer_id + campaign_id + merchant_id
```

### Required Metadata

The output includes:

```text
profitability_pipeline_run_id
profitability_rule_version
profitability_created_at
```

### Formula Checks

Margin:

```text
estimated_incremental_margin_amount =
    incremental_revenue_amount * normalized_merchant_margin_rate
```

COGS proxy:

```text
estimated_incremental_cogs_amount =
    incremental_revenue_amount - estimated_incremental_margin_amount
```

Platform fee base:

```text
platform_fee_revenue_base_amount =
    incremental_revenue_amount if incremental_revenue_amount > 0
    else 0
```

Platform fee:

```text
estimated_incremental_platform_fee_amount =
    platform_fee_revenue_base_amount * normalized_platform_fee_rate
```

Net profit:

```text
net_merchant_profit_amount =
    estimated_incremental_margin_amount
    - funded_reward_cost_amount
    - estimated_incremental_platform_fee_amount
```

Total offer cost:

```text
total_offer_cost_amount =
    funded_reward_cost_amount
    + estimated_incremental_platform_fee_amount
```

ROAS:

```text
reward_roas =
    incremental_revenue_amount / funded_reward_cost_amount
```

```text
total_cost_roas =
    incremental_revenue_amount / total_offer_cost_amount
```

```text
margin_roas =
    estimated_incremental_margin_amount / total_offer_cost_amount
```

```text
net_profit_roas =
    net_merchant_profit_amount / total_offer_cost_amount
```

---

## Important Interpretation

Negative incremental revenue is allowed.

Negative net merchant profit is allowed.

Negative ROAS values are allowed.

These are business outcomes, not necessarily technical failures.

However, these should not be negative:

```text
funded_reward_cost_amount
total_test_reward_amount
total_offer_cost_amount
minimum_spend_amount
reward_amount
```

Those are cost or rule fields and should be non-negative.

---

## Local Execution

Run only after `gold_offer_incrementality` exists.

Recommended local run order:

```bash
PYTHONPATH=src python spark_jobs/bronze_ingestion.py
PYTHONPATH=src python spark_jobs/silver_transformations.py
PYTHONPATH=src python spark_jobs/build_redemption_matching.py
PYTHONPATH=src python spark_jobs/build_scd_dimensions.py
PYTHONPATH=src python spark_jobs/build_merchant_economics.py
PYTHONPATH=src python spark_jobs/build_incrementality.py
PYTHONPATH=src python spark_jobs/build_incrementality_features.py
```

If upstream tables already exist, run only:

```bash
PYTHONPATH=src python spark_jobs/build_incrementality_features.py
```

Expected successful output:

```text
Gold write validation passed: gold_incrementality_features
All profitability output validations passed.
```

Check output folder:

```bash
ls data/lakehouse/gold/gold_incrementality_features
```

Expected:

```text
_delta_log
business_date=...
```

The `_delta_log` folder proves the output is a Delta table.

---

## Databricks Execution Notes

The job should run both locally and in Databricks.

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

PYTHONPATH=src python spark_jobs/build_incrementality_features.py
```

## Successful output

Starting Gold incrementality profitability feature build
================================================================================
Gold directory: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/gold
Pipeline run ID: incrementality_features_run_20260608_151919
Profitability rule version: incrementality_profitability_rules_v1
Input table: gold_offer_incrementality
Output table: gold_incrementality_features
================================================================================

================================================================================
Inspecting input table: gold_offer_incrementality
================================================================================

Schema:
root
 |-- business_date: date (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- minimum_spend_amount: double (nullable = true)
 |-- reward_amount: double (nullable = true)
 |-- merchant_margin_rate: double (nullable = true)
 |-- platform_fee_rate: double (nullable = true)
 |-- test_cardmember_count: long (nullable = true)
 |-- test_transaction_count: long (nullable = true)
 |-- test_redemption_count: long (nullable = true)
 |-- total_test_spend_amount: double (nullable = true)
 |-- total_test_reward_amount: double (nullable = true)
 |-- average_test_spend_per_cardmember: double (nullable = true)
 |-- average_test_reward_per_redemption: double (nullable = true)
 |-- control_cardmember_count: long (nullable = true)
 |-- control_transaction_count: long (nullable = true)
 |-- total_control_spend_amount: double (nullable = true)
 |-- average_control_spend_per_cardmember: double (nullable = true)
 |-- lift_per_cardmember: double (nullable = true)
 |-- lift_direction: string (nullable = true)
 |-- test_to_control_spend_ratio: double (nullable = true)
 |-- lift_percentage: double (nullable = true)
 |-- incremental_revenue_amount: double (nullable = true)
 |-- absolute_incremental_revenue_amount: double (nullable = true)
 |-- incremental_revenue_direction: string (nullable = true)
 |-- estimated_incremental_margin_amount: double (nullable = true)
 |-- estimated_incremental_platform_fee_amount: double (nullable = true)
 |-- estimated_incremental_value_after_reward: double (nullable = true)
 |-- incrementality_pipeline_run_id: string (nullable = true)
 |-- incrementality_rule_version: string (nullable = true)
 |-- incrementality_created_at: timestamp (nullable = true)


Row count:
gold_offer_incrementality: 87 rows

Sample rows:
+-------------+------------+---------------+---------------+--------------------+-------------+--------------------+-----------------+---------------------+----------------------+---------------------+-----------------------+------------------------+---------------------------------+----------------------------------+------------------------+-------------------------+--------------------------+------------------------------------+-------------------+--------------+---------------------------+--------------------+--------------------------+-----------------------------------+-----------------------------+-----------------------------------+-----------------------------------------+----------------------------------------+----------------------------------+---------------------------+--------------------------+
|business_date|offer_id    |campaign_id    |merchant_id    |minimum_spend_amount|reward_amount|merchant_margin_rate|platform_fee_rate|test_cardmember_count|test_transaction_count|test_redemption_count|total_test_spend_amount|total_test_reward_amount|average_test_spend_per_cardmember|average_test_reward_per_redemption|control_cardmember_count|control_transaction_count|total_control_spend_amount|average_control_spend_per_cardmember|lift_per_cardmember|lift_direction|test_to_control_spend_ratio|lift_percentage     |incremental_revenue_amount|absolute_incremental_revenue_amount|incremental_revenue_direction|estimated_incremental_margin_amount|estimated_incremental_platform_fee_amount|estimated_incremental_value_after_reward|incrementality_pipeline_run_id    |incrementality_rule_version|incrementality_created_at |
+-------------+------------+---------------+---------------+--------------------+-------------+--------------------+-----------------+---------------------+----------------------+---------------------+-----------------------+------------------------+---------------------------------+----------------------------------+------------------------+-------------------------+--------------------------+------------------------------------+-------------------+--------------+---------------------------+--------------------+--------------------------+-----------------------------------+-----------------------------+-----------------------------------+-----------------------------------------+----------------------------------------+----------------------------------+---------------------------+--------------------------+
|2026-04-30   |offer_000294|campaign_000009|merchant_000090|50.0                |0.0          |0.1046              |0.0253           |1                    |1                     |1                    |86.3                   |0.0                     |86.3                             |0.0                               |1                       |1                        |82.2                      |82.2                                |4.099999999999994  |positive_lift |1.0498783454987834         |0.04987834549878339 |4.099999999999994         |4.099999999999994                  |positive_incremental_revenue |0.4288599999999994                 |0.10372999999999985                      |0.3251299999999996                      |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:18.607501|
|2026-04-30   |offer_000272|campaign_000020|merchant_000009|100.0               |20.0         |0.3822              |0.02             |1                    |1                     |1                    |118.46                 |20.0                    |118.46                           |20.0                              |1                       |1                        |62.77                     |62.77                               |55.68999999999999  |positive_lift |1.887207264616855          |0.887207264616855   |55.68999999999999         |55.68999999999999                  |positive_incremental_revenue |21.284717999999994                 |1.1138                                   |0.17091799999999457                     |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:18.607501|
|2026-02-10   |offer_000187|campaign_000071|merchant_000041|250.0               |0.0          |0.1406              |0.0316           |1                    |1                     |1                    |298.95                 |0.0                     |298.95                           |0.0                               |2                       |2                        |133.36                    |66.68                               |232.26999999999998 |positive_lift |4.4833533293341326         |3.4833533293341326  |232.26999999999998        |232.26999999999998                 |positive_incremental_revenue |32.657162                          |7.339732                                 |25.31743                                |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:18.607501|
|2026-02-10   |offer_000273|campaign_000071|merchant_000123|50.0                |15.0         |0.5796              |0.011            |1                    |1                     |1                    |70.23                  |15.0                    |70.23                            |15.0                              |1                       |1                        |109.21                    |109.21                              |-38.97999999999999 |negative_lift |0.6430729786649575         |-0.3569270213350425 |-38.97999999999999        |38.97999999999999                  |negative_incremental_revenue |-22.592807999999994                |-0.4287799999999999                      |-37.16402799999999                      |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:18.607501|
|2026-03-28   |offer_000146|campaign_000037|merchant_000005|100.0               |0.0          |0.437               |0.0167           |1                    |1                     |1                    |106.07                 |0.0                     |106.07                           |0.0                               |1                       |1                        |102.2                     |102.2                               |3.8699999999999903 |positive_lift |1.0378669275929548         |0.037866927592954895|3.8699999999999903        |3.8699999999999903                 |positive_incremental_revenue |1.6911899999999958                 |0.06462899999999984                      |1.626560999999996                       |incrementality_run_20260608_145237|incrementality_rules_v1    |2026-06-08 14:54:18.607501|
+-------------+------------+---------------+---------------+--------------------+-------------+--------------------+-----------------+---------------------+----------------------+---------------------+-----------------------+------------------------+---------------------------------+----------------------------------+------------------------+-------------------------+--------------------------+------------------------------------+-------------------+--------------+---------------------------+--------------------+--------------------------+-----------------------------------+-----------------------------+-----------------------------------+-----------------------------------------+----------------------------------------+----------------------------------+---------------------------+--------------------------+
only showing top 5 rows

Profitability input summary
================================================================================
input table                                   gold_offer_incrementality
input rows                                              87
================================================================================

Profitability base metrics
================================================================================
profitability base rows                                 87
================================================================================

Profitability base sample
+-------------+------------+---------------+---------------+--------------------------+--------------------+-----------------+-------------------------------+----------------------------+-----------------------------------+---------------------------------+-----------------------------------------+------------------------+
|business_date|offer_id    |campaign_id    |merchant_id    |incremental_revenue_amount|merchant_margin_rate|platform_fee_rate|normalized_merchant_margin_rate|normalized_platform_fee_rate|estimated_incremental_margin_amount|estimated_incremental_cogs_amount|estimated_incremental_platform_fee_amount|total_test_reward_amount|
+-------------+------------+---------------+---------------+--------------------------+--------------------+-----------------+-------------------------------+----------------------------+-----------------------------------+---------------------------------+-----------------------------------------+------------------------+
|2026-04-19   |offer_000272|campaign_000020|merchant_000009|-33.0                     |0.3822              |0.02             |0.3822                         |0.02                        |-12.612599999999999                |-20.3874                         |0.0                                      |20.0                    |
|2026-04-19   |offer_000254|campaign_000002|merchant_000086|136.54                    |0.5322              |0.0269           |0.5322                         |0.0269                      |72.66658799999999                  |63.873412                        |3.672926                                 |15.0                    |
|2026-04-19   |offer_000139|campaign_000044|merchant_000066|158.69                    |0.321               |0.0126           |0.321                          |0.0126                      |50.93949                           |107.75050999999999               |1.9994939999999999                       |0.0                     |
|2026-06-15   |offer_000097|campaign_000052|merchant_000110|-242.36                   |0.1534              |0.0248           |0.1534                         |0.0248                      |-37.178024                         |-205.18197600000002              |0.0                                      |0.0                     |
|2026-06-15   |offer_000275|campaign_000046|merchant_000123|-414.01                   |0.5796              |0.011            |0.5796                         |0.011                       |-239.960196                        |-174.049804                      |0.0                                      |0.0                     |
|2026-06-15   |offer_000148|campaign_000052|merchant_000118|5.939999999999998         |0.1699              |0.0142           |0.1699                         |0.0142                      |1.0092059999999996                 |4.930793999999998                |0.08434799999999998                      |15.0                    |
|2026-05-06   |offer_000196|campaign_000018|merchant_000148|80.33999999999997         |0.1558              |0.0249           |0.1558                         |0.0249                      |12.516971999999996                 |67.82302799999998                |2.0004659999999994                       |0.0                     |
|2026-05-06   |offer_000254|campaign_000002|merchant_000086|21.349999999999994        |0.5322              |0.0269           |0.5322                         |0.0269                      |11.362469999999997                 |9.987529999999998                |0.5743149999999999                       |15.0                    |
|2026-04-22   |offer_000100|campaign_000047|merchant_000178|-45.019999999999996       |0.5746              |0.0117           |0.5746                         |0.0117                      |-25.868491999999996                |-19.151508                       |0.0                                      |5.0                     |
|2026-04-22   |offer_000067|campaign_000073|merchant_000144|-18.230000000000018       |0.3574              |0.021            |0.3574                         |0.021                       |-6.515402000000006                 |-11.714598000000013              |0.0                                      |50.0                    |
|2026-06-25   |offer_000070|campaign_000052|merchant_000129|3.5500000000000114        |0.1279              |0.0288           |0.1279                         |0.0288                      |0.4540450000000015                 |3.09595500000001                 |0.10224000000000033                      |5.0                     |
|2026-06-25   |offer_000086|campaign_000069|merchant_000070|37.469999999999985        |0.7311              |0.0297           |0.7311                         |0.0297                      |27.394316999999987                 |10.075682999999998               |1.1128589999999996                       |0.0                     |
|2026-04-30   |offer_000294|campaign_000009|merchant_000090|4.099999999999994         |0.1046              |0.0253           |0.1046                         |0.0253                      |0.4288599999999994                 |3.671139999999995                |0.10372999999999985                      |0.0                     |
|2026-04-30   |offer_000272|campaign_000020|merchant_000009|55.68999999999999         |0.3822              |0.02             |0.3822                         |0.02                        |21.284717999999994                 |34.405282                        |1.1138                                   |20.0                    |
|2026-02-10   |offer_000187|campaign_000071|merchant_000041|232.26999999999998        |0.1406              |0.0316           |0.1406                         |0.0316                      |32.657162                          |199.61283799999998               |7.339732                                 |0.0                     |
|2026-02-10   |offer_000273|campaign_000071|merchant_000123|-38.97999999999999        |0.5796              |0.011            |0.5796                         |0.011                       |-22.592807999999994                |-16.387191999999995              |0.0                                      |15.0                    |
|2026-04-08   |offer_000091|campaign_000005|merchant_000045|74.16                     |0.3736              |0.0178           |0.3736                         |0.0178                      |27.706176                          |46.453824                        |1.3200479999999999                       |0.0                     |
|2026-04-08   |offer_000270|campaign_000074|merchant_000111|167.4                     |0.6584              |0.0317           |0.6584                         |0.0317                      |110.21616                          |57.183840000000004               |5.30658                                  |0.0                     |
|2026-05-22   |offer_000298|campaign_000010|merchant_000088|196.74                    |0.6784              |0.0255           |0.6784                         |0.0255                      |133.46841600000002                 |63.27158399999999                |5.01687                                  |10.0                    |
|2026-05-22   |offer_000244|campaign_000033|merchant_000198|228.37                    |0.5232              |0.0215           |0.5232                         |0.0215                      |119.48318400000001                 |108.886816                       |4.909955                                 |5.0                     |
+-------------+------------+---------------+---------------+--------------------------+--------------------+-----------------+-------------------------------+----------------------------+-----------------------------------+---------------------------------+-----------------------------------------+------------------------+
only showing top 20 rows

Profitability base schema
root
 |-- business_date: date (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- minimum_spend_amount: double (nullable = true)
 |-- reward_amount: double (nullable = true)
 |-- merchant_margin_rate: double (nullable = true)
 |-- platform_fee_rate: double (nullable = true)
 |-- test_cardmember_count: long (nullable = true)
 |-- test_transaction_count: long (nullable = true)
 |-- test_redemption_count: long (nullable = true)
 |-- total_test_spend_amount: double (nullable = true)
 |-- total_test_reward_amount: double (nullable = true)
 |-- average_test_spend_per_cardmember: double (nullable = true)
 |-- average_test_reward_per_redemption: double (nullable = true)
 |-- control_cardmember_count: long (nullable = true)
 |-- control_transaction_count: long (nullable = true)
 |-- total_control_spend_amount: double (nullable = true)
 |-- average_control_spend_per_cardmember: double (nullable = true)
 |-- lift_per_cardmember: double (nullable = true)
 |-- lift_direction: string (nullable = true)
 |-- test_to_control_spend_ratio: double (nullable = true)
 |-- lift_percentage: double (nullable = true)
 |-- incremental_revenue_amount: double (nullable = true)
 |-- absolute_incremental_revenue_amount: double (nullable = true)
 |-- incremental_revenue_direction: string (nullable = true)
 |-- estimated_incremental_margin_amount: double (nullable = true)
 |-- estimated_incremental_platform_fee_amount: double (nullable = true)
 |-- estimated_incremental_value_after_reward: double (nullable = true)
 |-- incrementality_pipeline_run_id: string (nullable = true)
 |-- incrementality_rule_version: string (nullable = true)
 |-- incrementality_created_at: timestamp (nullable = true)
 |-- normalized_merchant_margin_rate: double (nullable = false)
 |-- normalized_platform_fee_rate: double (nullable = false)
 |-- estimated_incremental_cogs_amount: double (nullable = true)
 |-- platform_fee_revenue_base_amount: double (nullable = true)


Net merchant profit metrics
================================================================================
net profit rows                                         87
================================================================================

Net merchant profit sample
+-------------+------------+---------------+---------------+--------------------------+-----------------------------------+-------------------------+-----------------------------------------+--------------------------+------------------------------+----------------------------------------+
|business_date|offer_id    |campaign_id    |merchant_id    |incremental_revenue_amount|estimated_incremental_margin_amount|funded_reward_cost_amount|estimated_incremental_platform_fee_amount|net_merchant_profit_amount|net_profit_per_test_cardmember|net_profit_margin_on_incremental_revenue|
+-------------+------------+---------------+---------------+--------------------------+-----------------------------------+-------------------------+-----------------------------------------+--------------------------+------------------------------+----------------------------------------+
|2026-04-19   |offer_000272|campaign_000020|merchant_000009|-33.0                     |-12.612599999999999                |20.0                     |0.0                                      |-32.6126                  |-32.6126                      |0.9882606060606061                      |
|2026-04-19   |offer_000254|campaign_000002|merchant_000086|136.54                    |72.66658799999999                  |15.0                     |3.672926                                 |53.99366199999999         |53.99366199999999             |0.39544208290610805                     |
|2026-04-19   |offer_000139|campaign_000044|merchant_000066|158.69                    |50.93949                           |0.0                      |1.9994939999999999                       |48.939996                 |48.939996                     |0.3084                                  |
|2026-06-15   |offer_000097|campaign_000052|merchant_000110|-242.36                   |-37.178024                         |0.0                      |0.0                                      |-37.178024                |-37.178024                    |0.15339999999999998                     |
|2026-06-15   |offer_000275|campaign_000046|merchant_000123|-414.01                   |-239.960196                        |0.0                      |0.0                                      |-239.960196               |-119.980098                   |0.5796                                  |
|2026-06-15   |offer_000148|campaign_000052|merchant_000118|5.939999999999998         |1.0092059999999996                 |15.0                     |0.08434799999999998                      |-14.075142000000001       |-14.075142000000001           |-2.3695525252525265                     |
|2026-05-06   |offer_000196|campaign_000018|merchant_000148|80.33999999999997         |12.516971999999996                 |0.0                      |2.0004659999999994                       |10.516505999999996        |10.516505999999996            |0.1309                                  |
|2026-05-06   |offer_000254|campaign_000002|merchant_000086|21.349999999999994        |11.362469999999997                 |15.0                     |0.5743149999999999                       |-4.211845000000004        |-4.211845000000004            |-0.1972761124121782                     |
|2026-04-22   |offer_000100|campaign_000047|merchant_000178|-45.019999999999996       |-25.868491999999996                |5.0                      |0.0                                      |-30.868491999999996       |-30.868491999999996           |0.6856617503331852                      |
|2026-04-22   |offer_000067|campaign_000073|merchant_000144|-18.230000000000018       |-6.515402000000006                 |50.0                     |0.0                                      |-56.51540200000001        |-56.51540200000001            |3.1001317608337877                      |
|2026-06-25   |offer_000070|campaign_000052|merchant_000129|3.5500000000000114        |0.4540450000000015                 |5.0                      |0.10224000000000033                      |-4.6481949999999985       |-4.6481949999999985           |-1.3093507042253476                     |
|2026-06-25   |offer_000086|campaign_000069|merchant_000070|37.469999999999985        |27.394316999999987                 |0.0                      |1.1128589999999996                    

... [*** WARNING: max output size exceeded, skipping output. ***] ...

 test_redemption_count: long (nullable = true)
 |-- total_test_spend_amount: double (nullable = true)
 |-- total_test_reward_amount: double (nullable = true)
 |-- average_test_spend_per_cardmember: double (nullable = true)
 |-- average_test_reward_per_redemption: double (nullable = true)
 |-- control_cardmember_count: long (nullable = true)
 |-- control_transaction_count: long (nullable = true)
 |-- total_control_spend_amount: double (nullable = true)
 |-- average_control_spend_per_cardmember: double (nullable = true)
 |-- lift_per_cardmember: double (nullable = true)
 |-- lift_direction: string (nullable = true)
 |-- test_to_control_spend_ratio: double (nullable = true)
 |-- lift_percentage: double (nullable = true)
 |-- incremental_revenue_amount: double (nullable = true)
 |-- absolute_incremental_revenue_amount: double (nullable = true)
 |-- incremental_revenue_direction: string (nullable = true)
 |-- estimated_incremental_margin_amount: double (nullable = true)
 |-- estimated_incremental_platform_fee_amount: double (nullable = true)
 |-- estimated_incremental_value_after_reward: double (nullable = true)
 |-- incrementality_pipeline_run_id: string (nullable = true)
 |-- incrementality_rule_version: string (nullable = true)
 |-- incrementality_created_at: timestamp (nullable = true)
 |-- normalized_merchant_margin_rate: double (nullable = false)
 |-- normalized_platform_fee_rate: double (nullable = false)
 |-- estimated_incremental_cogs_amount: double (nullable = true)
 |-- platform_fee_revenue_base_amount: double (nullable = true)
 |-- funded_reward_cost_amount: double (nullable = false)
 |-- net_merchant_profit_amount: double (nullable = true)
 |-- net_profit_per_test_cardmember: double (nullable = true)
 |-- net_profit_margin_on_incremental_revenue: double (nullable = true)
 |-- profitability_status: string (nullable = false)
 |-- incremental_revenue_positive_flag: boolean (nullable = true)
 |-- net_profit_positive_flag: boolean (nullable = true)
 |-- profitable_incremental_offer_flag: boolean (nullable = true)
 |-- spend_lift_but_profit_loss_flag: boolean (nullable = true)
 |-- negative_lift_and_unprofitable_flag: boolean (nullable = true)
 |-- profitability_explanation: string (nullable = false)


ROAS and efficiency metrics
================================================================================
efficiency metric rows                                  87
================================================================================

ROAS and efficiency sample
+-------------+------------+---------------+---------------+--------------------------+-------------------------+-----------------------------------------+-----------------------+--------------------------+--------------------+--------------------+--------------------+--------------------+-----------------------------------+------------------------------------+
|business_date|offer_id    |campaign_id    |merchant_id    |incremental_revenue_amount|funded_reward_cost_amount|estimated_incremental_platform_fee_amount|total_offer_cost_amount|net_merchant_profit_amount|reward_roas         |total_cost_roas     |margin_roas         |net_profit_roas     |cost_per_incremental_revenue_dollar|efficiency_status                   |
+-------------+------------+---------------+---------------+--------------------------+-------------------------+-----------------------------------------+-----------------------+--------------------------+--------------------+--------------------+--------------------+--------------------+-----------------------------------+------------------------------------+
|2026-04-19   |offer_000272|campaign_000020|merchant_000009|-33.0                     |20.0                     |0.0                                      |20.0                   |-32.6126                  |-1.65               |-1.65               |-0.6306299999999999 |-1.63063            |-0.6060606060606061                |negative_incrementality             |
|2026-04-19   |offer_000254|campaign_000002|merchant_000086|136.54                    |15.0                     |3.672926                                 |18.672926              |53.99366199999999         |9.102666666666666   |7.312190922836624   |3.891548009133651   |2.891548009133651   |0.1367579170938919                 |highly_efficient                    |
|2026-04-19   |offer_000139|campaign_000044|merchant_000066|158.69                    |0.0                      |1.9994939999999999                       |1.9994939999999999     |48.939996                 |0.0                 |79.36507936507937   |25.476190476190478  |24.476190476190478  |0.0126                             |highly_efficient                    |
|2026-06-15   |offer_000097|campaign_000052|merchant_000110|-242.36                   |0.0                      |0.0                                      |0.0                    |-37.178024                |0.0                 |0.0                 |0.0                 |0.0                 |-0.0                               |negative_incrementality             |
|2026-06-15   |offer_000275|campaign_000046|merchant_000123|-414.01                   |0.0                      |0.0                                      |0.0                    |-239.960196               |0.0                 |0.0                 |0.0                 |0.0                 |-0.0                               |negative_incrementality             |
|2026-06-15   |offer_000148|campaign_000052|merchant_000118|5.939999999999998         |15.0                     |0.08434799999999998                      |15.084348              |-14.075142000000001       |0.39599999999999985 |0.3937856644516553  |0.06690418439033623 |-0.9330958156096638 |2.539452525252526                  |revenue_positive_but_profit_negative|
|2026-05-06   |offer_000196|campaign_000018|merchant_000148|80.33999999999997         |0.0                      |2.0004659999999994                       |2.0004659999999994     |10.516505999999996        |0.0                 |40.16064257028113   |6.257028112449799   |5.257028112449799   |0.024900000000000002               |highly_efficient                    |
|2026-05-06   |offer_000254|campaign_000002|merchant_000086|21.349999999999994        |15.0                     |0.5743149999999999                       |15.574315              |-4.211845000000004        |1.423333333333333   |1.3708468077087175  |0.7295646710625794  |-0.2704353289374206 |0.7294761124121782                 |revenue_positive_but_profit_negative|
|2026-04-22   |offer_000100|campaign_000047|merchant_000178|-45.019999999999996       |5.0                      |0.0                                      |5.0                    |-30.868491999999996       |-9.004              |-9.004              |-5.173698399999999  |-6.173698399999999  |-0.11106175033318526               |negative_incrementality             |
|2026-04-22   |offer_000067|campaign_000073|merchant_000144|-18.230000000000018       |50.0                     |0.0                                      |50.0                   |-56.51540200000001        |-0.36460000000000037|-0.36460000000000037|-0.13030804000000012|-1.13030804         |-2.7427317608337876                |negative_incrementality             |
|2026-06-25   |offer_000070|campaign_000052|merchant_000129|3.5500000000000114        |5.0                      |0.10224000000000033                      |5.10224                |-4.6481949999999985       |0.7100000000000023  |0.6957728370284446  |0.08898934585593807 |-0.9110106541440619 |1.4372507042253475                 |revenue_positive_but_profit_negative|
|2026-06-25   |offer_000086|campaign_000069|merchant_000070|37.469999999999985        |0.0                      |1.1128589999999996                       |1.1128589999999996     |26.281457999999986        |0.0                 |33.67003367003367   |24.616161616161612  |23.616161616161612  |0.0297                             |highly_efficient                    |
|2026-04-30   |offer_000294|campaign_000009|merchant_000090|4.099999999999994         |0.0                      |0.10372999999999985                      |0.10372999999999985    |0.3251299999999996        |0.0                 |39.52569169960474   |4.134387351778656   |3.1343873517786567  |0.0253                             |highly_efficient                    |
|2026-04-30   |offer_000272|campaign_000020|merchant_000009|55.68999999999999         |20.0                     |1.1138                                   |21.1138                |0.17091799999999457       |2.7844999999999995  |2.637611420019134   |1.0080950847313128  |0.008095084731312912|0.37913090321422166                |efficient                           |
|2026-02-10   |offer_000187|campaign_000071|merchant_000041|232.26999999999998        |0.0                      |7.339732                                 |7.339732               |25.31743                  |0.0                 |31.645569620253163  |4.449367088607595   |3.449367088607595   |0.0316                             |highly_efficient                    |
|2026-02-10   |offer_000273|campaign_000071|merchant_000123|-38.97999999999999        |15.0                     |0.0                                      |15.0                   |-37.59280799999999        |-2.598666666666666  |-2.598666666666666  |-1.5061871999999996 |-2.5061871999999994 |-0.3848127244740894                |negative_incrementality             |
|2026-04-08   |offer_000091|campaign_000005|merchant_000045|74.16                     |0.0                      |1.3200479999999999                       |1.3200479999999999     |26.386128                 |0.0                 |56.17977528089888   |20.98876404494382   |19.98876404494382   |0.0178                             |highly_efficient                    |
|2026-04-08   |offer_000270|campaign_000074|merchant_000111|167.4                     |0.0                      |5.30658                                  |5.30658                |104.90958                 |0.0                 |31.545741324921135  |20.769716088328074  |19.769716088328074  |0.0317                             |highly_efficient                    |
|2026-05-22   |offer_000298|campaign_000010|merchant_000088|196.74                    |10.0                     |5.01687                                  |15.01687               |118.45154600000002        |19.674              |13.101265443464584  |8.887898476846374   |7.887898476846375   |0.07632850462539392                |highly_efficient                    |
|2026-05-22   |offer_000244|campaign_000033|merchant_000198|228.37                    |5.0                      |4.909955                                 |9.909955               |109.57322900000001        |45.674              |23.044504238414806  |12.056884617538627  |11.056884617538628  |0.0433942943468932                 |highly_efficient                    |
+-------------+------------+---------------+---------------+--------------------------+-------------------------+-----------------------------------------+-----------------------+--------------------------+--------------------+--------------------+--------------------+--------------------+-----------------------------------+------------------------------------+
only showing top 20 rows

Efficiency status summary
+------------------------------------+-----+
|efficiency_status                   |count|
+------------------------------------+-----+
|negative_incrementality             |35   |
|revenue_positive_but_profit_negative|8    |
|highly_efficient                    |41   |
|efficient                           |3    |
+------------------------------------+-----+


ROAS and efficiency schema
root
 |-- business_date: date (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- minimum_spend_amount: double (nullable = true)
 |-- reward_amount: double (nullable = true)
 |-- merchant_margin_rate: double (nullable = true)
 |-- platform_fee_rate: double (nullable = true)
 |-- test_cardmember_count: long (nullable = true)
 |-- test_transaction_count: long (nullable = true)
 |-- test_redemption_count: long (nullable = true)
 |-- total_test_spend_amount: double (nullable = true)
 |-- total_test_reward_amount: double (nullable = true)
 |-- average_test_spend_per_cardmember: double (nullable = true)
 |-- average_test_reward_per_redemption: double (nullable = true)
 |-- control_cardmember_count: long (nullable = true)
 |-- control_transaction_count: long (nullable = true)
 |-- total_control_spend_amount: double (nullable = true)
 |-- average_control_spend_per_cardmember: double (nullable = true)
 |-- lift_per_cardmember: double (nullable = true)
 |-- lift_direction: string (nullable = true)
 |-- test_to_control_spend_ratio: double (nullable = true)
 |-- lift_percentage: double (nullable = true)
 |-- incremental_revenue_amount: double (nullable = true)
 |-- absolute_incremental_revenue_amount: double (nullable = true)
 |-- incremental_revenue_direction: string (nullable = true)
 |-- estimated_incremental_margin_amount: double (nullable = true)
 |-- estimated_incremental_platform_fee_amount: double (nullable = true)
 |-- estimated_incremental_value_after_reward: double (nullable = true)
 |-- incrementality_pipeline_run_id: string (nullable = true)
 |-- incrementality_rule_version: string (nullable = true)
 |-- incrementality_created_at: timestamp (nullable = true)
 |-- normalized_merchant_margin_rate: double (nullable = false)
 |-- normalized_platform_fee_rate: double (nullable = false)
 |-- estimated_incremental_cogs_amount: double (nullable = true)
 |-- platform_fee_revenue_base_amount: double (nullable = true)
 |-- funded_reward_cost_amount: double (nullable = false)
 |-- net_merchant_profit_amount: double (nullable = true)
 |-- net_profit_per_test_cardmember: double (nullable = true)
 |-- net_profit_margin_on_incremental_revenue: double (nullable = true)
 |-- profitability_status: string (nullable = false)
 |-- incremental_revenue_positive_flag: boolean (nullable = true)
 |-- net_profit_positive_flag: boolean (nullable = true)
 |-- profitable_incremental_offer_flag: boolean (nullable = true)
 |-- spend_lift_but_profit_loss_flag: boolean (nullable = true)
 |-- negative_lift_and_unprofitable_flag: boolean (nullable = true)
 |-- profitability_explanation: string (nullable = false)
 |-- total_offer_cost_amount: double (nullable = true)
 |-- reward_roas: double (nullable = true)
 |-- total_cost_roas: double (nullable = true)
 |-- margin_roas: double (nullable = true)
 |-- net_profit_roas: double (nullable = true)
 |-- cost_per_incremental_revenue_dollar: double (nullable = true)
 |-- reward_cost_share_of_incremental_revenue: double (nullable = true)
 |-- platform_fee_share_of_incremental_revenue: double (nullable = true)
 |-- efficiency_status: string (nullable = false)


Final incrementality profitability features
================================================================================
final feature rows                                      87
================================================================================

Final feature sample
+-------------+------------+---------------+---------------+--------------------------+--------------------------+--------------------+--------------------+--------------------+------------------------------------+-------------------------------------------+-------------------------------------+--------------------------+
|business_date|offer_id    |campaign_id    |merchant_id    |incremental_revenue_amount|net_merchant_profit_amount|profitability_status|total_cost_roas     |net_profit_roas     |efficiency_status                   |profitability_pipeline_run_id              |profitability_rule_version           |profitability_created_at  |
+-------------+------------+---------------+---------------+--------------------------+--------------------------+--------------------+--------------------+--------------------+------------------------------------+-------------------------------------------+-------------------------------------+--------------------------+
|2026-04-19   |offer_000272|campaign_000020|merchant_000009|-33.0                     |-32.6126                  |unprofitable        |-1.65               |-1.63063            |negative_incrementality             |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-04-19   |offer_000254|campaign_000002|merchant_000086|136.54                    |53.99366199999999         |profitable          |7.312190922836624   |2.891548009133651   |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-04-19   |offer_000139|campaign_000044|merchant_000066|158.69                    |48.939996                 |profitable          |79.36507936507937   |24.476190476190478  |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-06-15   |offer_000097|campaign_000052|merchant_000110|-242.36                   |-37.178024                |unprofitable        |0.0                 |0.0                 |negative_incrementality             |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-06-15   |offer_000275|campaign_000046|merchant_000123|-414.01                   |-239.960196               |unprofitable        |0.0                 |0.0                 |negative_incrementality             |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-06-15   |offer_000148|campaign_000052|merchant_000118|5.939999999999998         |-14.075142000000001       |unprofitable        |0.3937856644516553  |-0.9330958156096638 |revenue_positive_but_profit_negative|incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-05-06   |offer_000196|campaign_000018|merchant_000148|80.33999999999997         |10.516505999999996        |profitable          |40.16064257028113   |5.257028112449799   |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-05-06   |offer_000254|campaign_000002|merchant_000086|21.349999999999994        |-4.211845000000004        |unprofitable        |1.3708468077087175  |-0.2704353289374206 |revenue_positive_but_profit_negative|incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-04-22   |offer_000100|campaign_000047|merchant_000178|-45.019999999999996       |-30.868491999999996       |unprofitable        |-9.004              |-6.173698399999999  |negative_incrementality             |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-04-22   |offer_000067|campaign_000073|merchant_000144|-18.230000000000018       |-56.51540200000001        |unprofitable        |-0.36460000000000037|-1.13030804         |negative_incrementality             |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-06-25   |offer_000070|campaign_000052|merchant_000129|3.5500000000000114        |-4.6481949999999985       |unprofitable        |0.6957728370284446  |-0.9110106541440619 |revenue_positive_but_profit_negative|incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-06-25   |offer_000086|campaign_000069|merchant_000070|37.469999999999985        |26.281457999999986        |profitable          |33.67003367003367   |23.616161616161612  |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-04-30   |offer_000294|campaign_000009|merchant_000090|4.099999999999994         |0.3251299999999996        |profitable          |39.52569169960474   |3.1343873517786567  |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-04-30   |offer_000272|campaign_000020|merchant_000009|55.68999999999999         |0.17091799999999457       |profitable          |2.637611420019134   |0.008095084731312912|efficient                           |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-02-10   |offer_000187|campaign_000071|merchant_000041|232.26999999999998        |25.31743                  |profitable          |31.645569620253163  |3.449367088607595   |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-02-10   |offer_000273|campaign_000071|merchant_000123|-38.97999999999999        |-37.59280799999999        |unprofitable        |-2.598666666666666  |-2.5061871999999994 |negative_incrementality             |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-04-08   |offer_000091|campaign_000005|merchant_000045|74.16                     |26.386128                 |profitable          |56.17977528089888   |19.98876404494382   |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-04-08   |offer_000270|campaign_000074|merchant_000111|167.4                     |104.90958                 |profitable          |31.545741324921135  |19.769716088328074  |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-05-22   |offer_000298|campaign_000010|merchant_000088|196.74                    |118.45154600000002        |profitable          |13.101265443464584  |7.887898476846375   |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
|2026-05-22   |offer_000244|campaign_000033|merchant_000198|228.37                    |109.57322900000001        |profitable          |23.044504238414806  |11.056884617538628  |highly_efficient                    |incrementality_features_run_20260608_151919|incrementality_profitability_rules_v1|2026-06-08 15:20:16.223871|
+-------------+------------+---------------+---------------+--------------------------+--------------------------+--------------------+--------------------+--------------------+------------------------------------+-------------------------------------------+-------------------------------------+--------------------------+
only showing top 20 rows

Writing Gold incrementality features Delta table
================================================================================
Wrote Gold Delta table: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/gold/gold_incrementality_features

Written Gold table validation
================================================================================
table                                         gold_incrementality_features
expected rows                                           87
actual rows                                             87
================================================================================
Gold write validation passed: gold_incrementality_features

Gold incrementality features Delta table written and validated.

Validating profitability outputs
================================================================================

Gold incrementality features business-rule validation
================================================================================
duplicate grain rows                                               0
null required rows                                                 0
invalid rate rows                                                  0
negative cost rows                                                 0
invalid profitability status rows                                  0
invalid efficiency status rows                                     0
margin formula mismatch rows                                       0
COGS formula mismatch rows                                         0
platform fee formula mismatch rows                                 0
net profit formula mismatch rows                                   0
total offer cost formula mismatch rows                             0
ROAS formula mismatch rows                                         0
profitability label mismatch rows                                  0
decision flag mismatch rows                                        0
total validation failures                                          0
================================================================================

Profitability validation summary
================================================================================
table                                        failures       status
--------------------------------------------------------------------------------
gold_incrementality_features                        0       PASSED
================================================================================
All profitability output validations passed.