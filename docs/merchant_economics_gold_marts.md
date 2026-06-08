
# Gold Merchant Economics Marts

## Purpose

The Gold merchant economics job builds the first business-ready reporting marts in the MerchantLift BI lakehouse.

The job reads trusted Silver facts and SCD dimensions, calculates daily merchant and offer metrics, writes Gold Delta tables, and validates business rules before the outputs are used by downstream analytics.

Primary implementation file:

```text
spark_jobs/build_merchant_economics.py
```

Primary output tables:

```text
gold_merchant_daily
gold_offer_daily
```

Output locations:

```text
data/lakehouse/gold/gold_merchant_daily/
data/lakehouse/gold/gold_offer_daily/
```

---

## Why This Layer Exists

Silver tables answer:

```text
What happened row by row?
```

Gold tables answer:

```text
What happened for the business?
```

The Gold layer is designed for executive dashboards, merchant performance reporting, offer performance reporting, reward cost tracking, platform fee analysis, merchant margin proxy analysis, BigQuery loading, dbt marts, and Power BI dashboards.

The Gold tables are intentionally aggregated and dashboard-friendly. They reduce millions of trusted Silver rows into compact daily business metrics.

---

## Input Tables

The Gold merchant economics job reads:

```text
silver.fact_transactions_clean
silver.fact_matched_offer_redemptions_clean
silver.dim_merchant_scd
silver.dim_offer_scd
silver.dim_campaign_scd
```

### fact_transactions_clean

Used for merchant daily spend aggregation.

Required columns:

```text
transaction_id
merchant_id
transaction_date
transaction_amount
```

### fact_matched_offer_redemptions_clean

Used for reward and redemption aggregation.

Required columns:

```text
matched_redemption_id
transaction_id
offer_id
campaign_id
merchant_id
transaction_date
transaction_amount
calculated_reward_amount
```

### dim_merchant_scd

Used for merchant economics fields.

Required columns:

```text
merchant_id
merchant_margin_rate
platform_fee_rate
is_current
```

### dim_offer_scd

Used for offer context.

Required columns:

```text
offer_id
campaign_id
merchant_id
minimum_spend_amount
reward_amount
is_current
```

### dim_campaign_scd

Used for campaign context.

Required columns:

```text
campaign_id
campaign_name
campaign_start_date
campaign_end_date
is_current
```

---

## Output Table: gold_merchant_daily

### Purpose

`gold_merchant_daily` is the merchant-level daily economics mart.

It answers:

```text
How did each merchant perform each day?
```

### Grain

One row represents:

```text
one merchant
one business date
```

Grain columns:

```text
business_date
merchant_id
```

### Core Metrics

```text
transaction_count
gross_spend_amount
average_transaction_amount
matched_redemption_count
redeemed_transaction_count
redemption_rate
reward_cost_amount
average_reward_amount
merchant_margin_rate
platform_fee_rate
platform_fee_amount
estimated_merchant_margin_amount
merchant_net_after_reward
```

### Business Formulas

Gross spend:

```text
gross_spend_amount = sum(transaction_amount)
```

Transaction count:

```text
transaction_count = count distinct transaction_id
```

Reward cost:

```text
reward_cost_amount = sum(calculated_reward_amount)
```

Platform fee:

```text
platform_fee_amount = gross_spend_amount * platform_fee_rate
```

Estimated merchant margin:

```text
estimated_merchant_margin_amount = gross_spend_amount * merchant_margin_rate
```

Merchant net after reward:

```text
merchant_net_after_reward =
    estimated_merchant_margin_amount
    - platform_fee_amount
    - reward_cost_amount
```

Redemption rate:

```text
redemption_rate =
    redeemed_transaction_count / transaction_count
```

If transaction count is zero, redemption rate is set to zero.

### Interpretation

`gold_merchant_daily` is a merchant economics proxy.

It does not yet prove true incrementality.

It answers:

```text
What were daily merchant economics after reward cost and platform fee?
```

It does not yet answer:

```text
Did the offer create incremental profitable spend?
```

True incrementality comes later when test group spend is compared against control group spend.

---

## Output Table: gold_offer_daily

### Purpose

`gold_offer_daily` is the offer-level daily performance mart.

It answers:

```text
How did each individual offer perform each day?
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

### Core Metrics

```text
matched_redemption_count
redeemed_transaction_count
gross_redeemed_spend_amount
reward_cost_amount
average_redeemed_transaction_amount
average_reward_amount
reward_to_redeemed_spend_ratio
```

### Context Columns

```text
campaign_name
campaign_start_date
campaign_end_date
minimum_spend_amount
reward_amount
```

### Business Formulas

Gross redeemed spend:

```text
gross_redeemed_spend_amount = sum(transaction_amount from matched redemptions)
```

Reward cost:

```text
reward_cost_amount = sum(calculated_reward_amount)
```

Average redeemed transaction amount:

```text
average_redeemed_transaction_amount =
    avg(transaction_amount from matched redemptions)
```

Average reward amount:

```text
average_reward_amount =
    avg(calculated_reward_amount)
```

Reward-to-redeemed-spend ratio:

```text
reward_to_redeemed_spend_ratio =
    reward_cost_amount / gross_redeemed_spend_amount
```

If gross redeemed spend is zero, reward-to-spend ratio is set to zero.

### Interpretation

`gold_offer_daily` helps compare offers.

It answers:

```text
Which offers generated redemptions?
How much redeemed spend did each offer produce?
How much reward cost did each offer create?
How expensive was the reward relative to redeemed spend?
Which campaigns are tied to each offer?
```

This table supports later dashboard and dbt marts such as:

```text
mart_offer_performance
mart_reward_liability
mart_offer_abuse_risk
mart_offer_incrementality
```

---

## Difference Between gold_merchant_daily and gold_offer_daily

### gold_merchant_daily

Grain:

```text
merchant_id + business_date
```

Answers:

```text
How did the merchant perform overall today?
```

Example use cases:

```text
merchant profitability
daily merchant reward cost
merchant-level redemption rate
platform fee tracking
merchant net after reward
```

### gold_offer_daily

Grain:

```text
offer_id + campaign_id + merchant_id + business_date
```

Answers:

```text
How did this specific offer perform today?
```

Example use cases:

```text
offer-level redemption tracking
offer reward cost
reward-to-spend ratio
campaign comparison
offer performance dashboards
```

Mental model:

```text
gold_merchant_daily tells the merchant story.
gold_offer_daily tells the offer story.
```

---

## Processing Flow

The job follows this flow:

```text
1. Read Silver transactions.
2. Read matched Silver redemptions.
3. Read current merchant SCD rows.
4. Read current offer SCD rows.
5. Read current campaign SCD rows.
6. Build merchant daily spend aggregation.
7. Build merchant daily reward aggregation.
8. Join spend + rewards + merchant economics.
9. Build offer daily aggregation.
10. Write Gold Delta tables.
11. Read back written Gold tables.
12. Validate row counts.
13. Validate Gold business rules.
```

---

## Merchant Daily Spend Aggregation

Source:

```text
fact_transactions_clean
```

Grouping:

```text
transaction_date as business_date
merchant_id
```

Metrics:

```text
transaction_count
gross_spend_amount
average_transaction_amount
```

This is the foundation of merchant economics.

---

## Merchant Daily Reward Aggregation

Source:

```text
fact_matched_offer_redemptions_clean
```

Grouping:

```text
transaction_date as business_date
merchant_id
```

Metrics:

```text
matched_redemption_count
redeemed_transaction_count
reward_cost_amount
average_reward_amount
```

This captures the daily offer cost for each merchant.

---

## Merchant Daily Economics Join

The job joins:

```text
merchant_daily_spend
LEFT JOIN merchant_daily_rewards
LEFT JOIN current dim_merchant_scd
```

Why left join from spend to rewards?

A merchant can have spend on a day without redemptions.

That should produce:

```text
reward_cost_amount = 0
matched_redemption_count = 0
```

not a missing merchant/day row.

---

## Offer Daily Aggregation

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

Then the job enriches the result with:

```text
current dim_offer_scd
current dim_campaign_scd
```

This creates offer-level daily performance metrics.

---

## Current SCD Usage

For the initial Gold mart, the job uses current SCD rows:

```text
is_current = true
```

This is acceptable for the first Gold implementation.

Future enhancement:

```text
point-in-time SCD joins
```

A future point-in-time join would match fact rows to the dimension version where:

```text
fact_date >= effective_start_date
AND (
    fact_date <= effective_end_date
    OR effective_end_date IS NULL
)
```

This would make historical analytics fully time-aware.

---

## Output Validation

The job validates both Gold outputs.

### gold_merchant_daily validation

Checks:

```text
unique grain: business_date + merchant_id
required columns are not null
financial metrics are non-negative
rates are between 0 and 1
redeemed_transaction_count <= transaction_count
financial formulas match expected calculations
```

Formula checks:

```text
platform_fee_amount = gross_spend_amount * platform_fee_rate
estimated_merchant_margin_amount = gross_spend_amount * merchant_margin_rate
merchant_net_after_reward =
    estimated_merchant_margin_amount
    - platform_fee_amount
    - reward_cost_amount
```

### gold_offer_daily validation

Checks:

```text
unique grain: business_date + offer_id + campaign_id + merchant_id
required columns are not null
financial metrics are non-negative
reward_to_redeemed_spend_ratio is between 0 and 1
redeemed_transaction_count <= matched_redemption_count
reward-to-spend formula is correct
```

Formula check:

```text
reward_to_redeemed_spend_ratio =
    reward_cost_amount / gross_redeemed_spend_amount
```

---

## Important Note on Incrementality

These Gold tables are not final causal incrementality tables.

They are daily business economics rollups.

They provide:

```text
gross spend
reward cost
redemption activity
platform fee
merchant margin proxy
offer performance
```

They do not yet compare:

```text
test group spend
vs
control group spend
```

True incrementality will be implemented later using test/control logic.

Therefore:

```text
merchant_net_after_reward is an economics proxy
not a causal incremental profit measurement
```

## Databricks Execution Notes

The Gold merchant economics job should run both locally and in Databricks.

Local execution proves the Spark logic works.

Databricks execution proves the same logic works in the lakehouse runtime against Delta tables.

### Required Input Tables

The job expects these Silver Delta tables to already exist:

```text
silver/fact_transactions_clean
silver/fact_matched_offer_redemptions_clean
silver/dim_merchant_scd
silver/dim_offer_scd
silver/dim_campaign_scd
```

The job writes these Gold Delta tables:

```text
gold/gold_merchant_daily
gold/gold_offer_daily
```

### Databricks Path Strategy

Local paths look like:

```text
data/lakehouse/silver
data/lakehouse/gold
```

Databricks paths should use DBFS driver-local paths for this project:

```text
/dbfs/FileStore/merchantlift/data/lakehouse/silver
/dbfs/FileStore/merchantlift/data/lakehouse/gold
```

Important distinction:

```text
dbfs:/FileStore/...     Spark path
/dbfs/FileStore/...     driver-local filesystem path
```

Because the project code uses Python Path, environment variables should use `/dbfs/...`.

### Required Environment Variables

Before running the Gold job in Databricks, set:

```bash
export MERCHANTLIFT_LAKEHOUSE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse
export MERCHANTLIFT_BRONZE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/bronze
export MERCHANTLIFT_SILVER_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/silver
export MERCHANTLIFT_GOLD_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/gold
```

If raw ingestion is also needed:

```bash
export MERCHANTLIFT_RAW_DATA_DIR=/dbfs/FileStore/merchantlift/data/raw
```

### Recommended Databricks Run Order

Run the upstream pipeline first:

```bash
PYTHONPATH=src python spark_jobs/bronze_ingestion.py
PYTHONPATH=src python spark_jobs/silver_transformations.py
PYTHONPATH=src python spark_jobs/build_redemption_matching.py
PYTHONPATH=src python spark_jobs/build_scd_dimensions.py
```

Then run the Gold merchant economics job:

```bash
PYTHONPATH=src python spark_jobs/build_merchant_economics.py
```

If Bronze, Silver, matched redemptions, and SCD outputs already exist, run only:

```bash
PYTHONPATH=src python spark_jobs/build_merchant_economics.py
```

### Databricks Notebook Shell Execution

From a Databricks notebook shell cell:

```bash
%sh
cd /Workspace/Repos/<your-folder>/merchantlift-bi

export MERCHANTLIFT_LAKEHOUSE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse
export MERCHANTLIFT_BRONZE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/bronze
export MERCHANTLIFT_SILVER_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/silver
export MERCHANTLIFT_GOLD_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/gold

PYTHONPATH=src python spark_jobs/build_merchant_economics.py


Successful output:

Starting Gold merchant economics build
================================================================================
Silver directory: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/silver
Gold directory: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/gold
Pipeline run ID: merchant_economics_run_20260608_141454
Gold rule version: merchant_economics_rules_v1
Output table: gold_merchant_daily
Output table: gold_offer_daily
================================================================================

================================================================================
Inspecting input table: fact_transactions_clean
================================================================================

Schema:
root
 |-- transaction_id: string (nullable = true)
 |-- tokenized_cardmember_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- category_id: string (nullable = true)
 |-- location_id: string (nullable = true)
 |-- transaction_timestamp: timestamp (nullable = true)
 |-- transaction_date: date (nullable = true)
 |-- transaction_amount: double (nullable = true)
 |-- transaction_status: string (nullable = true)
 |-- shopper_behavior_type: string (nullable = true)
 |-- segment_id: string (nullable = true)
 |-- is_test_group: boolean (nullable = true)
 |-- is_control_group: boolean (nullable = true)
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
fact_transactions_clean: 12,266 rows

Sample rows:
+---------------+-----------------------+---------------+---------------+-------------+---------------------+----------------+------------------+------------------+---------------------+-----------+-------------+----------------+--------------------------+--------------------------+-----------------+-----------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|transaction_id |tokenized_cardmember_id|merchant_id    |category_id    |location_id  |transaction_timestamp|transaction_date|transaction_amount|transaction_status|shopper_behavior_type|segment_id |is_test_group|is_control_group|created_at                |ingestion_timestamp       |source_table_name|source_file_path                                                                   |pipeline_run_id           |record_hash                                                     |silver_transformed_at     |silver_pipeline_run_id    |quality_status|validation_rule_version|
+---------------+-----------------------+---------------+---------------+-------------+---------------------+----------------+------------------+------------------+---------------------+-----------+-------------+----------------+--------------------------+--------------------------+-----------------+-----------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|tx_000003330   |cm_tok_000427          |merchant_000151|category_000005|location_0024|2026-02-22 12:04:32  |2026-02-22      |1792.26           |settled           |lapsed_reactivated   |segment_002|true         |true            |2026-06-03 00:58:16.68745 |2026-06-07 19:47:26.186106|fact_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_transactions|bronze_run_20260607_194640|813bf42c4c365d2c8806b3ea06b2d61ee5dd23d4ce668ff9bb9b5d2127d04e15|2026-06-07 23:53:34.616386|silver_run_20260607_235211|passed        |silver_rules_v1        |
|tx_000005715   |cm_tok_002165          |merchant_000161|category_000004|location_0024|2026-02-22 10:47:42  |2026-02-22      |624.34            |settled           |loyal_existing       |segment_006|true         |false           |2026-06-03 00:58:16.696989|2026-06-07 19:47:26.186106|fact_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_transactions|bronze_run_20260607_194640|42861b69b70bf91b8ef9bbf32a5ebc280257ce88b8dc0abaf0288448ba443045|2026-06-07 23:53:34.616386|silver_run_20260607_235211|passed        |silver_rules_v1        |
|tx_000007437   |cm_tok_004472          |merchant_000176|category_000006|location_0010|2026-02-22 14:27:51  |2026-02-22      |120.06            |settled           |subsidized_shopper   |segment_006|false        |true            |2026-06-03 00:58:16.703855|2026-06-07 19:47:26.186106|fact_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_transactions|bronze_run_20260607_194640|5f67f5c7509d28ac2af6ab6fca562cfd6ceee2611df5d444d76cb7b2bfd18941|2026-06-07 23:53:34.616386|silver_run_20260607_235211|passed        |silver_rules_v1        |
|tx_000009347   |cm_tok_003038          |merchant_000020|category_000005|location_0025|2026-02-22 08:35:34  |2026-02-22      |3715.07           |authorized        |incremental_shopper  |segment_003|false        |false           |2026-06-03 00:58:16.711409|2026-06-07 19:47:26.186106|fact_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_transactions|bronze_run_20260607_194640|f921c56b557eda26a3a3818be80c16d0ba54075921912b79b62cfd12ddb58750|2026-06-07 23:53:34.616386|silver_run_20260607_235211|passed        |silver_rules_v1        |
|tx_offer_000006|cm_tok_004951          |merchant_000083|NULL           |NULL         |2026-02-22 14:53:21  |2026-02-22      |299.8             |settled           |NULL                 |NULL       |true         |false           |2026-06-03 00:58:17.184042|2026-06-07 19:47:26.186106|fact_transactions|/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/fact_transactions|bronze_run_20260607_194640|43e1a5b80b093ab443e4a184724f126a7309cf3bcfe6ffae29f40997d4b01dce|2026-06-07 23:53:34.616386|silver_run_20260607_235211|passed        |silver_rules_v1        |
+---------------+-----------------------+---------------+---------------+-------------+---------------------+----------------+------------------+------------------+---------------------+-----------+-------------+----------------+--------------------------+--------------------------+-----------------+-----------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
only showing top 5 rows

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
Inspecting input table: dim_campaign_scd
================================================================================

Schema:
root
 |-- surrogate_scd_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- campaign_name: string (nullable = true)
 |-- campaign_start_date: date (nullable = true)
 |-- campaign_end_date: date (nullable = true)
 |-- effective_start_date: date (nullable = true)
 |-- effective_end_date: date (nullable = true)
 |-- is_current: boolean (nullable = true)
 |-- scd_record_hash: string (nullable = true)
 |-- scd_created_at: timestamp (nullable = true)
 |-- scd_updated_at: timestamp (nullable = true)
 |-- scd_pipeline_run_id: string (nullable = true)
 |-- scd_rule_version: string (nullable = true)


Row count:
dim_campaign_scd: 80 rows

Sample rows:
+--------------------------------------------------------------------+---------------+------------------------------+-------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|surrogate_scd_id                                                    |campaign_id    |campaign_name                 |campaign_start_date|campaign_end_date|effective_start_date|effective_end_date|is_current|scd_record_hash                                                 |scd_created_at            |scd_updated_at            |scd_pipeline_run_id    |scd_rule_version|
+--------------------------------------------------------------------+---------------+------------------------------+-------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|scd_eb17a82d3b2f8d85bf54302ff67ab094e9cdc93e1d8835bf9edeec993ad7b02e|campaign_000022|Reactivation Campaign 22      |2026-04-12         |2026-05-12       |2026-01-01          |NULL              |true      |182dd6562e6ee6bc1ab1cb5fd4f9735f144e1310e04d1054541be3fe4111b85d|2026-06-08 05:42:23.481822|2026-06-08 05:42:23.481822|scd_run_20260608_054159|scd_rules_v1    |
|scd_1524d0c5f6a612d27fd9721b490a664ccfdd41101046f7b7e9ff4cd4897f8397|campaign_000052|Basket Size Growth Campaign 52|2026-06-05         |2026-07-20       |2026-01-01          |NULL              |true      |f3e69b5869414009b76c1070a9260385e0dd0c1bffc82ae894b1bfcf8883b378|2026-06-08 05:42:23.481822|2026-06-08 05:42:23.481822|scd_run_20260608_054159|scd_rules_v1    |
|scd_6c454baf6480c029cceb49d6a6b2fbf25f8aba3c7e1a6ad74895e331dd788db7|campaign_000003|Acquisition Campaign 3        |2026-03-17         |2026-04-16       |2026-01-01          |NULL              |true      |89308b33982190399410015767381e024deedf2b0c9f56948aaf3c54b4193294|2026-06-08 05:42:23.481822|2026-06-08 05:42:23.481822|scd_run_20260608_054159|scd_rules_v1    |
|scd_02df6964a808e09756dc2e0cd4d25e80d8a44f258

... [*** WARNING: max output size exceeded, skipping output. ***] ...

eemed_transaction_count|redemption_rate|reward_cost_amount|average_reward_amount|merchant_margin_rate|platform_fee_rate|platform_fee_amount|estimated_merchant_margin_amount|merchant_net_after_reward|gold_pipeline_run_id                  |gold_rule_version          |gold_created_at           |
+-------------+---------------+-----------------+------------------+--------------------------+------------------------+--------------------------+---------------+------------------+---------------------+--------------------+-----------------+-------------------+--------------------------------+-------------------------+--------------------------------------+---------------------------+--------------------------+
|2026-06-21   |merchant_000140|1                |176.31            |176.31                    |0                       |0                         |0.0            |0.0               |0.0                  |0.459               |0.0249           |4.390118999999999  |80.92629000000001               |76.53617100000001        |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-05-15   |merchant_000135|1                |53.39             |53.39                     |0                       |0                         |0.0            |0.0               |0.0                  |0.3294              |0.0161           |0.859579           |17.586666                       |16.727087                |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-02-20   |merchant_000085|2                |5457.88           |2728.94                   |0                       |0                         |0.0            |0.0               |0.0                  |0.5304              |0.0296           |161.553248         |2894.859552                     |2733.3063039999997       |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-02-11   |merchant_000029|1                |131.63            |131.63                    |0                       |0                         |0.0            |0.0               |0.0                  |0.6938              |0.013            |1.7111899999999998 |91.32489399999999               |89.61370399999998        |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-07-07   |merchant_000189|1                |104.21            |104.21                    |1                       |1                         |1.0            |50.0              |50.0                 |0.4623              |0.012            |1.2505199999999999 |48.176283                       |-3.0742370000000037      |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-06-13   |merchant_000048|1                |160.19            |160.19                    |1                       |1                         |1.0            |0.0               |0.0                  |0.6904              |0.0242           |3.876598           |110.595176                      |106.718578               |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-06-24   |merchant_000041|1                |102.89            |102.89                    |0                       |0                         |0.0            |0.0               |0.0                  |0.1406              |0.0316           |3.2513240000000003 |14.466334                       |11.21501                 |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-02-01   |merchant_000005|1                |221.68            |221.68                    |0                       |0                         |0.0            |0.0               |0.0                  |0.437               |0.0167           |3.7020560000000002 |96.87416                        |93.172104                |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-02-23   |merchant_000089|1                |38.39             |38.39                     |0                       |0                         |0.0            |0.0               |0.0                  |0.3235              |0.0302           |1.159378           |12.419165000000001              |11.259787000000001       |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-01-31   |merchant_000047|1                |56.94             |56.94                     |0                       |0                         |0.0            |0.0               |0.0                  |0.308               |0.0249           |1.417806           |17.53752                        |16.119714000000002       |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-05-22   |merchant_000052|2                |1493.79           |746.895                   |1                       |1                         |0.5            |5.0               |5.0                  |0.3429              |0.0102           |15.236658          |512.220591                      |491.98393300000004       |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-06-15   |merchant_000166|1                |67.25             |67.25                     |0                       |0                         |0.0            |0.0               |0.0                  |0.4105              |0.0203           |1.3651749999999998 |27.606125                       |26.240949999999998       |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-07-09   |merchant_000053|2                |5076.16           |2538.08                   |0                       |0                         |0.0            |0.0               |0.0                  |0.5996              |0.0153           |77.66524799999999  |3043.665536                     |2966.000288              |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-04-14   |merchant_000142|1                |60.03             |60.03                     |0                       |0                         |0.0            |0.0               |0.0                  |0.6627              |0.0114           |0.684342           |39.781881                       |39.097539                |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-06-24   |merchant_000064|1                |3398.18           |3398.18                   |0                       |0                         |0.0            |0.0               |0.0                  |0.5835              |0.0137           |46.555066          |1982.83803                      |1936.282964              |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-07-11   |merchant_000056|1                |71.65             |71.65                     |1                       |1                         |1.0            |0.0               |0.0                  |0.5529              |0.0239           |1.7124350000000002 |39.615285                       |37.90285                 |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-07-07   |merchant_000173|1                |164.96            |164.96                    |0                       |0                         |0.0            |0.0               |0.0                  |0.7299              |0.0178           |2.9362880000000002 |120.40430400000001              |117.468016               |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-06-13   |merchant_000096|1                |359.78            |359.78                    |0                       |0                         |0.0            |0.0               |0.0                  |0.3031              |0.0179           |6.440061999999999  |109.04931799999999              |102.60925599999999       |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-06-14   |merchant_000026|1                |49.3              |49.3                      |0                       |0                         |0.0            |0.0               |0.0                  |0.166               |0.0141           |0.6951299999999999 |8.1838                          |7.48867                  |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
|2026-06-22   |merchant_000149|1                |2406.25           |2406.25                   |0                       |0                         |0.0            |0.0               |0.0                  |0.5776              |0.0327           |78.684375          |1389.85                         |1311.1656249999999       |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:09.422587|
+-------------+---------------+-----------------+------------------+--------------------------+------------------------+--------------------------+---------------+------------------+---------------------+--------------------+-----------------+-------------------+--------------------------------+-------------------------+--------------------------------------+---------------------------+--------------------------+
only showing top 20 rows

Gold merchant daily schema
root
 |-- business_date: date (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- transaction_count: long (nullable = false)
 |-- gross_spend_amount: double (nullable = true)
 |-- average_transaction_amount: double (nullable = true)
 |-- matched_redemption_count: long (nullable = false)
 |-- redeemed_transaction_count: long (nullable = false)
 |-- redemption_rate: double (nullable = true)
 |-- reward_cost_amount: double (nullable = false)
 |-- average_reward_amount: double (nullable = false)
 |-- merchant_margin_rate: double (nullable = false)
 |-- platform_fee_rate: double (nullable = false)
 |-- platform_fee_amount: double (nullable = true)
 |-- estimated_merchant_margin_amount: double (nullable = true)
 |-- merchant_net_after_reward: double (nullable = true)
 |-- gold_pipeline_run_id: string (nullable = false)
 |-- gold_rule_version: string (nullable = false)
 |-- gold_created_at: timestamp (nullable = false)


Gold offer daily economics
================================================================================
gold offer daily rows                                  737
================================================================================

Gold offer daily sample
+-------------+------------+---------------+---------------+------------------------------+-------------------+-----------------+--------------------+-------------+------------------------+--------------------------+---------------------------+------------------+-----------------------------------+---------------------+------------------------------+--------------------------------------+---------------------------+--------------------------+
|business_date|offer_id    |campaign_id    |merchant_id    |campaign_name                 |campaign_start_date|campaign_end_date|minimum_spend_amount|reward_amount|matched_redemption_count|redeemed_transaction_count|gross_redeemed_spend_amount|reward_cost_amount|average_redeemed_transaction_amount|average_reward_amount|reward_to_redeemed_spend_ratio|gold_pipeline_run_id                  |gold_rule_version          |gold_created_at           |
+-------------+------------+---------------+---------------+------------------------------+-------------------+-----------------+--------------------+-------------+------------------------+--------------------------+---------------------------+------------------+-----------------------------------+---------------------+------------------------------+--------------------------------------+---------------------------+--------------------------+
|2026-06-13   |offer_000140|campaign_000046|merchant_000044|Acquisition Campaign 46       |2026-06-05         |2026-07-05       |250.0               |20.0         |1                       |1                         |268.32                     |20.0              |268.32                             |20.0                 |0.07453786523553965           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-05-18   |offer_000024|campaign_000043|merchant_000035|Reactivation Campaign 43      |2026-05-10         |2026-05-31       |100.0               |50.0         |1                       |1                         |152.21                     |50.0              |152.21                             |50.0                 |0.32849352867748505           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-04-29   |offer_000225|campaign_000002|merchant_000157|Acquisition Campaign 2        |2026-04-09         |2026-05-24       |150.0               |15.0         |1                       |1                         |205.12                     |15.0              |205.12                             |15.0                 |0.07312792511700468           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-03-27   |offer_000271|campaign_000023|merchant_000091|Acquisition Campaign 23       |2026-02-24         |2026-04-25       |75.0                |15.0         |1                       |1                         |85.77                      |15.0              |85.77                              |15.0                 |0.17488632388947184           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-06-28   |offer_000135|campaign_000041|merchant_000053|Acquisition Campaign 41       |2026-05-24         |2026-06-23       |150.0               |0.0          |1                       |1                         |176.09                     |0.0               |176.09                             |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-05-12   |offer_000219|campaign_000022|merchant_000014|Reactivation Campaign 22      |2026-04-12         |2026-05-12       |150.0               |0.0          |1                       |1                         |224.86                     |0.0               |224.86                             |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-05-13   |offer_000018|campaign_000080|merchant_000118|Category Share Campaign 80    |2026-04-11         |2026-05-02       |50.0                |20.0         |1                       |1                         |74.87                      |20.0              |74.87                              |20.0                 |0.2671296914652063            |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-06-25   |offer_000220|campaign_000013|merchant_000001|Category Share Campaign 13    |2026-06-22         |2026-07-22       |150.0               |5.0          |1                       |1                         |227.43                     |5.0               |227.43                             |5.0                  |0.021984786527722816          |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-02-19   |offer_000289|campaign_000038|merchant_000033|Basket Size Growth Campaign 38|2026-01-25         |2026-02-24       |50.0                |50.0         |1                       |1                         |74.44                      |50.0              |74.44                              |50.0                 |0.6716818914562064            |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-05-22   |offer_000260|campaign_000010|merchant_000052|Repeat Purchase Campaign 10   |2026-05-04         |2026-06-03       |250.0               |5.0          |1                       |1                         |417.71                     |5.0               |417.71                             |5.0                  |0.01197002705226114           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-03-27   |offer_000265|campaign_000023|merchant_000008|Acquisition Campaign 23       |2026-02-24         |2026-04-25       |250.0               |0.0          |1                       |1                         |347.25                     |0.0               |347.25                             |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-05-13   |offer_000157|campaign_000010|merchant_000088|Repeat Purchase Campaign 10   |2026-05-04         |2026-06-03       |150.0               |0.0          |1                       |1                         |224.79                     |0.0               |224.79                             |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-06-18   |offer_000027|campaign_000041|merchant_000080|Acquisition Campaign 41       |2026-05-24         |2026-06-23       |250.0               |0.0          |1                       |1                         |399.27                     |0.0               |399.27                             |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-06-13   |offer_000128|campaign_000043|merchant_000166|Reactivation Campaign 43      |2026-05-10         |2026-05-31       |150.0               |15.0         |1                       |1                         |209.54                     |15.0              |209.54                             |15.0                 |0.07158537749355733           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-07-07   |offer_000077|campaign_000015|merchant_000049|Acquisition Campaign 15       |2026-06-06         |2026-06-27       |150.0               |15.0         |1                       |1                         |249.84                     |15.0              |249.84                             |15.0                 |0.060038424591738714          |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-05-12   |offer_000213|campaign_000012|merchant_000188|Repeat Purchase Campaign 12   |2026-04-12         |2026-04-26       |150.0               |0.0          |1                       |1                         |226.25                     |0.0               |226.25                             |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-02-16   |offer_000187|campaign_000071|merchant_000041|Category Share Campaign 71    |2026-01-20         |2026-02-03       |250.0               |0.0          |1                       |1                         |419.01                     |0.0               |419.01                             |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-04-08   |offer_000190|campaign_000009|merchant_000148|Category Share Campaign 9     |2026-04-04         |2026-06-03       |250.0               |10.0         |1                       |1                         |388.01                     |10.0              |388.01                             |10.0                 |0.025772531635782584          |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-04-24   |offer_000300|campaign_000022|merchant_000156|Reactivation Campaign 22      |2026-04-12         |2026-05-12       |100.0               |0.0          |1                       |1                         |158.4                      |0.0               |158.4                              |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
|2026-06-16   |offer_000113|campaign_000049|merchant_000115|Basket Size Growth Campaign 49|2026-05-16         |2026-06-06       |250.0               |0.0          |1                       |1                         |298.68                     |0.0               |298.68                             |0.0                  |0.0                           |merchant_economics_run_20260608_141454|merchant_economics_rules_v1|2026-06-08 14:15:12.290839|
+-------------+------------+---------------+---------------+------------------------------+-------------------+-----------------+--------------------+-------------+------------------------+--------------------------+---------------------------+------------------+-----------------------------------+---------------------+------------------------------+--------------------------------------+---------------------------+--------------------------+
only showing top 20 rows

Gold offer daily schema
root
 |-- business_date: date (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- campaign_name: string (nullable = true)
 |-- campaign_start_date: date (nullable = true)
 |-- campaign_end_date: date (nullable = true)
 |-- minimum_spend_amount: double (nullable = true)
 |-- reward_amount: double (nullable = true)
 |-- matched_redemption_count: long (nullable = false)
 |-- redeemed_transaction_count: long (nullable = false)
 |-- gross_redeemed_spend_amount: double (nullable = true)
 |-- reward_cost_amount: double (nullable = true)
 |-- average_redeemed_transaction_amount: double (nullable = true)
 |-- average_reward_amount: double (nullable = true)
 |-- reward_to_redeemed_spend_ratio: double (nullable = true)
 |-- gold_pipeline_run_id: string (nullable = false)
 |-- gold_rule_version: string (nullable = false)
 |-- gold_created_at: timestamp (nullable = false)


Writing Gold Delta tables
================================================================================
Wrote Gold Delta table: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/gold/gold_merchant_daily

Written Gold table validation
================================================================================
table                                    gold_merchant_daily
expected rows                                  10,975
actual rows                                    10,975
================================================================================
Gold write validation passed: gold_merchant_daily
Wrote Gold Delta table: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/gold/gold_offer_daily

Written Gold table validation
================================================================================
table                                    gold_offer_daily
expected rows                                     737
actual rows                                       737
================================================================================
Gold write validation passed: gold_offer_daily

All Gold Delta tables written and validated.

Validating Gold outputs
================================================================================

Gold merchant daily business-rule validation
================================================================================
duplicate grain rows                                          0
null required rows                                            0
negative metric rows                                          0
invalid rate rows                                             0
redeemed count > transaction count rows                       0
formula mismatch rows                                         0
total validation failures                                     0
================================================================================

Gold offer daily business-rule validation
================================================================================
duplicate grain rows                                          0
null required rows                                            0
negative metric rows                                          0
invalid reward-to-spend ratio rows                            0
redeemed count > matched count rows                           0
formula mismatch rows                                         0
total validation failures                                     0
================================================================================

Gold validation summary
================================================================================
table                                   failures       status
--------------------------------------------------------------------------------
gold_merchant_daily                            0       PASSED
gold_offer_daily                               0       PASSED
================================================================================
All Gold output validations passed.



## What This Enables Next

These Gold outputs become the foundation for later work:

```text
incrementality features
merchant economics marts in dbt
offer performance marts in dbt
reward liability marts
fraud and abuse marts
BigQuery loading
Power BI dashboard model
executive reporting
```