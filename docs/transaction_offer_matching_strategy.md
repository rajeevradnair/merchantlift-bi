
# Transaction-to-Offer Matching Strategy

## Purpose

This document explains the Spark join strategy for matching card-linked transactions to activated merchant offers.

The goal is to avoid naive joins that create excessive shuffle, data skew, and explosive intermediate row counts.

This strategy prepares the implementation of:

```text
spark_jobs/build_redemption_matching.py
```

The matching job will eventually produce trusted redemption matches used by reward liability, merchant settlement, offer performance, incrementality, fraud/abuse, reconciliation, dbt marts, BigQuery reporting, and Power BI dashboards.

---

## Core Business Question

When a cardmember makes a transaction, did that transaction qualify for an activated offer?

A transaction can match an offer only when:

```text
same cardmember
same merchant
transaction occurs after activation
transaction occurs before offer expiry
transaction amount satisfies the offer rule
transaction status is eligible
```

This means transaction-to-offer matching is not a simple ID join.

It is a business-rule join.

---

## Why Naive Joins Are Dangerous

A naive transaction-to-offer join usually starts with a simple idea:

```text
Join transactions to activations by tokenized_cardmember_id.
```

At first, this sounds reasonable because both tables contain the cardmember identifier.

However, this is dangerous at scale.

A cardmember can have many transactions and many activated offers. If Spark joins only on cardmember ID, it may create a large intermediate result before later filters remove invalid matches.

Example:

```text
cardmember_001 has 100 transactions
cardmember_001 has 20 activated offers
```

A naive join can create:

```text
100 x 20 = 2,000 candidate rows
```

for just one cardmember.

Across millions of cardmembers, this can become a massive intermediate dataset.

The main failure modes are:

```text
row explosion
large shuffle
data skew
incorrect candidate matches
slow physical plans
high memory pressure
disk spill
unstable job runtime
```

---

## Failure Mode 1: Row Explosion

Row explosion happens when a join creates far more rows than either input table.

Bad join:

```text
transactions
JOIN activations
ON transactions.tokenized_cardmember_id = activations.tokenized_cardmember_id
```

Problem:

```text
same customer
many transactions
many activations
many possible combinations
```

This creates too many candidate rows.

Most of those rows will later be filtered out because they do not match the merchant, date window, offer rules, or transaction amount.

The better strategy is to narrow the join earlier:

```text
join on cardmember
join on merchant
filter by transaction timestamp window
filter by amount/status eligibility
```

Short memory hook:

```text
A broad join creates garbage candidates.
A narrow join creates useful candidates.
```

---

## Safe Transaction-to-Offer Matching Strategy

The safe matching strategy is designed to answer one business question:

```text
Which transactions truly qualify for activated offers?
```

The key principle is:

```text
Reduce the candidate data before joining.
```

A safe Spark matching job should progressively narrow the data.

Safe flow:

```text
1. Read trusted Silver transactions.
2. Read trusted Silver activations.
3. Read trusted Silver offers.
4. Filter transactions early.
5. Filter activations early.
6. Select only matching columns.
7. Join activations to offers to get offer rules.
8. Join transactions to activation-offer candidates.
9. Apply timestamp window predicates.
10. Apply amount and status predicates.
11. Deduplicate if multiple offers match the same transaction.
12. Validate row counts and orphan relationships.
13. Write matched output.
```

Short version:

```text
Filter early.
Join narrowly.
Validate aggressively.
```

---

## Strong Join Predicates

The core join should use both equality and window predicates.

Safe join condition:

```text
tx.tokenized_cardmember_id = act.tokenized_cardmember_id
AND tx.merchant_id = act.merchant_id
AND tx.transaction_timestamp >= act.activation_timestamp
AND tx.transaction_timestamp <= act.offer_expiry_timestamp
AND tx.transaction_amount >= act.minimum_spend_amount
```

This is stronger than joining by cardmember only.

Why it is safer:

```text
cardmember match controls user identity
merchant match controls merchant eligibility
timestamp window controls activation eligibility
amount rule controls offer qualification
```

Short memory hook:

```text
A valid redemption is a transaction inside an activated offer window.
```

---

## Spark Join Profiling Script

Before building the production matching job, the project includes a profiling script:

```text
scripts/profile_transaction_offer_join.py
```

This script reads the Silver tables:

```text
silver.fact_transactions_clean
silver.fact_offer_activations_clean
silver.dim_offer_clean
```

It reports:

```text
transaction row count
activation row count
offer row count
top merchants by transaction volume
top cardmembers by transaction volume
activation-offer candidate count
safe matched candidate count
join expansion ratios
sample matched candidates
Spark physical plan
```

The profiling script is not the final matching job.

It is a diagnostic tool used to measure join risk before building the production matching transformation.

Short memory hook:

```text
Profile before matching.
```

---

## Databricks Execution Notes

The transaction-to-offer join profiling script should run both locally and in Databricks.

Local execution is useful for development.

Databricks execution is useful for validating that the same Spark logic works in a cloud/lakehouse environment.

### Databricks Path Strategy

Local paths look like:

```text
data/lakehouse/silver/fact_transactions_clean
```

For the first cloud execution pass, this project uses DBFS-style paths:

```text
/dbfs/FileStore/merchantlift/data/lakehouse/silver
```

The important distinction is:

```text
dbfs:/FileStore/...     Spark path
/dbfs/FileStore/...     driver-local filesystem path
```

Because the project code uses Python Path, environment variables should use the driver-local /dbfs form.

### Required Environment Variables

Before running the profiling script in Databricks, set:

```bash
export MERCHANTLIFT_LAKEHOUSE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse
export MERCHANTLIFT_BRONZE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/bronze
export MERCHANTLIFT_SILVER_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/silver
export MERCHANTLIFT_GOLD_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/gold
```

If raw ingestion is also needed, set:

```bash
export MERCHANTLIFT_RAW_DATA_DIR=/dbfs/FileStore/merchantlift/data/raw
```

### Recommended Databricks Run Order

Run the pipeline in this order:

```text
1. Confirm raw data exists in Databricks storage.
2. Run Bronze ingestion if Bronze tables are missing.
3. Run Silver transformations if Silver tables are missing.
4. Run join profiling over Silver tables.
5. Review row counts, skew patterns, candidate counts, and physical plan.
```

The safe order is:

```bash
PYTHONPATH=src python spark_jobs/bronze_ingestion.py
PYTHONPATH=src python spark_jobs/silver_transformations.py
PYTHONPATH=src python scripts/profile_transaction_offer_join.py
```

### Databricks Notebook Shell Execution

From a Databricks notebook, run:

```bash
%sh
cd /Workspace/Repos/<your-folder>/merchantlift-bi

export MERCHANTLIFT_RAW_DATA_DIR=/dbfs/FileStore/merchantlift/data/raw
export MERCHANTLIFT_LAKEHOUSE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse
export MERCHANTLIFT_BRONZE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/bronze
export MERCHANTLIFT_SILVER_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/silver
export MERCHANTLIFT_GOLD_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/gold

PYTHONPATH=src python scripts/profile_transaction_offer_join.py
```

Replace:

```text
/Workspace/Repos/<your-folder>/merchantlift-bi
```

with the actual Databricks repo path.

### Databricks Notebook Interactive Inspection

After running the profiling script, inspect a Silver table directly:

```python
silver_path = "dbfs:/FileStore/merchantlift/data/lakehouse/silver/fact_transactions_clean"

transactions_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

display(transactions_df.limit(10))
```

Count rows:

```python
transactions_df.count()
```

Inspect key skew:

```python
from pyspark.sql import functions as F

(
    transactions_df
    .groupBy("merchant_id")
    .count()
    .orderBy(F.desc("count"))
    .show(20, truncate=False)
)
```

### Physical Plan Inspection

Use:

```python
matched_candidates_df.explain(mode="formatted")
```

Look for:

```text
BroadcastHashJoin
SortMergeJoin
Exchange
AdaptiveSparkPlan
PartitionFilters
PushedFilters
```

Important interpretation:

```text
BroadcastHashJoin = Spark broadcasted a smaller table.
SortMergeJoin = Spark shuffled and sorted both sides.
Exchange = Spark moved data across workers.
PartitionFilters = Spark pruned partitions.
AdaptiveSparkPlan = Adaptive Query Execution is active.
```

---

## Safe Join Strategy Summary

Bad strategy:

```text
join all transactions to all activations by cardmember only
then filter later
```

Safe strategy:

```text
filter transactions first
filter activations first
attach offer rules
select narrow columns
join on cardmember and merchant
apply timestamp window
apply amount/status rules
validate row counts
inspect physical plan
deduplicate matches
write trusted output
```