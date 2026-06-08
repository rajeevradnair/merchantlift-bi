# Redemption and Control-Group Event Generation

## Purpose

 generates two critical MerchantLift BI fact tables:

```text
fact_offer_redemptions
fact_control_group_transactions
```

These tables are the foundation for incrementality analysis.

`fact_offer_redemptions` captures qualified offer-side spend.

`fact_control_group_transactions` captures baseline no-offer spend from similar cardmembers.

Together, they help answer the central business question:

```text
Did the merchant offer create incremental profitable spend,
or did it simply subsidize customers who would have purchased anyway?
```

---

## Business Context

A merchant-funded offer does not automatically create incremental value.

A cardmember may:

```text
see an offer
activate the offer
make a transaction
qualify for a redemption
receive a reward
```

However, the merchant only benefits if the offer changes customer behavior.

That is why MerchantLift BI compares:

```text
test-side qualified spend
vs.
control-side baseline spend
```

---

## Tables Created

### fact_offer_redemptions

Grain:

```text
One row per qualifying transaction-offer redemption.
```

A transaction qualifies for redemption when:

```text
same tokenized_cardmember_id
same merchant_id
transaction timestamp is after activation
transaction timestamp is before offer expiry
transaction amount meets minimum spend
transaction status is settled
```

This table links together:

```text
assignment -> impression -> activation -> transaction -> redemption
```

Important lineage fields:

```text
transaction_id
activation_id
assignment_id
impression_id
tokenized_cardmember_id
offer_id
campaign_id
merchant_id
```

Every `fact_offer_redemptions.transaction_id` must exist in `fact_transactions.transaction_id`.

This prevents orphan redemptions.

---

### fact_control_group_transactions

Grain:

```text
One row per control-group baseline transaction.
```

Control transactions represent spend from cardmembers who were assigned to control and did not receive the offer exposure.

This table supports baseline comparison for incrementality.

Important fields:

```text
control_transaction_id
control_assignment_id
tokenized_cardmember_id
merchant_id
campaign_id
offer_id
segment_id
transaction_timestamp
transaction_amount
match_group_id
match_quality_score
shopper_behavior_type
```

---

## Why This Matters

Redemptions alone do not prove that an offer worked.

A redemption only proves that:

```text
a cardmember activated an offer and made a qualifying purchase
```

It does not prove that the purchase was incremental.

To estimate incrementality, MerchantLift BI compares:

```text
test/redeemed spend
against
matched control spend
```

Core formula:

```text
lift_per_user = test_avg_spend - control_avg_spend
```

If test spend is only slightly higher than control spend, but reward cost is high, the campaign may be cannibalizing organic behavior.

---

## Supplemental Transaction Handling

During local synthetic generation, natural transaction-to-activation matches may be sparse.

A redemption requires a transaction that satisfies all redemption rules.

If a natural transaction does not exist, the generator creates a supplemental qualifying transaction and appends it to `fact_transactions`.

This preserves strict lineage:

```text
fact_transactions.transaction_id
    -> fact_offer_redemptions.transaction_id
```

The project rule is:

```text
No redemption without a real transaction row.
```

---

## Output Files

 writes:

```text
data/raw/fact_transactions/part-00000.parquet
data/raw/fact_offer_redemptions/part-00000.parquet
data/raw/fact_control_group_transactions/part-00000.parquet
```

`fact_transactions` may be updated because supplemental qualifying transactions are appended when natural matches are insufficient.

---

## Validation Script

The inspection script validates:

```text
redemption row count
control transaction row count
duplicate redemption IDs
duplicate control transaction IDs
orphan redemption transaction IDs
negative reward amounts
null reward amounts
negative control transaction amounts
null control transaction amounts
control match score range
```

The most important check is:

```text
Orphan redemption transaction_ids: 0
```

This proves every redemption points to a real transaction.

---

##  Mental Model

```text
Redemptions = qualified offer-side spend.
Controls = no-offer baseline spend.
Lineage = trust.
```

Or shorter:

```text
Offer-side behavior needs a baseline.
No redemption without a transaction.
```

---

## Commands

Generate  outputs:

```bash
PYTHONPATH=src python data_generation/generate_redemptions_and_controls.py
```

Inspect  outputs:

```bash
PYTHONPATH=src python scripts/inspect_generated_redemptions_and_controls.py
```

Expected good validation values:

```text
Duplicate redemption_id: 0
Duplicate control_transaction_id: 0
Orphan redemption transaction_ids: 0
Negative reward amounts: 0
Null reward amounts: 0
Negative control amounts: 0
Null control amounts: 0
Match scores below 0.75: 0
Match scores above 0.98: 0
```

# Transaction-to-Offer Redemption Matching

## Purpose

The redemption matching job identifies which card-linked transactions qualify for activated merchant offers.

The job reads trusted Silver tables and produces a matched redemption output table.

Primary implementation file:

```text
spark_jobs/build_redemption_matching.py
```

Primary output table:

```text
data/lakehouse/silver/fact_matched_offer_redemptions_clean/
```

The output is a Silver table because it is still a trusted row-level fact table, not an aggregated Gold business mart.

---

## Core Business Question

When a cardmember makes a transaction, did that transaction qualify for an activated offer?

A transaction qualifies only when all required matching rules are true:

```text
same tokenized cardmember
same merchant
transaction happened after activation
transaction happened before offer expiry
transaction amount meets minimum spend
transaction is eligible
```

---

## Input Tables

The matching job reads these Silver Delta tables:

```text
fact_transactions_clean
fact_offer_activations_clean
dim_offer_clean
```

### fact_transactions_clean

Provides trusted transaction events.

Important columns:

```text
transaction_id
tokenized_cardmember_id
merchant_id
transaction_timestamp
transaction_date
transaction_amount
transaction_status
```

### fact_offer_activations_clean

Provides trusted offer activation events.

Important columns:

```text
activation_id
tokenized_cardmember_id
offer_id
activation_timestamp
offer_expiry_timestamp
activation_status
```

### dim_offer_clean

Provides trusted offer rules.

Important columns:

```text
offer_id
campaign_id
merchant_id
minimum_spend_amount
reward_amount
```

Note:

The implementation uses `reward_amount`, not `reward_rate`, because the current offer schema does not include `reward_rate`.

---

## Matching Flow

The matching job follows this flow:

```text
1. Read trusted Silver inputs.
2. Inspect schemas and row counts.
3. Build match-ready transactions.
4. Build match-ready activations.
5. Build offer rules.
6. Join activations to offers.
7. Join transactions to activation-offer candidates.
8. Calculate reward fields.
9. Generate deterministic matched redemption IDs.
10. Deduplicate multiple matches per transaction.
11. Validate output columns.
12. Write matched redemption Silver Delta table.
13. Read back the written table and validate row count.
```

---

## Match-Ready Transactions

The transaction filtering step removes rows that can never match an offer.

Required transaction fields:

```text
transaction_id
tokenized_cardmember_id
merchant_id
transaction_timestamp
transaction_date
transaction_amount
```

Transaction filters:

```text
transaction_id is not null
tokenized_cardmember_id is not null
merchant_id is not null
transaction_timestamp is not null
transaction_date is not null
transaction_amount is not null
transaction_amount >= 0
```

If `transaction_status` exists, the job keeps eligible transaction statuses such as:

```text
approved
posted
settled
```

Purpose:

```text
Remove impossible transaction candidates before the expensive join.
```

---

## Match-Ready Activations

The activation filtering step removes activation rows that cannot produce valid redemptions.

Required activation fields:

```text
activation_id
tokenized_cardmember_id
offer_id
activation_timestamp
offer_expiry_timestamp
```

Activation filters:

```text
activation_id is not null
tokenized_cardmember_id is not null
offer_id is not null
activation_timestamp is not null
offer_expiry_timestamp is not null
offer_expiry_timestamp >= activation_timestamp
```

If `activation_status` exists, the job keeps eligible statuses such as:

```text
active
activated
eligible
```

Purpose:

```text
Only valid activation windows should be used for matching.
```

---

## Offer Rules

The offer rule step extracts only the fields needed for matching.

Required offer fields:

```text
offer_id
campaign_id
merchant_id
minimum_spend_amount
reward_amount
```

Offer filters:

```text
offer_id is not null
campaign_id is not null
merchant_id is not null
minimum_spend_amount is not null
reward_amount is not null
minimum_spend_amount >= 0
reward_amount >= 0
```

The job deduplicates offer rules by:

```text
offer_id
```

Purpose:

```text
Each activation must carry merchant, campaign, minimum spend, and reward rules before it can be matched to a transaction.
```

---

## Activation-to-Offer Join

The job joins activations to offers using:

```text
activation.offer_id = offer.offer_id
```

The offer rule table is broadcast because it is a smaller dimension-style table.

This creates activation-offer candidates with:

```text
activation_id
tokenized_cardmember_id
offer_id
campaign_id
merchant_id
activation_timestamp
offer_expiry_timestamp
minimum_spend_amount
reward_amount
```

Mental model:

```text
Activation = user opted in.
Offer = rules of the deal.
Activation + Offer = matchable offer candidate.
```

---

## Transaction-to-Offer Candidate Join

The core matching join uses:

```text
tx.tokenized_cardmember_id = act.tokenized_cardmember_id
AND tx.merchant_id = act.merchant_id
AND tx.transaction_timestamp >= act.activation_timestamp
AND tx.transaction_timestamp <= act.offer_expiry_timestamp
AND tx.transaction_amount >= act.minimum_spend_amount
```

This is stronger than joining only by cardmember.

It protects against:

```text
wrong merchant matches
pre-activation transactions
post-expiry transactions
below-threshold transactions
broad row explosion
```

Mental model:

```text
A redemption candidate is a transaction inside an activated offer window.
```

---

## Reward Calculation

The current implementation uses a fixed reward amount:

```text
calculated_reward_amount = reward_amount
```

This matches offers such as:

```text
Spend $100, get $20 back.
```

The implementation intentionally does not use `reward_rate` because the current schema does not include that field.

Future enhancement:

```text
support reward_type = fixed_amount | percentage
support percentage rewards using transaction_amount * reward_percentage
support reward caps
```

---

## Matched Redemption ID

The job creates a deterministic matched redemption ID using:

```text
transaction_id
activation_id
offer_id
```

Conceptually:

```text
matched_redemption_id = "mred_" + sha256(transaction_id || activation_id || offer_id)
```

Why this matters:

```text
The same transaction-activation-offer combination always produces the same ID.
```

This supports reproducibility and lineage.

---

## Deduplication Rule

A single transaction may match multiple offers.

The current business rule is:

```text
one transaction can redeem at most one offer
choose the highest calculated reward
if tied, choose earliest activation timestamp
if still tied, choose lowest offer_id
```

Spark implements this with a window function:

```text
partition by transaction_id
order by calculated_reward_amount desc
order by activation_timestamp asc
order by offer_id asc
keep row_number = 1
```

The surviving row receives:

```text
match_deduplication_reason
```

with value:

```text
selected_highest_reward_then_earliest_activation_then_offer_id
```

Purpose:

```text
Prevent double-counting when multiple offers match the same transaction.
```

---

## Output Table

The matched redemption output table is:

```text
fact_matched_offer_redemptions_clean
```

Recommended output path:

```text
data/lakehouse/silver/fact_matched_offer_redemptions_clean/
```

The table is partitioned by:

```text
transaction_date
```

Required output columns:

```text
matched_redemption_id
transaction_id
activation_id
offer_id
campaign_id
merchant_id
tokenized_cardmember_id
transaction_timestamp
transaction_date
transaction_amount
activation_timestamp
offer_expiry_timestamp
minimum_spend_amount
reward_amount
calculated_reward_amount
match_rule_version
match_pipeline_run_id
matched_at
match_deduplication_reason
```

---

## Output Validation

The job validates:

```text
required output columns exist
written Delta table can be read back
read-back row count equals expected row count
```

Recommended additional validation:

```text
one row per transaction_id
no null matched_redemption_id
no null transaction_id
no null activation_id
no null offer_id
calculated_reward_amount >= 0
transaction_amount >= minimum_spend_amount
transaction_timestamp >= activation_timestamp
transaction_timestamp <= offer_expiry_timestamp
```

---

## Local Execution

Run this only after Bronze and Silver tables exist.

```bash
PYTHONPATH=src python spark_jobs/bronze_ingestion.py
PYTHONPATH=src python spark_jobs/silver_transformations.py
PYTHONPATH=src python spark_jobs/build_redemption_matching.py
```

Expected successful output includes:

```text
Wrote Silver Delta table: .../fact_matched_offer_redemptions_clean
Matched redemption write validation passed.
```

Check output:

```bash
ls data/lakehouse/silver/fact_matched_offer_redemptions_clean
```

Expected:

```text
_delta_log
transaction_date=...
```

The `_delta_log` folder proves the output is a Delta table.

---

## Databricks Execution

Run from a Databricks notebook shell cell:

```bash
%sh
cd /Workspace/Repos/<your-folder>/merchantlift-bi

export MERCHANTLIFT_LAKEHOUSE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse
export MERCHANTLIFT_BRONZE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/bronze
export MERCHANTLIFT_SILVER_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/silver
export MERCHANTLIFT_GOLD_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/gold

PYTHONPATH=src python spark_jobs/build_redemption_matching.py
```

Inspect output in Databricks:

```python
matched_path = "dbfs:/FileStore/merchantlift/data/lakehouse/silver/fact_matched_offer_redemptions_clean"

matched_df = (
    spark.read
    .format("delta")
    .load(matched_path)
)

display(matched_df.limit(20))
```

Count:

```python
matched_df.count()
```

Check one match per transaction:

```python
from pyspark.sql import functions as F

(
    matched_df
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .show(20, truncate=False)
)
```

Expected:

```text
no rows
```

---

## What This Enables Next

The matched redemption table becomes the trusted source for:

```text
reward liability
merchant settlements
offer performance
incrementality features
fraud and abuse monitoring
financial reconciliation
```

Future jobs can use:

```text
fact_matched_offer_redemptions_clean
```

instead of relying on pre-generated redemption facts.

This is a more realistic production-style design because the platform now derives redemptions from:

```text
transactions
activations
offer rules
```

instead of blindly trusting generated redemption rows.
