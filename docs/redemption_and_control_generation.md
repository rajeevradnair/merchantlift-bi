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
