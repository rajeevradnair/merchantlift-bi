# Synthetic Data Design

## Purpose

This document defines the synthetic data design for MerchantLift BI.

MerchantLift BI needs synthetic data that behaves like a real merchant-funded card-linked offer platform.

The goal is not only to create rows.

The goal is to create realistic business behavior that supports:

- incrementality measurement
- merchant economics
- reward liability
- fraud and offer-abuse detection
- financial reconciliation
- privacy-safe reporting
- Power BI dashboard storytelling

---

## 1. Core Synthetic Data Principle

Synthetic data should be fake but behaviorally meaningful.

The data should not include real card numbers, real customers, real merchants, real emails, real phone numbers, or real transaction records.

The project uses synthetic identifiers such as:

```text
cm_tok_000001
merchant_0001
offer_0001
campaign_0001
tx_000000001
```

---

## 2. Target Data Volume

The full project target is 10M+ synthetic rows.

Suggested split:

| Table | Target Rows |
|---|---:|
| `fact_transactions` | 6,000,000 |
| `fact_offer_impressions` | 1,500,000 |
| `fact_offer_activations` | 1,000,000 |
| `fact_offer_redemptions` | 500,000 |
| `fact_control_group_transactions` | 750,000 |
| `fact_reward_liability` | 250,000 |
| `fact_merchant_settlements` | 200,000 |
| `fact_fraud_risk_events` | 100,000 |
| `fact_data_quality_reconciliation` | 50,000 |

For local development, smaller sample volumes should be generated first.

---

## 3. Generation Order

Generate dimensions before facts.

Recommended order:

```text
dim_category
dim_location
dim_segment
dim_risk_rule
dim_merchant
dim_campaign
dim_offer
dim_cardmember_token
dim_privacy_consent
fact_offer_impressions
fact_offer_activations
fact_transactions
fact_offer_redemptions
fact_control_group_transactions
fact_reward_liability
fact_merchant_settlements
fact_fraud_risk_events
fact_data_quality_reconciliation
```

Why this matters:

Facts need valid dimension references.

Example:

```text
fact_transactions.merchant_id must exist in dim_merchant
fact_offer_redemptions.offer_id must exist in dim_offer
fact_fraud_risk_events.risk_rule_id must exist in dim_risk_rule
```

---

## 4. Merchant Category Behavior

Each merchant category should have different transaction behavior.

| Category | Basket Size Pattern | Margin Pattern | Example Behavior |
|---|---:|---:|---|
| Dining | low/medium | medium | More weekend activity |
| Grocery | low/medium | low | Frequent repeat purchases |
| Retail | medium | medium/high | Holiday spikes |
| Travel | high | medium | Seasonal booking spikes |
| Luxury | high | high | Fewer but larger transactions |
| Digital Goods | low/medium | high | Frequent small online purchases |

---

## 5. Customer Segments

Synthetic cardmembers should belong to segments.

| Segment | Meaning |
|---|---|
| `new_to_merchant` | No prior merchant spend |
| `lapsed_customer` | Has not purchased recently |
| `loyal_customer` | Already purchases frequently |
| `high_value_customer` | High spend potential |
| `bargain_seeker` | Highly reward-sensitive |
| `category_enthusiast` | Shops often in a category |

Segments help support:

- targeting
- test/control matching
- incrementality analysis
- dashboard segmentation
- privacy-safe cohort reporting

---

## 6. Shopper Behavior Types

The generator should create behavior types that make incrementality realistic.

| Behavior Type | Meaning |
|---|---|
| `incremental_shopper` | Offer causes additional spend |
| `subsidized_shopper` | Customer would have purchased anyway |
| `lapsed_reactivated` | Offer reactivates inactive customer |
| `loyal_existing` | Customer already buys regularly |
| `bargain_seeker` | Responds strongly to reward |
| `fraud_prone` | More likely to trigger suspicious patterns |
| `control_baseline` | Similar user excluded from offer |

These labels are synthetic truth labels.

They help us validate whether the later analytics logic can detect the patterns we intentionally created.

---

## 7. Test and Control Design

The synthetic data must support test/control analysis.

Test group:

```text
Users exposed to, activated, or redeemed an offer.
```

Control group:

```text
Similar users intentionally excluded from the offer.
```

Matching features may include:

- historical spend over 90 days
- historical transaction count over 90 days
- merchant affinity score
- category affinity score
- segment
- location
- customer tenure
- average ticket size

Future incrementality calculations compare:

```text
test_avg_spend - control_avg_spend
```

within an offer/campaign/merchant/segment/campaign-window context.

---

## 8. Offer Funnel Behavior

The synthetic offer funnel should follow this pattern:

```text
eligible cardmembers
→ impressions
→ activations
→ transactions
→ redemptions
→ reward liability
```

Not everyone who sees an offer activates.

Not everyone who activates transacts.

Not every transaction qualifies.

This makes the funnel realistic.

---

## 9. Fraud and Offer-Abuse Behavior

The synthetic data should include rare but meaningful fraud and abuse patterns.

Examples:

| Pattern | Meaning |
|---|---|
| `duplicate_redemption` | Same offer redeemed beyond allowed rules |
| `refund_after_reward` | Customer receives reward then refunds transaction |
| `high_redemption_velocity` | Too many redemptions in short time |
| `merchant_location_anomaly` | Activity appears unusual for merchant/location |
| `reward_gaming_pattern` | Behavior optimized mainly to harvest rewards |

These patterns feed:

```text
fact_fraud_risk_events
mart_offer_abuse_risk
Fraud and Offer Abuse Dashboard
```

---

## 10. Reconciliation Behavior

The synthetic data should include mostly clean records plus some mismatches.

Examples:

| Reconciliation Pattern | Meaning |
|---|---|
| `matched` | Transaction, fee, and settlement tie out |
| `mismatched` | Amounts do not tie out |
| `late_arriving` | Transaction or settlement arrived late |
| `refund_after_reward` | Reward paid before refund was detected |

This feeds:

```text
fact_data_quality_reconciliation
mart_data_quality_reconciliation
Data Quality and Reconciliation Dashboard
```

---

## 11. Privacy-Safe Reporting Behavior

The generator should create some small cohorts below the reporting threshold.

Default threshold:

```text
minimum_reportable_cohort_size = 50
```

This lets us test suppression logic later.

Example:

```text
If merchant + campaign + segment has only 12 users:
    suppress cohort in privacy-safe mart
```

---

## 12. Output Format

Local generated data should be written as Parquet.

Initial folder pattern:

```text
data/raw/<table_name>/
```

Example:

```text
data/raw/dim_merchant/
data/raw/fact_transactions/
```

Later Spark will read these raw Parquet files and create bronze/silver/gold Delta tables.

---

## 13. Summary

Synthetic data in MerchantLift BI must support the full business story:

```text
merchant creates offer
→ cardmembers see/activate
→ transactions occur
→ some redemptions are incremental
→ some are subsidized
→ reward liability is created
→ settlements happen
→ fraud/reconciliation issues appear
→ privacy-safe dashboards report trusted results
```
