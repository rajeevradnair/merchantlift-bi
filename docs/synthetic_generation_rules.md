# Synthetic Generation Rules

## Purpose

This document defines the behavioral rules that future data generation scripts should follow.

These rules will guide the Python + Faker + Polars implementation.

--

## 1. Identifier Rules

Use deterministic synthetic IDs.

| Entity | ID Pattern |
|---|---|
| Cardmember | `cm_tok_000001` |
| Merchant | `merchant_0001` |
| Campaign | `campaign_0001` |
| Offer | `offer_0001` |
| Transaction | `tx_000000001` |
| Impression | `imp_000000001` |
| Activation | `act_000000001` |
| Redemption | `red_000000001` |
| Reward Liability | `rew_000000001` |
| Settlement | `set_000000001` |
| Fraud Event | `fraud_000000001` |
| Reconciliation | `recon_000000001` |

Use business event IDs for fact events.

Use surrogate keys only for SCD dimension versions, such as:

```text
merchant_sk
offer_sk
campaign_sk
```

---

## 2. Category Rules

| Category | Basket Range | Default Margin | Seasonality |
|---|---:|---:|---|
| Dining | 20–150 | 0.35 | Weekend lift |
| Grocery | 25–250 | 0.15 | Weekly repeat |
| Retail | 40–600 | 0.45 | Holiday lift |
| Travel | 300–2500 | 0.30 | Seasonal spikes |
| Luxury | 200–5000 | 0.55 | Sparse high-value |
| Digital Goods | 10–300 | 0.70 | Frequent online |

---

## 3. Segment Rules

| Segment | Response Rate | Incrementality Rate | Notes |
|---|---:|---:|---|
| `new_to_merchant` | 0.10 | 0.45 | Good acquisition segment |
| `lapsed_customer` | 0.18 | 0.55 | Strong reactivation potential |
| `loyal_customer` | 0.25 | 0.15 | High cannibalization risk |
| `high_value_customer` | 0.20 | 0.35 | High spend but mixed incrementality |
| `bargain_seeker` | 0.35 | 0.25 | High reward sensitivity |
| `category_enthusiast` | 0.22 | 0.30 | Category-driven behavior |

---

## 4. Shopper Behavior Assignment

Recommended synthetic behavior mix:

| Behavior Type | Approximate Share |
|---|---:|
| `incremental_shopper` | 25% |
| `subsidized_shopper` | 25% |
| `lapsed_reactivated` | 15% |
| `loyal_existing` | 20% |
| `bargain_seeker` | 10% |
| `fraud_prone` | 5% |

These shares can be adjusted later.

---

## 5. Offer Funnel Rules

Use approximate funnel assumptions:

```text
impression_to_activation_rate = 0.20
activation_to_transaction_rate = 0.50
transaction_to_redemption_rate = 0.50
```

These should vary by segment and category in later versions.

---

## 6. Reward Rules

Offer types:

```text
fixed_cashback
percent_cashback
```

Example rules:

```text
fixed_cashback:
    minimum_spend_amount = 100
    reward_amount = 20

percent_cashback:
    reward_multiplier = 0.10
    max_reward_amount = 50
```

Reward calculation:

```text
fixed_cashback reward = reward_amount
percent_cashback reward = min(transaction_amount * reward_multiplier, max_reward_amount)
```

---

## 7. Control Group Rules

Control users should be similar to test users.

Matching features:

```text
historical_spend_90d
historical_transaction_count_90d
merchant_affinity_score
category_affinity_score
segment_id
location_id
customer_tenure_months
```

Control group users should not have offer impressions or activations for the offer context they are measuring.

---

## 8. Cannibalization Rules

Subsidized shoppers should have test spend close to control baseline.

Example:

```text
subsidized_shopper test_spend ≈ matched_control_spend
```

Incremental shoppers should have higher test spend.

Example:

```text
incremental_shopper test_spend > matched_control_spend
```

Later dbt/Spark logic should flag high cannibalization when:

```text
net_merchant_profit < 0
or lift_per_user is low relative to reward cost
```

---

## 9. Fraud Rules

Fraud events should be rare but present.

Approximate rates:

| Fraud Pattern | Approximate Rate |
|---|---:|
| Duplicate redemption | 0.3% of redemptions |
| Refund after reward | 1.0% of redemptions |
| High redemption velocity | 0.5% of active redeemers |
| Merchant-location anomaly | 0.2% of transactions |
| Reward gaming pattern | 0.4% of redeemers |

---

## 10. Reconciliation Rules

Most records should match.

Approximate distribution:

| Reconciliation Status | Approximate Share |
|---|---:|
| `matched` | 95% |
| `mismatched` | 2% |
| `late_arriving` | 2% |
| `refund_after_reward` | 1% |

Settlement delta formula:

```text
settlement_delta =
    authorized_amount
    - merchant_settlement_amount
    - platform_fee_amount
```

---

## 11. Privacy Rules

No generated dataset should include:

```text
card_number
pan
primary_account_number
email
phone_number
real_name
street_address
```

Allowed:

```text
tokenized_cardmember_id
synthetic_merchant_name
synthetic_location_id
zip_prefix
```

Reporting cohorts below 50 should later be suppressed.

---

## 12. Summary

These rules make the data generator realistic enough to support:

- test/control incrementality
- reward economics
- cannibalization
- fraud signals
- settlement reconciliation
- privacy-safe reporting
