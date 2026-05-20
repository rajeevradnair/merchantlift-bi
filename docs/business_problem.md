# Business Problem

## Project Name

MerchantLift BI: Privacy-Safe Merchant Offer Economics & Incrementality Intelligence Platform

## Central Business Question

Did the merchant offer create incremental profitable spend, or did it simply subsidize customers who would have purchased anyway?

## Business Domain

MerchantLift BI operates in the domain of card-linked merchant offers.

A card-linked offer is a promotion connected to a payment card account. A cardmember may activate an offer such as:

> Spend $100 at a merchant and receive $20 back.

When the cardmember later uses the card at the merchant, the platform can match the transaction to the activated offer and calculate whether a reward should be issued.

## Why Merchants Fund Offers

Merchants fund offers to influence customer behavior.

Common merchant goals include:

- Acquiring new customers
- Reactivating lapsed customers
- Increasing average order value
- Driving repeat purchase behavior
- Growing spend in a specific category
- Competing against similar merchants
- Improving campaign return on investment

## Why Redemptions Alone Are Misleading

A merchant-funded offer may appear successful if many customers redeem it.

However, redemption volume alone does not prove that the offer created new value.

Some customers may have purchased from the merchant even without the offer. When the merchant pays a reward to those customers, the offer subsidizes organic behavior instead of creating incremental behavior.

This is the cannibalization problem.

## Example

Suppose a merchant launches this offer:

> Spend $100 and get $20 back.

Two customers redeem the offer:

| Customer | Behavior | Business Interpretation |
|---|---|---|
| Customer A | Was already planning to spend $100 | Subsidized shopper |
| Customer B | Spent $100 because of the offer | Incremental shopper |

Both customers look identical in a redemption dashboard.

But economically, they are very different.

Customer A creates cost without new value.

Customer B creates incremental value.

## The Real Analytics Problem

The platform must distinguish between:

- Organic spend that would have happened anyway
- Incremental spend caused by the offer
- Reward cost paid to cardmembers
- Merchant profit after cost of goods sold
- Platform fees
- Fraud or offer-abuse leakage
- Settlement mismatches
- Privacy-safe reporting constraints

## Why Test and Control Groups Matter

A simple before-and-after analysis is weak because many outside factors can affect spend:

- Seasonality
- Holidays
- Merchant promotions
- Category trends
- Economic conditions
- Customer lifecycle changes
- Random variation

A stronger approach compares a test group against a similar control group.

The test group contains customers who saw, activated, or redeemed an offer.

The control group contains similar customers who were excluded from the offer.

By comparing the behavior of these groups, the platform can estimate incremental lift.

## Core Incrementality Idea

If the test group spends more than the control group during the campaign window, the difference may represent incremental spend.

At a simplified level:

```text
Lift per user = Average test-group spend - Average control-group spend
```

Then:

```text
Incremental revenue = Lift per user × Number of test-group users
```

But revenue is not enough.

Merchants care about profit.

So the platform also estimates:

```text
Net merchant profit =
    Incremental revenue × merchant margin
    - reward liability
    - platform fees
```

## Cannibalization Risk

Cannibalization happens when the merchant pays rewards for purchases that would have occurred anyway.

A campaign may have high spend and high redemptions but still be economically weak if:

- Test-group behavior is similar to control-group behavior
- Reward cost is too high
- Net merchant profit is negative
- Repeat behavior does not improve
- The offer mostly attracts existing loyal customers
- Fraud or refund-after-reward behavior increases

## Privacy Problem

Merchant analytics often involves sensitive customer and transaction data.

Even in a synthetic project, the system should be designed with privacy-safe principles:

- Do not expose raw cardmember-level data in reports
- Use tokenized cardmember identifiers only in intermediate processing
- Report only aggregated cohorts
- Suppress small cohorts below a privacy threshold
- Apply row-level access rules for merchant-specific reporting
- Apply column-level controls for sensitive fields
- Track access through audit logs

## Production-Grade Platform Goal

MerchantLift BI is designed to simulate an enterprise-grade analytics platform that supports:

- Merchant teams evaluating campaign performance
- Executives monitoring merchant offer economics
- Finance teams reconciling reward and settlement amounts
- Risk teams detecting offer abuse
- Compliance teams validating privacy-safe reporting
- Data engineers maintaining reliable pipelines
- Analysts using governed BI marts

## Final Business Outcome

The final platform should help answer:

1. Which offers created true incremental spend?
2. Which merchants earned profitable lift?
3. Which campaigns overpaid rewards?
4. Which customer segments responded profitably?
5. Which campaigns showed cannibalization risk?
6. Which transactions or redemptions looked suspicious?
7. Which settlements failed reconciliation checks?
8. Which reports were safe enough for merchant-facing analytics?

## Summary

MerchantLift BI is not just a dashboard project.

It is a production-simulated merchant intelligence platform that combines data engineering, analytics engineering, BI, privacy, governance, financial reconciliation, fraud monitoring, and incrementality measurement.

The project exists to answer a high-value business question:

> Are merchant-funded offers truly creating profitable incremental value?
