# Incrementality Problem

## Purpose

This document explains why MerchantLift BI cannot rely on redemption counts or simple before/after analysis.

The platform must estimate whether a merchant-funded offer created true incremental behavior or merely subsidized purchases that would have happened anyway.

---

## 1. Central Question

MerchantLift BI exists to answer:

> Did the merchant offer create incremental profitable spend, or did it simply subsidize customers who would have purchased anyway?

This question matters because a campaign can look successful on the surface while destroying merchant value underneath.

---

## 2. Why Redemptions Alone Are Misleading

A redemption means a customer qualified for the offer.

It does not prove that the offer caused the customer to purchase.

Different redemption cases can have very different business meanings:

| Customer Type | Example Behavior | Business Meaning |
|---|---|---|
| Incremental shopper | Purchased because the offer changed behavior | Valuable lift |
| Organic shopper | Would have purchased anyway | Subsidized behavior |
| Reward seeker | Purchased mainly to harvest reward | Low-quality behavior |
| Refund abuser | Redeemed then refunded purchase | Fraud or leakage risk |
| Loyal customer | Already buys regularly | Possible cannibalization |

A basic redemption dashboard treats these customers too similarly.

MerchantLift BI must separate them.

---

## 3. Why Before/After Analysis Is Weak

A simple approach compares spend before the campaign with spend during the campaign.

Example:

```text
Spend before campaign = $80,000
Spend during campaign = $120,000
Naive lift = $40,000