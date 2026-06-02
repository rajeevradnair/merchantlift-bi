# Offer Interaction Generation

## Purpose

This document explains how MerchantLift BI generates offer interaction events.

Offer funnel before redemption:

```text
fact_offer_customer_assignment -> fact_offer_impressions -> fact_offer_activations
```

A merchant-funded offer does not immediately become a redemption.

The lifecycle is:

eligible / assigned
    -> shown
    -> activated
    -> purchased
    -> redeemed
    -> reward liability

```text
fact_offer_customer_assignment
Grain: One row per cardmember-offer assignment.

fact_offer_impressions
Grain: One row per offer shown to one cardmember.

fact_offer_activations
Grain: One row per cardmember activating one offer.
```