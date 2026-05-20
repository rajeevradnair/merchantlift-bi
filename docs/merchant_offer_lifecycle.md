# Merchant Offer Lifecycle

## Purpose

This document explains the end-to-end business lifecycle behind MerchantLift BI.

MerchantLift BI analyzes merchant-funded card-linked offers to answer one central question:

> Did the merchant offer create incremental profitable spend, or did it simply subsidize customers who would have purchased anyway?

To answer this, the platform must understand how merchants, campaigns, offers, cardmembers, transactions, redemptions, rewards, settlements, fraud events, and reconciliation records relate to one another.

---

## 1. Business Domain Overview

Merchant-funded offers are promotions paid for by merchants to influence cardmember behavior.

A simple example is:

> Spend $50 at a merchant and receive $10 back.

The merchant funds the reward because the merchant expects the offer to create valuable behavior such as:

- new customer acquisition
- lapsed customer reactivation
- larger basket size
- repeat purchases
- category share growth
- profitable incremental spend

However, redemptions alone do not prove success.

A customer may redeem an offer even though they would have purchased anyway. In that case, the merchant paid a reward without creating new value.

MerchantLift BI exists to separate true incremental behavior from subsidized organic behavior.

---

## 2. Core Business Actors

### Merchant

A merchant is the business funding the offer.

Examples:

- restaurant
- hotel
- retailer
- grocery store
- luxury brand
- online marketplace

The merchant wants to understand whether the offer created profitable incremental spend.

### Cardmember

A cardmember is the customer using the card.

In this project, cardmembers are represented using synthetic tokenized identifiers.

Example:

```text
cm_tok_000001