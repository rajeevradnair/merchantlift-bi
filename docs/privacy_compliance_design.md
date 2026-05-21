# Privacy and Compliance Design

## Purpose

This document defines the privacy and compliance design for MerchantLift BI.

MerchantLift BI analyzes card-linked offer and transaction-like behavior, it must be designed with privacy, security, least privilege, and governed reporting in mind.

The goal is not only to build dashboards, but to build privacy-safe merchant intelligence.

---

## 1. Core Privacy Principle

MerchantLift BI follows this principle:

> Expose the minimum data required for each stakeholder to do their job.

A data engineer may need tokenized identifiers to debug joins.

A merchant analyst does not need row-level customer behavior.

An executive does not need customer-level records.

---

## 2. No Raw PAN Policy

PAN means Primary Account Number, which is the payment card number.

MerchantLift BI will not generate, store, process, or report raw PAN values.

The following fields are prohibited:

```text
card_number
pan
primary_account_number
account_number
full_card_number