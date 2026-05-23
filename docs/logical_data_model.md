# Logical Data Model

## Purpose

This document explains how MerchantLift BI entities relate to each other at a business level.

---

## Core Business Flow

```text
cardmember
    → sees offer
    → activates offer
    → makes transaction
    → may redeem offer
    → creates reward liability
    → merchant settlement occurs
    → reconciliation checks run
    → privacy-safe marts report results