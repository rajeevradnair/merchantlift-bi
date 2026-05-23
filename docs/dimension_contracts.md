# Dimension Contracts

## Purpose

This document provides data-contract-style definitions for the main MerchantLift BI dimension tables.

These contracts will guide future data generation, Spark transformations, dbt models, BigQuery schemas, and quality tests.

---

## 1. `dim_merchant`

### Grain

One row per merchant.

### Primary Key

```text
merchant_id