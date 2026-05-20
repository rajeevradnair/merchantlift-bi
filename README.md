# MerchantLift BI

## Privacy-Safe Merchant Offer Economics & Incrementality Intelligence Platform

MerchantLift BI is a production-grade data engineering and business intelligence platform for analyzing merchant-funded card-linked offers.

The platform answers one central business question:

> Did the merchant offer create incremental profitable spend, or did it simply subsidize customers who would have purchased anyway?

## Why This Project Exists

Merchant-funded offers can look successful when redemptions and total spend increase. However, high redemption volume does not automatically mean the offer created new value.

A merchant needs to know whether the offer caused new purchasing behavior or simply rewarded customers who were already planning to buy.

MerchantLift BI solves this by combining:

- Synthetic card-linked offer and transaction data
- Test-vs-control incrementality measurement
- Merchant economics modeling
- Reward liability tracking
- Fraud and offer-abuse detection
- Financial reconciliation
- Privacy-safe reporting
- Governed BI dashboards

## Target Scale

The platform is designed to process 100M+ synthetic events across (local implementation validated on 10M+ events):

- Transactions
- Offer impressions
- Offer activations
- Offer redemptions
- Control-group transactions
- Reward liability records
- Merchant settlements
- Fraud-risk events
- Data-quality reconciliation records

## High-Level Architecture

```text
Python + Faker + Polars
        ↓
Synthetic merchant / offer / transaction events
    ↓
Parquet files
    ↓
Raw data lake
    ↓
Spark / Databricks
    ↓
Bronze / Silver / Gold Delta Lake tables
    ↓
dbt
    ↓
Business metric models
    ↓
BigQuery
    ↓
Governed reporting marts
    ↓
Power BI
    ↓
Executive, merchant, finance, risk, and compliance dashboards