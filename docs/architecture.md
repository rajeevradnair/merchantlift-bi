# MerchantLift BI Architecture

## Purpose

This document defines the target architecture for MerchantLift BI.

MerchantLift BI is a production-simulated GCP merchant-offer analytics platform. It analyzes merchant-funded offers, card-linked transactions, incrementality, reward liability, merchant profitability, fraud and offer abuse, financial reconciliation, and privacy-safe reporting.

The central business question is:

> Did the merchant offer create incremental profitable spend, or did it simply subsidize customers who would have purchased anyway?

---

## 1. Architecture Principles

MerchantLift BI follows these architecture principles:

| Principle | Meaning |
|---|---|
| Layered data flow | Data moves through clear raw, bronze, silver, gold, warehouse, and reporting layers |
| Data contracts first | Every table has a defined schema, grain, quality rules, and privacy classification |
| Privacy by design | Reporting exposes aggregated cohorts, not customer-level behavior |
| Quality gates | Raw data and semantic marts are validated before dashboard use |
| Separation of concerns | Generation, processing, modeling, governance, orchestration, and BI are separated |
| Portfolio evidence | Every major design choice is documented and demo-ready |

---

## 2. Full Production-Grade Architecture

```text
Business Domain Layer
Merchant-funded offers
Card-linked transactions
Offer impressions
Offer activations
Offer redemptions
Reward liability
Merchant settlements
Fraud / abuse risk
Financial reconciliation
Privacy-safe merchant reporting

        ↓

Synthetic Data Generation Layer
Python + Faker + Polars

Generate 10M+ synthetic events:
- merchants
- campaigns
- offers
- cardmember tokens
- offer impressions
- offer activations
- transactions
- redemptions
- control-group transactions
- reward liability records
- merchant settlement records
- fraud-risk events
- data-quality reconciliation records

        ↓

File Storage Layer
Partitioned Parquet files
Stored first in local data lake folders
Later portable to cloud object storage patterns

        ↓

Raw Data Lake
data/raw/

Purpose:
- immutable source-of-truth files
- no business logic
- no overwrites
- schema contracts documented

        ↓

Data Quality Gate 1
Great Expectations

Validates:
- required IDs are present
- amounts are non-negative
- timestamps are valid
- merchant IDs exist
- reward amounts are within offer rules
- no raw PAN/card-number-like fields exist
- allowed status values are respected

        ↓

Lakehouse Processing Layer
Spark / Databricks

Bronze Delta Tables:
- lightly structured raw data
- ingestion metadata
- schema enforcement
- partitioned storage

Silver Delta Tables:
- cleaned records
- deduplicated transactions
- normalized timestamps
- valid merchant/offer references
- tokenized cardmember IDs
- redemption eligibility logic

Gold Delta Tables:
- merchant daily rollups
- offer daily rollups
- reward liability summaries
- incrementality feature tables
- fraud feature tables
- reconciliation tables

        ↓

Advanced Spark Business Logic Layer
- transaction-to-offer matching
- activation-to-transaction matching
- non-equi date-window joins
- reward calculation
- test/control comparison inputs
- subsidized shopper simulation
- cannibalization signals
- refund-after-reward detection
- settlement delta calculation
- late-arriving transaction handling

        ↓

Warehouse Landing Layer
BigQuery staging datasets

Purpose:
- receive curated gold outputs
- provide stable warehouse landing tables
- support dbt modeling
- enable partitioned and clustered warehouse tables

        ↓

Analytics Engineering Layer
dbt on BigQuery

dbt staging models:
- stg_transactions
- stg_offers
- stg_redemptions
- stg_merchants
- stg_reward_liability
- stg_settlements

dbt intermediate models:
- int_test_group_spend
- int_control_group_spend
- int_redemption_matching
- int_reward_costs
- int_settlement_reconciliation
- int_fraud_signals

dbt marts:
- mart_offer_performance
- mart_merchant_economics
- mart_offer_incrementality
- mart_reward_liability
- mart_offer_abuse_risk
- mart_data_quality_reconciliation
- mart_privacy_safe_offer_lift

        ↓

Data Quality Gate 2
dbt tests

Validates:
- unique keys
- not-null required fields
- relationship integrity
- accepted values
- reward cost <= gross spend
- no raw cardmember IDs in reporting marts
- privacy-safe cohorts meet threshold
- settlement deltas are within tolerance

        ↓

Cloud Warehouse and Governed Reporting Layer
BigQuery

Datasets:
- merchantlift_staging
- merchantlift_intermediate
- merchantlift_marts
- merchantlift_privacy_safe
- merchantlift_audit

Optimization:
- partitioning
- clustering
- incremental dbt models
- cost-aware SQL
- dashboard-friendly schemas

        ↓

Governance and Security Layer
Google Cloud DLP
BigQuery Policy Tags
BigQuery Row-Level Security
Cloud KMS
Dataplex / Data Catalog
Cloud Audit Logs
IAM service accounts

Controls:
- tokenized IDs only
- no raw card numbers
- analyst-safe masked views
- engineer-only join-resolution access
- merchant-specific row filtering
- cohort suppression for small groups
- audit trail for data access
- encryption-at-rest design

        ↓

Orchestration Layer
Airflow DAG

Pipeline flow:
generate synthetic data
→ validate raw data
→ run Spark bronze ingestion
→ run Spark silver cleaning
→ build gold tables
→ load curated outputs to BigQuery staging
→ run dbt staging/intermediate/marts
→ run dbt tests
→ run Great Expectations checkpoints
→ publish validation reports
→ refresh BI datasets

        ↓

BI and Reporting Layer
Power BI

Dashboards:
1. Executive Merchant Economics Dashboard
2. Offer Incrementality Dashboard
3. Merchant Pacing and Liability Dashboard
4. Segment Response Dashboard
5. Merchant Category Intelligence Dashboard
6. Privacy-Safe Analytics Dashboard
7. Fraud and Offer Abuse Dashboard
8. Data Quality and Reconciliation Dashboard

        ↓

Portfolio Evidence Layer
README
Architecture diagrams
Data lineage diagram
Business glossary
Release reports
Data quality report
Privacy compliance report
Performance report
Executive insights report
Final demo script
Resume bullet
Interview narrative