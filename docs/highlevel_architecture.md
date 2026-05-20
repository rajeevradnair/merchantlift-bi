MerchantLift BI — Full Production-Grade Architecture

Business Domain Layer
Merchant-funded offers
Card-linked transactions
Cardmember offer activations
Merchant settlements
Reward liability
Fraud / abuse risk
Privacy-safe merchant reporting

        ↓

Synthetic Data Generation Layer
Python + Faker + Polars
Generate synthetic:
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
Parquet files
Partitioned by event_date / transaction_date
Stored in local data lake first, later cloud object storage pattern

        ↓

Raw Data Lake
data/raw/
Immutable source-of-truth files
No business logic yet
No overwrites
Schema contracts documented

        ↓

Data Quality Gate 1 — Ingestion Edge
Great Expectations
Validate:
- no missing transaction IDs
- no negative transaction amounts
- no invalid merchant IDs
- no reward amount above max reward
- no raw PAN/card-number-like fields
- valid event dates
- valid offer status / transaction status

        ↓

Lakehouse Processing Layer
Spark / Databricks

Bronze Delta Tables:
- lightly structured raw data
- schema enforced
- partitioned files
- ingestion metadata

Silver Delta Tables:
- cleaned records
- deduplicated transactions
- normalized timestamps
- tokenized cardmember IDs
- valid merchant/offer references
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
- non-equi date-window joins
- activation-to-transaction matching
- reward calculation
- test/control group matching
- subsidized shopper simulation
- cannibalization signals
- refund-after-reward detection
- settlement delta calculation
- late-arriving transaction handling

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

Data Quality Gate 2 — Semantic Layer
dbt tests
Validate:
- unique transaction_id
- not_null merchant_id
- relationship checks
- accepted transaction statuses
- reward_cost <= gross_spend
- no raw cardmember IDs in reporting marts
- privacy-safe cohorts meet threshold
- settlement delta within tolerance

        ↓

Cloud Warehouse Layer
BigQuery

Datasets:
- merchantlift_staging
- merchantlift_intermediate
- merchantlift_marts
- merchantlift_privacy_safe
- merchantlift_audit

Tables optimized with:
- partitioning
- clustering
- cost-aware SQL
- incremental models
- reporting-friendly schemas

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
→ build gold marts
→ load BigQuery
→ run dbt
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