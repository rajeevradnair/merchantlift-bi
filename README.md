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
Business domain rules
        ↓
Synthetic data generator
Python + Faker + Polars
        ↓
Partitioned Parquet files
        ↓
Raw data lake
        ↓
Great Expectations raw validation
        ↓
Spark / Databricks
        ↓
Bronze Delta tables
        ↓
Silver Delta tables
        ↓
Gold Delta tables
        ↓
BigQuery staging
        ↓
dbt staging models
        ↓
dbt intermediate models
        ↓
dbt marts
        ↓
BigQuery governed reporting datasets
        ↓
Policy tags / row-level security / DLP / KMS / audit logs
        ↓
Power BI dashboards
        ↓
Executive, merchant, finance, risk, compliance insights

MerchantLift BI models the full lifecycle of a merchant-funded card-linked offer:
```

## Core Merchant Offer Lifecycle

```text
campaign → offer → impression → activation → transaction → redemption → reward liability → settlement → fraud checks → reconciliation → privacy-safe reporting
```

## Stakeholder Use Cases

MerchantLift BI is designed for multiple stakeholder groups:

- Executives monitoring profitable incremental growth
- Merchant teams optimizing campaigns and offers
- Finance teams validating reward liability and settlement reconciliation
- Risk teams detecting fraud and offer abuse
- Compliance teams enforcing privacy-safe reporting
- Data engineering teams maintaining reliable pipelines
- Analytics engineering teams building trusted marts and dashboards

The stakeholder map is documented in:

```text
docs/stakeholder_use_cases.md
```

## Incrementality Problem

MerchantLift BI does not treat redemptions as automatic proof of campaign success.

A redemption proves that a customer qualified for an offer, but it does not prove that the offer caused the purchase.

The incrementality design explains why simple before/after analysis is weak and why matched test/control groups are needed.

The incrementality problem framing is documented in:

```text
docs/incrementality_problem.md
```

## Privacy and Compliance Design

MerchantLift BI is synthetic, but it is designed with financial-services-grade privacy principles.

The platform uses:

- no raw PAN/card-number policy
- synthetic data only
- tokenized cardmember identifiers
- least-privilege access design
- aggregated reporting
- cohort suppression
- BigQuery policy tag design
- BigQuery row-level security design
- Google Cloud DLP inspection configuration
- Cloud KMS design notes
- IAM service account separation
- Cloud Audit Logs design

The privacy and compliance design is documented in:

```text
docs/privacy_compliance_design.md
```

The architecture is documented in:

```text
docs/incrementality_problem.md
```

The reusable data contract template is documented in:

```text
docs/incrementality_problem.md
```

The initial ERD is documented in:

```text
docs/initial_erd.md
docs/initial_erd.png
```

## Dimension Design

MerchantLift BI uses dimension tables to describe the business context around fact events.

Core dimensions include:

- `dim_cardmember_token`
- `dim_merchant`
- `dim_merchant_scd`
- `dim_offer`
- `dim_offer_scd`
- `dim_campaign`
- `dim_campaign_scd`
- `dim_location`
- `dim_category`
- `dim_segment`
- `dim_date`
- `dim_privacy_consent`
- `dim_risk_rule`

Dimension design and contracts are documented in:

```text
docs/dimension_design.md
docs/dimension_contracts.md
```

## Fact Design

MerchantLift BI uses fact tables to record business events and measurable activity.

Core fact tables include:

- `fact_transactions`
- `fact_offer_impressions`
- `fact_offer_activations`
- `fact_offer_redemptions`
- `fact_control_group_transactions`
- `fact_reward_liability`
- `fact_merchant_settlements`
- `fact_fraud_risk_events`
- `fact_data_quality_reconciliation`

Fact table design and contracts are documented in:

```text
docs/fact_table_design.md
docs/fact_contracts.md
```

## Synthetic Data Design

MerchantLift BI uses synthetic data that is fake but behaviorally realistic.

The synthetic data design includes:

- 10M+ full-scale target rows
- smaller local sample generation
- merchant category behavior
- customer segments
- shopper behavior types
- test/control design
- subsidized shopper simulation
- reward rules
- fraud and offer-abuse patterns
- reconciliation mismatches
- privacy-safe cohort behavior

Synthetic data design is documented in:

```text
docs/synthetic_data_design.md
docs/synthetic_generation_rules.md
```

## Core Dimension Generation

MerchantLift BI generates core dimension tables before fact events.

Generated dimensions:

- `dim_category`
- `dim_location`
- `dim_segment`
- `dim_risk_rule`
- `dim_merchant`
- `dim_campaign`
- `dim_offer`
- `dim_cardmember_token`
- `dim_privacy_consent`
- `dim_date`

Run the generator:

```bash
python3 data_generation/generate_dimensions.py
```

## Transaction Event Generation

MerchantLift BI generates synthetic card-linked transaction events after core dimensions are created.

`fact_transactions` represents one synthetic card-linked purchase by one tokenized cardmember at one merchant at one timestamp.

The transaction generator uses:

- `dim_cardmember_token`
- `dim_merchant`
- `dim_category`
- category-level basket ranges
- simple seasonality rules
- transaction status distributions
- shopper behavior labels
- early test/control markers

Run the generator:

```bash
PYTHONPATH=src python data_generation/generate_transactions.py
```

## Offer Interaction Generation

MerchantLift BI generates the offer interaction funnel after dimensions and transactions are created.

Generated tables:

- `fact_offer_customer_assignment`
- `fact_offer_impressions`
- `fact_offer_activations`

The funnel is:

```text
assignment / eligibility
-> impression / exposure
-> activation / intent
```

Run the generator:

```bash
PYTHONPATH=src python data_generation/generate_transactions.py
```


## Redemption and Control-Group Event Generation

MerchantLift BI generates redemption and control-group events after transactions and offer interactions are created.

Generated tables:

```text
fact_offer_redemptions
fact_control_group_transactions
```

`fact_offer_redemptions` captures qualified offer-side spend.

`fact_control_group_transactions` captures baseline no-offer spend from similar control users.

The Day 18 generator enforces the rule:

```text
No redemption without a real transaction row.
```

If natural transaction-to-activation matches are insufficient in the local sample, the generator creates supplemental qualifying transactions and appends them to `fact_transactions` before creating redemptions.

Run the generator:

```bash
PYTHONPATH=src python data_generation/generate_redemptions_and_controls.py
```

Inspect generated outputs:

```bash
PYTHONPATH=src python scripts/inspect_generated_redemptions_and_controls.py
```

Documentation:

```text
docs/redemption_and_control_generation.md
```



## Financial, Fraud, and Reconciliation Event Generation

MerchantLift BI generates the financial, fraud, and reconciliation layer after redemptions and control-group transactions are created.

Generated tables:

- fact_reward_liability
- fact_merchant_settlements
- fact_fraud_risk_events
- fact_data_quality_reconciliation

These tables support:

- reward cost tracking
- merchant settlement analytics
- platform fee analytics
- merchant net-after-reward analysis
- fraud and offer-abuse monitoring
- financial reconciliation
- data quality validation

Run the generator:

PYTHONPATH=src python data_generation/generate_financial_risk_events.py

Inspect generated outputs:

PYTHONPATH=src python scripts/inspect_generated_financial_risk_events.py

Documentation:

docs/financial_risk_reconciliation_generation.md



## Raw Lake Generation Pipeline

MerchantLift BI includes a raw lake generation orchestrator that runs all synthetic data generators in dependency order.

The orchestrator is:

data_generation/generate_all.py

It runs:

1. data_generation/10_generate_dimensions.py
2. data_generation/20_generate_transactions.py
3. data_generation/30_generate_offer_interactions.py
4. data_generation/40_generate_testgroupredemptions_and_controlgrouptxs.py
5. data_generation/50_generate_rewardliability_settlements_fraudriskevents_reconciliations.py

Run the full raw lake generation pipeline:

PYTHONPATH=src python data_generation/generate_all.py

The orchestrator:

- runs all generators in dependency order
- stops immediately if a generator fails
- validates all expected raw Parquet outputs
- prints row counts for every generated table
- logs total elapsed time

Expected raw outputs include:

- dimension tables
- transaction facts
- offer assignment, impression, and activation facts
- redemption and control-group facts
- reward liability
- merchant settlements
- fraud-risk events
- data-quality reconciliation records

Documentation:

docs/raw_lake_generation_pipeline.md


## Partitioning and Performance Strategy

The lakehouse uses a simple performance rule:

```text
Partition by time.
Cluster by business lookup keys.
```

Large fact tables should be partitioned by event date.

Examples:

| Table | Partition Column |
|---|---|
| `fact_transactions` | `transaction_date` |
| `fact_offer_impressions` | `impression_date` |
| `fact_offer_activations` | `activation_date` |
| `fact_offer_redemptions` | `redemption_date` |
| `fact_reward_liability` | `liability_date` |
| `fact_merchant_settlements` | `settlement_date` |
| `fact_fraud_risk_events` | `event_date` |
| `fact_data_quality_reconciliation` | `reconciliation_date` |

Common clustering or Z-ordering keys include:

```text
merchant_id
offer_id
campaign_id
transaction_id
tokenized_cardmember_id
risk_rule_name
reconciliation_status
```

This layout helps Spark, Delta Lake, BigQuery, and dashboard workloads avoid unnecessary scans.

---

## Schema Enforcement and Evolution

MerchantLift BI treats schema as a contract.

The more trusted the layer, the stricter the schema.

```text
Raw can be flexible.
Bronze should detect.
Silver should enforce.
Gold should protect business meaning.
```

Allowed non-breaking changes include:

```text
adding nullable metadata columns
adding optional descriptive fields
adding new documented metric columns
adding approved enum values
```

Risky or breaking changes include:

```text
renaming columns
dropping columns
changing data types
changing table grain
changing metric definitions
changing primary key logic
```

The core rule is:

```text
Do not let accidental schema changes become business truth.
```

---

## Data Quality Gates

MerchantLift BI defines quality gates by layer.

A quality gate asks:

```text
Is this data good enough to move forward?
```

Quality gates become stricter as data moves downstream.

| Layer | Quality Gate Focus |
|---|---|
| Raw | File exists, file is readable, minimum row count, expected columns |
| Bronze | Ingestion metadata, row-count match, schema drift detection |
| Silver | Primary keys, required fields, accepted values, lineage, financial formulas |
| Gold | Business formulas, aggregation grain, privacy suppression, dashboard-safe outputs |

Examples of important checks:

```text
No redemption without a real transaction.
No control transaction without a real transaction.
No reward liability without a redemption.
No merchant settlement without a transaction.
No exposed small cohort in privacy-safe reporting.
```

Short version:

```text
Quality gates prevent bad events from becoming bad decisions.
```

---

## Metric Lineage

MerchantLift BI includes a lineage strategy so Gold metrics can be traced back to trusted Silver rows and Raw source files.

The core idea is:

```text
Gold stores the number.
Lineage proves the number.
```

The lineage design uses two planned tables:

```text
gold_metric_lineage_summary
gold_metric_lineage_detail
```

### Summary Lineage

Summary lineage records:

```text
gold_table_name
gold_row_id
source_table_name
source_row_count
source_role
join_type
join_keys
filter_condition
aggregation_formula
metric_definition_version
pipeline_run_id
```

This proves how many source rows and which transformation logic contributed to a Gold row.

### Detail Lineage

Detail lineage records exact source records:

```text
gold_table_name
gold_row_id
source_table_name
source_primary_key_column
source_primary_key_value
source_role
pipeline_run_id
```

This proves exactly which Silver rows created an aggregated Gold metric.

Example:

```text
gold.merchant_daily_economics
    <- silver.fact_transactions_clean
    <- silver.fact_reward_liability_clean
    <- silver.fact_merchant_settlements_clean
```

Lineage makes dashboard numbers auditable for finance, risk, compliance, and engineering review.

---

## Lakehouse Documentation

Detailed lakehouse design is documented in:

```text
docs/lakehouse_architecture.md
```

## Silver Cleaning Layer

MerchantLift BI includes a Spark/Delta Silver cleaning layer that transforms Bronze Delta tables into trusted Silver Delta tables.

The Silver layer performs:

```text
primary-key deduplication
numeric casting
timestamp normalization
date normalization
Silver transformation metadata
row-count validation
required-column validation
referential integrity validation
```

The core purpose of Silver is to answer:

```text
Can downstream analytics trust this row?
```

Silver tables are written under:

```text
data/lakehouse/silver/
```

Example:

```text
data/lakehouse/silver/fact_transactions_clean/
```

The Silver pipeline validates critical relationships such as:

```text
redemptions -> transactions
control transactions -> transactions
reward liabilities -> redemptions
settlements -> transactions
fraud events -> transactions
reconciliation checks -> transactions
```

This ensures that downstream Gold metrics for merchant economics, incrementality, reward liability, fraud/abuse, and reconciliation are built on coherent, trusted data.

Primary files:

```text
src/merchantlift/silver_config.py
spark_jobs/silver_transformations.py
```

Run locally:

```bash
PYTHONPATH=src python spark_jobs/silver_transformations.py
```


## Transaction-to-Offer Redemption Matching

MerchantLift BI includes a Spark/Delta redemption matching job that derives matched redemptions from trusted Silver data.

Primary job:

```text
spark_jobs/build_redemption_matching.py
```

Primary output:

```text
data/lakehouse/silver/fact_matched_offer_redemptions_clean/
```

The job reads:

```text
fact_transactions_clean
fact_offer_activations_clean
dim_offer_clean
```

It applies the following matching rules:

```text
same tokenized cardmember
same merchant
transaction timestamp after activation
transaction timestamp before offer expiry
transaction amount >= minimum spend
eligible transaction status
eligible activation status
```

It calculates reward using the current offer schema:

```text
calculated_reward_amount = reward_amount
```

It generates deterministic matched redemption IDs from:

```text
transaction_id
activation_id
offer_id
```

It deduplicates multiple possible matches per transaction using:

```text
highest reward amount
earliest activation timestamp
lowest offer_id
```

It writes the trusted matched output as a partitioned Silver Delta table.

Run locally:

```bash
PYTHONPATH=src python spark_jobs/build_redemption_matching.py
```

Run in Databricks:

```bash
cd /Workspace/Repos/<your-folder>/merchantlift-bi

export MERCHANTLIFT_LAKEHOUSE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse
export MERCHANTLIFT_BRONZE_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/bronze
export MERCHANTLIFT_SILVER_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/silver
export MERCHANTLIFT_GOLD_DIR=/dbfs/FileStore/merchantlift/data/lakehouse/gold

PYTHONPATH=src python spark_jobs/build_redemption_matching.py
```

