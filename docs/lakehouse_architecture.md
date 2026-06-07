# Lakehouse Architecture

## Purpose

MerchantLift BI uses a lakehouse architecture to transform raw synthetic merchant-offer events into trusted business-ready analytics tables.

The high-level flow is:

```text
Raw Parquet files
    -> Bronze Delta tables
    -> Silver Delta tables
    -> Gold Delta tables
    -> dbt / BigQuery marts
    -> Power BI dashboards
```

## Core Principle

Each layer has a different responsibility.

```text
Raw preserves source evidence.
Bronze registers data into the lakehouse.
Silver cleans and validates data.
Gold creates business-ready analytics.
```

## Why This Architecture Exists

MerchantLift BI analyzes merchant-funded offer performance, card-linked offer behavior, incrementality, reward liability, merchant profitability, fraud and offer abuse, and financial reconciliation.

Those analytics require trustworthy data movement from generated source events into governed reporting marts.

The lakehouse design creates a controlled path from raw event files to curated business metrics.

Without this layering, the platform would mix raw events, cleaning rules, and business metrics in the same place. That would make the system harder to debug, harder to audit, and less credible as a production-grade analytics platform.

## Business Context

The central business question is:

```text
Did the merchant offer create incremental profitable spend,
or did it simply subsidize customers who would have purchased anyway?
```

To answer that question, the platform must preserve and transform several categories of data:

- merchant attributes
- offer and campaign definitions
- cardmember token records
- privacy consent records
- transaction activity
- offer assignment, impression, and activation events
- redemption events
- control-group baseline transactions
- reward liability records
- merchant settlement records
- fraud and offer-abuse events
- reconciliation records

The lakehouse organizes these records so each downstream layer can answer a more trusted version of the business question.

## Layer Summary

| Layer | Main Job | Example |
|---|---|---|
| Raw | Preserve generated source files exactly as produced | `data/raw/fact_transactions/part-00000.parquet` |
| Bronze | Ingest raw files into Delta tables with light structure | `bronze.fact_transactions` |
| Silver | Clean, deduplicate, normalize, and validate relationships | `silver.fact_transactions_clean` |
| Gold | Build analytics-ready business tables | `gold.merchant_daily_economics` |


The lakehouse layers are not just storage folders. They represent increasing levels of trust, structure, and business usability.

```text
Raw    = evidence
Bronze = ingestion
Silver = trust
Gold   = business value
```


## Raw Layer

The raw layer stores generated Parquet files from the Python and Polars data generators.

The raw layer is treated as immutable source evidence.

Raw files should not contain business corrections or analytics logic.

Example raw paths:

```text
data/raw/dim_merchant/part-00000.parquet
data/raw/dim_offer/part-00000.parquet
data/raw/fact_transactions/part-00000.parquet
data/raw/fact_offer_redemptions/part-00000.parquet
data/raw/fact_reward_liability/part-00000.parquet
data/raw/fact_merchant_settlements/part-00000.parquet
data/raw/fact_fraud_risk_events/part-00000.parquet
data/raw/fact_data_quality_reconciliation/part-00000.parquet
```

### Raw Layer Rules

- Preserve generated source records as-is.
- Do not deduplicate records in raw.
- Do not apply business metrics in raw.
- Do not overwrite meaning in raw.
- Use raw for replay, debugging, lineage, and audit evidence.

## Bronze Layer

The bronze layer ingests raw Parquet files into Delta tables.

Bronze tables preserve source-level detail while adding ingestion metadata.

Typical metadata fields include:

- `ingestion_timestamp`
- `source_file_path`
- `pipeline_run_id`
- `record_hash`

Bronze is still close to raw, but it is now registered as lakehouse tables.

Example bronze tables:

```text
bronze.dim_merchant
bronze.dim_offer
bronze.fact_transactions
bronze.fact_offer_redemptions
bronze.fact_control_group_transactions
bronze.fact_reward_liability
bronze.fact_merchant_settlements
bronze.fact_fraud_risk_events
bronze.fact_data_quality_reconciliation
```

### Bronze Layer Rules

- Load every raw table into a corresponding bronze Delta table.
- Preserve row-level detail.
- Add ingestion metadata.
- Enforce basic schema readability.
- Avoid heavy business transformations.
- Keep bronze close enough to raw for traceability.

## Silver Layer

The silver layer creates cleaned and trusted tables.

Silver logic includes:

- type normalization
- timestamp normalization
- duplicate removal
- foreign-key style relationship validation
- transaction-to-offer consistency checks
- reward and settlement sanity checks
- null handling
- valid status checks
- valid date-window checks

Example silver tables:

```text
silver.dim_merchant_clean
silver.dim_offer_clean
silver.fact_transactions_clean
silver.fact_offer_activations_clean
silver.fact_offer_redemptions_clean
silver.fact_control_group_transactions_clean
silver.fact_reward_liability_clean
silver.fact_merchant_settlements_clean
silver.fact_fraud_risk_events_clean
silver.fact_data_quality_reconciliation_clean
```

### Silver Layer Rules

- Remove duplicate business event IDs.
- Normalize timestamps and dates.
- Validate required foreign-key relationships.
- Validate that redemption transaction IDs exist in transactions.
- Validate that control transaction IDs exist in transactions.
- Validate that reward liability rows point to redemption rows.
- Validate that merchant settlement rows point to transaction rows.
- Validate that fraud events point to transaction rows.
- Validate that reconciliation rows point to transaction rows.
- Keep row-level detail, but make it trustworthy.

## Gold Layer

The gold layer creates business-ready tables for downstream analytics.

Gold tables support:

- merchant economics
- offer performance
- incrementality
- reward liability
- fraud and offer-abuse analytics
- financial reconciliation
- privacy-safe reporting

Example gold tables:

```text
gold.merchant_daily_economics
gold.offer_daily_performance
gold.offer_incrementality_features
gold.reward_liability_daily
gold.offer_abuse_risk_daily
gold.data_quality_reconciliation_daily
gold.privacy_safe_offer_lift
```

### Gold Layer Rules

- Create business-facing tables.
- Aggregate when useful.
- Preserve key dimensions like merchant, campaign, offer, segment, and date.
- Avoid exposing raw tokenized cardmember-level data to broad reporting users.
- Prepare tables for dbt and BigQuery marts.
- Make metrics understandable for executive, merchant, finance, risk, and compliance stakeholders.

## Downstream Consumption

Gold Delta tables become the source for dbt and BigQuery reporting marts.

Those marts eventually support Power BI dashboards for:

- executive stakeholders
- merchant teams
- finance teams
- risk teams
- compliance teams

The downstream flow is:

```text
Gold Delta tables
    -> dbt models
    -> BigQuery governed marts
    -> Power BI dashboards
```

## Design Rule

Do not mix responsibilities across layers.

Raw should not clean.

Bronze should not calculate business metrics.

Silver should not become executive reporting.

Gold should not preserve raw ingestion noise.

Each layer should make the data more trustworthy and more useful than the previous layer.

## Lakehouse Folder Structure

MerchantLift BI uses a predictable folder structure so raw data, Delta tables, Spark jobs, dbt models, and reports remain separated.

Recommended structure:

```text
merchantlift-bi/
├── data/
│   ├── raw/
│   │   ├── dim_merchant/
│   │   ├── dim_offer/
│   │   ├── fact_transactions/
│   │   ├── fact_offer_redemptions/
│   │   └── ...
│   │
│   └── lakehouse/
│       ├── bronze/
│       │   ├── dim_merchant/
│       │   ├── dim_offer/
│       │   ├── fact_transactions/
│       │   ├── fact_offer_redemptions/
│       │   └── ...
│       │
│       ├── silver/
│       │   ├── dim_merchant_clean/
│       │   ├── dim_offer_clean/
│       │   ├── fact_transactions_clean/
│       │   ├── fact_offer_redemptions_clean/
│       │   └── ...
│       │
│       └── gold/
│           ├── merchant_daily_economics/
│           ├── offer_daily_performance/
│           ├── offer_incrementality_features/
│           ├── reward_liability_summary/
│           ├── offer_abuse_risk_summary/
│           └── reconciliation_health_daily/
```

# Table Naming Convention

## Raw Table Names

Raw tables keep the source-style names generated by Python/Polars.

Examples:

dim_merchant
dim_offer
fact_transactions
fact_offer_redemptions
fact_reward_liability
fact_merchant_settlements
fact_fraud_risk_events
fact_data_quality_reconciliation

Rules:

Use lowercase.
Use snake_case.
Use dim_ prefix for dimensions.
Use fact_ prefix for event/fact tables.
Keep names close to source business meaning.

## Bronze Table Names

Bronze tables mirror raw names.

Examples:

bronze.dim_merchant
bronze.dim_offer
bronze.fact_transactions
bronze.fact_offer_redemptions

Rules:

Use same table name as raw.
Do not add _clean.
Do not add business aggregation names.
Bronze means ingested, not cleaned.

## Silver Table Names

Silver cleaned tables use _clean suffix.

Examples:

silver.dim_merchant_clean
silver.dim_offer_clean
silver.fact_transactions_clean
silver.fact_offer_redemptions_clean
silver.fact_control_group_transactions_clean
silver.fact_reward_liability_clean
silver.fact_merchant_settlements_clean
silver.fact_fraud_risk_events_clean
silver.fact_data_quality_reconciliation_clean

Rules:

Use _clean for cleaned atomic tables.
Keep dim_ and fact_ prefixes.
Preserve table grain.
Do not use executive metric names here.
Silver is trusted row-level data.

## Gold Table Names

Gold tables use business-purpose names.

Examples:

gold.merchant_daily_economics
gold.offer_daily_performance
gold.offer_incrementality_features
gold.reward_liability_summary
gold.offer_abuse_risk_summary
gold.reconciliation_health_daily
gold.privacy_safe_offer_lift

Rules:

Use business-friendly names.
Usually do not use fact_ or dim_ prefixes.
Use suffixes like _daily, _summary, _features, or _health when useful.
Names should describe the business question the table answers.


## Lakehouse Table Inventory by Layer

This section defines the planned table inventory across Raw, Bronze, Silver, and Gold layers.

The goal is to make the lakehouse predictable before Spark and Delta Lake jobs are implemented.

The inventory follows this pattern:

Raw table
    -> Bronze Delta table
    -> Silver cleaned table
    -> Gold business table, when applicable

Not every raw table maps directly to one gold table.

Gold tables usually combine multiple trusted silver tables to answer a business question.

---

## Raw Layer Inventory

The raw layer stores generated Parquet files exactly as produced by the Python/Polars data generators.

Raw tables live under:

data/raw/<table_name>/part-00000.parquet

### Raw Dimension Tables

| Raw Table | Business Meaning | Expected Use |
|---|---|---|
| dim_category | Merchant category hierarchy and basket behavior | Join to merchants and transaction behavior |
| dim_location | Synthetic geography and ZIP/location attributes | Merchant and reporting geography |
| dim_segment | Customer segment definitions | Segment response and incrementality analysis |
| dim_risk_rule | Fraud and offer-abuse rule definitions | Fraud-risk event generation and reporting |
| dim_merchant | Merchant attributes, margin, fee, category, location | Merchant economics and settlement logic |
| dim_campaign | Campaign metadata and objectives | Offer performance and campaign analysis |
| dim_offer | Offer rules, reward logic, minimum spend, expiry | Activation, redemption, and reward logic |
| dim_cardmember_token | Tokenized synthetic cardmember identity | Transaction and offer interaction linkage |
| dim_privacy_consent | Consent flags for analytics and merchant reporting | Privacy-safe eligibility and reporting |
| dim_date | Calendar table | Date joins, daily reporting, partition logic |

### Raw Fact Tables

| Raw Table | Business Meaning | Expected Use |
|---|---|---|
| fact_transactions | Broad card-linked transaction universe | Merchant spend, redemption matching, settlement |
| fact_offer_customer_assignment | Offer test/control/holdout assignment | Incrementality and control-group design |
| fact_offer_impressions | Offers shown to eligible cardmembers | Offer funnel and exposure analysis |
| fact_offer_activations | Offers activated by cardmembers | Redemption eligibility and intent analysis |
| fact_offer_redemptions | Transactions matched to activated offers | Reward qualification and offer performance |
| fact_control_group_transactions | Baseline no-offer spend from control users | Incrementality baseline |
| fact_reward_liability | Reward cost created by redemptions | Reward liability and pacing |
| fact_merchant_settlements | Merchant settlement, platform fee, net after reward | Finance and merchant payout analysis |
| fact_fraud_risk_events | Fraud or offer-abuse risk annotations | Risk monitoring |
| fact_data_quality_reconciliation | Settlement and reward tie-out checks | Finance trust and data quality |

---

## Bronze Layer Inventory

The bronze layer ingests each raw Parquet table into a Delta table.

Bronze tables live under:

data/lakehouse/bronze/<table_name>/

Bronze tables mirror raw table names.

### Bronze Dimension Tables

| Bronze Table | Source Raw Table | Responsibility |
|---|---|---|
| bronze.dim_category | dim_category | Ingest category source records |
| bronze.dim_location | dim_location | Ingest location source records |
| bronze.dim_segment | dim_segment | Ingest segment source records |
| bronze.dim_risk_rule | dim_risk_rule | Ingest risk-rule source records |
| bronze.dim_merchant | dim_merchant | Ingest merchant source records |
| bronze.dim_campaign | dim_campaign | Ingest campaign source records |
| bronze.dim_offer | dim_offer | Ingest offer source records |
| bronze.dim_cardmember_token | dim_cardmember_token | Ingest tokenized cardmember records |
| bronze.dim_privacy_consent | dim_privacy_consent | Ingest consent records |
| bronze.dim_date | dim_date | Ingest calendar records |

### Bronze Fact Tables

| Bronze Table | Source Raw Table | Responsibility |
|---|---|---|
| bronze.fact_transactions | fact_transactions | Ingest transaction source events |
| bronze.fact_offer_customer_assignment | fact_offer_customer_assignment | Ingest assignment source events |
| bronze.fact_offer_impressions | fact_offer_impressions | Ingest impression source events |
| bronze.fact_offer_activations | fact_offer_activations | Ingest activation source events |
| bronze.fact_offer_redemptions | fact_offer_redemptions | Ingest redemption source events |
| bronze.fact_control_group_transactions | fact_control_group_transactions | Ingest control spend source events |
| bronze.fact_reward_liability | fact_reward_liability | Ingest reward liability source events |
| bronze.fact_merchant_settlements | fact_merchant_settlements | Ingest settlement source events |
| bronze.fact_fraud_risk_events | fact_fraud_risk_events | Ingest fraud-risk source events |
| bronze.fact_data_quality_reconciliation | fact_data_quality_reconciliation | Ingest reconciliation source events |

### Bronze Standard Metadata

Each bronze table should add ingestion metadata:

ingestion_timestamp
source_file_path
source_table_name
pipeline_run_id
record_hash

Bronze does not calculate business metrics.

Bronze only proves that source files landed and were registered into the lakehouse.

---

## Silver Layer Inventory

The silver layer creates cleaned, trusted, relationship-safe Delta tables.

Silver tables live under:

data/lakehouse/silver/<table_name>/

Silver cleaned atomic tables use _clean.

### Silver Dimension Tables

| Silver Table | Source Bronze Table | Main Cleaning / Validation Responsibility |
|---|---|---|
| silver.dim_category_clean | bronze.dim_category | Validate category IDs and basket rule ranges |
| silver.dim_location_clean | bronze.dim_location | Validate geography fields and ZIP/location attributes |
| silver.dim_segment_clean | bronze.dim_segment | Validate segment IDs and segment names |
| silver.dim_risk_rule_clean | bronze.dim_risk_rule | Validate active risk-rule definitions |
| silver.dim_merchant_clean | bronze.dim_merchant | Validate merchant IDs, category references, fee and margin rates |
| silver.dim_campaign_clean | bronze.dim_campaign | Validate campaign IDs, date windows, objectives |
| silver.dim_offer_clean | bronze.dim_offer | Validate offer IDs, campaign references, reward rules, minimum spend |
| silver.dim_cardmember_token_clean | bronze.dim_cardmember_token | Validate tokenized IDs and segment references |
| silver.dim_privacy_consent_clean | bronze.dim_privacy_consent | Validate consent flags and token references |
| silver.dim_date_clean | bronze.dim_date | Validate calendar completeness |

### Silver Fact Tables

| Silver Table | Source Bronze Table | Main Cleaning / Validation Responsibility |
|---|---|---|
| silver.fact_transactions_clean | bronze.fact_transactions | Deduplicate transaction IDs, normalize timestamps, validate amounts and merchant/cardmember references |
| silver.fact_offer_customer_assignment_clean | bronze.fact_offer_customer_assignment | Validate assignment groups, assignment status, offer and cardmember references |
| silver.fact_offer_impressions_clean | bronze.fact_offer_impressions | Validate impression IDs, assignment references, channel values |
| silver.fact_offer_activations_clean | bronze.fact_offer_activations | Validate activation timestamps, expiry timestamps, impression references |
| silver.fact_offer_redemptions_clean | bronze.fact_offer_redemptions | Validate redemption IDs, transaction lineage, activation lineage, reward amounts |
| silver.fact_control_group_transactions_clean | bronze.fact_control_group_transactions | Validate control transaction lineage, match group, match score, control assignment references |
| silver.fact_reward_liability_clean | bronze.fact_reward_liability | Validate reward amount, liability owner, funding split, redemption references |
| silver.fact_merchant_settlements_clean | bronze.fact_merchant_settlements | Validate platform fee formula, merchant net formula, transaction references |
| silver.fact_fraud_risk_events_clean | bronze.fact_fraud_risk_events | Validate risk scores, risk rules, transaction references |
| silver.fact_data_quality_reconciliation_clean | bronze.fact_data_quality_reconciliation | Validate reconciliation status, settlement deltas, transaction references |

### Important Silver Relationship Checks

Silver should enforce relationship safety before Gold is created.

Examples:

fact_offer_redemptions.transaction_id
    must exist in fact_transactions.transaction_id

fact_control_group_transactions.transaction_id
    must exist in fact_transactions.transaction_id

fact_reward_liability.redemption_id
    must exist in fact_offer_redemptions.redemption_id

fact_merchant_settlements.transaction_id
    must exist in fact_transactions.transaction_id

fact_fraud_risk_events.transaction_id
    must exist in fact_transactions.transaction_id

fact_data_quality_reconciliation.transaction_id
    must exist in fact_transactions.transaction_id

Silver is the layer where data becomes trusted enough for analytics engineering.

---

## Gold Layer Inventory

The gold layer creates business-ready Delta tables.

Gold tables live under:

data/lakehouse/gold/<table_name>/

Gold tables answer business questions.

Gold tables are built from multiple silver tables.

### Gold Business Tables

| Gold Table | Main Source Silver Tables | Business Question |
|---|---|---|
| gold.merchant_daily_economics | transactions, redemptions, reward liability, settlements, merchant | Are merchants generating profitable offer-driven economics? |
| gold.offer_daily_performance | impressions, activations, redemptions, offers, campaigns | How is each offer performing through the funnel? |
| gold.offer_incrementality_features | redemptions, control transactions, assignments, transactions | Did the offer create spend above the control baseline? |
| gold.reward_liability_summary | reward liability, redemptions, offers, merchants | How much reward cost has accrued and who funds it? |
| gold.merchant_settlement_summary | merchant settlements, transactions, merchants | How much was settled to merchants and what platform fee was earned? |
| gold.offer_abuse_risk_summary | fraud risk events, redemptions, transactions, risk rules | Which offers, merchants, or segments show abuse risk? |
| gold.reconciliation_health_daily | reconciliation, settlements, transactions | Can finance trust settlement and transaction tie-out? |
| gold.segment_response_summary | impressions, activations, redemptions, control transactions, segments | Which customer segments respond profitably? |
| gold.category_performance_summary | transactions, merchants, categories, redemptions | Which merchant categories perform best? |
| gold.privacy_safe_offer_lift | incrementality features, assignments, segments, merchants | What offer lift can be safely reported without exposing small cohorts? |

---

## Gold Table Details

### gold.merchant_daily_economics

Purpose:

Summarizes merchant-level economics by day.

Possible fields:

reporting_date
merchant_id
gross_spend
redeemed_spend
control_spend
reward_cost
merchant_funded_reward
platform_funded_reward
platform_fee_amount
merchant_settlement_amount
merchant_net_after_reward
estimated_incremental_spend
estimated_incremental_profit
roas

Business use:

Executive Merchant Economics Dashboard.

---

### gold.offer_daily_performance

Purpose:

Summarizes offer funnel performance by day.

Possible fields:

reporting_date
offer_id
campaign_id
merchant_id
impressions
activations
redemptions
activation_rate
redemption_rate
breakage_rate
gross_redeemed_spend
reward_cost
cost_per_redemption

Business use:

Offer Incrementality and Offer Performance dashboards.

---

### gold.offer_incrementality_features

Purpose:

Creates test/control feature table for lift analysis.

Possible fields:

campaign_id
offer_id
merchant_id
segment_id
test_customer_count
control_customer_count
test_spend
control_spend
test_avg_spend
control_avg_spend
lift_per_user
estimated_incremental_spend
reward_cost
platform_fee_amount
merchant_margin_rate
estimated_incremental_profit
cannibalization_risk_flag

Business use:

Incrementality modeling and dbt marts.

---

### gold.reward_liability_summary

Purpose:

Summarizes reward liability by merchant, offer, campaign, and date.

Possible fields:

reporting_date
merchant_id
campaign_id
offer_id
reward_amount
merchant_funded_amount
platform_funded_amount
liability_owner
accrued_liability
paid_liability
reversed_liability
remaining_liability

Business use:

Reward liability and pacing dashboard.

---

### gold.merchant_settlement_summary

Purpose:

Summarizes settlement economics.

Possible fields:

settlement_date
merchant_id
gross_transaction_amount
platform_fee_amount
merchant_settlement_amount
merchant_funded_amount
platform_funded_amount
merchant_net_after_reward
settlement_count

Business use:

Finance and merchant payout analytics.

---

### gold.offer_abuse_risk_summary

Purpose:

Summarizes fraud and offer-abuse risk.

Possible fields:

event_date
merchant_id
offer_id
risk_rule_name
risk_category
risk_severity
risk_event_count
average_risk_score
open_event_count
reviewed_event_count

Business use:

Fraud and Offer Abuse Dashboard.

---

### gold.reconciliation_health_daily

Purpose:

Summarizes finance reconciliation health.

Possible fields:

reconciliation_date
merchant_id
transaction_count
matched_count
mismatched_count
match_rate
total_settlement_delta
average_settlement_delta
large_delta_count

Business use:

Data Quality and Reconciliation Dashboard.

---

### gold.privacy_safe_offer_lift

Purpose:

Creates privacy-safe incrementality reporting.

This table should apply cohort suppression rules.

Possible fields:

reporting_date
merchant_id
campaign_id
offer_id
segment_id
protected_cohort_size
visible_test_spend
visible_control_spend
visible_incremental_spend
visible_reward_cost
is_suppressed
suppression_reason

Rules:

If cohort_size < 50:
    suppress or null sensitive metric outputs

Business use:

Privacy-Safe Analytics Dashboard.

---

## Inventory Summary by Layer

| Layer | Table Count | Main Purpose |
|---|---:|---|
| Raw | 20 | Preserve generated Parquet source events |
| Bronze | 20 | Register raw files as Delta tables with ingestion metadata |
| Silver | 20 | Clean, deduplicate, normalize, and validate row-level data |
| Gold | 10 | Produce business-ready analytics tables |

Total planned lakehouse tables:

Raw:    20
Bronze: 20
Silver: 20
Gold:   10

---


## Lakehouse Partitioning and Clustering Strategy

This section defines how MerchantLift BI should physically organize lakehouse tables so Spark and Delta Lake can read data efficiently.

The goal is not just to store data.

The goal is to store data in a way that makes common business queries fast, predictable, and cost-efficient.

MerchantLift BI eventually supports analytics across:

- merchant economics
- offer performance
- test/control incrementality
- reward liability
- merchant settlement
- fraud and offer abuse
- financial reconciliation
- privacy-safe reporting

Those workloads repeatedly filter by:

- date
- merchant
- offer
- campaign
- segment
- transaction status
- risk severity

Partitioning and clustering help the lakehouse avoid scanning unnecessary data.

---

## First-Principles Intuition

Imagine a library.

If every book is randomly placed on the floor, finding one topic is slow.

Partitioning is like putting books into major sections.

Clustering is like ordering books inside each section by useful labels.

In data terms:

```text
Partitioning = split files into large physical folders by a common filter
Clustering = colocate similar rows inside those folders for faster skipping
```

Simple version:

```text
Partitioning helps Spark skip folders.
Clustering helps Spark skip files inside folders.
```

---

## What Partitioning Means

Partitioning physically separates table data into folders.

Example:

```text
data/lakehouse/silver/fact_transactions_clean/transaction_date=2026-01-01/
data/lakehouse/silver/fact_transactions_clean/transaction_date=2026-01-02/
data/lakehouse/silver/fact_transactions_clean/transaction_date=2026-01-03/
```

If a query asks for:

```sql
WHERE transaction_date = '2026-01-02'
```

Spark can skip the other date folders.

This is called partition pruning.

---

## What Clustering Means

Clustering groups related rows together inside the table files.

For example, within a transaction date, we may want rows for the same merchant or offer to be physically close together.

That helps queries like:

```sql
WHERE transaction_date BETWEEN '2026-01-01' AND '2026-01-31'
  AND merchant_id = 'merchant_000045'
```

Clustering does not usually create folders the way partitioning does.

It improves file-level skipping and local data organization.

In Databricks Delta Lake, this may be implemented using:

- Z-ORDER
- liquid clustering
- optimized writes
- file compaction

For this project, we will document the intended clustering keys even if local execution does not fully use Databricks-specific optimization yet.

---

## Partitioning Design Principles

Partitioning should be based on fields that are:

1. Commonly used in filters
2. Coarse enough to avoid too many partitions
3. Stable and predictable
4. Useful for pruning large date ranges
5. Aligned with downstream reporting

Good partition columns are usually low-to-medium cardinality time buckets, such as:

- transaction_date for moderate-size daily tables
- transaction_month for very large multi-year tables
- impression_date or impression_month
- activation_date or activation_month
- redemption_date or redemption_month
- settlement_date or settlement_month
- event_date or event_month
- reconciliation_date or reconciliation_month
- reporting_date for gold daily marts

Dates are not automatically safe partition keys. Daily partitioning is reasonable when the table has enough rows per day and the retention window is manageable. For very large multi-year tables, monthly partitioning may be better. Never partition by full timestamp because it creates too many tiny partitions.

Risky partition columns:

```text
transaction_id
tokenized_cardmember_id
redemption_id
fraud_event_id
```

Why?

Those IDs have too many unique values.

High-cardinality partitioning creates too many tiny folders and files.

That makes Spark slower, not faster.

---

## Project-Level Partitioning Rule

For MerchantLift BI:

```text
Partition large fact tables by event date.
Do not partition by customer ID or transaction ID.
Use merchant_id, campaign_id, offer_id, and segment_id as clustering or join optimization keys.
```

Short version:

```text
Partition by time.
Cluster by business entity.
```

---

## Raw Layer Partitioning

The raw layer currently stores one local Parquet file per table:

```text
data/raw/<table_name>/part-00000.parquet
```

For the local version, this is acceptable.

For larger 10M+ scale generation, raw files should eventually be partitioned by table-specific event date.

Recommended future raw layout examples:

```text
data/raw/fact_transactions/transaction_date=2026-01-01/part-00000.parquet
data/raw/fact_offer_impressions/impression_date=2026-01-01/part-00000.parquet
data/raw/fact_offer_redemptions/redemption_date=2026-01-01/part-00000.parquet
```

Raw layer principle:

```text
Raw partitioning should support replay and ingestion efficiency, not business metrics.
```

---

## Bronze Layer Partitioning

Bronze tables should generally preserve source-level structure but add ingestion metadata.

Bronze partitioning should be simple.

Recommended Bronze partition strategy:

| Bronze Table Type | Partition Column |
|---|---|
| Date-based fact tables | Event date column |
| Small dimension tables | No partition |
| Slowly changing dimensions later | effective_start_date or no partition, depending on size |

Bronze should avoid over-partitioning.

Bronze is primarily for ingestion and replay.

---

## Silver Layer Partitioning

Silver is where partitioning becomes more important because Spark transformations and downstream joins rely on cleaned tables.

Recommended Silver partitioning:

| Silver Table | Recommended Partition Column | Secondary Optimization Keys |
|---|---|---|
| silver.fact_transactions_clean | transaction_date | merchant_id, tokenized_cardmember_id, transaction_status |
| silver.fact_offer_customer_assignment_clean | assignment_date | offer_id, campaign_id, assignment_group |
| silver.fact_offer_impressions_clean | impression_date | offer_id, campaign_id, merchant_id, channel |
| silver.fact_offer_activations_clean | activation_date | offer_id, campaign_id, merchant_id |
| silver.fact_offer_redemptions_clean | redemption_date | offer_id, campaign_id, merchant_id, transaction_id |
| silver.fact_control_group_transactions_clean | transaction_date | offer_id, campaign_id, merchant_id, match_group_id |
| silver.fact_reward_liability_clean | liability_date | merchant_id, offer_id, campaign_id, liability_owner |
| silver.fact_merchant_settlements_clean | settlement_date | merchant_id, transaction_id |
| silver.fact_fraud_risk_events_clean | event_date | merchant_id, risk_rule_id, risk_severity |
| silver.fact_data_quality_reconciliation_clean | reconciliation_date | merchant_id, reconciliation_status |

Small silver dimensions usually should not be partitioned.

Examples:

```text
silver.dim_merchant_clean
silver.dim_offer_clean
silver.dim_campaign_clean
silver.dim_segment_clean
silver.dim_risk_rule_clean
```

These are better handled as broadcast join candidates in Spark.

---

## Gold Layer Partitioning

Gold tables are designed for analytics and dashboards.

They should be partitioned by reporting date when they are time-series tables.

Recommended Gold partitioning:

| Gold Table | Recommended Partition Column | Secondary Optimization Keys |
|---|---|---|
| gold.merchant_daily_economics | reporting_date | merchant_id |
| gold.offer_daily_performance | reporting_date | offer_id, campaign_id, merchant_id |
| gold.offer_incrementality_features | campaign_id or reporting_date | offer_id, merchant_id, segment_id |
| gold.reward_liability_summary | reporting_date | merchant_id, offer_id, liability_owner |
| gold.merchant_settlement_summary | settlement_date | merchant_id |
| gold.offer_abuse_risk_summary | event_date | merchant_id, offer_id, risk_severity |
| gold.reconciliation_health_daily | reconciliation_date | merchant_id, reconciliation_status |
| gold.segment_response_summary | reporting_date | segment_id, merchant_id |
| gold.category_performance_summary | reporting_date | category_id |
| gold.privacy_safe_offer_lift | reporting_date | merchant_id, campaign_id, segment_id |

Gold table principle:

```text
Partition by dashboard time grain.
Cluster by dashboard filter dimensions.
```

---

## Table-Specific Strategy

### fact_transactions

Most common filters:

```text
transaction_date
merchant_id
transaction_status
tokenized_cardmember_id
```

Recommended:

```text
Partition by transaction_date.
Cluster by merchant_id and transaction_status.
Use tokenized_cardmember_id for joins, not partitioning.
```

Why:

Transactions are high-volume and most business analysis is time-window based.

---

### fact_offer_redemptions

Most common filters:

```text
redemption_date
offer_id
campaign_id
merchant_id
```

Recommended:

```text
Partition by redemption_date.
Cluster by offer_id, campaign_id, merchant_id.
```

Why:

Redemption analysis usually happens by offer/campaign over time.

---

### fact_control_group_transactions

Most common filters:

```text
transaction_date
campaign_id
offer_id
merchant_id
match_group_id
```

Recommended:

```text
Partition by transaction_date.
Cluster by campaign_id, offer_id, merchant_id.
```

Why:

Control spend is compared against test/redemption spend by campaign window.

---

### fact_reward_liability

Most common filters:

```text
liability_date
merchant_id
offer_id
liability_owner
```

Recommended:

```text
Partition by liability_date.
Cluster by merchant_id, offer_id, liability_owner.
```

Why:

Finance teams often analyze liability over time by merchant and funding owner.

---

### fact_merchant_settlements

Most common filters:

```text
settlement_date
merchant_id
settlement_status
```

Recommended:

```text
Partition by settlement_date.
Cluster by merchant_id and settlement_status.
```

Why:

Settlement reporting is usually daily and merchant-specific.

---

### fact_fraud_risk_events

Most common filters:

```text
event_date
risk_severity
risk_rule_name
merchant_id
```

Recommended:

```text
Partition by event_date.
Cluster by risk_severity, risk_rule_id, merchant_id.
```

Why:

Risk dashboards often filter by date, severity, rule, and merchant.

---

### fact_data_quality_reconciliation

Most common filters:

```text
reconciliation_date
reconciliation_status
merchant_id
```

Recommended:

```text
Partition by reconciliation_date.
Cluster by reconciliation_status and merchant_id.
```

Why:

Finance trust checks often ask which records are mismatched on a given day.

---

## Avoid Over-Partitioning

Over-partitioning is a common beginner mistake.

Bad partition choices:

```text
transaction_id
redemption_id
tokenized_cardmember_id
fraud_event_id
control_transaction_id
```

Why these are bad:

```text
They create too many tiny partitions.
They make metadata overhead high.
They slow down Spark planning.
They do not match common dashboard filters.
```

Correct approach:

```text
Use IDs for joins and clustering.
Use dates for partitions.
```

---

## Small File Problem

At scale, too many tiny files hurt Spark performance.

Symptoms:

```text
Queries are slow even though data volume is not huge.
Spark spends time listing files.
Task overhead is high.
Metadata operations become expensive.
```

Mitigation strategies:

```text
Write larger Parquet/Delta files.
Compact small files.
Use optimized writes where available.
Avoid excessive partition cardinality.
Batch writes by date or table.
```

Future Spark/Databricks optimization options:

```text
OPTIMIZE table
ZORDER BY merchant_id, offer_id
Auto Optimize
Liquid Clustering
```

For the local project, we document these choices first and implement them progressively when Spark jobs are introduced.

---

## Partitioning Strategy Summary

| Layer | Partitioning Strategy | Clustering Strategy |
|---|---|---|
| Raw | Simple local files first; future date partitioning for large facts | None initially |
| Bronze | Date partition large fact tables; no partition for small dimensions | Source table and ingestion metadata |
| Silver | Date partition large cleaned facts | Merchant, offer, campaign, status, severity |
| Gold | Reporting-date partition business tables | Dashboard filter dimensions |

---

## Practical Rule of Thumb

Use this rule when deciding partition keys:

```text
If analysts commonly filter by it and it has reasonable cardinality, consider partitioning.
If it has too many unique values, do not partition by it.
If it is useful for joins or filters but too granular for partitioning, cluster by it.
```

Examples:

```text
transaction_date -> good partition key
merchant_id -> good clustering key
tokenized_cardmember_id -> join key, not partition key
transaction_id -> identifier, not partition key
```

---
| Table                              | Partition Column      | Reason                                                               |
| ---------------------------------- | --------------------- | -------------------------------------------------------------------- |
| `fact_transactions`                | `transaction_date`    | Merchant spend and transaction queries are usually date-bounded      |
| `fact_offer_customer_assignment`   | none initially        | Assignment table is moderate size and often joined by offer/campaign |
| `fact_offer_impressions`           | `impression_date`     | Offer funnel reporting uses impression date                          |
| `fact_offer_activations`           | `activation_date`     | Activation trends use activation date                                |
| `fact_offer_redemptions`           | `redemption_date`     | Redemption, reward, and offer reporting use redemption date          |
| `fact_control_group_transactions`  | `transaction_date`    | Control spend is compared within campaign windows                    |
| `fact_reward_liability`            | `liability_date`      | Finance liability is reported by liability date                      |
| `fact_merchant_settlements`        | `settlement_date`     | Finance payout reporting uses settlement date                        |
| `fact_fraud_risk_events`           | `event_date`          | Risk monitoring is time-window based                                 |
| `fact_data_quality_reconciliation` | `reconciliation_date` | Reconciliation health is tracked by date                             |
| Dimension tables                   | none                  | Dimensions are smaller and usually joined by IDs                     |

| Table                              | Suggested Columns                                          |
| ---------------------------------- | ---------------------------------------------------------- |
| `fact_transactions`                | `merchant_id`, `tokenized_cardmember_id`, `transaction_id` |
| `fact_offer_customer_assignment`   | `offer_id`, `campaign_id`, `assignment_group`              |
| `fact_offer_impressions`           | `offer_id`, `campaign_id`, `tokenized_cardmember_id`       |
| `fact_offer_activations`           | `offer_id`, `campaign_id`, `tokenized_cardmember_id`       |
| `fact_offer_redemptions`           | `offer_id`, `merchant_id`, `transaction_id`                |
| `fact_control_group_transactions`  | `offer_id`, `merchant_id`, `match_group_id`                |
| `fact_reward_liability`            | `merchant_id`, `offer_id`, `liability_owner`               |
| `fact_merchant_settlements`        | `merchant_id`, `transaction_id`                            |
| `fact_fraud_risk_events`           | `merchant_id`, `risk_rule_name`, `risk_severity`           |
| `fact_data_quality_reconciliation` | `merchant_id`, `reconciliation_status`                     |

## Schema Enforcement and Schema Evolution Policy

A schema defines the expected structure of a table.

For MerchantLift BI, schema governance is important because raw generated events eventually flow into Spark/Delta tables, dbt models, BigQuery marts, and Power BI dashboards.

If table structure changes unexpectedly, downstream analytics can break.

The guiding principle is:

Raw can be flexible.
Bronze should detect.
Silver should enforce.
Gold should protect business meaning.

---

## Schema Enforcement by Layer

| Layer | Schema Strictness | Purpose |
|---|---|---|
| Raw | Flexible | Preserve generated source files as evidence |
| Bronze | Light enforcement | Register source data and detect structural issues |
| Silver | Strict enforcement | Create trusted cleaned data |
| Gold | Business-contract enforcement | Protect reporting and analytics logic |

---

## Raw Layer Schema Policy

The raw layer stores generated Parquet files as produced.

Raw files should remain close to source output.

Raw policy:

- Do not manually reshape raw files.
- Do not clean raw records.
- Do not silently drop columns.
- Do not silently rename columns.
- Preserve source-like structure.
- Use raw files as replayable evidence.

Raw is allowed to contain source-level imperfections.

Raw should not be used directly for executive reporting.

---

## Bronze Layer Schema Policy

Bronze tables ingest raw files into Delta tables.

Bronze applies light schema enforcement.

Bronze policy:

- Require expected core columns.
- Add ingestion metadata.
- Track source file path.
- Track pipeline run ID.
- Track ingestion timestamp.
- Detect unexpected missing columns.
- Allow new nullable columns only when explicitly approved.
- Avoid heavy business transformations.

Typical bronze metadata columns:

ingestion_timestamp
source_file_path
source_table_name
pipeline_run_id
record_hash

Bronze should answer:

Did the source file land and register correctly?

---

## Silver Layer Schema Policy

Silver tables are cleaned and trusted.

Silver applies strict schema enforcement.

Silver policy:

- Enforce required columns.
- Enforce expected data types.
- Enforce primary-key style uniqueness.
- Enforce accepted value domains.
- Enforce relationship checks.
- Normalize timestamps and dates.
- Reject or quarantine structurally invalid rows.
- Maintain stable column names for downstream transformations.

Examples:

transaction_id must be non-null
transaction_amount must be numeric
transaction_amount must be >= 0
transaction_status must be an accepted value
redemption.transaction_id must exist in transactions
reward_liability.redemption_id must exist in redemptions

Silver should answer:

Can downstream analytics trust this row?

---

## Gold Layer Schema Policy

Gold tables are business-facing analytics tables.

Gold applies business-contract enforcement.

Gold policy:

- Keep metric definitions stable.
- Keep dashboard-facing column names stable.
- Do not expose unnecessary sensitive identifiers.
- Enforce privacy-safe output rules where required.
- Add new business metrics carefully and document them.
- Avoid breaking Power BI, dbt, or BigQuery consumers.

Gold should answer:

Can business stakeholders trust and act on this metric?

---

## Schema Evolution Policy

Schema evolution means safely changing a table schema over time.

Not all schema changes are equal.

### Allowed Non-Breaking Changes

These changes are usually safe when documented:

| Change | Example |
|---|---|
| Add nullable column | Add source_event_type |
| Add metadata column | Add pipeline_run_id |
| Add optional descriptive field | Add merchant_display_name |
| Add derived metric in gold | Add net_margin_rate |
| Add new accepted enum value with documentation | Add transaction status reversed |

### Risky or Breaking Changes

These changes require migration planning:

| Change | Why It Is Risky |
|---|---|
| Rename a column | Breaks Spark/dbt/BigQuery/Power BI references |
| Drop a column | Breaks downstream models |
| Change data type | May break joins, filters, aggregations |
| Change metric meaning | Breaks business interpretation |
| Change table grain | Breaks row-count and aggregation logic |
| Change primary key logic | Breaks lineage and deduplication |

Examples of breaking changes:

merchant_id -> merchant_identifier
transaction_amount: float -> string
one row per transaction -> one row per merchant per day
reward_amount now means total reward instead of per-redemption reward

---

## Table Grain Must Be Protected

Table grain means what one row represents.

Examples:

| Table | Grain |
|---|---|
| fact_transactions | One row per transaction |
| fact_offer_customer_assignment | One row per cardmember-offer assignment |
| fact_offer_impressions | One row per offer impression |
| fact_offer_activations | One row per offer activation |
| fact_offer_redemptions | One row per qualifying redemption |
| fact_reward_liability | One row per redemption-created reward liability |
| fact_merchant_settlements | One row per transaction settlement |
| fact_fraud_risk_events | One row per fraud or risk event |
| fact_data_quality_reconciliation | One row per reconciliation check |

Changing table grain is a major breaking change.

For example, changing fact_transactions from:

one row per transaction

to:

one row per merchant per day

would break downstream joins and metrics.

Aggregations belong in Gold, not in raw facts.

---

## Recommended Schema Contract Fields

Each table should eventually have a schema contract documenting:

table_name
layer
grain
primary_key
required_columns
optional_columns
data_types
accepted_values
foreign_key_relationships
partition_column
sensitive_columns
schema_evolution_rules

Example contract for fact_transactions:

table_name: fact_transactions
grain: one row per transaction
primary_key: transaction_id
required_columns:
  - transaction_id
  - tokenized_cardmember_id
  - merchant_id
  - transaction_timestamp
  - transaction_date
  - transaction_amount
  - transaction_status
accepted_values:
  transaction_status:
    - authorized
    - settled
    - refunded
    - reversed
relationships:
  merchant_id -> dim_merchant.merchant_id
  tokenized_cardmember_id -> dim_cardmember_token.tokenized_cardmember_id
partition_column: transaction_date
sensitive_columns:
  - tokenized_cardmember_id

---

## Schema Drift Detection

Schema drift happens when incoming data structure changes unexpectedly.

Examples:

a required column disappears
a column changes type
a new column appears unexpectedly
an enum value changes
a timestamp format changes

Bronze should detect schema drift.

Silver should reject or quarantine invalid records.

Gold should not receive schema-drifted data.

Recommended handling:

Detect in Bronze.
Validate in Silver.
Protect in Gold.
Document approved changes.

---

## Quarantine Strategy

Invalid records should not silently disappear.

When Silver detects invalid records, the pipeline should eventually write them to a quarantine location.

Recommended future folder:

data/lakehouse/quarantine/

Example quarantine tables:

quarantine.fact_transactions_invalid
quarantine.fact_offer_redemptions_invalid
quarantine.fact_reward_liability_invalid
quarantine.fact_merchant_settlements_invalid

Quarantine records should include:

source_table_name
source_file_path
pipeline_run_id
validation_error_code
validation_error_message
record_payload
quarantined_at

This gives engineers a way to investigate bad data without corrupting trusted tables.

---

## Schema Enforcement Examples

### Transaction Amount

Rule:

transaction_amount must be numeric and >= 0

Why:

Negative or string transaction amounts can break spend calculations.

### Redemption Lineage

Rule:

fact_offer_redemptions.transaction_id
must exist in fact_transactions.transaction_id

Why:

No redemption should exist without a real transaction.

### Control Transaction Lineage

Rule:

fact_control_group_transactions.transaction_id
must exist in fact_transactions.transaction_id

Why:

Control spend should trace back to real transaction rows.

### Reward Funding Split

Rule:

merchant_funded_amount + platform_funded_amount = reward_amount

Why:

Finance must know who funded the reward.

### Settlement Formula

Rule:

merchant_settlement_amount =
gross_transaction_amount - platform_fee_amount

Why:

Finance and merchant payout numbers must tie out.

---

## Schema Evolution Approval Rule

Use this rule:

Additive nullable columns are usually allowed.
Renames, drops, type changes, grain changes, and metric-definition changes require migration planning.

Migration planning should include:

- reason for change
- affected tables
- affected downstream models
- backfill requirement
- dashboard impact
- validation plan
- rollback plan

---

Raw is evidence, not analytics truth.

---

## Bronze Layer Quality Gates

The bronze layer ingests raw Parquet into Delta tables.

Bronze answers:

```text
Did the platform ingest the source data correctly?
```

Bronze should preserve source detail but add ingestion metadata.

### Bronze Gate 1: Ingestion Metadata Exists

Each bronze table should include:

```text
ingestion_timestamp
source_file_path
source_table_name
pipeline_run_id
record_hash
```

Why this matters:

These fields make ingestion auditable and replayable.

Failure handling:

```text
Fail bronze ingestion if metadata columns are missing.
```

### Bronze Gate 2: Source Row Count Matches Bronze Row Count

Bronze should not silently drop rows.

Example:

```text
raw.fact_transactions row count
should equal
bronze.fact_transactions row count
```

Unless the ingestion job explicitly quarantines corrupt rows.

Failure handling:

```text
Fail if row counts differ without documented quarantine.
```

### Bronze Gate 3: Record Hash Exists

Each bronze row should have a record hash.

The record hash helps detect duplicate or changed source records.

Example:

```text
record_hash = sha256(canonical row payload)
```

Failure handling:

```text
Fail or warn depending on table criticality.
```

### Bronze Gate 4: Schema Drift Detection

Bronze should detect if source structure changes unexpectedly.

Examples:

```text
required column missing
unexpected type change
unexpected new column
timestamp format changed
```

Failure handling:

```text
Fail or quarantine depending on the configured schema evolution policy.
```

### Bronze Gate 5: Duplicate Source File Protection

The same source file should not be ingested twice into the same pipeline run.

Bronze should track:

```text
source_file_path
pipeline_run_id
```

Failure handling:

```text
Fail or skip duplicate source files.
```

### Bronze Layer Quality Summary

Bronze is not cleaned business data.

Bronze should prove:

```text
the source was ingested
the rows were preserved
the schema was detected
the ingestion is auditable
```

---

## Silver Layer Quality Gates

The silver layer creates trusted row-level data.

Silver answers:

```text
Can analytics trust this row?
```

Silver has the most important row-level quality gates.

### Silver Gate 1: Primary-Key Uniqueness

Each fact and dimension table should have a unique primary key.

Examples:

```text
fact_transactions.transaction_id is unique
fact_offer_redemptions.redemption_id is unique
fact_reward_liability.reward_liability_id is unique
fact_merchant_settlements.settlement_id is unique
fact_fraud_risk_events.fraud_event_id is unique
```

Failure handling:

```text
Deduplicate when safe.
Quarantine duplicates when unsafe.
Fail if primary-key uniqueness cannot be restored.
```

### Silver Gate 2: Required Fields Are Not Null

Critical identifiers should not be null.

Examples:

```text
transaction_id is not null
merchant_id is not null
offer_id is not null where applicable
campaign_id is not null where applicable
tokenized_cardmember_id is not null where applicable
```

Failure handling:

```text
Quarantine invalid rows.
Fail if null rate exceeds tolerance.
```

### Silver Gate 3: Accepted Values

Categorical fields should only contain approved values.

Examples:

```text
transaction_status in authorized, settled, refunded, reversed
assignment_group in test, control, holdout
assignment_status in eligible, ineligible
activation_status in active, expired, cancelled
liability_owner in merchant, platform, shared, none
reconciliation_status in matched, mismatched
```

Failure handling:

```text
Quarantine invalid rows.
Update schema contract if a new enum value is approved.
```

### Silver Gate 4: Numeric Sanity Checks

Financial and numeric fields should be valid.

Examples:

```text
transaction_amount >= 0
calculated_reward_amount >= 0
reward_amount >= 0
merchant_funded_amount >= 0
platform_funded_amount >= 0
platform_fee_amount >= 0
merchant_settlement_amount >= 0
match_quality_score between 0 and 1
risk_score between 0 and 1
```

Failure handling:

```text
Quarantine invalid rows.
Fail if financial formula checks are broken.
```

### Silver Gate 5: Timestamp and Date Consistency

Event timestamps and dates should align.

Examples:

```text
transaction_date = date(transaction_timestamp)
activation_date = date(activation_timestamp)
redemption_date = date(redemption_timestamp)
liability_date = date(liability_timestamp)
event_date = date(event_timestamp)
```

Failure handling:

```text
Normalize when deterministic.
Quarantine when ambiguous.
```

### Silver Gate 6: Redemption Lineage

Every redemption must point to a real transaction.

Rule:

```text
fact_offer_redemptions.transaction_id
must exist in
fact_transactions.transaction_id
```

Why this matters:

No redemption should exist without a real transaction.

Failure handling:

```text
Fail or quarantine orphan redemptions.
```

### Silver Gate 7: Control Transaction Lineage

Every control-group transaction should point to a real transaction.

Rule:

```text
fact_control_group_transactions.transaction_id
must exist in
fact_transactions.transaction_id
```

Why this matters:

Control baseline spend should be traceable to real transaction rows.

Failure handling:

```text
Fail or quarantine orphan control rows.
```

### Silver Gate 8: Reward Liability Lineage

Every reward liability should point to a real redemption.

Rule:

```text
fact_reward_liability.redemption_id
must exist in
fact_offer_redemptions.redemption_id
```

Why this matters:

Reward cost should only exist because a redemption occurred.

Failure handling:

```text
Fail or quarantine orphan liability rows.
```

### Silver Gate 9: Merchant Settlement Lineage

Every settlement should point to a real transaction.

Rule:

```text
fact_merchant_settlements.transaction_id
must exist in
fact_transactions.transaction_id
```

Failure handling:

```text
Fail or quarantine orphan settlement rows.
```

### Silver Gate 10: Fraud Event Lineage

Every fraud-risk event should point to a real transaction.

Rule:

```text
fact_fraud_risk_events.transaction_id
must exist in
fact_transactions.transaction_id
```

If redemption_id is populated, it should exist in redemptions.

Failure handling:

```text
Fail or quarantine invalid fraud events.
```

### Silver Gate 11: Reward Funding Split

Reward funding should tie out.

Rule:

```text
merchant_funded_amount + platform_funded_amount = reward_amount
```

Tolerance:

```text
absolute difference <= 0.01
```

Failure handling:

```text
Fail finance-quality validation.
```

### Silver Gate 12: Merchant Settlement Formula

Settlement math should tie out.

Rules:

```text
platform_fee_amount =
gross_transaction_amount * platform_fee_rate

merchant_settlement_amount =
gross_transaction_amount - platform_fee_amount

merchant_net_after_reward =
merchant_settlement_amount - merchant_funded_amount
```

Tolerance:

```text
absolute difference <= 0.01
```

Failure handling:

```text
Fail finance-quality validation.
```

### Silver Gate 13: Reconciliation Formula

Reconciliation should correctly calculate settlement delta.

Rule:

```text
settlement_delta =
transaction_amount
- merchant_settlement_amount
- platform_fee_amount
```

Expected clean value:

```text
settlement_delta near 0
```

Failure handling:

```text
Flag mismatched rows.
Do not hide mismatches.
```

Important:

A reconciliation mismatch is not always a pipeline failure.

It may represent a real business issue, such as:

```text
missing settlement
late-arriving transaction
refund after reward
settlement discrepancy
```

### Silver Gate 14: Privacy Consent Eligibility

Rows used for merchant reporting should respect consent flags.

Example:

```text
merchant_reporting_consent_flag = true
```

should be required before user-level data contributes to merchant-facing reporting.

Failure handling:

```text
Exclude from merchant-facing reporting.
Flag or quarantine if consent is missing unexpectedly.
```

### Silver Layer Quality Summary

Silver should prove:

```text
IDs are unique.
Required fields exist.
Types are correct.
Values are valid.
Lineage is intact.
Financial formulas tie out.
Privacy flags are respected.
```

Silver is the main trust layer.

---

## Gold Layer Quality Gates

The gold layer creates business-ready analytics tables.

Gold answers:

```text
Can business stakeholders act on this metric?
```

Gold quality checks focus on metric correctness, aggregation logic, and privacy-safe outputs.

### Gold Gate 1: Metric Formula Validation

Business metrics should follow documented formulas.

Examples:

```text
activation_rate = activations / impressions
redemption_rate = redemptions / activations
breakage_rate = 1 - redemption_rate
lift_per_user = test_avg_spend - control_avg_spend
estimated_incremental_spend = lift_per_user * test_customer_count
platform_fee_amount = sum(platform_fee_amount)
reward_cost = sum(reward_amount)
```

Failure handling:

```text
Fail gold model validation if formulas do not match expected definitions.
```

### Gold Gate 2: Aggregation Grain Validation

Gold tables must preserve their documented grain.

Examples:

```text
merchant_daily_economics:
one row per merchant per reporting_date

offer_daily_performance:
one row per offer per reporting_date

reconciliation_health_daily:
one row per merchant per reconciliation_date
```

Failure handling:

```text
Fail if duplicate rows exist at the declared grain.
```

### Gold Gate 3: Test / Control Integrity

Incrementality tables must separate test and control behavior correctly.

Checks:

```text
test_customer_count > 0
control_customer_count > 0
test and control groups are not mixed
control users did not receive impressions
control spend comes from control assignments
```

Failure handling:

```text
Fail or flag incrementality table as invalid.
```

### Gold Gate 4: Cannibalization Risk Logic

Cannibalization flags should follow documented rules.

Example:

```text
if estimated_incremental_profit < 0:
    cannibalization_risk_flag = true
```

Other possible signals:

```text
low lift_per_user
high reward_cost
test spend close to control spend
reward cost greater than incremental margin
```

Failure handling:

```text
Fail if risk flag logic is inconsistent with documented formula.
```

### Gold Gate 5: Privacy-Safe Cohort Suppression

Privacy-safe reporting should suppress small cohorts.

Rule:

```text
if cohort_size < 50:
    suppress sensitive metrics
```

Example suppressed fields:

```text
visible_test_spend
visible_control_spend
visible_incremental_spend
visible_reward_cost
protected_cohort_size
```

Failure handling:

```text
Fail privacy-safe mart validation if small cohorts are exposed.
```

### Gold Gate 6: No Unnecessary Sensitive Identifiers

Gold tables used for broad analytics should not expose unnecessary row-level identifiers.

Avoid exposing these in broad reporting marts:

```text
tokenized_cardmember_id
transaction_id
activation_id
redemption_id
```

Exception:

Secure internal audit or lineage tables may retain identifiers with restricted access.

Failure handling:

```text
Flag or fail privacy/governance validation depending on mart type.
```

### Gold Gate 7: Reconciliation Health Thresholds

Finance-facing gold tables should summarize reconciliation quality.

Example metrics:

```text
match_rate
mismatched_count
total_settlement_delta
large_delta_count
```

Possible quality expectation:

```text
match_rate >= configured threshold
```

Failure handling:

```text
Warn or fail depending on finance reporting policy.
```

### Gold Layer Quality Summary

Gold should prove:

```text
metrics are correct
aggregations preserve declared grain
privacy rules are enforced
business definitions are stable
dashboards can be trusted
```

Gold is the business trust layer.

---

## Quality Gate Severity Levels

Not every issue should be handled the same way.

Recommended severity model:

| Severity | Meaning | Example | Action |
|---|---|---|---|
| Critical | Data cannot be trusted | Missing transaction_id, broken primary key, orphan redemption | Fail pipeline |
| High | Data may corrupt analytics | Invalid reward formula, missing merchant reference | Quarantine or fail |
| Medium | Business review needed | Settlement mismatch, high fraud risk | Flag and report |
| Low | Informational | New nullable column, minor row-count variance | Warn and document |

---

## Promotion Rules

Data should only move forward when it passes the correct gate.

### Raw to Bronze

Promote when:

```text
file exists
file is readable
minimum row count passes
core columns exist
```

### Bronze to Silver

Promote when:

```text
schema is valid
required fields exist
duplicates are handled
types are normalized
relationships are valid or invalid rows are quarantined
```

### Silver to Gold

Promote when:

```text
trusted facts and dimensions are available
business formulas can be calculated
metric grain is correct
privacy rules can be enforced
```

### Gold to Reporting

Promote when:

```text
metrics are validated
cohort suppression is applied
sensitive identifiers are removed or protected
business definitions are documented
```

---

## Recommended Future Quality Tooling

MerchantLift BI should use multiple validation layers.

### Great Expectations

Best for:

```text
raw and bronze data contracts
column existence
row counts
null checks
accepted values
numeric ranges
```

### Spark Validation Logic

Best for:

```text
large-scale relationship checks
deduplication
lineage validation
financial formula validation
quarantine table creation
```

### dbt Tests

Best for:

```text
semantic model tests
relationship tests
unique/not_null tests
accepted values
business metric tests
```

### BigQuery Governance Tests

Best for:

```text
row-level security validation
policy tag validation
privacy-safe reporting validation
audit queries
```

---

## Example Quality Checks by Table

### fact_transactions

Checks:

```text
transaction_id unique
transaction_id not null
merchant_id not null
tokenized_cardmember_id not null
transaction_amount >= 0
transaction_status in accepted values
transaction_date = date(transaction_timestamp)
```

### fact_offer_redemptions

Checks:

```text
redemption_id unique
transaction_id exists in fact_transactions
activation_id exists in fact_offer_activations
calculated_reward_amount >= 0
redemption_status in accepted values
```

### fact_control_group_transactions

Checks:

```text
control_transaction_id unique
transaction_id exists in fact_transactions
control_assignment_id exists in fact_offer_customer_assignment
match_quality_score between 0 and 1
transaction_amount >= 0
```

### fact_reward_liability

Checks:

```text
reward_liability_id unique
redemption_id exists in fact_offer_redemptions
reward_amount >= 0
liability_owner in merchant, platform, shared
merchant_funded_amount + platform_funded_amount = reward_amount
```

### fact_merchant_settlements

Checks:

```text
settlement_id unique
transaction_id exists in fact_transactions
gross_transaction_amount >= 0
platform_fee_amount >= 0
merchant_settlement_amount = gross_transaction_amount - platform_fee_amount
merchant_net_after_reward = merchant_settlement_amount - merchant_funded_amount
```

### fact_fraud_risk_events

Checks:

```text
fraud_event_id unique
transaction_id exists in fact_transactions
risk_rule_id exists in dim_risk_rule
risk_score between 0 and 1
risk_event_status in accepted values
```

### fact_data_quality_reconciliation

Checks:

```text
reconciliation_id unique
transaction_id exists in fact_transactions
settlement_delta calculated correctly
reconciliation_status in matched, mismatched
```

---

# Data lineage

Lineage means:

Can I trace a Gold metric back to the trusted Silver rows, Bronze ingestion records, and Raw source files that created it?

The core idea is:

Gold stores the number.
Lineage proves the number.

For example, if a Gold table says:

merchant_daily_economics.reward_cost = 4,000

lineage should prove:

- which reward liability rows contributed
- which redemptions created those liabilities
- which transactions created those redemptions
- which raw files originally produced the source records
- which pipeline run transformed them

The recommended design is to use:

1. Lightweight metadata on Gold tables
2. gold_metric_lineage_summary
3. gold_metric_lineage_detail

Summary lineage proves source tables, row counts, join types, join keys, filters, and formulas.

Detail lineage proves exact source record IDs.

Short memory hook:

Lineage turns a dashboard number into a provable story.

============================================================
SECTION TO PASTE
============================================================

## Lineage from Raw Facts to Gold Marts

Lineage explains how data moves from source events to business metrics.

For MerchantLift BI, lineage answers:

```text
Where did this business number come from?
Which source rows created it?
Which transformations touched it?
Can finance, risk, compliance, and merchant teams trust it?
```

The guiding principle is:

```text
Every important Gold metric should be explainable backward to trusted Silver rows and ultimately to Raw source files.
```

---

## Lineage by Layer

| Layer | Lineage Question | Mechanism |
|---|---|---|
| Raw | What did the generator produce? | Stable business IDs and raw file paths |
| Bronze | Which raw file was ingested? | source_file_path, pipeline_run_id, record_hash |
| Silver | Which rows passed validation? | Preserved IDs, quality status, validation metadata |
| Gold | Which trusted rows created the metric? | Lineage summary and lineage detail tables |

---

## Raw Lineage

Raw lineage preserves original generated source events.

Raw tables keep stable identifiers such as:

```text
transaction_id
assignment_id
impression_id
activation_id
redemption_id
reward_liability_id
settlement_id
fraud_event_id
reconciliation_id
merchant_id
offer_id
campaign_id
tokenized_cardmember_id
```

Raw answers:

```text
What event was generated?
```

Example:

```text
data/raw/fact_transactions/part-00000.parquet
```

contains transaction rows with stable transaction_id values.

Raw lineage is the starting evidence trail.

---

## Bronze Lineage

Bronze lineage proves which raw file was ingested into a Delta table.

Bronze should add technical metadata:

```text
source_file_path
source_table_name
pipeline_run_id
ingestion_timestamp
record_hash
```

Bronze answers:

```text
Which raw file did this ingested row come from?
When was it ingested?
Which pipeline run created it?
Has the record changed?
```

Example:

```text
bronze.fact_transactions.transaction_id = tx_000001
source_file_path = data/raw/fact_transactions/part-00000.parquet
pipeline_run_id = run_2026_03_01_001
```

Bronze lineage is technical ingestion lineage.

It proves:

```text
raw file -> bronze row -> pipeline run
```

---

## Silver Lineage

Silver lineage proves that a row was cleaned, validated, and trusted.

Silver should preserve source business identifiers and add transformation metadata.

Recommended Silver metadata:

```text
bronze_record_hash
silver_transformed_at
silver_pipeline_run_id
quality_status
validation_rule_version
```

Silver answers:

```text
Did this row pass validation?
Which Bronze row did it come from?
Which validation version approved it?
```

Examples:

```text
silver.fact_offer_redemptions_clean.transaction_id
must exist in
silver.fact_transactions_clean.transaction_id
```

```text
silver.fact_reward_liability_clean.redemption_id
must exist in
silver.fact_offer_redemptions_clean.redemption_id
```

Silver lineage is the trust bridge between ingested data and business metrics.

It proves:

```text
bronze row -> validated silver row -> trusted downstream input
```

---

## Gold Lineage

Gold lineage explains which trusted Silver rows created a business metric.

Gold tables should include lightweight metric metadata.

Recommended Gold metadata:

```text
gold_row_id
gold_pipeline_run_id
metric_definition_version
aggregation_grain
created_at
```

But detailed row-level proof should live in dedicated lineage tables.

MerchantLift BI should use two lineage tables:

```text
gold_metric_lineage_summary
gold_metric_lineage_detail
```

---

## Why Gold Needs Separate Lineage Tables

Gold tables are aggregated.

A single Gold row may be created from many Silver rows.

Example:

```text
5 transaction rows
joined with
15 reward liability rows
to produce
1 merchant_daily_economics row
```

If the Gold table only stores the final metric, you cannot prove which exact Silver records created it.

That is why lineage tables are needed.

The Gold table stores the business metric.

The lineage tables store the proof.

---

## Gold Lineage Summary Table

The summary table stores one row per Gold row per source Silver table.

Purpose:

```text
Show which source tables contributed, how many rows contributed, and what join/filter logic was used.
```

Recommended schema:

```text
gold_table_name
gold_row_id
gold_pipeline_run_id
metric_definition_version
source_table_name
source_row_count
source_role
join_type
join_keys
filter_condition
aggregation_formula
created_at
```

Example:

```text
gold_table_name: gold.merchant_daily_economics
gold_row_id: gme_2026_03_01_merchant_001
gold_pipeline_run_id: run_2026_03_01_001
metric_definition_version: merchant_economics_v1
source_table_name: silver.fact_transactions_clean
source_row_count: 5
source_role: gross_spend_input
join_type: inner
join_keys: merchant_id, transaction_date
filter_condition: transaction_status = settled
aggregation_formula: sum(transaction_amount)
```

This proves:

```text
The Gold row used 5 transaction rows as gross spend input.
```

---

## Gold Lineage Detail Table

The detail table stores one row per Silver source record used to create a Gold row.

Purpose:

```text
Show exactly which Silver records contributed to a Gold metric.
```

Recommended schema:

```text
gold_table_name
gold_row_id
gold_pipeline_run_id
source_table_name
source_primary_key_column
source_primary_key_value
source_role
created_at
```

Example:

```text
gold_table_name: gold.merchant_daily_economics
gold_row_id: gme_2026_03_01_merchant_001
gold_pipeline_run_id: run_2026_03_01_001
source_table_name: silver.fact_transactions_clean
source_primary_key_column: transaction_id
source_primary_key_value: tx_000001
source_role: gross_spend_input
```

This proves:

```text
Transaction tx_000001 contributed to this Gold row.
```

---

## Example: 5 Silver Rows Joined with 15 Silver Rows

Suppose one Gold row is created by joining:

```text
5 rows from silver.fact_transactions_clean
with
15 rows from silver.fact_reward_liability_clean
```

using an inner join on:

```text
transaction_id
```

The Gold row might look like:

```text
gold_table_name: gold.merchant_daily_economics
gold_row_id: gme_2026_03_01_merchant_001
merchant_id: merchant_001
reporting_date: 2026-03-01
gross_spend: 500.00
reward_cost: 40.00
merchant_net_after_reward: 450.00
```

Summary lineage would contain:

```text
gold_row_id: gme_2026_03_01_merchant_001
source_table_name: silver.fact_transactions_clean
source_row_count: 5
source_role: gross_spend_input
join_type: inner
join_keys: transaction_id
aggregation_formula: sum(transaction_amount)
```

and:

```text
gold_row_id: gme_2026_03_01_merchant_001
source_table_name: silver.fact_reward_liability_clean
source_row_count: 15
source_role: reward_cost_input
join_type: inner
join_keys: transaction_id
aggregation_formula: sum(reward_amount)
```

Detail lineage would list exact records:

```text
gold_row_id                         source_table_name                       source_primary_key_value  source_role
gme_2026_03_01_merchant_001          silver.fact_transactions_clean           tx_001                    gross_spend_input
gme_2026_03_01_merchant_001          silver.fact_transactions_clean           tx_002                    gross_spend_input
gme_2026_03_01_merchant_001          silver.fact_transactions_clean           tx_003                    gross_spend_input
gme_2026_03_01_merchant_001          silver.fact_transactions_clean           tx_004                    gross_spend_input
gme_2026_03_01_merchant_001          silver.fact_transactions_clean           tx_005                    gross_spend_input
gme_2026_03_01_merchant_001          silver.fact_reward_liability_clean       rew_001                   reward_cost_input
gme_2026_03_01_merchant_001          silver.fact_reward_liability_clean       rew_002                   reward_cost_input
```

This proves:

```text
which exact Silver records created the aggregate number
```

and:

```text
how the records were joined and aggregated
```

---

## Example: Merchant Daily Economics Lineage

Gold table:

```text
gold.merchant_daily_economics
```

Gold row:

```text
gold_row_id = gme_2026_03_01_merchant_001
merchant_id = merchant_001
reporting_date = 2026-03-01
gross_spend = 500.00
reward_cost = 40.00
merchant_net_after_reward = 450.00
```

Possible source Silver tables:

```text
silver.fact_transactions_clean
silver.fact_offer_redemptions_clean
silver.fact_reward_liability_clean
silver.fact_merchant_settlements_clean
silver.dim_merchant_clean
```

Summary lineage example:

```text
gold_row_id: gme_2026_03_01_merchant_001
source_table_name: silver.fact_transactions_clean
source_row_count: 5
source_role: gross_spend_input
join_type: inner
join_keys: merchant_id, reporting_date
aggregation_formula: sum(transaction_amount)
```

```text
gold_row_id: gme_2026_03_01_merchant_001
source_table_name: silver.fact_reward_liability_clean
source_row_count: 2
source_role: reward_cost_input
join_type: left
join_keys: transaction_id
aggregation_formula: sum(reward_amount)
```

Detail lineage example:

```text
gold_row_id | source_table_name                 | source_primary_key_value | source_role
gme_001     | silver.fact_transactions_clean     | tx_001                   | gross_spend_input
gme_001     | silver.fact_transactions_clean     | tx_002                   | gross_spend_input
gme_001     | silver.fact_reward_liability_clean | rew_001                  | reward_cost_input
gme_001     | silver.fact_reward_liability_clean | rew_002                  | reward_cost_input
```

---

## Example: Offer Incrementality Lineage

Gold table:

```text
gold.offer_incrementality_features
```

Business metric:

```text
estimated_incremental_spend
```

Formula:

```text
test_avg_spend = test_spend / test_customer_count
control_avg_spend = control_spend / control_customer_count
lift_per_user = test_avg_spend - control_avg_spend
estimated_incremental_spend = lift_per_user * test_customer_count
```

Source Silver tables:

```text
silver.fact_transactions_clean
silver.fact_offer_redemptions_clean
silver.fact_control_group_transactions_clean
silver.fact_offer_customer_assignment_clean
silver.dim_offer_clean
silver.dim_merchant_clean
```

Lineage proves:

```text
which test transactions created test_spend
which control transactions created control_spend
which assignments created test/control membership
which offer and merchant defined the business context
```

Summary lineage might show:

```text
gold_row_id: oif_merchant_001_offer_020_segment_003
source_table_name: silver.fact_offer_redemptions_clean
source_row_count: 120
source_role: test_redeemed_spend_input
join_type: inner
join_keys: offer_id, merchant_id, tokenized_cardmember_id
aggregation_formula: sum(transaction_amount)
```

```text
gold_row_id: oif_merchant_001_offer_020_segment_003
source_table_name: silver.fact_control_group_transactions_clean
source_row_count: 150
source_role: control_baseline_spend_input
join_type: inner
join_keys: offer_id, merchant_id, segment_id
aggregation_formula: sum(transaction_amount)
```

---

## Example: Reconciliation Health Lineage

Gold table:

```text
gold.reconciliation_health_daily
```

Business metric:

```text
match_rate
```

Formula:

```text
match_rate = matched_count / total_reconciliation_count
```

Source Silver table:

```text
silver.fact_data_quality_reconciliation_clean
```

Lineage proves:

```text
which reconciliation rows counted as matched
which rows counted as mismatched
which transaction_ids had settlement deltas
```

This is especially important for finance and audit.

---

## Lineage and Privacy

Lineage tables may contain sensitive row-level identifiers.

Examples:

```text
transaction_id
tokenized_cardmember_id
redemption_id
```

Governance rules:

```text
Do not expose detailed lineage tables broadly.
Restrict detailed lineage to data engineering, audit, and compliance roles.
Use Gold marts for business users.
Use privacy-safe marts for merchant-facing analytics.
```

Gold business tables may hide row-level identifiers, but lineage tables can retain them under stricter access control.

---

## Lineage Implementation Guidance

In Spark, lineage rows can be generated during Gold table construction.

For each Gold aggregate:

1. Create the Gold row with a stable gold_row_id.
2. Collect source table names and source row counts.
3. Write one or more rows to gold_metric_lineage_summary.
4. For audit-sensitive metrics, write source primary keys to gold_metric_lineage_detail.
5. Include pipeline_run_id and metric_definition_version.
6. Store the Gold table and lineage tables in the same pipeline run.

The key design rule:

```text
Create the metric and its lineage in the same transformation job.
```

This keeps the business metric and its proof synchronized.

---


## Silver Layer Implementation Notes

The Silver layer is implemented as a config-driven Spark transformation pipeline.

The configuration is stored in:

```text
src/merchantlift/silver_config.py
```

The transformation job is stored in:

```text
spark_jobs/silver_transformations.py
```

The job reads Bronze Delta tables from:

```text
data/lakehouse/bronze/
```

and writes Silver Delta tables to:

```text
data/lakehouse/silver/
```

Silver tables use the `_clean` suffix.

Example:

```text
bronze.fact_transactions
    -> silver.fact_transactions_clean
```

The Silver job applies common cleaning rules across all configured tables:

```text
drop null primary keys
deduplicate by primary key
cast configured numeric columns
normalize configured timestamp columns
normalize configured date columns
add Silver transformation metadata
```

After writing Silver tables, the job validates:

```text
read-back row counts
required columns
Silver metadata
critical parent-child relationships
```

The critical referential checks ensure that child fact tables remain connected to parent transaction and redemption records.



