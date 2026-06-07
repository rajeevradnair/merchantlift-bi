 
# Silver Cleaning Layer

## Purpose

The Silver cleaning layer transforms Bronze Delta tables into trusted, typed, deduplicated, relationship-safe Silver Delta tables.

The Bronze layer preserves raw-like ingested data with ingestion metadata.

The Silver layer creates data that downstream Spark jobs can safely use for Gold business metrics.

Silver answers this question:

```text
Can downstream analytics trust this row?
```

The Silver layer is not responsible for final business KPIs such as ROAS, incremental spend, net merchant profit, or cannibalization risk.

Those calculations belong in the Gold layer.

---

## Pipeline Flow

The Silver transformation flow is:

```text
data/lakehouse/bronze/<table_name>/
        ↓
spark_jobs/silver_transformations.py
        ↓
data/lakehouse/silver/<table_name>_clean/
```

Example:

```text
data/lakehouse/bronze/fact_transactions/
        ↓
data/lakehouse/silver/fact_transactions_clean/
```

---

## Silver Layer Responsibilities

The Silver layer performs structural and row-level trust work.

Responsibilities:

- read Bronze Delta tables
- remove rows with null primary keys
- deduplicate rows by primary key
- cast configured numeric columns
- normalize configured timestamp columns
- normalize configured date columns
- preserve Bronze ingestion metadata
- add Silver transformation metadata
- write Silver Delta tables
- validate row counts
- validate required columns
- validate referential relationships across key fact tables

---

## Silver Metadata Columns

Every Silver table should include:

```text
silver_transformed_at
silver_pipeline_run_id
quality_status
validation_rule_version
```

### `silver_transformed_at`

The timestamp when the row was transformed into the Silver layer.

### `silver_pipeline_run_id`

The unique identifier of the Silver transformation run.

Example:

```text
silver_run_20260607_130000
```

### `quality_status`

The row-level quality status.

Current value:

```text
passed
```

Future values may include:

```text
failed
quarantined
warning
```

### `validation_rule_version`

The version of the Silver cleaning and validation rules applied to the row.

Example:

```text
silver_rules_v1
```

This allows future changes to Silver rules without losing auditability.

---

## Silver Table Inventory

The Silver transformation pipeline creates the following cleaned Delta tables:

```text
dim_category_clean
dim_location_clean
dim_segment_clean
dim_risk_rule_clean
dim_merchant_clean
dim_campaign_clean
dim_offer_clean
dim_cardmember_token_clean
dim_privacy_consent_clean
dim_date_clean
fact_transactions_clean
fact_offer_customer_assignment_clean
fact_offer_impressions_clean
fact_offer_activations_clean
fact_offer_redemptions_clean
fact_control_group_transactions_clean
fact_reward_liability_clean
fact_merchant_settlements_clean
fact_fraud_risk_events_clean
fact_data_quality_reconciliation_clean
```

---

## Generic Silver Cleaning Rules

The generic Silver cleaner applies the following rules to each configured table.

### Primary-Key Rule

If the configured primary key exists:

```text
drop rows where primary key is null
deduplicate by primary key
```

Example:

```text
fact_transactions_clean uses transaction_id
fact_offer_redemptions_clean uses redemption_id
fact_reward_liability_clean uses reward_liability_id
```

Why this matters:

```text
A trusted row must have one stable identity.
```

### Numeric Casting Rule

Configured numeric columns are cast to double.

Examples:

```text
transaction_amount
calculated_reward_amount
reward_amount
merchant_funded_amount
platform_funded_amount
platform_fee_amount
merchant_settlement_amount
merchant_net_after_reward
risk_score
settlement_delta
```

Why this matters:

```text
Business formulas require numeric fields, not strings.
```

### Timestamp Normalization Rule

Configured timestamp columns are converted to Spark timestamp type.

Examples:

```text
transaction_timestamp
impression_timestamp
activation_timestamp
redemption_timestamp
liability_timestamp
settlement_timestamp
event_timestamp
reconciliation_timestamp
```

Why this matters:

```text
Time-window joins and date-based analysis require reliable timestamps.
```

### Date Normalization Rule

Configured date columns are converted to Spark date type.

Examples:

```text
transaction_date
impression_date
activation_date
redemption_date
liability_date
settlement_date
event_date
reconciliation_date
```

Why this matters:

```text
Partitioning, reporting, and campaign-window analysis depend on reliable date fields.
```

---

## Silver Validation Rules

Silver validation happens after each table is written.

Each Silver table must pass:

```text
Spark can read the written Silver Delta table
read-back row count matches the transformed DataFrame row count
Silver row count is not greater than Bronze row count
configured required columns exist
Silver metadata columns exist
```

Important rule:

```text
Silver rows may be fewer than Bronze rows.
Silver rows should not be greater than Bronze rows.
```

Why?

Silver may remove:

```text
duplicate primary keys
null primary keys
invalid typed values
invalid date or timestamp rows
```

Bronze preserves source data.

Silver creates trusted data.

---

## Critical Referential Integrity Checks

After all Silver tables are written, the pipeline validates parent-child relationships.

The following relationships are checked:

```text
fact_offer_redemptions_clean.transaction_id
    -> fact_transactions_clean.transaction_id

fact_control_group_transactions_clean.transaction_id
    -> fact_transactions_clean.transaction_id

fact_reward_liability_clean.redemption_id
    -> fact_offer_redemptions_clean.redemption_id

fact_reward_liability_clean.transaction_id
    -> fact_transactions_clean.transaction_id

fact_merchant_settlements_clean.transaction_id
    -> fact_transactions_clean.transaction_id

fact_fraud_risk_events_clean.transaction_id
    -> fact_transactions_clean.transaction_id

fact_data_quality_reconciliation_clean.transaction_id
    -> fact_transactions_clean.transaction_id
```

These checks prove:

```text
No redemption without a transaction.
No control spend without a transaction.
No reward liability without a redemption.
No settlement without a transaction.
No fraud event without a transaction.
No reconciliation check without a transaction.
```

---

## How Referential Validation Works

The Silver pipeline uses a left anti join.

Plain English:

```text
Find child keys that do not exist in the parent table.
```

Example:

```text
fact_offer_redemptions_clean.transaction_id
LEFT ANTI JOIN
fact_transactions_clean.transaction_id
```

If the orphan count is zero:

```text
all redemption rows point to valid transactions
```

If the orphan count is greater than zero:

```text
some redemption rows are orphaned
```

The pipeline fails when orphan keys are found.

---

## Local Execution

Run Bronze ingestion first if Bronze tables do not exist:

```bash
PYTHONPATH=src python spark_jobs/bronze_ingestion.py
```

Then run Silver transformations:

```bash
PYTHONPATH=src python spark_jobs/silver_transformations.py
```

Expected completion message:

```text
Silver referential integrity validation passed.
Silver transformations complete.
```

Confirm Silver tables exist:

```bash
ls data/lakehouse/silver
```

Check one Delta table:

```bash
ls data/lakehouse/silver/fact_transactions_clean
```

Expected output includes:

```text
_delta_log
```

The `_delta_log` folder proves the table is a Delta table.

---

## Databricks Execution

In Databricks:

1. Sync or pull the latest repository code.
2. Confirm the cluster has access to the Bronze Delta table location.
3. Run Bronze ingestion if Bronze tables are missing.
4. Run the Silver transformation script from the repo.
5. Confirm Silver Delta paths are created.
6. Inspect sample Silver tables using Spark.

Example notebook inspection:

```python
silver_path = "dbfs:/FileStore/merchantlift/data/lakehouse/silver/fact_transactions_clean"

df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

display(df.limit(10))
```

For Unity Catalog or cloud object storage, replace the path with the correct catalog volume or cloud storage location.

---

## GCP, BigQuery, and dbt Notes

No new BigQuery or dbt setup is required for this Silver cleaning step.

Current layer:

```text
Spark / Delta Lake
```

Later layers:

```text
BigQuery setup and loading
dbt staging and mart models
policy tags and row-level security
Power BI dashboards
```

The Silver layer prepares trusted Delta outputs that later BigQuery/dbt layers can consume.

---

## Output Files

Primary implementation file:

```text
spark_jobs/silver_transformations.py
```

Configuration file:

```text
src/merchantlift/silver_config.py
```

Supporting utility:

```text
src/merchantlift/spark.py
```

Output location:

```text
data/lakehouse/silver/
```

---

## Completion Criteria

This implementation is complete when:

```text
20 Bronze tables are transformed
20 Silver clean Delta tables are written
each Silver table is readable
Silver row counts are controlled
required columns exist
Silver metadata exists
critical referential checks pass
```

---