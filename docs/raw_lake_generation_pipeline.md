
# Raw Lake Generation Pipeline

## Purpose

This document explains the MerchantLift BI raw data generation pipeline.

The pipeline uses one orchestration script to run all raw-data generators in dependency order.

The goal is simple:

One command should rebuild the complete raw synthetic data lake.

The orchestrator is:

data_generation/generate_all.py

It coordinates existing generator scripts. It does not contain business-generation logic.

## Why This Exists

Before this pipeline, each raw table had to be generated manually by running separate scripts.

That is fine during early development, but a production-style data platform needs repeatable orchestration.

The pipeline solves that problem by running the generators in the correct order, validating outputs, and printing a final row-count summary.

## Core Intuition

A data lake is only trustworthy if it can be rebuilt predictably.

The mental model is:

Create the world.
Create purchase behavior.
Create offer exposure.
Create offer qualification.
Create finance and risk records.
Validate the raw lake.

Short version:

Many generators become one controlled pipeline.

## Generator Order

The raw lake generation pipeline runs scripts in this order:

1. data_generation/generate_dimensions.py
2. data_generation/generate_transactions.py
3. data_generation/generate_offer_interactions.py
4. data_generation/generate_redemptions_and_controls.py
5. data_generation/generate_financial_risk_events.py

## Why This Order Matters

The generators have data dependencies.

Dimensions must be created before facts.

Transactions need valid cardmembers, merchants, categories, and locations.

Offer interactions need offers, campaigns, cardmembers, and consent records.

Redemptions need activations and transactions.

Control-group transactions need assignment records.

Reward liability needs redemptions.

Merchant settlements need transactions, merchants, and reward liability.

Fraud events need transactions, redemptions, and risk rules.

Reconciliation needs transactions and settlements.

Dependency chain:

dimensions
    -> transactions
    -> offer assignments / impressions / activations
    -> redemptions / control-group transactions
    -> reward liability / settlements / fraud / reconciliation

## Raw Output Tables

The pipeline validates that all expected raw tables exist.

Expected dimension tables:

- dim_category
- dim_location
- dim_segment
- dim_risk_rule
- dim_merchant
- dim_campaign
- dim_offer
- dim_cardmember_token
- dim_privacy_consent
- dim_date

Expected fact tables:

- fact_transactions
- fact_offer_customer_assignment
- fact_offer_impressions
- fact_offer_activations
- fact_offer_redemptions
- fact_control_group_transactions
- fact_reward_liability
- fact_merchant_settlements
- fact_fraud_risk_events
- fact_data_quality_reconciliation

Each table is expected at:

data/raw/<table_name>/part-00000.parquet

## What the Orchestrator Does

The orchestrator performs four major jobs:

1. Run generator scripts in dependency order
2. Stop immediately if any generator fails
3. Validate that every expected raw table exists
4. Print a row-count summary for all generated tables

## What the Orchestrator Does Not Do

The orchestrator does not generate business records directly.

It does not contain merchant, offer, transaction, redemption, reward, settlement, fraud, or reconciliation logic.

That logic remains inside the dedicated generator scripts.

This is intentional.

The design principle is:

The orchestrator coordinates.
The generator scripts generate.

## Failure Handling

The pipeline uses subprocess execution with check=True.

If a generator fails, the pipeline stops immediately.

This prevents downstream tables from being generated from incomplete or inconsistent upstream data.

The failure log shows:

- which generator failed
- the exit code
- elapsed seconds before failure
- a clear message that the pipeline stopped to avoid inconsistent downstream data

Example failure behavior:

Generator failed.
Generator: data_generation/generate_redemptions_and_controls.py
Exit code: 1
Elapsed seconds: 2.31
Stopping pipeline to avoid creating inconsistent downstream data.

Raw lake generation pipeline failed.
Elapsed seconds before failure: 8.72

## Output Validation

After all generators finish, the pipeline validates each expected table.

For each table, it checks whether this file exists:

data/raw/<table_name>/part-00000.parquet

If any table is missing, the pipeline raises a FileNotFoundError.

This catches problems where a generator completes but fails to write an expected output.

Expected validation success shape:

FOUND   dim_category
FOUND   dim_location
FOUND   dim_segment
FOUND   dim_risk_rule
FOUND   dim_merchant
FOUND   dim_campaign
FOUND   dim_offer
FOUND   dim_cardmember_token
FOUND   dim_privacy_consent
FOUND   dim_date
FOUND   fact_transactions
FOUND   fact_offer_customer_assignment
FOUND   fact_offer_impressions
FOUND   fact_offer_activations
FOUND   fact_offer_redemptions
FOUND   fact_control_group_transactions
FOUND   fact_reward_liability
FOUND   fact_merchant_settlements
FOUND   fact_fraud_risk_events
FOUND   fact_data_quality_reconciliation

All expected raw outputs are present.

## Row-Count Summary

After validation, the pipeline prints row counts for every raw table.

This helps verify that the pipeline produced real data, not just empty files.

Example summary shape:

dim_category                                      6 rows
dim_location                                    ... rows
dim_segment                                     ... rows
dim_risk_rule                                   ... rows
dim_merchant                                    200 rows
dim_campaign                                     80 rows
dim_offer                                       300 rows
dim_cardmember_token                          5,000 rows
dim_privacy_consent                           5,000 rows
dim_date                                        365 rows
fact_transactions                            12,xxx rows
fact_offer_customer_assignment                5,000 rows
fact_offer_impressions                        3,000 rows
fact_offer_activations                        x,xxx rows
fact_offer_redemptions                        1,000 rows
fact_control_group_transactions               1,500 rows
fact_reward_liability                         1,000 rows
fact_merchant_settlements                     1,000 rows
fact_fraud_risk_events                          200 rows
fact_data_quality_reconciliation                200 rows

The exact row counts may vary depending on local configuration and generated activation behavior.

## Main Script

The main orchestration file is:

data_generation/generate_all.py

Run the full raw lake generation pipeline:

PYTHONPATH=src python data_generation/generate_all.py

## Expected Success Output

A successful run should end with:

All expected raw outputs are present.

Raw table row-count summary:
...
TOTAL                                      36,451 rows

Raw lake generation pipeline complete.
Total elapsed seconds: xx.xx

## Technical Design

The orchestrator uses:

- pathlib.Path for filesystem paths
- subprocess.run for script execution
- sys.executable to use the current Python interpreter
- os.environ to set PYTHONPATH=src
- time.perf_counter for elapsed-time logging
- polars.read_parquet for row-count summaries

## Important Structural Concepts

### PROJECT_ROOT

PROJECT_ROOT points to the repository root.

This allows the script to run generator files reliably from any location.

### GENERATOR_SCRIPTS

GENERATOR_SCRIPTS is the ordered list of generator scripts.

The order encodes dependency logic.

### EXPECTED_RAW_TABLES

EXPECTED_RAW_TABLES is the full list of expected raw lake outputs.

This list is used for output validation and row-count summaries.

### run_generator()

Runs one generator script as a subprocess.

If the generator fails, the function logs useful debugging details and raises the error.

### validate_expected_outputs()

Checks whether all required raw Parquet files exist.

### print_raw_table_summary()

Reads each expected Parquet output and prints row counts.

## Production-Style Behaviors Added

The raw lake generation pipeline now has:

- dependency-aware execution order
- one-command regeneration
- fast failure on broken upstream step
- explicit output validation
- row-count summary
- elapsed-time logging
- clear failure messages
- separation between orchestration and business logic