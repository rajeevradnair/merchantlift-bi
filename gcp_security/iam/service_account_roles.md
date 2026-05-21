# IAM Service Account Role Design

## Purpose

This document defines the planned IAM service accounts for MerchantLift BI.

The design follows least privilege:

> Each service account receives only the permissions required for its job.

---

## Service Accounts

| Service Account | Purpose | Example Permissions |
|---|---|---|
| `merchantlift-data-generator` | Generate synthetic local/cloud data | Write raw files to storage |
| `merchantlift-spark-runner` | Run Spark/Databricks transformations | Read raw/bronze, write silver/gold |
| `merchantlift-dbt-runner` | Run dbt models and tests in BigQuery | Read staging, write marts |
| `merchantlift-airflow-orchestrator` | Coordinate pipeline tasks | Trigger jobs, read task status |
| `merchantlift-bi-viewer` | Read dashboard-ready marts | Read privacy-safe marts only |
| `merchantlift-compliance-auditor` | Review DLP/audit findings | Read logs and compliance reports |

---

## Human Role Separation

| Human Role | Access Boundary |
|---|---|
| Data Engineer | Can access tokenized IDs for join debugging |
| Analytics Engineer | Can access modeled staging/intermediate data |
| BI Analyst | Can access aggregated marts |
| Merchant Analyst | Can access authorized merchant rows only |
| Executive | Can access aggregate KPIs only |
| Compliance Analyst | Can access DLP findings and audit logs |

---

## Explicit Non-Goals

The following should not be allowed:

- Merchant users querying raw transaction tables
- BI users seeing `tokenized_cardmember_id`
- Analysts bypassing cohort suppression
- Shared admin credentials
- Broad owner/editor access for normal pipeline jobs

---

## Design Summary

MerchantLift BI separates duties across service accounts and human roles.

This makes the platform look more like an enterprise financial-services analytics system rather than a local-only dashboard project.