# Stakeholder Use Cases

## Purpose

This document defines the main stakeholders for MerchantLift BI and explains what each group needs from the platform.

MerchantLift BI is not just a dashboard project. It is a production-simulated merchant intelligence platform that serves executive, merchant, finance, risk, compliance, data engineering, and analytics stakeholders.

The central business question is:

> Did the merchant offer create incremental profitable spend, or did it simply subsidize customers who would have purchased anyway?

Different stakeholders answer this question from different angles.

---

## 1. Executive Stakeholders

### Main Question

Is the merchant-funded offer program creating profitable incremental value?

### Why Executives Care

Executives need to understand whether the offer program is creating business value at scale.

They care about whether merchant-funded offers are:

- growing spend
- improving merchant relationships
- producing profitable lift
- increasing repeat behavior
- avoiding excessive reward cost
- avoiding cannibalization
- supporting privacy-safe reporting

### Key Metrics

| Metric | Meaning |
|---|---|
| Incremental spend | Additional spend attributed to the offer |
| Net merchant profit | Profit after reward cost and platform fees |
| ROAS | Return on advertising or reward spend |
| Reward cost | Total cost of cardmember rewards |
| Cannibalization risk | Risk that the offer subsidized organic purchases |
| Category performance | Performance by merchant category |

### Future Dashboards

- Executive Merchant Economics Dashboard
- Merchant Category Intelligence Dashboard

### Future Marts

- `mart_merchant_economics`
- `mart_offer_incrementality`
- `mart_reward_liability`

---

## 2. Merchant Growth / Partner Team

### Main Question

Which merchants, campaigns, offers, and customer segments are performing best?

### Why Merchant Teams Care

Merchant teams work directly with merchants. They need evidence to recommend which offers to launch, renew, modify, or stop.

They need to know whether a merchant offer drove:

- activations
- redemptions
- incremental transactions
- repeat purchases
- lapsed customer reactivation
- profitable customer segment response

### Key Metrics

| Metric | Meaning |
|---|---|
| Impression count | Number of times offers were shown |
| Activation rate | Percent of impressions that became activations |
| Redemption rate | Percent of activations that redeemed |
| Breakage | Activated offers that did not redeem |
| Repeat purchase rate | Whether customers returned after redemption |
| Segment lift | Incremental behavior by customer segment |

### Future Dashboards

- Offer Incrementality Dashboard
- Segment Response Dashboard
- Merchant Category Intelligence Dashboard

### Future Marts

- `mart_offer_performance`
- `mart_offer_incrementality`
- `mart_merchant_economics`

---

## 3. Finance Team

### Main Question

Can we trust the money movement, reward liability, and merchant settlement numbers?

### Why Finance Cares

Finance teams need to validate that transactions, rewards, platform fees, and settlements reconcile.

A merchant-offer platform can create financial risk when:

- rewards are calculated incorrectly
- refunds occur after rewards are paid
- merchant settlement amounts do not match transaction records
- late-arriving transactions change campaign totals
- platform fees are missing or inconsistent

### Key Metrics

| Metric | Meaning |
|---|---|
| Reward liability | Amount owed as cardmember rewards |
| Actual reward paid | Amount actually paid |
| Expected merchant settlement | Amount merchant should receive |
| Actual merchant settlement | Amount merchant did receive |
| Platform fee | Amount retained by platform |
| Settlement delta | Difference between expected and actual financial records |

### Future Dashboards

- Merchant Pacing and Liability Dashboard
- Data Quality and Reconciliation Dashboard

### Future Marts

- `mart_reward_liability`
- `mart_data_quality_reconciliation`

---

## 4. Risk / Fraud Team

### Main Question

Are offers being abused or creating financial leakage?

### Why Risk Teams Care

Offer programs can be exploited.

Risk teams need to detect unusual behavior before it becomes expensive.

Common abuse patterns include:

- duplicate redemptions
- refund-after-reward behavior
- unusually high redemption velocity
- suspicious merchant-location activity
- reward-seeking behavior
- abnormal redemption clusters

### Key Metrics

| Metric | Meaning |
|---|---|
| Duplicate redemption count | Multiple redemptions beyond offer rules |
| Refund-after-reward count | Refunded transactions after reward creation |
| Suspicious velocity score | Unusually rapid redemption behavior |
| Merchant-location anomaly count | Unusual activity by merchant/location |
| Abuse risk score | Composite fraud or abuse indicator |

### Future Dashboard

- Fraud and Offer Abuse Dashboard

### Future Mart

- `mart_offer_abuse_risk`

---

## 5. Compliance / Privacy Team

### Main Question

Can teams analyze merchant-offer performance without exposing sensitive cardmember-level behavior?

### Why Compliance Cares

Merchant analytics can involve sensitive transaction behavior. Even in a synthetic project, MerchantLift BI is designed with enterprise-grade privacy principles.

Compliance teams need confidence that:

- raw card numbers do not exist
- cardmember identifiers are tokenized
- reporting does not expose row-level customer behavior
- small cohorts are suppressed
- sensitive fields have access controls
- merchant users only see authorized merchant data
- access is auditable

### Key Controls

| Control | Purpose |
|---|---|
| Tokenized cardmember IDs | Enables joins without exposing real identity |
| No raw PAN/card numbers | Avoids payment data exposure |
| Aggregated reporting only | Prevents customer-level leakage |
| Cohort suppression | Reduces re-identification risk |
| BigQuery policy tags | Protects sensitive columns |
| BigQuery row-level security | Limits merchant-specific access |
| Google Cloud DLP | Detects sensitive data patterns |
| Cloud Audit Logs | Tracks access activity |
| Cloud KMS | Documents encryption/key-management approach |

### Future Dashboard

- Privacy-Safe Analytics Dashboard

### Future Mart

- `mart_privacy_safe_offer_lift`

### Future Security Artifacts

- `bigquery/policy_tags/`
- `bigquery/row_level_security/`
- `gcp_security/dlp/`
- `gcp_security/kms/`
- `gcp_security/iam/`
- `gcp_security/audit_logs/`

---

## 6. Data Engineering Team

### Main Question

Is the platform reliable, scalable, testable, and production-like?

### Why Data Engineering Cares

Data engineers are responsible for moving data safely and reliably from raw events to trusted business tables.

They care about:

- data generation quality
- raw file integrity
- partitioning
- file sizing
- schema contracts
- Spark job reliability
- Delta Lake table organization
- Airflow orchestration
- validation checks
- performance reports

### Key Concerns

| Concern | Platform Layer |
|---|---|
| Raw event validation | Great Expectations |
| Scalable transformation | Spark / Databricks |
| Storage organization | Delta Lake bronze/silver/gold |
| Workflow automation | Airflow |
| Warehouse optimization | BigQuery partitioning and clustering |
| Semantic testing | dbt tests |
| Pipeline evidence | Release reports and validation reports |

### Future Artifacts

- `spark_jobs/`
- `great_expectations/`
- `airflow/dags/`
- `reports/data_quality_report.md`
- `reports/performance_report.md`
- `docs/lakehouse_architecture.md`

---

## 7. Analytics Engineering / BI Team

### Main Question

Are metrics consistent, marts reusable, and dashboards trustworthy?

### Why Analytics Engineering Cares

Analytics engineers convert cleaned data into trusted business definitions.

They care about:

- consistent metric definitions
- dbt lineage
- source freshness
- relationship tests
- accepted values
- dashboard-ready marts
- business glossary
- semantic model clarity

### Key Concerns

| Concern | Example |
|---|---|
| Metric consistency | ROAS has one standard definition |
| Mart quality | dbt tests pass before dashboard use |
| Dashboard usability | Power BI uses governed marts |
| Business glossary | Terms are clearly defined |
| Data lineage | Raw-to-dashboard flow is explainable |

### Future Artifacts

- `dbt_merchantlift/`
- `docs/business_glossary.md`
- `docs/data_lineage.md`
- `powerbi/dashboard_notes.md`

---

## 8. Stakeholder-to-Dashboard Map

| Stakeholder | Dashboard |
|---|---|
| Executives | Executive Merchant Economics Dashboard |
| Merchant Growth Team | Offer Incrementality Dashboard |
| Merchant Growth Team | Segment Response Dashboard |
| Merchant Growth Team | Merchant Category Intelligence Dashboard |
| Finance Team | Merchant Pacing and Liability Dashboard |
| Finance Team | Data Quality and Reconciliation Dashboard |
| Risk Team | Fraud and Offer Abuse Dashboard |
| Compliance Team | Privacy-Safe Analytics Dashboard |

---

## 9. Stakeholder-to-Mart Map

| Stakeholder | Primary Marts |
|---|---|
| Executives | `mart_merchant_economics`, `mart_offer_incrementality`, `mart_reward_liability` |
| Merchant Growth Team | `mart_offer_performance`, `mart_offer_incrementality`, `mart_merchant_economics` |
| Finance Team | `mart_reward_liability`, `mart_data_quality_reconciliation` |
| Risk Team | `mart_offer_abuse_risk` |
| Compliance Team | `mart_privacy_safe_offer_lift` |
| Data Engineering Team | Bronze, silver, gold Delta tables; validation reports |
| Analytics Engineering Team | dbt staging, intermediate, and mart models |

---

## 10. Stakeholder-to-Technical-Control Map

| Stakeholder | Technical Control |
|---|---|
| Executives | Curated executive marts and Power BI dashboards |
| Merchant Growth Team | Offer, segment, and category marts |
| Finance Team | Reconciliation checks and liability marts |
| Risk Team | Fraud and abuse feature marts |
| Compliance Team | Policy tags, row-level security, DLP, audit logs |
| Data Engineering Team | Great Expectations, Spark jobs, Airflow DAGs |
| Analytics Engineering Team | dbt tests, dbt docs, lineage, semantic models |

---

## 11. Design Rule

Every technical asset in MerchantLift BI should map to a stakeholder decision.

| Technical Asset | Stakeholder Decision It Supports |
|---|---|
| `fact_transactions` | What purchases occurred? |
| `fact_offer_redemptions` | Which purchases qualified for offers? |
| `fact_reward_liability` | How much reward cost was created? |
| `fact_merchant_settlements` | Did merchants receive correct settlement? |
| `fact_fraud_risk_events` | Which activity looks suspicious? |
| `fact_data_quality_reconciliation` | Can finance trust the numbers? |
| `mart_offer_incrementality` | Did the offer create true lift? |
| `mart_privacy_safe_offer_lift` | Can results be shared safely? |
| Power BI dashboards | What actions should stakeholders take? |
| Great Expectations checks | Can raw data be trusted? |
| dbt tests | Can business marts be trusted? |
| Airflow DAG | Can the workflow run reliably? |

---

## 12. Summary

MerchantLift BI serves multiple stakeholder groups:

- executives
- merchant growth teams
- finance teams
- risk teams
- compliance teams
- data engineering teams
- analytics engineering teams

Each stakeholder asks a different question, but all questions connect to the same platform mission:

> Measure whether merchant-funded card-linked offers create profitable incremental value in a privacy-safe, trustworthy, and production-grade way.