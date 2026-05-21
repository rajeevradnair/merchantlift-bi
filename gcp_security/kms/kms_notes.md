# Cloud KMS Design Notes

## Purpose

This document captures the planned encryption and key-management design for MerchantLift BI.

MerchantLift BI is synthetic, but the platform simulates financial-services-grade analytics, so encryption and key ownership should be documented.

---

## Key Management Principles

- Use managed encryption for storage layers.
- Limit key administration to platform/security administrators.
- Allow pipeline service accounts to use keys only when required.
- Audit key usage.

---

## Candidate Key Boundaries

| Data Area | Sensitivity | KMS Need |
|---|---|---|
| Raw synthetic files | Medium | Standard managed encryption acceptable |
| Silver tokenized join tables | Higher | Candidate for stricter key controls |
| BigQuery staging tables | Medium/high | Candidate for controlled access |
| Privacy-safe marts | Lower | Aggregated reporting data |
| Audit logs | High | Protect governance evidence |

---

## Summary

KMS is part of the enterprise story because it shows that MerchantLift BI considers not only analytics logic, but also how sensitive data is protected at rest.