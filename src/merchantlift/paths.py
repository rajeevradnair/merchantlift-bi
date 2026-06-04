"""Central path definitions for MerchantLift BI.

This module keeps important project paths in one place so future scripts
do not hardcode folder locations repeatedly.
"""
import os
from pathlib import Path

# Project root is two levels above this file:
# src/merchantlift/paths.py -> src/merchantlift -> src -> project root
PROJECT_ROOT = Path(
    os.getenv(
        "MERCHANTLIFT_PROJECT_ROOT",
        Path(__file__).resolve().parents[2],
    )
)

CONFIG_DIR = PROJECT_ROOT / "config"
DOCS_DIR = PROJECT_ROOT / "docs"
REPORTS_DIR = PROJECT_ROOT / "reports"
DATA_GENERATION_DIR = PROJECT_ROOT / "data_generation"
SPARK_JOBS_DIR = PROJECT_ROOT / "spark_jobs"
BIGQUERY_DIR = PROJECT_ROOT / "bigquery"
GCP_SECURITY_DIR = PROJECT_ROOT / "gcp_security"
POWERBI_DIR = PROJECT_ROOT / "powerbi"

DATA_DIR = Path(
    os.getenv(
        "MERCHANTLIFT_DATA_ROOT",
        PROJECT_ROOT / "data",
    )
)

RAW_DATA_DIR = DATA_DIR / "raw"
BRONZE_DATA_DIR = DATA_DIR / "bronze"
SILVER_DATA_DIR = DATA_DIR / "silver"
GOLD_DATA_DIR = DATA_DIR / "gold"
SAMPLE_DATA_DIR = DATA_DIR / "samples"


LAKEHOUSE_DIR = DATA_DIR / "lakehouse"
BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
GOLD_DIR = LAKEHOUSE_DIR / "gold"

