"""Spark session utilities for MerchantLift BI."""

from __future__ import annotations

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """Create or reuse a Spark session."""
    print("=" * 80)
    print("create_spark_session called")
    print(f"Requested app name: {app_name}")

    active_session = SparkSession.getActiveSession()

    if active_session is not None:
        print("Using existing active Spark session")
        print(f"Spark version: {active_session.version}")
        print("=" * 80)
        return active_session

    print("No active Spark session found. Creating new Spark session.")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    print("Created new Spark session")
    print(f"Spark version: {spark.version}")
    print("=" * 80)

    return spark