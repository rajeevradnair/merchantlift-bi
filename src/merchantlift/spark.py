"""Spark session utilities for MerchantLift BI."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def running_in_databricks() -> bool:
    """Return whether code is running inside Databricks."""
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "DATABRICKS_CLUSTER_ID" in os.environ
        or "SPARK_CONNECT_MODE_ENABLED" in os.environ
    )


def create_spark_session(app_name: str) -> SparkSession:
    """Create or reuse a Spark session.

    Args:
        app_name: Human-readable Spark application name.

    Returns:
        SparkSession.
    """
    active_session = SparkSession.getActiveSession()

    if active_session is not None:
        return active_session

    if running_in_databricks():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()