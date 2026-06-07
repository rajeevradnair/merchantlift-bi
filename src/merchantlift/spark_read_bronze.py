"""Spark session utilities for MerchantLift BI."""

from __future__ import annotations

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(app_name: str) -> SparkSession:

    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session
    # Only configure master if not in Databricks
    return SparkSession.builder.appName(app_name).getOrCreate()


def create_spark_session_local(app_name: str) -> SparkSession:
    """Create a local Spark session configured for Delta Lake.

    Args:
        app_name: Human-readable Spark application name.

    Returns:
        Configured SparkSession.
    """
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