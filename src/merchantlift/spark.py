"""Spark session utilities for MerchantLift BI."""

from __future__ import annotations

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session configured for local Delta Lake work.

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


if __name__ == "__main__":
    # Quick smoke test to verify Spark session creation works.
    ss = create_spark_session("Spark Session Smoke Test")
    print(ss.appName)