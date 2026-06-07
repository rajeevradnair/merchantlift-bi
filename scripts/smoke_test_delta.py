"""Smoke test Delta Lake support."""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def main() -> None:
    """Write and read a tiny Delta table."""
    builder = (
        SparkSession.builder
        .appName("delta-smoke-test")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    output_path = Path("data") / "tmp" / "delta_smoke_test"

    (
        spark.range(5)
        .write
        .format("delta")
        .mode("overwrite")
        .save(str(output_path))
    )

    df = (
        spark.read
        .format("delta")
        .load(str(output_path))
    )

    df.show()

    spark.stop()


if __name__ == "__main__":
    main()