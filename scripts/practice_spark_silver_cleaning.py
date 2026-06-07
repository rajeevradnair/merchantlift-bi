from pyspark.sql import functions as F

from merchantlift.spark import create_spark_session


def main() -> None:
    spark = create_spark_session("merchantlift-silver-practice")

    df = spark.createDataFrame(
        [
            ("tx_001", "merchant_001", "42.50", "2026-03-01 10:15:00"),
            ("tx_001", "merchant_001", "42.50", "2026-03-01 10:15:00"),
            ("tx_002", "merchant_002", "15.25", "2026-03-01 12:20:00"),
        ],
        [
            "transaction_id",
            "merchant_id",
            "transaction_amount",
            "transaction_timestamp",
        ],
    )

    print("Raw practice data:")
    df.show(truncate=False)

    cleaned_df = (
        df
        .dropDuplicates(["transaction_id"])
        .withColumn(
            "transaction_amount",
            F.col("transaction_amount").cast("double"),
        )
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp("transaction_timestamp"),
        )
        .withColumn(
            "transaction_date",
            F.to_date("transaction_timestamp"),
        )
    )

    print("Cleaned practice data:")
    cleaned_df.printSchema()
    cleaned_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()