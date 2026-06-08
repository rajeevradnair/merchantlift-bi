from merchantlift.spark import create_spark_session_local
from pyspark.sql import functions as F

spark = create_spark_session_local("join-strategy")

print(spark)

transactions = spark.createDataFrame (

    [
            ("tx_001", "card_001", "merchant_001", "2026-03-01 10:00:00", 120.00),
            ("tx_002", "card_001", "merchant_001", "2026-03-05 12:00:00", 80.00),
            ("tx_003", "card_001", "merchant_002", "2026-03-02 09:00:00", 50.00),
            ("tx_004", "card_002", "merchant_001", "2026-03-03 14:00:00", 200.00),
        ],
        [
            "transaction_id",
            "tokenized_cardmember_id",
            "merchant_id",
            "transaction_timestamp",
            "transaction_amount",
        ],
    ).withColumn(
        "transaction_timestamp",
        F.to_timestamp("transaction_timestamp")
    )

activations = spark.createDataFrame(
        [
            (
                "activation_001",
                "card_001",
                "merchant_001",
                "offer_001",
                "2026-03-01 00:00:00",
                "2026-03-04 23:59:59",
                100.00,
            ),
            (
                "activation_002",
                "card_002",
                "merchant_001",
                "offer_002",
                "2026-03-01 00:00:00",
                "2026-03-10 23:59:59",
                150.00,
            ),
        ],
        [
            "activation_id",
            "tokenized_cardmember_id",
            "merchant_id",
            "offer_id",
            "activation_timestamp",
            "offer_expiry_timestamp",
            "minimum_spend_amount",
        ],
    ).withColumn(
        "activation_timestamp",
        F.to_timestamp("activation_timestamp"),
    ).withColumn(
        "offer_expiry_timestamp",
        F.to_timestamp("offer_expiry_timestamp"),
    )


print(transactions.count(), activations.count())

matched = transactions.alias("tx") \
    .join(
        activations.alias("act"),
        (
            (F.col("tx.tokenized_cardmember_id") == F.col("act.tokenized_cardmember_id"))
            & (F.col("tx.merchant_id") == F.col("act.merchant_id"))
            & (F.col("tx.transaction_timestamp") >= F.col("act.activation_timestamp"))
            & (F.col("tx.transaction_timestamp") <= F.col("act.offer_expiry_timestamp"))
            & (F.col("tx.transaction_amount") >= F.col("act.minimum_spend_amount"))
        ),
        "inner"
    )

matched.explain(True)

broadcast_matched = transactions.alias("tx") \
    .join(
        F.broadcast(activations.alias("act")),
        (
            (F.col("tx.tokenized_cardmember_id") == F.col("act.tokenized_cardmember_id"))
            & (F.col("tx.merchant_id") == F.col("act.merchant_id"))
            & (F.col("tx.transaction_timestamp") >= F.col("act.activation_timestamp"))
            & (F.col("tx.transaction_timestamp") <= F.col("act.offer_expiry_timestamp"))
            & (F.col("tx.transaction_amount") >= F.col("act.minimum_spend_amount"))
        ),
        "inner"
    )

broadcast_matched.explain(True)


spark.stop()