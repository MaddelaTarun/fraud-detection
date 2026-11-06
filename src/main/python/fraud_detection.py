
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    unix_timestamp,
    count,
    sum,
    avg,
    collect_set,
    size,
    concat_ws
)
from pyspark.sql.window import Window


# Initialize Spark
spark = (
    SparkSession.builder
    .appName("FraudDetection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Read transactions from HDFS
transactions = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("hdfs://localhost:9000/fraud-detection/transactions/live-input")
)


# Convert timestamp to timestamp type
transactions = transactions.withColumn(
    "timestamp",
    col("timestamp").cast("timestamp")
)


# Add useful derived columns
transactions = transactions.withColumn(
    "transaction_hour",
    hour(col("timestamp"))
).withColumn(
    "event_time",
    unix_timestamp(col("timestamp"))
)


# Account-level aggregation
account_summary = transactions.groupBy("account_id").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("average_amount")
)


# Detect high-value transactions
high_value = transactions.filter(
    col("amount") >= 50000
)


# Detect rapid transactions and geographic anomalies
window_spec = (
    Window.partitionBy("account_id")
    .orderBy("event_time")
    .rangeBetween(-600, 0)
)

transactions = (
    transactions
    .withColumn(
        "recent_transaction_count",
        count("*").over(window_spec)
    )
    .withColumn(
        "recent_locations",
        collect_set("location").over(window_spec)
    )
)


# Detect rapid transactions
rapid_transactions = transactions.filter(
    col("recent_transaction_count") >= 3
)


# Detect geographic anomalies
geographic_anomalies = transactions.filter(
    (size(col("recent_locations")) >= 2) &
    (
        (col("amount") >= 50000) |
        (col("recent_transaction_count") >= 3)
    )
)

# Detect unusual transaction hours (midnight to 5 AM)
unusual_hours = transactions.filter(
    (col("transaction_hour") >= 0) &
    (col("transaction_hour") < 5)
)


# Display results
print("\n========== ACCOUNT SUMMARY ==========")
account_summary.orderBy(
    col("total_amount").desc()
).show(truncate=False)


print("\n========== HIGH VALUE TRANSACTIONS ==========")
high_value.show(truncate=False)


print("\n========== RAPID TRANSACTIONS ==========")
rapid_transactions.show(truncate=False)


print("\n========== GEOGRAPHIC ANOMALIES ==========")
geographic_anomalies.show(truncate=False)


print("\n========== UNUSUAL TRANSACTION HOURS ==========")
unusual_hours.show(truncate=False)


# Save analysis results to HDFS
OUTPUT_BASE = "hdfs://localhost:9000/fraud-detection/results"


account_summary.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{OUTPUT_BASE}/account-summary")


high_value.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{OUTPUT_BASE}/high-value")


# Convert array column to string before CSV export
rapid_transactions.withColumn(
    "recent_locations",
    concat_ws(", ", col("recent_locations"))
).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{OUTPUT_BASE}/rapid-transactions")


geographic_anomalies.withColumn(
    "recent_locations",
    concat_ws(", ", col("recent_locations"))
).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{OUTPUT_BASE}/geographic-anomalies")


unusual_hours.withColumn(
    "recent_locations",
    concat_ws(", ", col("recent_locations"))
).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{OUTPUT_BASE}/unusual-hours")


print("\nAnalysis results successfully saved to HDFS.")


spark.stop()

