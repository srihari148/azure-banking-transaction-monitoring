# Databricks notebook source
from pyspark.sql.functions import (lit, col, upper, trim, when, sum as spark_sum,
    count, max as spark_max, window, to_date, datediff, current_date, row_number)
from pyspark.sql.window import Window
from datetime import datetime

client_id       = "<your-client-id>"
tenant_id       = "<your-tenant-id>"
client_secret   = "<your-client-secret>"
storage_account = "<your-storage-account>"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

def get_path(container, folder):
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{folder}"

silver_transactions  = get_path("silver", "transactions")
silver_customer      = get_path("silver", "customer")
gold_fraud_alerts    = get_path("gold", "fraud_alerts")

print("Config ready ✅")

# COMMAND ----------

txn_df = spark.read.format("delta").load(silver_transactions)
print(f"Total transactions: {txn_df.count()}")
txn_df.show(3, truncate=False)

# COMMAND ----------

print("=" * 55)
print("FRAUD RULE 1 - HIGH VALUE TRANSACTIONS")
print("=" * 55)

high_value_df = txn_df \
    .filter(col("amount") > 100000) \
    .withColumn("fraud_type", lit("HIGH_VALUE_TRANSACTION")) \
    .withColumn("fraud_detected_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"High value transactions found: {high_value_df.count()}")
high_value_df.select("transaction_id","account_number",
                     "amount","transaction_timestamp","fraud_type").show(5, truncate=False)

# COMMAND ----------

print("=" * 55)
print("FRAUD RULE 2 - RAPID BURST TRANSACTIONS")
print("=" * 55)

# Use 10-minute tumbling window to count transactions per account
burst_df = txn_df \
    .groupBy(
        col("account_number"),
        window(col("transaction_timestamp"), "10 minutes")
    ).agg(
        count("transaction_id").alias("txn_count"),
        spark_sum("amount").alias("total_amount")
    ).filter(col("txn_count") > 3) \
    .withColumn("fraud_type", lit("RAPID_BURST_TRANSACTIONS")) \
    .withColumn("fraud_detected_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

print(f"Burst accounts found: {burst_df.count()}")
burst_df.show(5, truncate=False)

# COMMAND ----------

print("=" * 55)
print("FRAUD RULE 3 - HIGH DAILY SPEND")
print("=" * 55)

daily_spend_df = txn_df \
    .filter(col("transaction_type") == "DEBIT") \
    .groupBy("account_number", "transaction_date") \
    .agg(
        spark_sum("amount").alias("daily_spend"),
        count("transaction_id").alias("txn_count")
    ).filter(col("daily_spend") > 1000000) \
    .withColumn("fraud_type", lit("HIGH_DAILY_SPEND")) \
    .withColumn("fraud_detected_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"High daily spend accounts: {daily_spend_df.count()}")
daily_spend_df.show(5, truncate=False)

# COMMAND ----------

print("=" * 55)
print("FRAUD RULE 4 - DORMANT ACCOUNT ACTIVITY")
print("=" * 55)

from pyspark.sql.functions import min as spark_min, max as spark_max

account_activity_df = txn_df \
    .groupBy("account_number") \
    .agg(
        spark_min("transaction_date").alias("first_txn_date"),
        spark_max("transaction_date").alias("last_txn_date"),
        count("transaction_id").alias("total_txns")
    )

# Relaxed: ≤ 10 transactions, recently active, gap > 180 days
dormant_df = account_activity_df \
    .filter(
        (col("total_txns") <= 10) &
        (col("last_txn_date") >= lit("2025-10-01"))
    ) \
    .withColumn("gap_days",
                datediff(col("last_txn_date"), col("first_txn_date"))) \
    .filter(col("gap_days") > 180) \
    .withColumn("fraud_type", lit("DORMANT_ACCOUNT_ACTIVE")) \
    .withColumn("fraud_detected_at",
                lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"Dormant accounts suddenly active: {dormant_df.count()}")
dormant_df.show(10, truncate=False)

# COMMAND ----------

print("=" * 55)
print("WRITING FRAUD ALERTS TO GOLD")
print("=" * 55)

# High value → gold/fraud_alerts/high_value
high_value_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .save(get_path("gold", "fraud_alerts/high_value"))
print(f"High value alerts written    : {high_value_df.count()} ✅")

# Burst → gold/fraud_alerts/burst
burst_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .save(get_path("gold", "fraud_alerts/burst"))
print(f"Burst alerts written         : {burst_df.count()} ✅")

# Daily spend → gold/fraud_alerts/high_daily_spend
daily_spend_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .save(get_path("gold", "fraud_alerts/high_daily_spend"))
print(f"High daily spend written     : {daily_spend_df.count()} ✅")

# Dormant → gold/fraud_alerts/dormant
if dormant_df.count() > 0:
    dormant_df.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .format("delta") \
        .save(get_path("gold", "fraud_alerts/dormant"))
    print(f"Dormant account alerts written: {dormant_df.count()} ✅")
else:
    print("Dormant: 0 records — skipping write ✅")

# COMMAND ----------

print("=" * 55)
print("FRAUD DETECTION SUMMARY")
print("=" * 55)
print(f"Rule 1 - High Value Transactions : {high_value_df.count()}")
print(f"Rule 2 - Rapid Burst             : {burst_df.count()}")
print(f"Rule 3 - High Daily Spend        : {daily_spend_df.count()}")
print(f"Rule 4 - Dormant Account Active  : {dormant_df.count()}")
print(f"Total fraud alerts               : {high_value_df.count() + burst_df.count() + daily_spend_df.count() + dormant_df.count()}")
print("\nFraud Detection Complete ✅")