# Databricks notebook source
from pyspark.sql.functions import (lit, col, sum as spark_sum, avg, 
    max as spark_max, min as spark_min, count, 
    round as spark_round, month, year, when)
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

silver_transactions   = get_path("silver", "transactions")
silver_customer       = get_path("silver", "customer")
gold_customer_summary = get_path("gold", "customer_summary")

print("Config ready ✅")

# COMMAND ----------

txn_df  = spark.read.format("delta").load(silver_transactions)
cust_df = spark.read.format("delta").load(silver_customer)

print(f"Transactions : {txn_df.count()}")
print(f"Customers    : {cust_df.count()}")

# COMMAND ----------

print("=" * 55)
print("CUSTOMER MONTHLY SUMMARY")
print("=" * 55)

monthly_summary_df = txn_df \
    .filter(col("transaction_type") == "DEBIT") \
    .withColumn("txn_year",  year(col("transaction_timestamp"))) \
    .withColumn("txn_month", month(col("transaction_timestamp"))) \
    .groupBy("account_number", "txn_year", "txn_month") \
    .agg(
        spark_round(spark_sum("amount"), 2).alias("monthly_spend"),
        spark_round(avg("amount"), 2).alias("avg_transaction"),
        spark_round(spark_max("amount"), 2).alias("max_transaction"),
        spark_round(spark_min("amount"), 2).alias("min_transaction"),
        count("transaction_id").alias("txn_count")
    )

print(f"Monthly summary records: {monthly_summary_df.count()}")
monthly_summary_df.show(5, truncate=False)

# COMMAND ----------

# Join to get customer name, account type, branch, city
customer_summary_df = monthly_summary_df.join(
    cust_df.select("account_number", "customer_id", "customer_name",
                   "account_type", "branch_id", "city", "state", "status"),
    on="account_number",
    how="left"
).withColumn("gold_load_timestamp",
             lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"Final customer summary records: {customer_summary_df.count()}")
customer_summary_df.show(5, truncate=False)

# COMMAND ----------

print("=" * 55)
print("OVERALL CUSTOMER KPIs")
print("=" * 55)

overall_kpi_df = txn_df.join(
    cust_df.select(
        "account_number", "customer_id", "customer_name",
        "account_type", "branch_id",
        col("city").alias("customer_city"),
        col("state").alias("customer_state")
    ),
    on="account_number",
    how="left"
).groupBy(
    "account_number", "customer_id", "customer_name",
    "account_type", "customer_city", "customer_state"
).agg(
    spark_round(spark_sum(
        when(col("transaction_type") == "DEBIT", col("amount")).otherwise(0)
    ), 2).alias("total_debit"),
    spark_round(spark_sum(
        when(col("transaction_type") == "CREDIT", col("amount")).otherwise(0)
    ), 2).alias("total_credit"),
    count("transaction_id").alias("total_transactions"),
    spark_round(avg("amount"), 2).alias("avg_transaction_amount"),
    spark_round(spark_max("amount"), 2).alias("highest_transaction")
).withColumn("gold_load_timestamp",
             lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"Overall KPI records: {overall_kpi_df.count()}")
overall_kpi_df.show(5, truncate=False)

# COMMAND ----------

print("=" * 55)
print("WRITING TO GOLD LAYER")
print("=" * 55)

# Monthly summary
customer_summary_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .save(get_path("gold", "customer_summary/monthly"))
print(f"Monthly summary written : {customer_summary_df.count()} ✅")

# Overall KPIs
overall_kpi_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .save(get_path("gold", "customer_summary/overall_kpi"))
print(f"Overall KPI written     : {overall_kpi_df.count()} ✅")

# COMMAND ----------

monthly_df = spark.read.format("delta").load(get_path("gold", "customer_summary/monthly"))
kpi_df     = spark.read.format("delta").load(get_path("gold", "customer_summary/overall_kpi"))

print("=" * 55)
print("GOLD CUSTOMER SUMMARY - FINAL")
print("=" * 55)
print(f"Monthly summary records : {monthly_df.count()}")
print(f"Overall KPI records     : {kpi_df.count()}")
print("\nTop 5 customers by total debit:")
kpi_df.orderBy(col("total_debit").desc()).show(5, truncate=False)