# Databricks notebook source
from pyspark.sql.functions import (lit, col, sum as spark_sum, avg,
    max as spark_max, count, round as spark_round, when, countDistinct)
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

silver_transactions = get_path("silver", "transactions")
silver_customer     = get_path("silver", "customer")
silver_branch       = get_path("silver", "branch")

print("Config ready ✅")

# COMMAND ----------

txn_df    = spark.read.format("delta").load(silver_transactions)
cust_df   = spark.read.format("delta").load(silver_customer)
branch_df = spark.read.format("delta").load(silver_branch)

print(f"Transactions : {txn_df.count()}")
print(f"Customers    : {cust_df.count()}")
print(f"Branches     : {branch_df.count()}")

# COMMAND ----------

# Link transactions to branch via customer
# Drop city from txn_df to avoid ambiguity with branch city
txn_with_branch_df = txn_df.drop("city").join(
    cust_df.select("account_number", "branch_id"),
    on="account_number",
    how="left"
).join(
    branch_df.select("branch_id", "branch_name", "city", "state", "region"),
    on="branch_id",
    how="left"
)

print(f"Joined records: {txn_with_branch_df.count()}")
txn_with_branch_df.show(3, truncate=False)

# COMMAND ----------

print("=" * 55)
print("BRANCH SUMMARY")
print("=" * 55)

branch_summary_df = txn_with_branch_df \
    .groupBy("branch_id", "branch_name", "city", "state", "region") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        spark_round(spark_sum("amount"), 2).alias("total_amount"),
        spark_round(avg("amount"), 2).alias("avg_transaction_amount"),
        spark_round(spark_max("amount"), 2).alias("highest_transaction"),
        countDistinct("account_number").alias("unique_customers"),
        spark_round(spark_sum(
            when(col("transaction_type") == "DEBIT", col("amount")).otherwise(0)
        ), 2).alias("total_debit"),
        spark_round(spark_sum(
            when(col("transaction_type") == "CREDIT", col("amount")).otherwise(0)
        ), 2).alias("total_credit")
    ).withColumn("gold_load_timestamp",
                 lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"Branch summary records: {branch_summary_df.count()}")
branch_summary_df.orderBy(col("total_amount").desc()).show(10, truncate=False)

# COMMAND ----------

print("=" * 55)
print("REGIONAL SUMMARY")
print("=" * 55)

regional_summary_df = txn_with_branch_df \
    .groupBy("region") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        spark_round(spark_sum("amount"), 2).alias("total_amount"),
        countDistinct("account_number").alias("unique_customers"),
        countDistinct("branch_id").alias("total_branches")
    ).withColumn("gold_load_timestamp",
                 lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"Regional summary records: {regional_summary_df.count()}")
regional_summary_df.orderBy(col("total_amount").desc()).show(truncate=False)

# COMMAND ----------

print("=" * 55)
print("WRITING TO GOLD LAYER")
print("=" * 55)

branch_summary_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .save(get_path("gold", "branch_summary/by_branch"))
print(f"Branch summary written  : {branch_summary_df.count()} ✅")

regional_summary_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .save(get_path("gold", "branch_summary/by_region"))
print(f"Regional summary written: {regional_summary_df.count()} ✅")

# COMMAND ----------

branch_df_gold  = spark.read.format("delta").load(get_path("gold", "branch_summary/by_branch"))
region_df_gold  = spark.read.format("delta").load(get_path("gold", "branch_summary/by_region"))

print("=" * 55)
print("GOLD BRANCH SUMMARY - FINAL")
print("=" * 55)
print(f"Branch records  : {branch_df_gold.count()}")
print(f"Regional records: {region_df_gold.count()}")
print("\nTop 5 branches by total amount:")
branch_df_gold.orderBy(col("total_amount").desc()).show(5, truncate=False)
print("\nRegion wise summary:")
region_df_gold.orderBy(col("total_amount").desc()).show(truncate=False)