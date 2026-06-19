# Databricks notebook source
# MAGIC %run "/Users/sahithya.bangla10@gmail.com/BankingProject/Common/00_ADLS_Connection_Config"

# COMMAND ----------

from datetime import datetime 
from pyspark.sql.functions import lit



print("=" * 50)
print("BRONZE LOAD - CUSTOMER")
print("=" * 50)

# Read raw CSV
customer_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(raw_customer)

# Add audit columns
customer_df = customer_df \
    .withColumn("bronze_load_timestamp", 
                lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) \
    .withColumn("source_file", lit("raw/customer"))

# Write as Delta
customer_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save(bronze_customer)

print(f"Records loaded : {customer_df.count()}")
print("Customer Bronze load complete ✅")

# COMMAND ----------

print("=" * 50)
print("BRONZE LOAD - BRANCH")
print("=" * 50)

branch_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(raw_branch)

branch_df = branch_df \
    .withColumn("bronze_load_timestamp",
                lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) \
    .withColumn("source_file", lit("raw/branch"))

branch_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save(bronze_branch)

print(f"Records loaded : {branch_df.count()}")
print("Branch Bronze load complete ✅")

# COMMAND ----------

print("=" * 50)
print("BRONZE LOAD - TRANSACTIONS")
print("=" * 50)

txn_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(raw_transactions)

txn_df = txn_df \
    .withColumn("bronze_load_timestamp",
                lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) \
    .withColumn("source_file", lit("raw/transactions"))

txn_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save(bronze_transactions)

print(f"Records loaded : {txn_df.count()}")
print("Transactions Bronze load complete ✅")

# COMMAND ----------

print("=" * 50)
print("VERIFICATION")
print("=" * 50)

print(f"Customer    rows : {spark.read.format('delta').load(bronze_customer).count()}")
print(f"Branch      rows : {spark.read.format('delta').load(bronze_branch).count()}")
print(f"Transaction rows : {spark.read.format('delta').load(bronze_transactions).count()}")