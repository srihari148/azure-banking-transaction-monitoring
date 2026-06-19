# Databricks notebook source
from pyspark.sql.functions import (lit, col, upper, trim, when, 
    to_timestamp, current_timestamp, count)
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

bronze_transactions  = get_path("bronze", "transactions")
silver_transactions  = get_path("silver", "transactions")
rejected_transactions = get_path("rejected", "transactions")

print("Config ready ✅")

# COMMAND ----------

df = spark.read.format("delta").load(bronze_transactions)
print(f"Bronze records: {df.count()}")
df.printSchema()
df.show(3, truncate=False)

# COMMAND ----------

print("=" * 55)
print("DATA QUALITY REPORT - TRANSACTIONS")
print("=" * 55)

total = df.count()

null_txn_id    = df.filter(col("transaction_id").isNull()).count()
null_account   = df.filter(col("account_number").isNull()).count()
null_amount    = df.filter(col("amount").isNull()).count()
duplicate_txns = total - df.dropDuplicates(["transaction_id"]).count()
negative_amt   = df.filter(col("amount") < 0).count()
future_dates   = df.filter(
    to_timestamp(col("transaction_timestamp")) > current_timestamp()
).count()
invalid_type   = df.filter(
    ~col("transaction_type").isin(["DEBIT","CREDIT"])
).count()
invalid_channel = df.filter(
    ~col("channel").isin(["UPI","ATM","CARD","NETBANKING"])
).count()

print(f"Total records       : {total}")
print(f"Null transaction_id : {null_txn_id}")
print(f"Null account_number : {null_account}")
print(f"Null amount         : {null_amount}")
print(f"Duplicate txn_ids   : {duplicate_txns}")
print(f"Negative amounts    : {negative_amt}")
print(f"Future dates        : {future_dates}")
print(f"Invalid txn_type    : {invalid_type}")
print(f"Invalid channel     : {invalid_channel}")

# COMMAND ----------

# ── Rejected records ─────────────────────────────────────────────
rejected_df = df.filter(
    col("transaction_id").isNull() |
    col("account_number").isNull() |
    col("amount").isNull() |
    (col("amount") < 0) |
    to_timestamp(col("transaction_timestamp")).isNull()
).withColumn("rejection_reason", 
    when(col("transaction_id").isNull(), "Null transaction_id")
    .when(col("account_number").isNull(), "Null account_number")
    .when(col("amount").isNull(), "Null amount")
    .when(col("amount") < 0, "Negative amount")
    .otherwise("Invalid timestamp")
).withColumn("rejected_timestamp", 
             lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

# ── Clean records ─────────────────────────────────────────────────
clean_df = df.filter(
    col("transaction_id").isNotNull() &
    col("account_number").isNotNull() &
    col("amount").isNotNull() &
    (col("amount") > 0) &
    to_timestamp(col("transaction_timestamp")).isNotNull()
).dropDuplicates(["transaction_id"])

print(f"Clean records   : {clean_df.count()}")
print(f"Rejected records: {rejected_df.count()}")

# COMMAND ----------

silver_df = clean_df \
    .withColumn("transaction_timestamp", 
                to_timestamp(col("transaction_timestamp"))) \
    .withColumn("city",             upper(trim(col("city")))) \
    .withColumn("transaction_type", upper(trim(col("transaction_type")))) \
    .withColumn("channel",          upper(trim(col("channel")))) \
    .withColumn("merchant_name",    trim(col("merchant_name"))) \
    .withColumn("amount",           col("amount").cast("double")) \
    .withColumn("silver_load_timestamp", 
                lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) \
    .withColumn("transaction_date", 
                col("transaction_timestamp").cast("date"))

print(f"Silver records ready: {silver_df.count()}")
silver_df.show(3, truncate=False)

# COMMAND ----------

# Write silver
silver_df.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("transaction_date") \
    .save(silver_transactions)

print("Silver transactions written ✅")

# Write rejected
if rejected_df.count() > 0:
    rejected_df.write.mode("overwrite").format("delta").save(rejected_transactions)
    print(f"Rejected records written ✅")
else:
    print("No rejected records ✅")

# COMMAND ----------

final_df = spark.read.format("delta").load(silver_transactions)
print("=" * 55)
print("SILVER TRANSACTIONS - FINAL SUMMARY")
print("=" * 55)
print(f"Total records  : {final_df.count()}")
print(f"Columns        : {final_df.columns}")
final_df.show(3, truncate=False)