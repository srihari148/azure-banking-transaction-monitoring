# Databricks notebook source
from pyspark.sql.functions import lit, col, upper, trim, when
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

bronze_customer  = get_path("bronze", "customer")
silver_customer  = get_path("silver", "customer")
rejected_path    = get_path("rejected", "customer")

print("Config ready ✅")

# COMMAND ----------

# Read from Bronze Delta
df = spark.read.format("delta").load(bronze_customer)

print(f"Bronze record count : {df.count()}")
print(f"Columns             : {df.columns}")
df.show(5, truncate=False)

# COMMAND ----------

print("=" * 50)
print("DATA QUALITY REPORT - CUSTOMER")
print("=" * 50)

total = df.count()

null_customer_id    = df.filter(col("customer_id").isNull()).count()
null_account_number = df.filter(col("account_number").isNull()).count()
duplicate_accounts  = total - df.dropDuplicates(["account_number"]).count()
invalid_acc_type    = df.filter(
    ~col("account_type").isin(["SAVINGS","CURRENT","SALARY","NRI"])
).count()

print(f"Total records         : {total}")
print(f"Null customer_id      : {null_customer_id}")
print(f"Null account_number   : {null_account_number}")
print(f"Duplicate accounts    : {duplicate_accounts}")
print(f"Invalid account_type  : {invalid_acc_type}")

# COMMAND ----------

# ── Bad records → rejected ───────────────────────────────────────
rejected_df = df.filter(
    col("customer_id").isNull() |
    col("account_number").isNull() |
    col("account_type").isNull()
).withColumn("rejection_reason", lit("Null in critical field")) \
 .withColumn("rejected_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

# ── Good records → silver ────────────────────────────────────────
clean_df = df.filter(
    col("customer_id").isNotNull() &
    col("account_number").isNotNull() &
    col("account_type").isNotNull()
).dropDuplicates(["account_number"])

print(f"Clean records   : {clean_df.count()}")
print(f"Rejected records: {rejected_df.count()}")

# COMMAND ----------

# ── Standardize text fields ──────────────────────────────────────
silver_df = clean_df \
    .withColumn("city",         upper(trim(col("city")))) \
    .withColumn("state",        upper(trim(col("state")))) \
    .withColumn("account_type", upper(trim(col("account_type")))) \
    .withColumn("status",       upper(trim(col("status")))) \
    .withColumn("silver_load_timestamp", 
                lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) \
    .withColumn("is_active", 
                when(col("status") == "ACTIVE", True).otherwise(False))

silver_df.show(5, truncate=False)
print(f"Silver records ready: {silver_df.count()}")

# COMMAND ----------

# ── Write clean data to Silver ───────────────────────────────────
silver_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save(silver_customer)

print(f"Silver customer written ✅")

# ── Write rejected records ───────────────────────────────────────
if rejected_df.count() > 0:
    rejected_df.write \
        .mode("overwrite") \
        .format("delta") \
        .save(rejected_path)
    print(f"Rejected records written ✅")
else:
    print("No rejected records ✅")

# COMMAND ----------

final_df = spark.read.format("delta").load(silver_customer)
print("=" * 50)
print("SILVER CUSTOMER - FINAL SUMMARY")
print("=" * 50)
print(f"Total records  : {final_df.count()}")
print(f"Columns        : {final_df.columns}")
final_df.show(3, truncate=False)