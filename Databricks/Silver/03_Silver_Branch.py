# Databricks notebook source
from pyspark.sql.functions import lit, col, upper, trim
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

bronze_branch  = get_path("bronze", "branch")
silver_branch  = get_path("silver", "branch")
rejected_branch = get_path("rejected", "branch")

print("Config ready ✅")

# COMMAND ----------

df = spark.read.format("delta").load(bronze_branch)
print(f"Bronze records: {df.count()}")
df.show(5, truncate=False)

# COMMAND ----------

print("=" * 50)
print("DATA QUALITY REPORT - BRANCH")
print("=" * 50)

total            = df.count()
null_branch_id   = df.filter(col("branch_id").isNull()).count()
null_branch_name = df.filter(col("branch_name").isNull()).count()
duplicate_branch = total - df.dropDuplicates(["branch_id"]).count()
invalid_region   = df.filter(
    ~col("region").isin(["North","South","East","West","Central"])
).count()

print(f"Total records       : {total}")
print(f"Null branch_id      : {null_branch_id}")
print(f"Null branch_name    : {null_branch_name}")
print(f"Duplicate branch_id : {duplicate_branch}")
print(f"Invalid region      : {invalid_region}")

# COMMAND ----------

# Bad records
rejected_df = df.filter(
    col("branch_id").isNull() |
    col("branch_name").isNull()
).withColumn("rejection_reason", lit("Null in critical field")) \
 .withColumn("rejected_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

# Good records
silver_df = df.filter(
    col("branch_id").isNotNull() &
    col("branch_name").isNotNull()
).dropDuplicates(["branch_id"]) \
 .withColumn("city",   upper(trim(col("city")))) \
 .withColumn("state",  upper(trim(col("state")))) \
 .withColumn("region", upper(trim(col("region")))) \
 .withColumn("silver_load_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"Clean records   : {silver_df.count()}")
print(f"Rejected records: {rejected_df.count()}")

# COMMAND ----------

silver_df.write.mode("overwrite").format("delta").save(silver_branch)
print("Silver branch written ✅")

if rejected_df.count() > 0:
    rejected_df.write.mode("overwrite").format("delta").save(rejected_branch)
    print("Rejected records written ✅")
else:
    print("No rejected records ✅")

# COMMAND ----------

final_df = spark.read.format("delta").load(silver_branch)
print("=" * 50)
print("SILVER BRANCH - FINAL SUMMARY")
print("=" * 50)
print(f"Total records : {final_df.count()}")
final_df.show(5, truncate=False)