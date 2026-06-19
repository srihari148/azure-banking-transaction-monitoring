# Databricks notebook source
# MAGIC %pip install snowflake-connector-python pandas

# COMMAND ----------

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

print("ADLS Config ready ✅")

# COMMAND ----------

import snowflake.connector
import pandas as pd

# Snowflake credentials
SF_ACCOUNT  = "<your-snowflake-account>"
SF_USER     = "<your-snowflake-username>"
SF_PASSWORD = "<your-snowflake-password>"
SF_DATABASE = "<your-snowflake-database>"
SF_SCHEMA   = "<your-snowflake-schema>"
SF_WAREHOUSE = "<your-snowflake-warehouse>"

# Test connection
conn = snowflake.connector.connect(
    account   = SF_ACCOUNT,
    user      = SF_USER,
    password  = SF_PASSWORD,
    database  = SF_DATABASE,
    schema    = SF_SCHEMA,
    warehouse = SF_WAREHOUSE
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION()")
print(f"Snowflake connected ✅ Version: {cursor.fetchone()[0]}")

# COMMAND ----------

def load_to_snowflake(delta_path, table_name):
    print(f"Loading {table_name}...")
    
    # Read Delta from ADLS
    spark_df = spark.read.format("delta").load(delta_path)
    
    # Convert all complex types to string before pandas conversion
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType, DateType, StructType, ArrayType
    
    for field in spark_df.schema.fields:
        if isinstance(field.dataType, (TimestampType, DateType)):
            spark_df = spark_df.withColumn(field.name, 
                F.col(field.name).cast("string"))
        elif isinstance(field.dataType, (StructType, ArrayType)):
            spark_df = spark_df.withColumn(field.name, 
                F.to_json(F.col(field.name)))
    
    # Convert to Pandas directly
    pandas_df = spark_df.toPandas()
    
    # Uppercase column names for Snowflake
    pandas_df.columns = [c.upper() for c in pandas_df.columns]
    
    # Truncate table
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    
    # Write to Snowflake
    from snowflake.connector.pandas_tools import write_pandas
    success, nchunks, nrows, _ = write_pandas(
        conn, pandas_df, table_name,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )
    
    print(f"  ✅ {table_name}: {nrows} rows loaded")
    return nrows

print("Helper function ready ✅")

# COMMAND ----------

# DBTITLE 1,Cell 5
print("=" * 55)
print("LOADING GOLD TABLES TO SNOWFLAKE")
print("=" * 55)

tables = [
    (get_path("gold", "customer_summary/overall_kpi"),  "CUSTOMER_KPI"),
    (get_path("gold", "customer_summary/monthly"),       "CUSTOMER_MONTHLY"),
    (get_path("gold", "branch_summary/by_branch"),       "BRANCH_SUMMARY"),
    (get_path("gold", "branch_summary/by_region"),       "REGION_SUMMARY"),
    (get_path("gold", "fraud_alerts/high_value"),        "FRAUD_HIGH_VALUE"),
    (get_path("gold", "fraud_alerts/burst"),             "FRAUD_BURST"),
    (get_path("gold", "fraud_alerts/high_daily_spend"),  "FRAUD_HIGH_SPEND"),
    (get_path("gold", "fraud_alerts/dormant"),           "FRAUD_DORMANT"),
]

_original_to_parquet = pd.DataFrame.to_parquet

def _safe_to_parquet(self, *args, **kwargs):
    original_attrs = getattr(self, "attrs", {}).copy()
    try:
        self.attrs = {}
        return _original_to_parquet(self, *args, **kwargs)
    finally:
        self.attrs = original_attrs

pd.DataFrame.to_parquet = _safe_to_parquet

total_rows = 0
try:
    for path, table in tables:
        rows = load_to_snowflake(path, table)
        total_rows += rows
finally:
    pd.DataFrame.to_parquet = _original_to_parquet

print("=" * 55)
print(f"Total rows loaded to Snowflake: {total_rows} ✅")
print("=" * 55)

# COMMAND ----------

print("=" * 55)
print("VERIFICATION - ROW COUNTS IN SNOWFLAKE")
print("=" * 55)

verify_tables = [
    "CUSTOMER_KPI", "CUSTOMER_MONTHLY", "BRANCH_SUMMARY",
    "REGION_SUMMARY", "FRAUD_HIGH_VALUE", "FRAUD_BURST",
    "FRAUD_HIGH_SPEND", "FRAUD_DORMANT"
]

for table in verify_tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"{table:25} : {count} rows")

cursor.close()
conn.close()
print("\nSnowflake connection closed ✅")