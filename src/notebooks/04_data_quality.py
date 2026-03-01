# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Data Quality & Ingestion
# MAGIC
# MAGIC | Scenario | Problem |
# MAGIC |---|---|
# MAGIC | 4A | Auto Loader schema inference on evolving JSON |
# MAGIC | 4B | Duplicate records from non-idempotent writes |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4A — Auto Loader Schema Inference on Evolving JSON
# MAGIC **Customer says:** "Auto Loader was working fine but now it's rejecting files and quarantining records."
# MAGIC
# MAGIC > **Note:** Requires `CREATE VOLUME` privilege on `interview_practice.ingestion_autoloader`.

# COMMAND ----------

import json
import random

CATALOG = "interview_practice"
SCHEMA_NAME = "ingestion_autoloader"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

VOLUME_PATH = f"/Volumes/interview_practice/ingestion_autoloader/landing"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.landing")

# V1 files — score is a float
for i in range(5):
    records = [{"id": j, "name": f"user_{j}", "score": round(random.random() * 100, 2)} for j in range(100)]
    dbutils.fs.put(f"{VOLUME_PATH}/v1/file_{i}.json",
                   "\n".join(json.dumps(r) for r in records), overwrite=True)

# V2 files — score type changed to string (breaks original schema)
for i in range(5):
    records = [{"id": j, "name": f"user_{j}", "score": f"tier_{random.randint(1,3)}"} for j in range(100)]
    dbutils.fs.put(f"{VOLUME_PATH}/v2/file_{i}.json",
                   "\n".join(json.dumps(r) for r in records), overwrite=True)

print("Files written to volume.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis

# COMMAND ----------

# Auto Loader puts mismatched fields into `_rescued_data` column
# Check: df.select("_rescued_data").filter(col("_rescued_data").isNotNull()).show()
# Inspect cloudFiles.schemaEvolutionMode — default "addNewColumns" doesn't handle type changes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — Schema Evolution with Rescued Data

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.bronze_users
    (id BIGINT, name STRING, score STRING, _rescued_data STRING)
    USING DELTA
""")

query = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"dbfs:/tmp/{SCHEMA}/schema_store")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(VOLUME_PATH)
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"dbfs:/tmp/{SCHEMA}/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{SCHEMA}.bronze_users")
)
query.awaitTermination()

from pyspark.sql.functions import col
rescued = spark.table(f"{SCHEMA}.bronze_users").filter(col("_rescued_data").isNotNull())
print(f"Rescued (type-mismatch) records: {rescued.count()}")
print("✅ Scenario 4A complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 4B — Duplicate Records from Non-Idempotent Writes
# MAGIC **Customer says:** "Our Gold table has duplicate rows and we don't know how they got there."

# COMMAND ----------

CATALOG = "interview_practice"
SCHEMA_NAME = "ingestion_duplicates"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

base_data = spark.range(10000).selectExpr("id as order_id", "round(rand() * 500, 2) as amount")

# First run
base_data.write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.gold_orders")
# Simulated retry — writes same data again
base_data.write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.gold_orders")

dup_count = spark.sql(f"""
    SELECT count(*) as dups FROM (
        SELECT order_id, count(*) as cnt FROM {SCHEMA}.gold_orders
        GROUP BY order_id HAVING cnt > 1
    )
""").collect()[0]["dups"]
print(f"Duplicate order_ids: {dup_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

# Step 1: Deduplicate the existing table with ROW_NUMBER
spark.sql(f"""
    CREATE OR REPLACE TABLE {SCHEMA}.gold_orders AS
    SELECT * EXCEPT(rn) FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) as rn
        FROM {SCHEMA}.gold_orders
    ) WHERE rn = 1
""")

print("After dedup:", spark.table(f"{SCHEMA}.gold_orders").count())

# Step 2: Prevent future duplicates — MERGE instead of append
def write_with_dedup(df, target_table, key_col):
    df.createOrReplaceTempView("new_data")
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING new_data s ON t.{key_col} = s.{key_col}
        WHEN NOT MATCHED THEN INSERT *
    """)

write_with_dedup(base_data, f"{SCHEMA}.gold_orders", "order_id")
print("After idempotent MERGE:", spark.table(f"{SCHEMA}.gold_orders").count())
print("✅ Scenario 4B complete")
