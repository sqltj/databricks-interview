# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Delta Lake Troubleshooting
# MAGIC
# MAGIC | Scenario | Problem |
# MAGIC |---|---|
# MAGIC | 1A | Schema evolution breaking downstream reads |
# MAGIC | 1B | Small files degrading query performance |
# MAGIC | 1C | Accidental DELETE — time travel restore |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1A — Schema Evolution Breaking Downstream Reads
# MAGIC **Customer says:** "Our Silver table job started failing after the upstream team added a new column to the source data."

# COMMAND ----------

from pyspark.sql.types import *
import random

CATALOG = "interview_practice"
SCHEMA_NAME = "delta_schema_evolution"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

# V1 schema — original data
data_v1 = [(i, f"user_{i}", random.randint(18, 65)) for i in range(1000)]
schema_v1 = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])
df_v1 = spark.createDataFrame(data_v1, schema_v1)
df_v1.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.bronze_users")

# V2 schema — new column added by upstream
data_v2 = [(i, f"user_{i}", random.randint(18, 65), f"tier_{random.randint(1,3)}") for i in range(1000, 2000)]
schema_v2 = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("tier", StringType()),  # NEW COLUMN
])
df_v2 = spark.createDataFrame(data_v2, schema_v2)
df_v2.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{SCHEMA}.bronze_users")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {SCHEMA}.bronze_users").show(truncate=False)
# Downstream job fails with AnalysisException: cannot resolve column
# because it was reading with a hardcoded schema that doesn't include 'tier'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

# Option 1: Read managed table — schema tracked in metastore, always current
df = spark.read.table(f"{SCHEMA}.bronze_users")
print(f"Schema after evolution: {df.schema.simpleString()}")

# Option 2: mergeSchema on write to Silver
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{SCHEMA}.silver_users")

# Option 3: Column mapping — rename/drop columns without rewriting data files
spark.sql(f"""
  ALTER TABLE {SCHEMA}.bronze_users
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')
""")

print("✅ Scenario 1A complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1B — Small Files Problem
# MAGIC **Customer says:** "Our Delta table queries are getting slower every week even though the data size hasn't changed much."

# COMMAND ----------

from pyspark.sql.functions import lit

CATALOG = "interview_practice"
SCHEMA_NAME = "delta_small_files"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

# Simulate thousands of small file writes (streaming microbatch pattern)
for i in range(500):
    mini_df = spark.range(100).withColumn("batch", lit(i))
    mini_df.write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.events")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis

# COMMAND ----------

# numFiles in the thousands for a small dataset = problem
spark.sql(f"DESCRIBE DETAIL {SCHEMA}.events").select("numFiles", "sizeInBytes").show()
# Spark UI: many small tasks, low data per task, high overhead

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

# Option 1: OPTIMIZE (bin-packing — merges small files into ~1 GB target)
spark.sql(f"OPTIMIZE {SCHEMA}.events")

# Option 2: OPTIMIZE with Z-ORDER (co-locate data for selective queries)
spark.sql(f"OPTIMIZE {SCHEMA}.events ZORDER BY (batch)")

# Option 3: Auto-optimize on future writes (prevent recurrence)
spark.sql(f"""
  ALTER TABLE {SCHEMA}.events
  SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
  )
""")

spark.sql(f"DESCRIBE DETAIL {SCHEMA}.events").select("numFiles", "sizeInBytes").show()
print("✅ Scenario 1B complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 1C — Time Travel / Accidental Data Delete
# MAGIC **Customer says:** "Someone ran a DELETE with the wrong filter and wiped out a week of orders."

# COMMAND ----------

CATALOG = "interview_practice"
SCHEMA_NAME = "delta_time_travel"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

orders = spark.range(10000).selectExpr(
    "id as order_id",
    "date_sub(current_date(), cast(rand()*30 as int)) as order_date",
    "round(rand() * 500, 2) as amount"
)
orders.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.orders")

# Simulate bad delete
spark.sql(f"DELETE FROM {SCHEMA}.orders WHERE order_date >= '2026-02-01'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {SCHEMA}.orders").show(truncate=False)
print("Row count before delete (version 0):")
spark.sql(f"SELECT count(*) FROM {SCHEMA}.orders VERSION AS OF 0").show()
print("Row count after bad delete:")
spark.sql(f"SELECT count(*) FROM {SCHEMA}.orders").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

spark.sql(f"RESTORE TABLE {SCHEMA}.orders TO VERSION AS OF 0")
print("Row count after restore:")
spark.sql(f"SELECT count(*) FROM {SCHEMA}.orders").show()
print("✅ Scenario 1C complete")
