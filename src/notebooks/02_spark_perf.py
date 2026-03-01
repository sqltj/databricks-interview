# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Spark Performance & Debugging
# MAGIC
# MAGIC | Scenario | Problem |
# MAGIC |---|---|
# MAGIC | 2A | Data skew — one task runs 2.5 hours while others finish in minutes |
# MAGIC | 2B | OOM / shuffle spill during large join |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2A — Data Skew
# MAGIC **Customer says:** "Our join job takes 3 hours. The Spark UI shows one task running for 2.5 hours while the rest finish in minutes."

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, floor, rand, explode, array
import random

CATALOG = "interview_practice"
SCHEMA_NAME = "spark_data_skew"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

# 80% of rows have customer_id = 1  →  extreme skew
skewed_data = [
    (1 if random.random() < 0.8 else random.randint(2, 1000),
     random.randint(1, 100),
     round(random.uniform(10, 500), 2))
    for _ in range(1_000_000)
]
df_skewed = spark.createDataFrame(skewed_data, ["customer_id", "product_id", "amount"])
df_skewed.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.transactions")

customers = spark.range(1000).selectExpr("id as customer_id", "concat('Customer_', id) as name")
customers.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis

# COMMAND ----------

# Distribution check — customer_id=1 should dominate
spark.table(f"{SCHEMA}.transactions") \
    .groupBy("customer_id").count().orderBy(col("count").desc()).show(5)

# In Spark UI: Stages → task duration distribution shows massive outlier

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

df_transactions = spark.table(f"{SCHEMA}.transactions")
df_customers = spark.table(f"{SCHEMA}.customers")

# ── Option 1: AQE skew join handling (zero code change, works automatically) ──
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
result_aqe = df_transactions.join(df_customers, "customer_id")
result_aqe.count()
print("AQE result count:", result_aqe.count())

# ── Option 2: Manual salting (when AQE isn't enough) ──
salt_factor = 10

df_salted = df_transactions \
    .withColumn("salt", floor(rand() * salt_factor).cast("string")) \
    .withColumn("customer_id_salted", concat(col("customer_id").cast("string"), lit("_"), col("salt")))

customers_exploded = df_customers \
    .withColumn("salt", explode(array([lit(str(i)) for i in range(salt_factor)]))) \
    .withColumn("customer_id_salted", concat(col("customer_id").cast("string"), lit("_"), col("salt")))

result_salted = df_salted.join(customers_exploded, "customer_id_salted").drop("salt", "customer_id_salted")
print("Salted result count:", result_salted.count())
print("✅ Scenario 2A complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2B — OOM / Shuffle Spill
# MAGIC **Customer says:** "Our job is failing with OutOfMemoryError during a large join."

# COMMAND ----------

from pyspark.sql.functions import broadcast

CATALOG = "interview_practice"
SCHEMA_NAME = "spark_oom_spill"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

df_large = spark.range(5_000_000).selectExpr(
    "id",
    "cast(rand() * 1000 as int) as category",
    "rand() * 100 as value"
)
df_large.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.large_facts")

spark.range(1000).selectExpr(
    "id as category",
    "concat('cat_', id) as category_name"
).write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.dim_categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis

# COMMAND ----------

# Spark UI → Stages → look for "Spill (Memory)" and "Spill (Disk)" columns
# Large spill = executors ran out of heap during shuffle
# Also check: shuffle read/write size per stage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

large = spark.table(f"{SCHEMA}.large_facts")
small = spark.table(f"{SCHEMA}.dim_categories")

# ── Option 1: Broadcast join (eliminate shuffle entirely for small tables) ──
result = large.join(broadcast(small), "category")
print("Broadcast join count:", result.count())

# ── Option 2: More shuffle partitions (reduce per-partition memory pressure) ──
spark.conf.set("spark.sql.shuffle.partitions", "400")

# ── Option 3: AQE with coalesce (auto-sizes partitions after shuffle) ──
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

result2 = large.join(small, "category")
print("AQE join count:", result2.count())
print("✅ Scenario 2B complete")
