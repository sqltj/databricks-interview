# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Orchestration & Jobs
# MAGIC
# MAGIC | Scenario | Problem |
# MAGIC |---|---|
# MAGIC | 6A | Making a pipeline idempotent / restartable |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 6A — Making a Pipeline Idempotent / Restartable
# MAGIC **Customer says:** "When our daily job fails halfway through and we restart it, we get incorrect data."

# COMMAND ----------

from pyspark.sql.functions import col

CATALOG = "interview_practice"
SCHEMA_NAME = "orchestration_idempotent"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

daily_data = spark.range(10000).selectExpr(
    "id as order_id",
    "'2026-03-01' as event_date",
    "round(rand() * 500, 2) as amount"
)
daily_data.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.staging_orders")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_orders
    (order_id BIGINT, event_date STRING, amount DOUBLE)
    USING DELTA
    PARTITIONED BY (event_date)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bad Pattern — Append Duplicates on Retry

# COMMAND ----------

# daily_data.write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.fact_orders")
# ^ Re-running this after a failure appends the same rows again → duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

# ── Option 1: replaceWhere — atomically replace only today's partition ──
# Safe to retry: always produces exactly one copy of today's data
daily_data.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "event_date = '2026-03-01'") \
    .saveAsTable(f"{SCHEMA}.fact_orders")

print("After replaceWhere:", spark.table(f"{SCHEMA}.fact_orders").count())

# ── Option 2: MERGE — idempotent upsert (handles inserts AND updates) ──
daily_data.createOrReplaceTempView("staging")
spark.sql(f"""
    MERGE INTO {SCHEMA}.fact_orders t
    USING staging s ON t.order_id = s.order_id AND t.event_date = s.event_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("After MERGE:", spark.table(f"{SCHEMA}.fact_orders").count())

# Verify no duplicates
dupes = spark.sql(f"""
    SELECT count(*) as dup_count FROM (
        SELECT order_id FROM {SCHEMA}.fact_orders
        GROUP BY order_id HAVING count(*) > 1
    )
""").collect()[0]["dup_count"]
print(f"Duplicate rows: {dupes}")
print("✅ Scenario 6A complete")
