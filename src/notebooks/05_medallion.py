# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Medallion Architecture Design
# MAGIC
# MAGIC | Scenario | Problem |
# MAGIC |---|---|
# MAGIC | 5A | Design a Bronze/Silver/Gold pipeline for clickstream events |
# MAGIC | 5B | SCD Type 2 historical tracking with Delta MERGE |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5A — Bronze / Silver / Gold Clickstream Pipeline
# MAGIC **Customer says:** "We're ingesting clickstream events from S3 and need a reporting layer for marketing."
# MAGIC
# MAGIC ```
# MAGIC S3 JSON Files
# MAGIC      │
# MAGIC      ▼
# MAGIC [BRONZE] — Raw ingest via Auto Loader. No transforms. Keep _rescued_data.
# MAGIC      │
# MAGIC      ▼
# MAGIC [SILVER] — Parse, deduplicate, cast types, filter nulls.
# MAGIC      │
# MAGIC      ▼
# MAGIC [GOLD] — Aggregations: daily active users, funnel conversion, session rollups.
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, sum

CATALOG = "interview_practice"
SCHEMA_NAME = "medallion_clickstream"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

# ── Bronze: raw landing — no transforms, keep all fields ──
clickstream = spark.range(1_000_000).selectExpr(
    "uuid() as event_id",
    "cast(rand() * 10000 as int) as user_id",
    "case when rand() < 0.4 then 'page_view' when rand() < 0.7 then 'click' else 'purchase' end as event_type",
    "date_sub(current_date(), cast(rand() * 30 as int)) as event_date",
    "current_timestamp() - (rand() * 86400 * interval 1 second) as event_time",
    "round(rand() * 200, 2) as revenue"
)

clickstream.write.format("delta").partitionBy("event_date").mode("overwrite") \
    .saveAsTable(f"{SCHEMA}.bronze_clickstream")

print(f"Bronze rows: {spark.table(f'{SCHEMA}.bronze_clickstream').count():,}")

# COMMAND ----------

# ── Silver: parse, deduplicate, validate ──
silver = (
    spark.table(f"{SCHEMA}.bronze_clickstream")
    .dropDuplicates(["event_id"])
    .filter(col("user_id").isNotNull())
    .filter(col("event_type").isin("page_view", "click", "purchase"))
)

silver.write.format("delta").partitionBy("event_date").mode("overwrite") \
    .saveAsTable(f"{SCHEMA}.silver_clickstream")

print(f"Silver rows: {spark.table(f'{SCHEMA}.silver_clickstream').count():,}")

# COMMAND ----------

# ── Gold: daily aggregation for marketing dashboards ──
gold = (
    spark.table(f"{SCHEMA}.silver_clickstream")
    .groupBy("event_date", "event_type")
    .agg(
        countDistinct("user_id").alias("unique_users"),
        count("event_id").alias("event_count"),
        sum("revenue").alias("total_revenue"),
    )
)

gold.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.gold_daily_summary")

spark.table(f"{SCHEMA}.gold_daily_summary").orderBy("event_date", "event_type").show(10)
print("✅ Scenario 5A complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 5B — SCD Type 2 with Delta MERGE
# MAGIC **Customer says:** "We need to track historical changes to customer records — when their tier changes, we keep the old record."

# COMMAND ----------

from pyspark.sql.functions import current_date, lit, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType
import datetime

CATALOG = "interview_practice"
SCHEMA_NAME = "medallion_scd2"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

# Initial state — use explicit DATE type to match current_date() in the SCD2 MERGE step
customers_v1 = spark.createDataFrame([
    (1, "Alice", "Gold",   datetime.date(2024, 1, 1), datetime.date(9999, 12, 31), True),
    (2, "Bob",   "Silver", datetime.date(2024, 1, 1), datetime.date(9999, 12, 31), True),
], ["customer_id", "name", "tier", "effective_date", "end_date", "is_current"])

customers_v1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.dim_customers")
print("Initial state:")
spark.table(f"{SCHEMA}.dim_customers").show()

# Incoming update: Alice upgraded to Platinum
updates = spark.createDataFrame([(1, "Alice", "Platinum")], ["customer_id", "name", "tier"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — Two-Step SCD Type 2 MERGE

# COMMAND ----------

updates.createOrReplaceTempView("updates")

# Step 1: Close the current record (set end_date + is_current=false)
spark.sql(f"""
    MERGE INTO {SCHEMA}.dim_customers t
    USING updates s ON t.customer_id = s.customer_id AND t.is_current = true
    WHEN MATCHED AND t.tier != s.tier THEN
        UPDATE SET t.end_date = current_date(), t.is_current = false
""")

# Step 2: Insert new current record
new_records = updates.select(
    col("customer_id"),
    col("name"),
    col("tier"),
    current_date().alias("effective_date"),
    to_date(lit("9999-12-31")).alias("end_date"),
    lit(True).alias("is_current"),
)
new_records.write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.dim_customers")

print("After SCD Type 2 update:")
spark.table(f"{SCHEMA}.dim_customers").orderBy("customer_id", "effective_date").show()
print("✅ Scenario 5B complete")
