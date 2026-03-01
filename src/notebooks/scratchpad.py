# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Interview Scratchpad
# MAGIC
# MAGIC **Fill in this table during Phase 1 (Discovery) before writing any code.**
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |---|---|
# MAGIC | Data domain / business context? | |
# MAGIC | Approximate row count / volume? | |
# MAGIC | Batch or streaming? Update frequency? | |
# MAGIC | Who are the downstream consumers? | |
# MAGIC | Transformations / business rules needed? | |
# MAGIC | Data quality concerns (nulls, dupes, late data)? | |
# MAGIC | Output format — table, dashboard, API? | |
# MAGIC | Retention / SLA requirements? | |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — fill in after discovery

# COMMAND ----------

# ── Fill these in based on the interview prompt ────────────────────────────────
CATALOG = "interview"
SCHEMA  = "main"           # one schema per scenario is fine
TABLE   = "bronze_raw"     # rename to something domain-specific

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✅ Working in: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serverless Diagnostic Toolkit
# MAGIC
# MAGIC > **On serverless there is no Spark UI.** Use these instead.
# MAGIC
# MAGIC | Goal | Command | Replaces |
# MAGIC |------|---------|---------|
# MAGIC | See the query execution plan | `df.explain("formatted")` | DAG / Stages tab |
# MAGIC | Check join strategy chosen | Look for `BroadcastHashJoin` vs `SortMergeJoin` in plan | Stages → shuffle size |
# MAGIC | Stage timings, shuffle bytes | Click **query profile bar** under cell output | Stages tab |
# MAGIC | File count and avg file size | `DESCRIBE DETAIL table` → `numFiles`, `sizeInBytes` | Storage UI |
# MAGIC | Confirm AQE is on (it is by default) | `spark.conf.get("spark.sql.adaptive.enabled")` | N/A |
# MAGIC | Diagnose data skew | `df.groupBy("key").count().orderBy(desc("count")).show(10)` | Task duration histogram |
# MAGIC | Rebuild file statistics for better plans | `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` | N/A |
# MAGIC
# MAGIC **Mental model shift:** On classic clusters you tune at the *executor level* (heap, GC, task count).
# MAGIC On serverless you tune at the *data level* — file sizes, key distribution, join strategy, query plan.
# MAGIC The cluster manages itself; your job is to give it good data and good queries.
# MAGIC
# MAGIC ### The 5 performance problems you can solve on serverless
# MAGIC
# MAGIC | Problem | Signal | Fix |
# MAGIC |---------|--------|-----|
# MAGIC | **Data skew** | Top key has 10x+ more rows than others | AQE (default on) or manual salting |
# MAGIC | **Small files** | `numFiles` in thousands, avg size < 32 MB | `OPTIMIZE table ZORDER BY (col)` |
# MAGIC | **Wrong join strategy** | `SortMergeJoin` in plan for a small table | `broadcast()` hint |
# MAGIC | **Full table scan** | `DataFilters` applied *above* `FileScan` in plan | `OPTIMIZE ZORDER BY (filter_col)` |
# MAGIC | **Bad query plans** | Unexpected join order or missing broadcast | `ANALYZE TABLE ... COMPUTE STATISTICS` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Generate Synthetic Dataset
# MAGIC
# MAGIC > See `00_data_generator.py` for ready-made domain templates
# MAGIC > (e-commerce, IoT, financial, user events).

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random, datetime

# ── Define schema to match the interview prompt ────────────────────────────────
# Think out loud: Start by naming your columns and types explicitly.
# This documents your understanding of the data contract before touching any data.
schema = StructType([
    StructField("id",         LongType(),      False),
    StructField("created_at", TimestampType(), True),
    # TODO: add fields that match the prompt
    # StructField("customer_id", IntegerType(), True),
    # StructField("amount",      DoubleType(),  True),
    # StructField("status",      StringType(),  True),
])

# ── Generate rows ──────────────────────────────────────────────────────────────
NUM_ROWS = 100_000  # adjust to make the scenario interesting but fast

now = datetime.datetime.now()
data = [
    Row(
        id         = i,
        created_at = now - datetime.timedelta(days=random.randint(0, 90)),
        # TODO: populate the fields you defined above
    )
    for i in range(NUM_ROWS)
]

df_raw = spark.createDataFrame(data, schema)
df_raw.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")

print(f"✅ Wrote {NUM_ROWS:,} rows to {CATALOG}.{SCHEMA}.{TABLE}")
df_raw.printSchema()
df_raw.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Bronze (Raw Ingest)
# MAGIC
# MAGIC **Talking point:** Bronze is the untouched raw record, preserved exactly as received.
# MAGIC We never transform in Bronze — it's the replay buffer. If something goes wrong
# MAGIC downstream, we can always reprocess from here without re-ingesting from source.

# COMMAND ----------

df_bronze = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}")

# Quick sanity check — confirm row count and spot-check nulls
print(f"Bronze row count: {df_bronze.count():,}")
df_bronze.select([count(when(col(c).isNull(), c)).alias(c) for c in df_bronze.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Silver (Cleanse & Validate)
# MAGIC
# MAGIC **Talking point:** Silver enforces the schema contract and data quality rules.
# MAGIC Downstream consumers trust Silver to be clean and consistent — nulls removed,
# MAGIC duplicates deduplicated, types cast correctly. Changes here propagate downstream,
# MAGIC so Silver transformations should be idempotent (safe to re-run).

# COMMAND ----------

df_silver = (
    df_bronze
    .filter(col("id").isNotNull())          # drop nulls on primary key
    .dropDuplicates(["id"])                 # dedup on natural key
    # .withColumn("date", col("created_at").cast("date"))   # useful for partitioning
    # TODO: add transformations that match the problem
    # .filter(...)
    # .withColumn(...)
    # .withColumnRenamed(...)
)

# Write Silver — mergeSchema handles column additions gracefully
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_clean")

print(f"Silver row count: {df_silver.count():,}")
df_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Gold (Business Aggregations)
# MAGIC
# MAGIC **Talking point:** Gold answers a specific business question. Each Gold table maps
# MAGIC to one stakeholder use case — a dashboard, report, or ML feature. By keeping Gold
# MAGIC narrow and purpose-built, we avoid "everything table" anti-patterns that make
# MAGIC queries slow and schemas confusing.

# COMMAND ----------

df_silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_clean")

df_gold = (
    df_silver
    # TODO: aggregate to answer the business question from the prompt
    # .groupBy("date")
    # .agg(
    #     count("*").alias("total_records"),
    #     sum("amount").alias("total_revenue"),
    #     avg("amount").alias("avg_order_value"),
    # )
    # .orderBy("date")
)

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_summary")

print(f"Gold row count: {df_gold.count():,}")
df_gold.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Diagnostics & Validation

# COMMAND ----------

# ── Row counts across all layers ───────────────────────────────────────────────
for tbl in [TABLE, "silver_clean", "gold_summary"]:
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{tbl}").count()
        print(f"  {tbl}: {n:,} rows")
    except Exception as e:
        print(f"  {tbl}: MISSING ({e})")

# COMMAND ----------

# Schema and Delta history for Silver (shows mergeSchema events, optimizations, etc.)
spark.sql(f"DESCRIBE EXTENDED {CATALOG}.{SCHEMA}.silver_clean").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY  {CATALOG}.{SCHEMA}.silver_clean").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown
# MAGIC
# MAGIC > Uncomment and run after the interview to keep the workspace clean.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
# spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")
# print("✅ Cleaned up")
