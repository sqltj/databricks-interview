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
# MAGIC
# MAGIC ### What's happening under the hood
# MAGIC
# MAGIC When Spark shuffles data for a join or aggregation, it assigns each row to a
# MAGIC partition using a **hash of the join key**: `partition = hash(customer_id) % numPartitions`.
# MAGIC
# MAGIC If 80% of rows share the same `customer_id`, 80% of the data lands on **one
# MAGIC partition → one task → one CPU core**. The rest of the cluster sits idle waiting
# MAGIC for that single task to finish. The job's total duration equals the slowest task.
# MAGIC
# MAGIC **Stakeholder translation:** "The job is slow because almost all the work ends up
# MAGIC on one machine. It's like routing every package in a warehouse through one worker."

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
# In production this appears as a "VIP customer", "default bucket", or catch-all category
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
# This one query tells you the root cause without needing the Spark UI
spark.table(f"{SCHEMA}.transactions") \
    .groupBy("customer_id").count().orderBy(col("count").desc()).show(5)

# ── How to read the Spark UI ───────────────────────────────────────────────────
# Go to: Stages tab → click the shuffle/join stage → "Task Metrics" section
# Look for:
#   - Task duration distribution: one bar dwarfs the others (the hot partition)
#   - "Input Size / Records": one task shows 10x the bytes of its siblings
#   - "GC Time": skewed task may show high GC if it's memory-pressured
#
# The median task time vs max task time ratio tells you the skew factor.
# If median = 2s and max = 2h, skew factor ≈ 3600x.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

df_transactions = spark.table(f"{SCHEMA}.transactions")
df_customers = spark.table(f"{SCHEMA}.customers")

# ── Option 1: AQE skew join handling (zero code change) ───────────────────────
# How AQE works: during the shuffle, Spark collects statistics on each partition's
# byte size. If a partition is > skewedPartitionFactor * median size, AQE
# automatically splits it into sub-tasks that run in parallel.
# Trade-off: works automatically but only kicks in after the shuffle completes,
# so you still pay the shuffle cost. Best for moderate skew.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
result_aqe = df_transactions.join(df_customers, "customer_id")
result_aqe.count()
print("AQE result count:", result_aqe.count())

# ── Option 2: Manual salting (when AQE isn't enough) ──────────────────────────
# How salting works:
#   1. Add a random "salt" suffix to the hot key: customer_id=1 becomes 1_0, 1_1, ... 1_9
#   2. Explode the dimension table so each salt value gets a copy of the customer row
#   3. Join on the salted key — now the 80% skew is spread across 10 partitions
#
# Trade-off: the dimension table grows by salt_factor (10x here). Acceptable when
# the dim table is small. If the dim is also large, consider a two-pass approach.
salt_factor = 10

df_salted = df_transactions \
    .withColumn("salt", floor(rand() * salt_factor).cast("string")) \
    .withColumn("customer_id_salted", concat(col("customer_id").cast("string"), lit("_"), col("salt")))

# Explode dim: each customer gets salt_factor copies (one per salt bucket)
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
# MAGIC
# MAGIC ### What's happening under the hood
# MAGIC
# MAGIC By default, Spark uses a **sort-merge join** for large tables:
# MAGIC 1. Both sides are **shuffled** — every row is sent across the network to the
# MAGIC    partition where its join key lands. This is the most expensive operation in Spark.
# MAGIC 2. Each partition is **sorted** in memory, then the two sorted sides are merged.
# MAGIC
# MAGIC OOM happens when a partition is too large to sort in the executor's heap.
# MAGIC Spark spills to disk first; if it can't spill fast enough, the executor crashes.
# MAGIC
# MAGIC **Reading the Spark UI:** Stages tab → shuffle stage → look for:
# MAGIC - "Spill (Memory)": how much data was evicted from heap to disk buffers
# MAGIC - "Spill (Disk)": how much landed on disk — high disk spill = risk of OOM
# MAGIC - "Shuffle Read Size": total bytes every task received after the shuffle
# MAGIC
# MAGIC **Stakeholder translation:** "The job is copying all the data across every machine
# MAGIC in the cluster before joining it, and each machine is running out of memory
# MAGIC trying to sort its share."

# COMMAND ----------

from pyspark.sql.functions import broadcast

CATALOG = "interview_practice"
SCHEMA_NAME = "spark_oom_spill"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

# Large fact table — 5M rows, triggers sort-merge join by default
df_large = spark.range(5_000_000).selectExpr(
    "id",
    "cast(rand() * 1000 as int) as category",
    "rand() * 100 as value"
)
df_large.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.large_facts")

# Small dimension table — 1,000 rows, a good broadcast candidate
spark.range(1000).selectExpr(
    "id as category",
    "concat('cat_', id) as category_name"
).write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.dim_categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis

# COMMAND ----------

# Spark UI → Stages → look for "Spill (Memory)" and "Spill (Disk)" columns
# Large spill = executors ran out of heap during shuffle sort phase
#
# Also useful: check the shuffle read/write size per stage
# Rule of thumb: if shuffle write > 10GB on a standard cluster, investigate alternatives

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

large = spark.table(f"{SCHEMA}.large_facts")
small = spark.table(f"{SCHEMA}.dim_categories")

# ── Option 1: Broadcast join (eliminate the shuffle entirely) ─────────────────
# Instead of shuffling both sides, Spark copies the small table to every executor.
# The large table never moves — each executor joins its local partition against
# its local copy of the dim table. Zero network shuffle for the large side.
#
# Threshold: Spark auto-broadcasts tables < spark.sql.autoBroadcastJoinThreshold
# (default 10MB). The explicit broadcast() hint overrides this for larger dims
# that you know are safe to copy (e.g., up to a few hundred MB).
#
# Trade-off: if the "small" table is actually 500MB, broadcasting it to 100
# executors uses 50GB of executor heap — potentially causing the OOM you're
# trying to fix. Always check the table's byte size before broadcasting.
result = large.join(broadcast(small), "category")
print("Broadcast join count:", result.count())

# ── Option 2: More shuffle partitions (reduce per-partition memory pressure) ──
# Default is 200 partitions. For a 5M-row shuffle, each partition holds ~25k rows.
# Doubling partition count halves the per-task memory footprint.
# Rule of thumb: aim for 100-200MB per partition after the shuffle.
spark.conf.set("spark.sql.shuffle.partitions", "400")

# ── Option 3: AQE with coalesce (auto-sizes partitions post-shuffle) ──────────
# AQE measures actual partition sizes after the shuffle completes, then coalesces
# tiny partitions together and (implicitly) avoids the overhead of too-small tasks.
# This is the "set it and forget it" option — enable it everywhere.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

result2 = large.join(small, "category")
print("AQE join count:", result2.count())
print("✅ Scenario 2B complete")
