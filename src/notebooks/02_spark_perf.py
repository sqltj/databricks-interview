# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Spark Performance & Debugging
# MAGIC
# MAGIC | Scenario | Problem |
# MAGIC |---|---|
# MAGIC | 2A | Data skew — one task runs 2.5 hours while others finish in minutes |
# MAGIC | 2B | OOM / shuffle spill during large join |
# MAGIC | 2C | Small files degrading scan performance |
# MAGIC | 2D | Wrong join strategy — SortMergeJoin where broadcast would work |
# MAGIC | 2E | Full table scan — missing filter pushdown and Z-ORDER |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Serverless Diagnostic Toolkit
# MAGIC
# MAGIC **There is no Spark UI on serverless compute.** These are your replacements:
# MAGIC
# MAGIC | Goal | Command | Replaces |
# MAGIC |------|---------|---------|
# MAGIC | See query execution plan | `df.explain("formatted")` | DAG / Stages tab |
# MAGIC | Identify join strategy | Look for `BroadcastHashJoin` vs `SortMergeJoin` in plan | Stages → shuffle size |
# MAGIC | Stage timings + shuffle bytes | Click **query profile bar** under any cell output | Stages tab |
# MAGIC | File count and avg file size | `DESCRIBE DETAIL table` → `numFiles`, `sizeInBytes` | Storage UI |
# MAGIC | Diagnose data skew | `df.groupBy("key").count().orderBy(desc("count")).show(10)` | Task duration histogram |
# MAGIC | Confirm AQE is on | `spark.conf.get("spark.sql.adaptive.enabled")` | N/A |
# MAGIC | Rebuild column statistics | `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` | N/A |
# MAGIC
# MAGIC **Mental model shift:** On classic clusters you tune at the *executor level* (heap, GC, task count).
# MAGIC On serverless you tune at the *data level* — file sizes, key distribution, join strategy, query plan.
# MAGIC The cluster auto-scales and manages itself. Your job is to give it good data and good queries.

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

from pyspark.sql.functions import col, lit, concat, floor, rand, explode, array, desc
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

# ── Primary diagnosis: distribution check (works on serverless, no Spark UI needed) ──
# This one query tells you the root cause before writing any fix.
spark.table(f"{SCHEMA}.transactions") \
    .groupBy("customer_id").count().orderBy(col("count").desc()).show(5)

# If top key has 10x+ more rows than the median key, you have actionable skew.
# Skew factor = max_count / median_count — anything above ~5x is worth addressing.

# ── On serverless: use the notebook query profile ──────────────────────────────
# After running a join cell, click the bar chart icon in the cell output.
# Look for: one stage bar dramatically longer than the others → hot partition.
# "Shuffle Read" column shows total bytes shuffled — compare per-task distribution.

# ── On classic clusters (Spark UI): ───────────────────────────────────────────
# Stages tab → click the join stage → Task Metrics → duration distribution.
# One bar dwarfs the rest = skew. Also check "Input Size / Records" per task.

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
# On serverless, AQE is ENABLED BY DEFAULT — check before reaching for salting.
# Trade-off: only kicks in after the shuffle completes, so you still pay shuffle cost.
print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
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
# MAGIC **On serverless:** You can't see spill metrics directly, but you can:
# MAGIC - Check `df.explain("formatted")` to confirm a SortMergeJoin is occurring
# MAGIC - Use the notebook query profile to see shuffle read size per stage
# MAGIC - If the job is killed without OOM, serverless auto-scaled and ran out of capacity
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

large = spark.table(f"{SCHEMA}.large_facts")
small = spark.table(f"{SCHEMA}.dim_categories")

# ── Serverless diagnosis: read the query plan ──────────────────────────────────
# SortMergeJoin = both sides shuffled across the network (expensive, can OOM)
# BroadcastHashJoin = small table copied to every executor (no shuffle, safe)
print("=== Join plan WITHOUT broadcast hint ===")
large.join(small, "category").explain("formatted")

# On classic clusters: Stages tab → "Spill (Memory)" and "Spill (Disk)" columns.
# Rule of thumb: if shuffle write > 10GB on a standard cluster, investigate alternatives.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution

# COMMAND ----------

# ── Option 1: Broadcast join (eliminate the shuffle entirely) ─────────────────
# Instead of shuffling both sides, Spark copies the small table to every executor.
# The large table never moves — each executor joins its local partition against
# its local copy of the dim table. Zero network shuffle for the large side.
#
# Threshold: Spark auto-broadcasts tables < spark.sql.autoBroadcastJoinThreshold
# (default 10MB). The explicit broadcast() hint overrides this for larger dims.
#
# Trade-off: if the "small" table is actually 500MB, broadcasting it to 100
# executors uses 50GB of executor heap — potentially causing the OOM you're
# trying to fix. Always verify table size before broadcasting.
result = large.join(broadcast(small), "category")
print("=== Join plan WITH broadcast hint ===")
result.explain("formatted")
# Now look for BroadcastHashJoin in the plan — shuffle is gone
print("Broadcast join count:", result.count())

# ── Option 2: More shuffle partitions (reduce per-partition memory pressure) ──
# Default is 200. Doubling partition count halves per-task memory footprint.
# Rule of thumb: aim for 100–200 MB per partition after the shuffle.
spark.conf.set("spark.sql.shuffle.partitions", "400")

# ── Option 3: AQE coalesce (auto-sizes partitions post-shuffle) ───────────────
# AQE measures actual partition sizes after the shuffle and coalesces small ones.
# Enable everywhere — zero downside on serverless.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

result2 = large.join(small, "category")
print("AQE join count:", result2.count())
print("✅ Scenario 2B complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2C — Small Files Degrading Scan Performance
# MAGIC **Customer says:** "Our query used to take 5 seconds. After a week of streaming
# MAGIC writes it now takes 4 minutes — and we haven't changed any code."
# MAGIC
# MAGIC ### What's happening under the hood
# MAGIC
# MAGIC Delta Lake writes one file per partition per write operation. Streaming jobs,
# MAGIC frequent micro-batch writes, and high-partition-count operations accumulate
# MAGIC thousands of tiny files over time.
# MAGIC
# MAGIC Each small file is a **separate network round-trip** to cloud object storage (S3/ADLS/GCS).
# MAGIC A query that could read 40 large files now makes 4,000 requests, spending most of
# MAGIC its time on I/O overhead rather than actually processing data.
# MAGIC
# MAGIC **On serverless: `DESCRIBE DETAIL` is your Spark UI replacement here.**
# MAGIC The `numFiles` and `sizeInBytes` columns tell you the exact file count and average
# MAGIC file size without needing any cluster metrics.
# MAGIC
# MAGIC **Stakeholder translation:** "We're making thousands of tiny trips to the data
# MAGIC warehouse instead of a handful of large ones. OPTIMIZE consolidates those files
# MAGIC the same way you'd pack a box efficiently before shipping."

# COMMAND ----------

CATALOG = "interview_practice"
SCHEMA_NAME = "spark_small_files"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

# Simulate a table that accumulated many small files via repeated small writes
# (mirrors what happens with streaming micro-batches or frequent incremental loads)
base_df = spark.range(1_000_000).selectExpr(
    "id",
    "cast(rand() * 100 as int) as category",
    "cast(rand() * 1000 as int) as customer_id",
    "rand() * 500 as amount"
)

# Write 50 "micro-batches" of 20k rows each — produces ~50 small files
for batch in range(50):
    batch_df = base_df.filter((col("id") % 50) == batch)
    batch_df.write.format("delta").mode("append").saveAsTable(f"{SCHEMA}.events")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis — DESCRIBE DETAIL (serverless-friendly)

# COMMAND ----------

# ── Primary serverless diagnostic: DESCRIBE DETAIL ────────────────────────────
detail = spark.sql(f"DESCRIBE DETAIL {SCHEMA}.events")
detail.select("numFiles", "sizeInBytes").show()

# Calculate average file size
num_files  = detail.collect()[0]["numFiles"]
total_size = detail.collect()[0]["sizeInBytes"]
avg_mb = (total_size / num_files) / (1024 * 1024) if num_files else 0
print(f"Files: {num_files}, Avg size: {avg_mb:.2f} MB")
print(f"⚠️  Target: 128–256 MB per file. Below 32 MB is a small-files problem.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — OPTIMIZE + ZORDER

# COMMAND ----------

# ── OPTIMIZE: bin-packs small files into ~256 MB target file size ──────────────
# This is a Delta Lake operation — it rewrites files in the background and
# updates the transaction log. Reads continue uninterrupted during the operation.
spark.sql(f"OPTIMIZE {SCHEMA}.events ZORDER BY (customer_id)")
# ZORDER BY co-locates rows with the same customer_id in the same files.
# This lets Delta skip entire files when filtering by customer_id,
# because each file's min/max stats in the Delta log will exclude non-matching rows.

# Verify: file count should drop dramatically
detail_after = spark.sql(f"DESCRIBE DETAIL {SCHEMA}.events")
detail_after.select("numFiles", "sizeInBytes").show()

num_after = detail_after.collect()[0]["numFiles"]
print(f"Files before: {num_files} → after OPTIMIZE: {num_after}")
print(f"Reduction: {((num_files - num_after) / num_files * 100):.0f}%")

# Time a filtered query before vs after (Delta file skipping benefit of ZORDER)
import time
spark.sql(f"SELECT count(*) FROM {SCHEMA}.events WHERE customer_id = 42").show()

spark.sql(f"DESCRIBE HISTORY {SCHEMA}.events").select("version","operation","operationMetrics").show(5, truncate=False)
print("✅ Scenario 2C complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2D — Wrong Join Strategy
# MAGIC **Customer says:** "A simple lookup join takes forever — it's joining 5 million
# MAGIC transactions against a 1,000-row reference table."
# MAGIC
# MAGIC ### What's happening under the hood
# MAGIC
# MAGIC Without a size hint, Spark's optimizer may not know the reference table is small
# MAGIC (especially after a filter, or if table statistics are stale). It defaults to
# MAGIC **SortMergeJoin**: shuffles both tables across the network, sorts each partition,
# MAGIC then merges. For a 1,000-row table this is massively wasteful.
# MAGIC
# MAGIC **`df.explain("formatted")` is the key diagnostic here.** Look for:
# MAGIC - `SortMergeJoin` → both sides shuffled, expensive
# MAGIC - `BroadcastHashJoin` → small side broadcast, no shuffle on the large side
# MAGIC
# MAGIC **This reuses the tables from Scenario 2B** — no regeneration needed.

# COMMAND ----------

CATALOG = "interview_practice"
SCHEMA_NAME = "spark_oom_spill"   # tables already written by Scenario 2B
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Reading from: {SCHEMA}")

large = spark.table(f"{SCHEMA}.large_facts")     # 5M rows
small = spark.table(f"{SCHEMA}.dim_categories")  # 1K rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis — df.explain() reveals the join strategy

# COMMAND ----------

# ── Step 1: check what plan Spark actually chose ───────────────────────────────
# Read the output carefully: look for the join node near the top of the physical plan.
# "SortMergeJoin" → both sides shuffled (expensive)
# "BroadcastHashJoin" → small side broadcast, large side local (fast)
print("=== Default plan — what did Spark choose? ===")
large.join(small, "category").explain("formatted")

# ── Step 2: confirm dim table size ────────────────────────────────────────────
dim_detail = spark.sql(f"DESCRIBE DETAIL {SCHEMA}.dim_categories")
dim_size_mb = dim_detail.collect()[0]["sizeInBytes"] / (1024 * 1024)
print(f"\ndim_categories size: {dim_size_mb:.2f} MB")
print(f"autoBroadcastJoinThreshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')} bytes (default 10MB)")
# If dim_size_mb < 10 MB but Spark didn't broadcast → stats may be stale

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — broadcast() hint or ANALYZE TABLE

# COMMAND ----------

from pyspark.sql.functions import broadcast

# ── Option 1: explicit broadcast() hint ───────────────────────────────────────
# Overrides the threshold. Use when you know the table is safe to broadcast
# even if Spark's statistics don't reflect that.
result_broadcast = large.join(broadcast(small), "category")
print("=== Plan with broadcast() hint ===")
result_broadcast.explain("formatted")
# Confirm: BroadcastHashJoin should now appear in the plan
print("Count:", result_broadcast.count())

# ── Option 2: ANALYZE TABLE to refresh statistics ─────────────────────────────
# If the broadcast threshold is correct but stats are stale, this fixes the plan
# without needing a code hint. Better for production — no code change required.
spark.sql(f"ANALYZE TABLE {SCHEMA}.dim_categories COMPUTE STATISTICS FOR ALL COLUMNS")
# After ANALYZE, Spark knows the real row count and byte size and may auto-broadcast
result_analyzed = large.join(small, "category")
print("\n=== Plan after ANALYZE TABLE ===")
result_analyzed.explain("formatted")
print("✅ Scenario 2D complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 2E — Full Table Scan / Missing Filter Pushdown
# MAGIC **Customer says:** "A simple filter query on our events table reads all 500 GB
# MAGIC even though we're only looking at one customer."
# MAGIC
# MAGIC ### What's happening under the hood
# MAGIC
# MAGIC Delta Lake stores **column-level statistics** (min, max, null count) for each
# MAGIC file in the transaction log. When you filter, Delta compares your predicate against
# MAGIC these statistics and **skips files** where the min/max range can't contain a match.
# MAGIC
# MAGIC If data is randomly distributed (the default after writes), many files will have
# MAGIC overlapping min/max ranges for any given column → no files can be skipped → full scan.
# MAGIC
# MAGIC **ZORDER BY co-locates similar values in the same files**, tightening the min/max
# MAGIC range per file and enabling Delta to skip most of the table on a point query.
# MAGIC
# MAGIC **How to diagnose on serverless:**
# MAGIC - `df.explain("formatted")` shows `DataFilters` — whether the filter is present.
# MAGIC - The notebook **query profile** (cell output bar chart) shows "Files Read" vs
# MAGIC   "Files Skipped" — this is the smoking gun for missing file-level skipping.
# MAGIC
# MAGIC **Stakeholder translation:** "We sorted the filing cabinet by customer name.
# MAGIC Now instead of reading every folder to find one customer, we go straight to the
# MAGIC right drawer and skip everything else."

# COMMAND ----------

CATALOG = "interview_practice"
SCHEMA_NAME = "spark_filter_pushdown"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

# Write 500k rows randomly distributed — customer_id values are spread across all files.
# Delta file stats for each file will show min=1, max=50000 → no files can be skipped.
spark.range(500_000).selectExpr(
    "id as event_id",
    "cast(rand() * 50000 as int) as customer_id",
    "cast(rand() * 100 as int) as category",
    "rand() * 500 as amount",
    "current_date() - cast(rand() * 365 as int) as event_date"
).write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.events")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis — explain() + query profile

# COMMAND ----------

from pyspark.sql.functions import col

# ── Step 1: confirm the filter is present in the plan ─────────────────────────
filtered = spark.table(f"{SCHEMA}.events").filter(col("customer_id") == 12345)
print("=== Plan before ZORDER ===")
filtered.explain("formatted")
# Look for: DataFilters: [isnotnull(customer_id), (customer_id = 12345)]
# This confirms Spark is filtering — but file-level skipping still depends on Z-ORDER.
# After running, check the query profile: "Files Read" will equal total file count.

# ── Step 2: confirm no file skipping is happening ─────────────────────────────
before_detail = spark.sql(f"DESCRIBE DETAIL {SCHEMA}.events")
total_files = before_detail.collect()[0]["numFiles"]
print(f"\nTotal files in table: {total_files}")
print("Without ZORDER, Delta reads ALL files even with a customer_id filter.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — OPTIMIZE ZORDER BY (customer_id)

# COMMAND ----------

# ── ZORDER BY customer_id: co-locate rows with the same customer_id ────────────
# After ZORDER, each file contains a narrow range of customer_id values.
# Delta log stats show tight min/max per file → most files skipped on point queries.
spark.sql(f"OPTIMIZE {SCHEMA}.events ZORDER BY (customer_id)")

print("=== Plan after ZORDER (filter logic is unchanged) ===")
filtered_after = spark.table(f"{SCHEMA}.events").filter(col("customer_id") == 12345)
filtered_after.explain("formatted")
# The plan looks the same — the improvement is in I/O, not the logical plan.
# Check the query profile after running this cell:
# "Files Skipped" should now be >> "Files Read"

# ── Validate: compare result counts and check history ─────────────────────────
print(f"\nRows for customer 12345: {filtered_after.count()}")

after_detail = spark.sql(f"DESCRIBE DETAIL {SCHEMA}.events")
after_files = after_detail.collect()[0]["numFiles"]
print(f"Files before OPTIMIZE: {total_files} → after: {after_files}")

spark.sql(f"DESCRIBE HISTORY {SCHEMA}.events") \
    .select("version", "operation", "operationMetrics") \
    .show(3, truncate=False)

# ── When to use PARTITION BY instead ──────────────────────────────────────────
# PARTITION BY physically separates data into subdirectories — best for low-
# cardinality columns (date, region, status) with < ~1000 distinct values.
# ZORDER BY is better for high-cardinality columns (customer_id, product_id)
# where PARTITION BY would create millions of tiny directories.
print("✅ Scenario 2E complete")
