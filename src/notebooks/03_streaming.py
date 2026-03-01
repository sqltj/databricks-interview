# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Streaming Pipeline Issues
# MAGIC
# MAGIC | Scenario | Problem |
# MAGIC |---|---|
# MAGIC | 3A | Late-arriving data breaking watermarks |
# MAGIC | 3B | Checkpoint corruption / reprocessing duplicates |
# MAGIC
# MAGIC > **Job execution note:** Streaming queries use `trigger(availableNow=True)` so each task
# MAGIC > drains all available data and terminates cleanly within the job run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3A — Late-Arriving Data Breaking Watermarks
# MAGIC **Customer says:** "Our streaming aggregations are missing events that arrive late — sometimes events come in 2 hours after they happened."
# MAGIC
# MAGIC ### What's happening under the hood
# MAGIC
# MAGIC Structured Streaming tracks two timestamps per query:
# MAGIC - **Event time:** the timestamp embedded in the data (when the event actually happened)
# MAGIC - **Processing time:** wall-clock time when Spark received the record
# MAGIC
# MAGIC The **watermark** is the boundary between "windows that might still receive late data"
# MAGIC and "windows that are finalized and can be emitted." Spark computes it as:
# MAGIC
# MAGIC ```
# MAGIC watermark = max(event_time seen so far) - watermark_delay
# MAGIC ```
# MAGIC
# MAGIC Any window whose **end timestamp < watermark** is finalized: Spark emits the result
# MAGIC and drops the state. Any event arriving after its window has been finalized is
# MAGIC silently discarded — it will never appear in the output.
# MAGIC
# MAGIC **Why a tight watermark breaks things:** If `watermark_delay = "10 minutes"` and
# MAGIC an event arrives 2 hours late, its window was finalized 1h50m ago. The event is
# MAGIC dropped without error, producing silently incorrect aggregation totals.
# MAGIC
# MAGIC **Stakeholder translation:** "We told the pipeline to wait 10 minutes for stragglers.
# MAGIC But some data is 2 hours late, so by the time it arrives, we've already published
# MAGIC that hour's totals and thrown away the in-progress state for it."

# COMMAND ----------

import datetime
import random
from pyspark.sql.functions import col, sum, window
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DoubleType

CATALOG = "interview_practice"
SCHEMA_NAME = "streaming_late_data"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

on_time = [
    (i, datetime.datetime.now() - datetime.timedelta(minutes=random.randint(0, 30)),
     random.random() * 100)
    for i in range(1000)
]
late_events = [
    (i + 1000, datetime.datetime.now() - datetime.timedelta(hours=3),
     random.random() * 100)
    for i in range(200)
]

schema = StructType([
    StructField("event_id", IntegerType()),
    StructField("event_time", TimestampType()),
    StructField("value", DoubleType()),
])

all_events = spark.createDataFrame(on_time + late_events, schema)
all_events.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.raw_events")

# Pre-create output table to avoid schema inference errors on first run
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.hourly_agg
    (window STRUCT<start:TIMESTAMP,end:TIMESTAMP>, total_value DOUBLE)
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosis

# COMMAND ----------

# Wrong: tight watermark drops late events
# .withWatermark("event_time", "10 minutes")  ← events 3h late are silently discarded

# ── How to size a watermark correctly ─────────────────────────────────────────
# 1. Check streamingQuery.recentProgress — look at eventTime.watermark and
#    eventTime.max. The gap between them should be >= your worst-case lateness.
# 2. Look at your source system's SLA: "events are guaranteed within X hours"
# 3. When in doubt, size wider. A wider watermark uses more state memory but
#    produces correct results. A tight watermark produces fast but wrong results.
#
# Monitoring command while the query runs:
#   spark.streams.active[0].recentProgress

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — Wider Watermark Retains Late Events

# COMMAND ----------

stream_df = spark.readStream.table(f"{SCHEMA}.raw_events")

query = (
    stream_df
    # Watermark of 3 hours means: finalize a window only after we've seen
    # event_time values that are 3+ hours newer than that window's end.
    # This retains in-memory state for all windows that might still receive data.
    .withWatermark("event_time", "3 hours")
    .groupBy(window("event_time", "1 hour"))
    .agg(sum("value").alias("total_value"))
    .writeStream
    .format("delta")
    # append mode: only emit a window result once it's finalized (end < watermark).
    # update mode would re-emit partial results every micro-batch — wrong for dashboards.
    .outputMode("append")
    .option("checkpointLocation", f"dbfs:/tmp/{SCHEMA}/checkpoint_late")
    # availableNow: process all available data in one batch, then stop.
    # Equivalent to a triggered batch run — useful for scheduled jobs.
    .trigger(availableNow=True)
    .toTable(f"{SCHEMA}.hourly_agg")
)
query.awaitTermination()

spark.table(f"{SCHEMA}.hourly_agg").orderBy("window").show(truncate=False)
print("✅ Scenario 3A complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3B — Checkpoint Corruption / Reprocessing
# MAGIC **Customer says:** "Our streaming job crashed and when it restarted it reprocessed old data and created duplicates."
# MAGIC
# MAGIC ### What's happening under the hood
# MAGIC
# MAGIC A Structured Streaming checkpoint stores two things:
# MAGIC 1. **Offsets:** the position in the source where the query last committed
# MAGIC    (e.g., for a Delta source, this is the Delta table version number).
# MAGIC 2. **State:** in-memory aggregation state (watermark position, partial window sums).
# MAGIC
# MAGIC Without a checkpoint, Spark has no memory of what it already processed. On restart
# MAGIC it reads the source from the beginning (or from the latest offset, depending on
# MAGIC source config) — potentially reprocessing and re-appending rows it already wrote.
# MAGIC
# MAGIC **Why duplicates appear even with a checkpoint:**
# MAGIC If the job crashes *after* processing a micro-batch but *before* committing the
# MAGIC checkpoint, Spark replays that batch on restart. With a plain `append` write,
# MAGIC that batch is written twice → duplicates.
# MAGIC
# MAGIC **The fix — two-part guarantee:**
# MAGIC - `checkpointLocation` on a durable filesystem (DBFS, not /tmp) ensures offsets
# MAGIC   survive executor restarts.
# MAGIC - `foreachBatch` + `MERGE INTO` makes each micro-batch write **idempotent**:
# MAGIC   the same event_id can arrive multiple times but will only produce one row
# MAGIC   in the output table.
# MAGIC
# MAGIC **Stakeholder translation:** "We added a bookmark so the pipeline remembers where
# MAGIC it left off, and we changed the write pattern so that replaying a batch produces
# MAGIC the same result as running it once."

# COMMAND ----------

CATALOG = "interview_practice"
SCHEMA_NAME = "streaming_checkpoints"
SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print(f"✅ Writing to: {SCHEMA}")

events = spark.range(5000).selectExpr(
    "id as event_id",
    "current_timestamp() - (rand() * 3600 * interval 1 second) as event_time",
    "rand() * 100 as value"
)
events.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.source_events")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.output_events
    (event_id BIGINT, event_time TIMESTAMP, value DOUBLE)
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bad Pattern — No Checkpoint → Full Reprocess on Restart

# COMMAND ----------

# stream_df = spark.readStream.table(f"{SCHEMA}.source_events")
# stream_df.writeStream.format("delta").outputMode("append").start(f"{SCHEMA}.output_events")
# ^ Two problems:
#   1. No checkpointLocation → on restart, Spark has no offset record; replays from start
#   2. Plain append → each replay adds duplicate rows; no deduplication at write time

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — Checkpoint + MERGE for Idempotent Writes

# COMMAND ----------

stream_df = spark.readStream.table(f"{SCHEMA}.source_events")

def upsert_to_delta(batch_df, batch_id):
    # foreachBatch gives us a regular DataFrame for each micro-batch.
    # We register it as a temp view so we can reference it in SQL.
    batch_df.createOrReplaceTempView("updates")
    # MERGE INTO is the key to idempotency:
    # - If event_id already exists → do nothing (WHEN MATCHED is omitted)
    # - If event_id is new → insert it
    # Running this function twice with the same batch produces identical output.
    spark.sql(f"""
        MERGE INTO {SCHEMA}.output_events t
        USING updates u ON t.event_id = u.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

query = (
    stream_df
    .writeStream
    .foreachBatch(upsert_to_delta)
    # checkpointLocation must be on a durable, distributed filesystem (dbfs:/ or /Volumes/).
    # Local /tmp/ is wiped when the executor is recycled — defeats the purpose.
    .option("checkpointLocation", f"dbfs:/tmp/{SCHEMA}/checkpoint_merge")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

print("Output row count:", spark.table(f"{SCHEMA}.output_events").count())
print("✅ Scenario 3B complete")
