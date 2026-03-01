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

# Pre-create output table to avoid schema inference errors
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
# .withWatermark("event_time", "10 minutes")  ← events 3h late are discarded

# Check: streamingQuery.recentProgress["eventTime"] shows max seen vs watermark gap
# Look at processingTime vs eventTime lag to size the watermark correctly

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — Wider Watermark Retains Late Events

# COMMAND ----------

stream_df = spark.readStream.table(f"{SCHEMA}.raw_events")

query = (
    stream_df
    .withWatermark("event_time", "3 hours")      # wide enough to capture 3h-late events
    .groupBy(window("event_time", "1 hour"))
    .agg(sum("value").alias("total_value"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/tmp/{SCHEMA}/checkpoint_late")
    .trigger(availableNow=True)                   # batch-style: drain all data then stop
    .toTable(f"{SCHEMA}.hourly_agg")
)
query.awaitTermination()

spark.table(f"{SCHEMA}.hourly_agg").orderBy("window").show(truncate=False)
print("✅ Scenario 3A complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario 3B — Checkpoint Corruption / Reprocessing
# MAGIC **Customer says:** "Our streaming job crashed and when it restarted it reprocessed old data and created duplicates."

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
# ^ Missing checkpointLocation = reprocesses ALL data on every restart → duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution — Checkpoint + MERGE for Idempotent Writes

# COMMAND ----------

stream_df = spark.readStream.table(f"{SCHEMA}.source_events")

def upsert_to_delta(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    spark.sql(f"""
        MERGE INTO {SCHEMA}.output_events t
        USING updates u ON t.event_id = u.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

query = (
    stream_df
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", f"/tmp/{SCHEMA}/checkpoint_merge")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

print("Output row count:", spark.table(f"{SCHEMA}.output_events").count())
print("✅ Scenario 3B complete")
