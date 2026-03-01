# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧱 Interview Practice — Environment Setup
# MAGIC
# MAGIC Creates the `interview_practice` Unity Catalog catalog and one schema per scenario.
# MAGIC Run this notebook **first** before any scenario notebook.

# COMMAND ----------

CATALOG = "interview_practice"

# Requires CREATE CATALOG privilege or metastore admin role
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

schemas = [
    "delta_schema_evolution",    # 1A — schema evolution
    "delta_small_files",         # 1B — small files
    "delta_time_travel",         # 1C — accidental delete / restore
    "spark_data_skew",           # 2A — data skew
    "spark_oom_spill",           # 2B — OOM / shuffle spill
    "streaming_late_data",       # 3A — late-arriving data
    "streaming_checkpoints",     # 3B — checkpoint / reprocessing
    "ingestion_autoloader",      # 4A — Auto Loader schema drift
    "ingestion_duplicates",      # 4B — duplicate records
    "medallion_clickstream",     # 5A — Bronze/Silver/Gold design
    "medallion_scd2",            # 5B — SCD Type 2
    "orchestration_idempotent",  # 6A — idempotent pipelines
]

for s in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
    print(f"✅ {CATALOG}.{s}")

print("\n🎯 Practice environment ready!")
