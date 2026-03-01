# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧹 Teardown — Drop Practice Catalog
# MAGIC
# MAGIC **WARNING:** Drops the entire `interview_practice` catalog and all data inside it.
# MAGIC This task always runs (`run_if: ALL_DONE`) so the workspace is cleaned up
# MAGIC regardless of whether scenario notebooks succeeded or failed.

# COMMAND ----------

CATALOG = "interview_practice"
spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")
print("🧹 Practice environment cleaned up.")
