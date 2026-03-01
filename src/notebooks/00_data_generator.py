# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Synthetic Data Generator
# MAGIC
# MAGIC Ready-made generators for common interview domains. Pick one section,
# MAGIC run it, then switch to `scratchpad.py` to build your pipeline.
# MAGIC
# MAGIC | Section | Domain | Key fields |
# MAGIC |---|---|---|
# MAGIC | A | E-commerce orders | order_id, customer_id, product, amount, status, ts |
# MAGIC | B | IoT sensor readings | device_id, sensor_type, reading, location, ts |
# MAGIC | C | Financial transactions | txn_id, account_id, merchant, amount, is_fraud, ts |
# MAGIC | D | User clickstream | user_id, session_id, event_type, page, duration, ts |
# MAGIC
# MAGIC **Tip:** Adjust `NUM_ROWS` and the skew/anomaly parameters to make scenarios
# MAGIC interesting without making cells slow. 100k rows is usually a good default.

# COMMAND ----------

import random
import datetime
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col

CATALOG = "interview"
SCHEMA  = "generated_data"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Writing to: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A — E-Commerce Orders
# MAGIC
# MAGIC Good for: Delta Lake troubleshooting, schema evolution, medallion architecture,
# MAGIC data quality (null prices, invalid statuses), skew on a popular product.

# COMMAND ----------

NUM_ROWS = 200_000
now = datetime.datetime.now()

PRODUCTS   = [f"SKU-{i:04d}" for i in range(500)]
CATEGORIES = ["electronics", "clothing", "home", "sports", "books"]
STATUSES   = ["pending", "shipped", "delivered", "cancelled", "returned"]
# Skew: product SKU-0001 is a viral item — intentionally over-represented
SKEWED_PRODUCT_PROB = 0.30

orders = [
    Row(
        order_id    = i,
        customer_id = random.randint(1, 10_000),
        product_id  = "SKU-0001" if random.random() < SKEWED_PRODUCT_PROB
                      else random.choice(PRODUCTS),
        category    = random.choice(CATEGORIES),
        amount      = round(random.uniform(5.0, 500.0), 2)
                      if random.random() > 0.02 else None,  # 2% null prices
        status      = random.choice(STATUSES),
        created_at  = now - datetime.timedelta(
                          days=random.randint(0, 365),
                          hours=random.randint(0, 23)
                      ),
        is_returned = random.random() < 0.08,  # 8% return rate
    )
    for i in range(NUM_ROWS)
]

schema_orders = StructType([
    StructField("order_id",    LongType(),      False),
    StructField("customer_id", IntegerType(),   True),
    StructField("product_id",  StringType(),    True),
    StructField("category",    StringType(),    True),
    StructField("amount",      DoubleType(),    True),   # nullable — test quality checks
    StructField("status",      StringType(),    True),
    StructField("created_at",  TimestampType(), True),
    StructField("is_returned", BooleanType(),   True),
])

df_orders = spark.createDataFrame(orders, schema_orders)
df_orders.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.orders")

print(f"✅ orders: {df_orders.count():,} rows")
df_orders.show(5, truncate=False)

# ── Useful follow-up: demonstrate schema evolution ─────────────────────────────
# After writing the initial table, add a new column to show mergeSchema behavior:
# df_v2 = df_orders.withColumn("discount_pct", (rand() * 0.3).cast("double"))
# df_v2.write.format("delta").mode("append").option("mergeSchema","true")
#     .saveAsTable(f"{CATALOG}.{SCHEMA}.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B — IoT Sensor Readings
# MAGIC
# MAGIC Good for: streaming pipelines, late-arriving data, watermarks,
# MAGIC Auto Loader ingest, data quality (out-of-range readings), time-series aggregations.

# COMMAND ----------

NUM_ROWS = 500_000
now = datetime.datetime.now()

DEVICE_TYPES  = ["temperature", "humidity", "pressure", "vibration", "co2"]
LOCATIONS     = ["plant-A", "plant-B", "plant-C", "warehouse-1", "warehouse-2"]
DEVICE_COUNT  = 200

# Normal ranges per sensor type
RANGES = {
    "temperature": (15.0, 85.0),
    "humidity":    (20.0, 90.0),
    "pressure":    (900.0, 1100.0),
    "vibration":   (0.0, 10.0),
    "co2":         (300.0, 2000.0),
}

readings = []
for i in range(NUM_ROWS):
    sensor_type = random.choice(DEVICE_TYPES)
    lo, hi = RANGES[sensor_type]
    # 3% of readings are anomalous (out of range)
    if random.random() < 0.03:
        reading = round(random.uniform(hi * 1.5, hi * 2.0), 3)
    else:
        reading = round(random.uniform(lo, hi), 3)

    # 5% arrive late — simulates network delay or buffering
    late = random.random() < 0.05
    ts = now - datetime.timedelta(
        hours  = random.randint(0, 2) if not late else random.randint(3, 6),
        minutes= random.randint(0, 59)
    )

    readings.append(Row(
        reading_id  = i,
        device_id   = f"device-{random.randint(1, DEVICE_COUNT):03d}",
        sensor_type = sensor_type,
        reading     = reading,
        unit        = "°C" if sensor_type == "temperature" else
                      "%" if sensor_type == "humidity" else
                      "hPa" if sensor_type == "pressure" else
                      "g" if sensor_type == "vibration" else "ppm",
        location    = random.choice(LOCATIONS),
        event_time  = ts,
        is_late     = late,
    ))

schema_iot = StructType([
    StructField("reading_id",  LongType(),      False),
    StructField("device_id",   StringType(),    True),
    StructField("sensor_type", StringType(),    True),
    StructField("reading",     DoubleType(),    True),
    StructField("unit",        StringType(),    True),
    StructField("location",    StringType(),    True),
    StructField("event_time",  TimestampType(), True),
    StructField("is_late",     BooleanType(),   True),
])

df_iot = spark.createDataFrame(readings, schema_iot)
df_iot.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.sensor_readings")

print(f"✅ sensor_readings: {df_iot.count():,} rows")
df_iot.groupBy("sensor_type").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section C — Financial Transactions
# MAGIC
# MAGIC Good for: fraud detection, skew (high-volume merchants), MERGE/upsert patterns,
# MAGIC data quality (duplicate transaction IDs), multi-currency aggregation.

# COMMAND ----------

NUM_ROWS = 300_000
now = datetime.datetime.now()

MERCHANTS  = [f"merchant_{i:03d}" for i in range(200)]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD"]
TXN_TYPES  = ["purchase", "refund", "transfer", "withdrawal", "deposit"]

# Skew: merchant_001 is a large retailer — 25% of all transactions
HOT_MERCHANT_PROB = 0.25

transactions = []
seen_ids = set()
for i in range(NUM_ROWS):
    # 1% duplicate transaction IDs — a real data quality problem
    if random.random() < 0.01 and seen_ids:
        txn_id = random.choice(list(seen_ids))
    else:
        txn_id = f"TXN-{i:08d}"
        seen_ids.add(txn_id)

    merchant = "merchant_001" if random.random() < HOT_MERCHANT_PROB \
               else random.choice(MERCHANTS)

    transactions.append(Row(
        txn_id      = txn_id,
        account_id  = random.randint(1, 50_000),
        merchant    = merchant,
        amount      = round(random.uniform(1.0, 5000.0), 2),
        currency    = random.choice(CURRENCIES),
        txn_type    = random.choice(TXN_TYPES),
        # Fraud signals: high amount + unusual hour
        is_fraud    = random.random() < 0.005,  # 0.5% fraud rate
        hour_of_day = random.randint(0, 23),
        created_at  = now - datetime.timedelta(days=random.randint(0, 180)),
    ))

schema_txn = StructType([
    StructField("txn_id",      StringType(),    False),
    StructField("account_id",  IntegerType(),   True),
    StructField("merchant",    StringType(),    True),
    StructField("amount",      DoubleType(),    True),
    StructField("currency",    StringType(),    True),
    StructField("txn_type",    StringType(),    True),
    StructField("is_fraud",    BooleanType(),   True),
    StructField("hour_of_day", IntegerType(),   True),
    StructField("created_at",  TimestampType(), True),
])

df_txn = spark.createDataFrame(transactions, schema_txn)
df_txn.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.transactions")

print(f"✅ transactions: {df_txn.count():,} rows")
dupe_count = df_txn.count() - df_txn.dropDuplicates(["txn_id"]).count()
print(f"   duplicate txn_ids: {dupe_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section D — User Clickstream / Events
# MAGIC
# MAGIC Good for: sessionization, funnel analysis, window functions, streaming,
# MAGIC high-cardinality joins, late-arriving events.

# COMMAND ----------

NUM_ROWS = 400_000
now = datetime.datetime.now()

EVENT_TYPES = ["page_view", "click", "search", "add_to_cart", "checkout", "purchase", "logout"]
PAGES       = ["/home", "/search", "/product", "/cart", "/checkout", "/account", "/help"]
DEVICES     = ["mobile", "desktop", "tablet"]
CHANNELS    = ["organic", "paid_search", "email", "social", "direct"]

events = []
for i in range(NUM_ROWS):
    user_id    = random.randint(1, 20_000)
    session_id = f"sess_{user_id}_{random.randint(1, 5)}"  # ~5 sessions per user

    events.append(Row(
        event_id   = i,
        user_id    = user_id,
        session_id = session_id,
        event_type = random.choice(EVENT_TYPES),
        page       = random.choice(PAGES),
        duration_s = round(random.expovariate(1/45), 1),  # exponential dist, mean ~45s
        device     = random.choice(DEVICES),
        channel    = random.choice(CHANNELS),
        # Some events arrive late (mobile apps batch events)
        event_time = now - datetime.timedelta(
            hours  = random.randint(0, 72),
            minutes= random.randint(0, 59),
            seconds= random.randint(0, 59)
        ),
        # Nullable fields to test quality handling
        referrer   = f"https://ref-{random.randint(1,100)}.com"
                     if random.random() > 0.3 else None,
    ))

schema_events = StructType([
    StructField("event_id",   LongType(),      False),
    StructField("user_id",    IntegerType(),   True),
    StructField("session_id", StringType(),    True),
    StructField("event_type", StringType(),    True),
    StructField("page",       StringType(),    True),
    StructField("duration_s", DoubleType(),    True),
    StructField("device",     StringType(),    True),
    StructField("channel",    StringType(),    True),
    StructField("event_time", TimestampType(), True),
    StructField("referrer",   StringType(),    True),  # nullable
])

df_events = spark.createDataFrame(events, schema_events)
df_events.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.user_events")

print(f"✅ user_events: {df_events.count():,} rows")
df_events.groupBy("event_type").count().orderBy(col("count").desc()).show()
