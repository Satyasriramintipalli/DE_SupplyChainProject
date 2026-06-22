# Databricks notebook source
current_df = spark.table(
    "catalog.supply_chain_schema.valid_silver_orders"
)

# COMMAND ----------

incoming_updates = current_df.limit(1000)

# COMMAND ----------

from pyspark.sql.functions import col

incoming_updates = incoming_updates.withColumn(
    "sales",
    col("sales") * 1.1
)

# COMMAND ----------

new_records = current_df.limit(100)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

new_records = (
    new_records
    .withColumn(
        "order_id",
        monotonically_increasing_id() + 9999999
    )
    .withColumn(
        "order_item_id",
        monotonically_increasing_id() + 8888888
    )
)

# COMMAND ----------

cdc_batch = incoming_updates.union(new_records)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

cdc_batch = cdc_batch.withColumn(
    "cdc_timestamp",
    current_timestamp()
)

# COMMAND ----------

duplicate_updates = (
    cdc_batch.limit(50)
    .withColumn(
        "sales",
        col("sales") * 1.5
    )
)

cdc_batch = cdc_batch.union(duplicate_updates)

# COMMAND ----------

cdc_batch.groupBy("order_item_id") \
    .count() \
    .filter("count > 1") \
    .show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy(
    "order_item_id"
).orderBy(
    col("cdc_timestamp").desc()
)

# COMMAND ----------

cdc_batch_dedup = (
    cdc_batch
    .withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

cdc_batch_dedup.createOrReplaceTempView(
    "cdc_updates_dedup"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO catalog.supply_chain_schema.valid_silver_orders AS target
# MAGIC USING cdc_updates_dedup AS source
# MAGIC ON target.order_item_id = source.order_item_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM catalog.supply_chain_schema.valid_silver_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY catalog.supply_chain_schema.valid_silver_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM catalog.supply_chain_schema.valid_silver_orders VERSION AS OF 0
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental loader

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC catalog.supply_chain_schema.pipeline_watermark
# MAGIC (
# MAGIC     pipeline_name STRING,
# MAGIC     last_processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO catalog.supply_chain_schema.pipeline_watermark
# MAGIC VALUES
# MAGIC (
# MAGIC     'orders_incremental_pipeline',
# MAGIC     TIMESTAMP('2020-01-01')
# MAGIC )

# COMMAND ----------

watermark_df = spark.table(
    "catalog.supply_chain_schema.pipeline_watermark"
)

last_watermark = watermark_df.collect()[0][1]

print(last_watermark)

# COMMAND ----------

incremental_df = current_df.filter(
    col("silver_processed_timestamp") > last_watermark
)

# COMMAND ----------

print(
    "Incremental Records:",
    incremental_df.count()
)

# COMMAND ----------

from pyspark.sql.functions import max

new_watermark = incremental_df.select(
    max("silver_processed_timestamp")
).collect()[0][0]

print(new_watermark)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM catalog.supply_chain_schema.pipeline_watermark
# MAGIC WHERE pipeline_name = 'orders_incremental_pipeline'

# COMMAND ----------

spark.sql(f"""
INSERT INTO catalog.supply_chain_schema.pipeline_watermark
VALUES (
    'orders_incremental_pipeline',
    TIMESTAMP('{new_watermark}')
)
""")

# COMMAND ----------

from pyspark.sql.functions import col

current_df = spark.table(
    "catalog.supply_chain_schema.valid_silver_orders"
)

watermark_df = spark.table(
    "catalog.supply_chain_schema.pipeline_watermark"
)

latest_watermark = watermark_df.collect()[0][1]

incremental_df_second_run = current_df.filter(
    col("silver_processed_timestamp") > latest_watermark
)

print(
    "Second Run Incremental Count:",
    incremental_df_second_run.count()
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

new_arrivals = (
    current_df.limit(5)
    .withColumn(
        "order_item_id",
        col("order_item_id") + 999999
    )
    .withColumn(
        "silver_processed_timestamp",
        current_timestamp()
    )
)

# COMMAND ----------

(
    new_arrivals.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "catalog.supply_chain_schema.valid_silver_orders"
    )
)

# COMMAND ----------

updated_df = spark.table(
    "catalog.supply_chain_schema.valid_silver_orders"
)

incremental_new_data = updated_df.filter(
    col("silver_processed_timestamp") > latest_watermark
)

print(
    "New Incremental Records:",
    incremental_new_data.count()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM catalog.supply_chain_schema.pipeline_watermark

# COMMAND ----------

