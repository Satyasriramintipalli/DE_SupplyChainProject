# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS
# MAGIC catalog.gold_supply_chain

# COMMAND ----------

silver_df = spark.table(
    "catalog.supply_chain_schema.valid_silver_orders"
)

# COMMAND ----------

from pyspark.sql.functions import col

# Read from bronze table which has all customer detail columns
bronze_df = spark.table("catalog.supply_chain_schema.bronze_orders")

dim_customer = bronze_df.select(
    col("Customer Id").alias("customer_id"),
    col("Customer Fname").alias("customer_fname"),
    col("Customer Lname").alias("customer_lname"),
    col("Customer City").alias("customer_city"),
    col("Customer State").alias("customer_state"),
    col("Customer Country").alias("customer_country"),
    col("Customer Segment").alias("customer_segment")
).dropDuplicates()

# COMMAND ----------

print(
    "Customer Dimension Count:",
    dim_customer.count()
)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

dim_customer = dim_customer.withColumn(
    "customer_sk",
    monotonically_increasing_id()
)

# COMMAND ----------

from pyspark.sql.functions import current_date, lit

dim_customer = (
    dim_customer
    .withColumn(
        "effective_date",
        current_date()
    )
    .withColumn(
        "expiry_date",
        lit(None).cast("date")
    )
    .withColumn(
        "is_current",
        lit(True)
    )
)

# COMMAND ----------

(
    dim_customer.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "catalog.gold_supply_chain.dim_customer"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM catalog.gold_supply_chain.dim_customer
# MAGIC LIMIT 10

# COMMAND ----------

dim_customer_current = spark.table(
    "catalog.gold_supply_chain.dim_customer"
)

# COMMAND ----------

customer_updates = (
    dim_customer_current
    .limit(5)
    .withColumn(
        "customer_state",
        lit("Texas")
    )
)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

changed_customers = (
    customer_updates.alias("updates")
    .join(
        dim_customer_current.alias("current"),
        on="customer_id"
    )
    .filter(
        col("updates.customer_state") !=
        col("current.customer_state")
    )
    .select("updates.*")
)

# COMMAND ----------

from pyspark.sql.functions import current_date

expired_records = (
    dim_customer_current.alias("current")
    .join(
        changed_customers.alias("updates"),
        on="customer_id"
    )
    .filter(col("current.is_current") == True)
    .select("current.*")
    .withColumn(
        "expiry_date",
        current_date()
    )
    .withColumn(
        "is_current",
        lit(False)
    )
)

# COMMAND ----------

new_customer_versions = (
    changed_customers
    .withColumn(
        "customer_sk",
        monotonically_increasing_id()
    )
    .withColumn(
        "effective_date",
        current_date()
    )
    .withColumn(
        "expiry_date",
        lit(None).cast("date")
    )
    .withColumn(
        "is_current",
        lit(True)
    )
)

# COMMAND ----------

unchanged_records = (
    dim_customer_current.alias("current")
    .join(
        changed_customers.alias("updates"),
        on="customer_id",
        how="left_anti"
    )
)

# COMMAND ----------

final_dim_customer = (
    unchanged_records
    .unionByName(expired_records)
    .unionByName(new_customer_versions)
)

# COMMAND ----------

(
    final_dim_customer.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "catalog.gold_supply_chain.dim_customer"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_state,
# MAGIC     effective_date,
# MAGIC     expiry_date,
# MAGIC     is_current
# MAGIC FROM catalog.gold_supply_chain.dim_customer
# MAGIC WHERE customer_id IN (
# MAGIC     SELECT customer_id
# MAGIC     FROM catalog.gold_supply_chain.dim_customer
# MAGIC     GROUP BY customer_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC ORDER BY customer_id, effective_date

# COMMAND ----------

# Read from bronze table which has all product detail columns
dim_product = bronze_df.select(
    col("Product Card Id").alias("product_card_id"),
    col("Product Name").alias("product_name"),
    col("Category Id").alias("category_id"),
    col("Category Name").alias("category_name"),
    col("Department Id").alias("department_id"),
    col("Department Name").alias("department_name"),
    col("Product Price").alias("product_price"),
    col("Product Status").alias("product_status")
).dropDuplicates()

# COMMAND ----------

dim_product = dim_product.withColumn(
    "product_sk",
    monotonically_increasing_id()
)

# COMMAND ----------

(
    dim_product.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "catalog.gold_supply_chain.dim_product"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM catalog.gold_supply_chain.dim_product
# MAGIC LIMIT 10

# COMMAND ----------

# Read from bronze table which has all shipping detail columns
dim_shipping = bronze_df.select(
    col("Shipping Mode").alias("shipping_mode"),
    col("Delivery Status").alias("delivery_status"),
    col("Late_delivery_risk").alias("late_delivery_risk"),
    col("Days for shipping (real)").alias("days_for_shipping_real"),
    col("Days for shipment (scheduled)").alias("days_for_shipment_scheduled")
).dropDuplicates()

# COMMAND ----------

dim_shipping = dim_shipping.withColumn(
    "shipping_sk",
    monotonically_increasing_id()
)

# COMMAND ----------

(
    dim_shipping.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "catalog.gold_supply_chain.dim_shipping"
    )
)

# COMMAND ----------

dim_customer = spark.table(
    "catalog.gold_supply_chain.dim_customer"
)

dim_product = spark.table(
    "catalog.gold_supply_chain.dim_product"
)

dim_shipping = spark.table(
    "catalog.gold_supply_chain.dim_shipping"
)

# COMMAND ----------

# Use bronze_df as base since it has all columns needed for joins
fact_orders = (
    bronze_df.alias("f")

    .join(
        dim_customer.alias("c"),
        col("f.Customer Id") == col("c.customer_id"),
        how="left"
    )

    .join(
        dim_product.alias("p"),
        col("f.Product Card Id") == col("p.product_card_id"),
        how="left"
    )

    .join(
        dim_shipping.alias("s"),
        (col("f.Shipping Mode") == col("s.shipping_mode")) &
        (col("f.Delivery Status") == col("s.delivery_status")) &
        (col("f.Late_delivery_risk") == col("s.late_delivery_risk")) &
        (col("f.Days for shipping (real)") == col("s.days_for_shipping_real")) &
        (col("f.Days for shipment (scheduled)") == col("s.days_for_shipment_scheduled")),
        how="left"
    )

    .select(
        col("f.Order Item Id").alias("order_item_id"),
        col("f.Order Id").alias("order_id"),
        col("f.order date (DateOrders)").alias("order_date_dateorders"),

        col("c.customer_sk"),
        col("p.product_sk"),
        col("s.shipping_sk"),

        col("f.Sales").alias("sales"),
        col("f.Order Item Quantity").alias("order_item_quantity"),
        col("f.Benefit per order").alias("benefit_per_order"),
        col("f.Order Item Profit Ratio").alias("order_item_profit_ratio"),
        col("f.Order Item Discount").alias("order_item_discount"),
        col("f.Order Item Total").alias("order_item_total")
    )
)

# COMMAND ----------

(
    fact_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "catalog.gold_supply_chain.fact_orders"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM catalog.gold_supply_chain.fact_orders
# MAGIC LIMIT 10

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

# Use bronze_df as base since it has all columns needed for joins
# Note: broadcast() is not supported in Spark Connect
optimized_fact_orders = (
    bronze_df.alias("f")

    .join(
        dim_customer.alias("c"),
        col("f.Customer Id") == col("c.customer_id"),
        how="left"
    )

    .join(
        dim_product.alias("p"),
        col("f.Product Card Id") == col("p.product_card_id"),
        how="left"
    )

    .join(
        dim_shipping.alias("s"),
        (col("f.Shipping Mode") == col("s.shipping_mode")) &
        (col("f.Delivery Status") == col("s.delivery_status")) &
        (col("f.Late_delivery_risk") == col("s.late_delivery_risk")) &
        (col("f.Days for shipping (real)") == col("s.days_for_shipping_real")) &
        (col("f.Days for shipment (scheduled)") == col("s.days_for_shipment_scheduled")),
        how="left"
    )
)

# COMMAND ----------

optimized_fact_orders.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC dim_customer.cache()
# MAGIC
# MAGIC dim_product.cache()
# MAGIC
# MAGIC dim_shipping.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC dim_customer.count()
# MAGIC
# MAGIC dim_product.count()
# MAGIC
# MAGIC dim_shipping.count()

# COMMAND ----------

# MAGIC %md
# MAGIC spark.catalog.isCached(
# MAGIC     "catalog.gold_supply_chain.dim_customer"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Partition Pruning

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

# Use bronze_df as base since it has all columns needed for joins
# Note: broadcast() is not supported in Spark Connect
fact_orders_optimized = (
    bronze_df.alias("f")

    .join(
        dim_customer.alias("c"),
        col("f.Customer Id") == col("c.customer_id"),
        how="left"
    )

    .join(
        dim_product.alias("p"),
        col("f.Product Card Id") == col("p.product_card_id"),
        how="left"
    )

    .join(
        dim_shipping.alias("s"),
        (col("f.Shipping Mode") == col("s.shipping_mode")) &
        (col("f.Delivery Status") == col("s.delivery_status")) &
        (col("f.Late_delivery_risk") == col("s.late_delivery_risk")) &
        (col("f.Days for shipping (real)") == col("s.days_for_shipping_real")) &
        (col("f.Days for shipment (scheduled)") == col("s.days_for_shipment_scheduled")),
        how="left"
    )

    .select(

        # Business Keys
        col("f.Order Item Id").alias("order_item_id"),
        col("f.Order Id").alias("order_id"),

        # Geography
        col("f.Order Region").alias("order_region"),
        col("f.Order Country").alias("order_country"),
        col("f.Order State").alias("order_state"),

        # Dates
        col("f.order date (DateOrders)").alias("order_date_dateorders"),
        col("f.shipping date (DateOrders)").alias("shipping_date_dateorders"),

        # Surrogate Keys
        col("c.customer_sk"),
        col("p.product_sk"),
        col("s.shipping_sk"),

        # Measures
        col("f.Sales").alias("sales"),
        col("f.Order Item Quantity").alias("order_item_quantity"),
        col("f.Benefit per order").alias("benefit_per_order"),
        col("f.Order Item Profit Ratio").alias("order_item_profit_ratio"),
        col("f.Order Item Discount").alias("order_item_discount"),
        col("f.Order Item Discount Rate").alias("order_item_discount_rate"),
        col("f.Order Item Total").alias("order_item_total"),

        # Operational Metrics
        col("f.Late_delivery_risk").alias("late_delivery_risk")
    )
)

# COMMAND ----------

(
    fact_orders_optimized.write
    .format("delta")
    .partitionBy("order_region")
    .mode("overwrite")
    .saveAsTable(
        "catalog.gold_supply_chain.fact_orders_partitioned"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS
# MAGIC catalog.gold_supply_chain.fact_orders_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC Z-ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE catalog.gold_supply_chain.fact_orders_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE catalog.gold_supply_chain.fact_orders_partitioned
# MAGIC ZORDER BY (customer_sk, product_sk)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY catalog.gold_supply_chain.fact_orders_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_sk, SUM(sales)
# MAGIC FROM catalog.gold_supply_chain.fact_orders_partitioned
# MAGIC WHERE order_region = 'South Asia'
# MAGIC GROUP BY customer_sk

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN
# MAGIC SELECT customer_sk, SUM(sales)
# MAGIC FROM catalog.gold_supply_chain.fact_orders_partitioned
# MAGIC WHERE order_region = 'South Asia'
# MAGIC GROUP BY customer_sk