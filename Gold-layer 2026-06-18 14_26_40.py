# Databricks notebook source
fact_orders = spark.table(
    "catalog.gold_supply_chain.fact_orders_partitioned"
)

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

analytics_df = (
    fact_orders.alias("f")

    .join(
        dim_customer.alias("c"),
        on="customer_sk",
        how="left"
    )

    .join(
        dim_product.alias("p"),
        on="product_sk",
        how="left"
    )

    .join(
        dim_shipping.alias("s"),
        on="shipping_sk",
        how="left"
    )
)

# COMMAND ----------

from pyspark.sql.functions import *

gold_sales_kpi = (
    analytics_df.groupBy("order_region")

    .agg(

        round(sum("sales"), 2)
        .alias("total_sales"),

        round(sum("benefit_per_order"), 2)
        .alias("total_profit"),

        round(avg("sales"), 2)
        .alias("avg_order_value"),

        sum("order_item_quantity")
        .alias("total_quantity"),

        countDistinct("order_id")
        .alias("total_orders")
    )
)

# COMMAND ----------

(
    gold_sales_kpi.write
    .format("delta")
    .mode("overwrite")

    .saveAsTable(
        "catalog.supply_chain_schema.gold_sales_kpi"
    )
)

# COMMAND ----------

gold_customer_kpi = (

    analytics_df.groupBy(

        "customer_sk",
        "customer_fname",
        "customer_lname",
        "customer_segment",
        "customer_country"

    )

    .agg(

        round(sum("sales"), 2)
        .alias("customer_lifetime_value"),

        countDistinct("order_id")
        .alias("total_orders"),

        round(avg("sales"), 2)
        .alias("avg_customer_order_value")
    )
)

# COMMAND ----------

(
    gold_customer_kpi.write
    .format("delta")
    .mode("overwrite")

    .saveAsTable(
        "catalog.supply_chain_schema.gold_customer_kpi"
    )
)

# COMMAND ----------

from pyspark.sql.functions import *

gold_shipping_kpi = (

    analytics_df.groupBy(

        col("s.shipping_mode"),
        col("s.delivery_status")

    )

    .agg(

        count("*")
        .alias("total_shipments"),

        sum(col("f.late_delivery_risk"))
        .alias("late_shipments"),

        round(
            avg(col("s.days_for_shipping_real")),
            2
        ).alias("avg_shipping_days")
    )
)

# COMMAND ----------

(
    gold_shipping_kpi.write
    .format("delta")
    .mode("overwrite")

    .saveAsTable(
        "catalog.supply_chain_schema.gold_shipping_kpi"
    )
)

# COMMAND ----------

gold_product_kpi = (

    analytics_df.groupBy(

        "product_name",
        "category_name",
        "department_name"

    )

    .agg(

        round(sum("sales"), 2)
        .alias("total_sales"),

        sum("order_item_quantity")
        .alias("units_sold"),

        round(avg("benefit_per_order"), 2)
        .alias("avg_profit")
    )
)

# COMMAND ----------

(
    gold_product_kpi.write
    .format("delta")
    .mode("overwrite")

    .saveAsTable(
        "catalog.supply_chain_schema.gold_product_kpi"
    )
)

# COMMAND ----------

from pyspark.sql.functions import *

gold_region_kpi = (

    analytics_df.groupBy(

        col("f.order_region"),
        col("f.order_country")

    )

    .agg(

        round(
            sum(col("f.sales")),
            2
        ).alias("regional_sales"),

        round(
            sum(col("f.benefit_per_order")),
            2
        ).alias("regional_profit"),

        countDistinct(
            col("f.customer_sk")
        ).alias("unique_customers")
    )
)

# COMMAND ----------

(
    gold_region_kpi.write
    .format("delta")
    .mode("overwrite")

    .saveAsTable(
        "catalog.supply_chain_schema.gold_region_kpi"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Audit logging

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

audit_data = [

    Row(
        pipeline_name="supply_chain_pipeline",

        layer="gold",

        total_records=gold_sales_kpi.count(),

        rejected_records=0,

        processed_timestamp=datetime.now(),

        pipeline_status="SUCCESS"
    )
]

audit_df = spark.createDataFrame(audit_data)

display(audit_df)

# COMMAND ----------

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "catalog.supply_chain_schema.pipeline_audit_log"
    )
)