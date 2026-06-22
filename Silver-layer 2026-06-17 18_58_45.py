# Databricks notebook source
bronze_df = spark.table(
    "catalog.supply_chain_schema.bronze_orders"
)

# COMMAND ----------

bronze_df.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col

clean_columns = [
    col(c).alias(
        c.lower()
         .replace(" ", "_")
         .replace("(", "")
         .replace(")", "")
         .replace("-", "_")
    )
    for c in bronze_df.columns
]

silver_df = bronze_df.select(clean_columns) 

# COMMAND ----------

from pyspark.sql.types import *

enterprise_schema = StructType([

    StructField("order_id", IntegerType(), False),

    StructField("order_item_id", IntegerType(), False),

    StructField("customer_id", IntegerType(), True),

    StructField("sales", DoubleType(), True),

    StructField("order_item_quantity", IntegerType(), True),

    StructField("order_status", StringType(), True),

    StructField("shipping_mode", StringType(), True),

    StructField("order_region", StringType(), True),

    StructField("order_country", StringType(), True)
])

# COMMAND ----------

silver_df = silver_df.select(

    col("order_id").cast("int"),

    col("order_item_id").cast("int"),

    col("customer_id").cast("int"),

    col("sales").cast("double"),

    col("order_item_quantity").cast("int"),

    col("order_status").cast("string"),

    col("shipping_mode").cast("string"),

    col("order_region").cast("string"),

    col("order_country").cast("string")
)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df.select(
    "order_id",
    "customer_id",
    "order_status"
).show(10)

# COMMAND ----------

before_count = silver_df.count()

silver_df = silver_df.dropDuplicates()

after_count = silver_df.count()

print("Before:", before_count)
print("After:", after_count)

# COMMAND ----------

duplicate_sample = silver_df.limit(100)

silver_df_with_duplicates = silver_df.union(duplicate_sample)

print("Original Count:", silver_df.count())
print("After Adding Duplicates:", silver_df_with_duplicates.count())

# COMMAND ----------

before_count = silver_df.count()

silver_df = silver_df.dropDuplicates()

after_count = silver_df.count()

print("Before:", before_count)
print("After:", after_count)

# COMMAND ----------

from pyspark.sql.functions import sum, when

null_counts = silver_df.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in silver_df.columns
])

display(null_counts)

# COMMAND ----------

# product_description was already excluded in cell 5's column selection
# No need to drop it here

# COMMAND ----------

silver_df = silver_df.filter(
    col("order_id").isNotNull()
)

# COMMAND ----------

# Fill NA for columns that exist in the 9-column selection
silver_df = silver_df.fillna({
    "shipping_mode": "Unknown"
})

# COMMAND ----------

# Zipcode imputation skipped - order_zipcode, order_city, order_state 
# were not included in the 9-column selection in cell 5

# COMMAND ----------

# Zipcode join skipped - columns not included in cell 5's selection

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# Zipcode columns not included in cell 5's selection

# COMMAND ----------

# Fill NA for numeric columns that exist in the 9-column selection
numeric_columns = [
    "sales",
    "order_item_quantity"
]

silver_df = silver_df.fillna(0, subset=numeric_columns)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

silver_df = silver_df.withColumn(
    "silver_processed_timestamp",
    current_timestamp()
)

# COMMAND ----------

from pyspark.sql.functions import lit

silver_df = silver_df.withColumn(
    "data_source",
    lit("dataco_supply_chain")
)

# COMMAND ----------

from pyspark.sql.functions import lit
import uuid

pipeline_run_id = str(uuid.uuid4())

silver_df = silver_df.withColumn(
    "pipeline_run_id",
    lit(pipeline_run_id)
)

# COMMAND ----------

from pyspark.sql.functions import sum, when

post_clean_nulls = silver_df.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in silver_df.columns
])

display(post_clean_nulls)

# COMMAND ----------

valid_df = silver_df.filter(
    (col("order_id").isNotNull()) &
    (col("sales") >= 0) &
    (col("order_item_quantity") > 0)
)

rejected_df = silver_df.filter(
    (col("order_id").isNull()) |
    (col("sales") < 0) |
    (col("order_item_quantity") <= 0)
)

# COMMAND ----------

rejected_df = rejected_df.withColumn(
    "rejection_reason",
    when(col("order_id").isNull(), "NULL_ORDER_ID")
    .when(col("sales") < 0, "NEGATIVE_SALES")
    .when(col("order_item_quantity") <= 0, "INVALID_QUANTITY")
)

# COMMAND ----------

(
    rejected_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "catalog.supply_chain_schema.rejected_orders"
    )
)

# COMMAND ----------

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .option("overwriteSchema", "true")
    .saveAsTable("catalog.supply_chain_schema.valid_silver_orders")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM catalog.supply_chain_schema.silver_orders;

# COMMAND ----------

total_records = silver_df.count()
rejected_records = rejected_df.count()

rejection_percentage = (
    rejected_records / total_records
) * 100

print("Total Records:", total_records)
print("Rejected Records:", rejected_records)
print("Rejection Percentage:", rejection_percentage)

# COMMAND ----------

if rejection_percentage > 5:
    raise Exception(
        "PIPELINE FAILED: Rejected records exceed threshold"
    )

# COMMAND ----------

# DBTITLE 1,Test 1: Schema Validation
# Test 1: Schema Validation
def test_schema_validation():
    """
    Validate that the silver DataFrame has all expected columns
    and correct data types.
    """
    test_name = "Schema Validation"
    
    # Expected columns
    expected_columns = [
        "order_id", "order_item_id", "customer_id", "sales",
        "order_item_quantity", "order_status", "shipping_mode",
        "order_region", "order_country", "silver_processed_timestamp",
        "data_source", "pipeline_run_id"
    ]
    
    # Get actual columns
    actual_columns = valid_df.columns
    
    # Check for missing columns
    missing_columns = set(expected_columns) - set(actual_columns)
    
    if missing_columns:
        print(f"❌ {test_name} FAILED: Missing columns: {missing_columns}")
        return False
    
    # Validate key data types
    schema = valid_df.schema
    type_checks = {
        "order_id": "IntegerType",
        "sales": "DoubleType",
        "silver_processed_timestamp": "TimestampType"
    }
    
    for col_name, expected_type in type_checks.items():
        actual_type = str(schema[col_name].dataType)
        if expected_type not in actual_type:
            print(f"❌ {test_name} FAILED: {col_name} has type {actual_type}, expected {expected_type}")
            return False
    
    print(f"✅ {test_name} PASSED")
    return True

test_schema_validation()

# COMMAND ----------

# DBTITLE 1,Test 2: Critical Field Null Check
# Test 2: Critical Field Null Check
def test_critical_nulls():
    """
    Ensure that critical fields have no null values in valid_df.
    """
    test_name = "Critical Field Null Check"
    
    critical_fields = ["order_id", "order_item_id", "sales", "order_item_quantity"]
    
    null_counts = {}
    for field in critical_fields:
        null_count = valid_df.filter(col(field).isNull()).count()
        null_counts[field] = null_count
    
    # Check if any critical field has nulls
    failed_fields = {k: v for k, v in null_counts.items() if v > 0}
    
    if failed_fields:
        print(f"❌ {test_name} FAILED: Critical fields with nulls: {failed_fields}")
        return False
    
    print(f"✅ {test_name} PASSED: No nulls in critical fields")
    return True

test_critical_nulls()

# COMMAND ----------

# DBTITLE 1,Test 3: Duplicate Detection
# Test 3: Duplicate Detection
def test_no_duplicates():
    """
    Verify that no duplicates exist based on primary key (order_item_id).
    """
    test_name = "Duplicate Detection"
    
    total_count = valid_df.count()
    distinct_count = valid_df.select("order_item_id").distinct().count()
    
    duplicate_count = total_count - distinct_count
    
    if duplicate_count > 0:
        print(f"❌ {test_name} FAILED: Found {duplicate_count} duplicate records")
        return False
    
    print(f"✅ {test_name} PASSED: No duplicates found")
    return True

test_no_duplicates()

# COMMAND ----------

# DBTITLE 1,Test 4: Business Rules Validation
# Test 4: Business Rules Validation
def test_business_rules():
    """
    Validate business logic rules:
    - Sales must be >= 0
    - Quantity must be > 0
    - Order status must be valid
    """
    test_name = "Business Rules Validation"
    
    # Test 1: Sales validation
    negative_sales = valid_df.filter(col("sales") < 0).count()
    if negative_sales > 0:
        print(f"❌ {test_name} FAILED: Found {negative_sales} records with negative sales")
        return False
    
    # Test 2: Quantity validation
    invalid_quantity = valid_df.filter(col("order_item_quantity") <= 0).count()
    if invalid_quantity > 0:
        print(f"❌ {test_name} FAILED: Found {invalid_quantity} records with invalid quantity")
        return False
    
    # Test 3: Ensure no extreme outliers in sales (optional)
    max_sales = valid_df.agg({"sales": "max"}).collect()[0][0]
    if max_sales > 1000000:  # Adjust threshold based on your domain
        print(f"⚠️ {test_name} WARNING: Max sales value is very high: ${max_sales:,.2f}")
    
    print(f"✅ {test_name} PASSED: All business rules satisfied")
    return True

test_business_rules()

# COMMAND ----------

# DBTITLE 1,Test 5: Data Completeness
# Test 5: Data Completeness
def test_data_completeness():
    """
    Check that we haven't lost too much data during transformations.
    """
    test_name = "Data Completeness"
    
    bronze_count = bronze_df.count()
    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    
    # Calculate data loss percentage
    data_loss_pct = ((bronze_count - valid_count) / bronze_count) * 100
    
    print(f"Bronze records: {bronze_count:,}")
    print(f"Valid records: {valid_count:,}")
    print(f"Rejected records: {rejected_count:,}")
    print(f"Data loss: {data_loss_pct:.2f}%")
    
    # Ensure bronze = valid + rejected
    if bronze_count != (valid_count + rejected_count):
        print(f"❌ {test_name} FAILED: Record count mismatch!")
        return False
    
    # Warning if data loss is too high
    if data_loss_pct > 10:
        print(f"⚠️ {test_name} WARNING: Data loss exceeds 10%")
    
    print(f"✅ {test_name} PASSED: Data completeness verified")
    return True

test_data_completeness()

# COMMAND ----------

# DBTITLE 1,Test 6: Transformation Correctness
# Test 6: Transformation Correctness
def test_transformations():
    """
    Verify that transformations were applied correctly.
    """
    test_name = "Transformation Correctness"
    
    # Test 1: Verify timestamp was added
    null_timestamps = valid_df.filter(col("silver_processed_timestamp").isNull()).count()
    if null_timestamps > 0:
        print(f"❌ {test_name} FAILED: {null_timestamps} records missing timestamp")
        return False
    
    # Test 2: Verify data_source was added
    null_source = valid_df.filter(col("data_source").isNull()).count()
    if null_source > 0:
        print(f"❌ {test_name} FAILED: {null_source} records missing data_source")
        return False
    
    # Test 3: Verify pipeline_run_id was added
    distinct_run_ids = valid_df.select("pipeline_run_id").distinct().count()
    if distinct_run_ids != 1:
        print(f"❌ {test_name} FAILED: Expected 1 pipeline_run_id, found {distinct_run_ids}")
        return False
    
    # Test 4: Verify null filling worked
    sample_nulls = valid_df.filter(
        (col("customer_segment").isNull()) |
        (col("shipping_mode").isNull())
    ).count()
    
    if sample_nulls > 0:
        print(f"⚠️ {test_name} WARNING: Found {sample_nulls} unfilled nulls")
    
    print(f"✅ {test_name} PASSED: All transformations applied correctly")
    return True

test_transformations()

# COMMAND ----------

# DBTITLE 1,Test 7: Data Distribution Check
# Test 7: Data Distribution Check
def test_data_distribution():
    """
    Check for data skew and distribution issues.
    """
    test_name = "Data Distribution Check"
    
    # Check regional distribution
    region_counts = valid_df.groupBy("order_region").count().collect()
    
    if len(region_counts) == 0:
        print(f"❌ {test_name} FAILED: No regions found")
        return False
    
    # Check for single region domination (>90% of data)
    total = valid_df.count()
    max_region_count = max([row['count'] for row in region_counts])
    max_region_pct = (max_region_count / total) * 100
    
    if max_region_pct > 90:
        print(f"⚠️ {test_name} WARNING: One region has {max_region_pct:.1f}% of data (potential skew)")
    
    print(f"✅ {test_name} PASSED: Found {len(region_counts)} regions")
    return True

test_data_distribution()

# COMMAND ----------

# DBTITLE 1,Test Summary Report
# Run All Tests and Generate Report
def run_all_tests():
    """
    Execute all tests and generate a summary report.
    """
    print("="*60)
    print(" " * 15 + "TEST EXECUTION REPORT")
    print("="*60)
    print()
    
    tests = [
        ("Schema Validation", test_schema_validation),
        ("Critical Nulls", test_critical_nulls),
        ("Duplicate Detection", test_no_duplicates),
        ("Business Rules", test_business_rules),
        ("Data Completeness", test_data_completeness),
        ("Transformations", test_transformations),
        ("Data Distribution", test_data_distribution)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
            print()
        except Exception as e:
            print(f"❌ {test_name} ERROR: {str(e)}")
            results[test_name] = False
            print()
    
    # Summary
    print("="*60)
    passed = __builtins__.sum(1 for v in results.values() if v)
    total = len(results)
    success_rate = (passed / total) * 100
    
    print(f"Tests Passed: {passed}/{total} ({success_rate:.1f}%)")
    print("="*60)
    
    if passed == total:
        print("✅ ALL TESTS PASSED - Data quality validated!")
    else:
        failed_tests = [k for k, v in results.items() if not v]
        print(f"❌ FAILED TESTS: {', '.join(failed_tests)}")
        print("⚠️ Review failed tests before proceeding to Gold layer")
    
    print("="*60)
    return results

# Execute all tests
test_results = run_all_tests()

# COMMAND ----------

# DBTITLE 1,Performance Metrics
# Performance Metrics
print("\n" + "="*60)
print(" " * 18 + "PERFORMANCE METRICS")
print("="*60)

# Data quality metrics
print(f"\n📊 Data Quality Metrics:")
print(f"  Total Bronze Records:    {bronze_df.count():>12,}")
print(f"  Valid Silver Records:    {valid_df.count():>12,}")
print(f"  Rejected Records:        {rejected_df.count():>12,}")
print(f"  Rejection Rate:          {rejection_percentage:>11.2f}%")

# Schema info
print(f"\n📋 Schema Information:")
print(f"  Total Columns:           {len(valid_df.columns):>12}")
print(f"  Added Metadata Columns:  {3:>12}")

# Data source tracking
print(f"\n🔍 Tracking Information:")
print(f"  Pipeline Run ID:         {pipeline_run_id}")
print(f"  Data Source:             dataco_supply_chain")

print("\n" + "="*60)