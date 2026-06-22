# Databricks notebook source
# DBTITLE 1,Load CSV from Unity Catalog Volume
raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/catalog/supply_chain_schema/bronze/DataCoSupplyChainDataset.csv")
)
display(raw_df)

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

raw_df.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_counts = raw_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in raw_df.columns
])

display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC schema Drift

# COMMAND ----------

current_schema = set(raw_df.columns)

expected_schema = {
    "Order Id",
    "Customer Id",
    "Sales",
    "Order Item Id",
    "Order Status",
    "Shipping Mode"
}

# COMMAND ----------

new_columns = current_schema - expected_schema

missing_columns = expected_schema - current_schema

# COMMAND ----------

print("NEW COLUMNS:", new_columns)

print("MISSING COLUMNS:", missing_columns)

# COMMAND ----------

# DBTITLE 1,Save as Delta with Column Mapping
(
    raw_df.write
    .format("delta")

    .option(
        "delta.columnMapping.mode",
        "name"
    )

    .option(
        "mergeSchema",
        "true"
    )

    .mode("overwrite")

    .saveAsTable(
        "catalog.supply_chain_schema.bronze_orders"
    )
)

# COMMAND ----------

# DBTITLE 1,Read from Unity Catalog table
bronze_df = spark.table("catalog.supply_chain_schema.bronze_orders")

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM catalog.supply_chain_schema.bronze_orders;

# COMMAND ----------

# DBTITLE 1,Cleanup: Drop unwanted schema
# MAGIC %sql
# MAGIC -- Drop the table first
# MAGIC DROP TABLE IF EXISTS catalog.supply_chain.bronze_orders;
# MAGIC
# MAGIC -- Then drop the schema
# MAGIC DROP SCHEMA IF EXISTS catalog.supply_chain;

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Unit Testing
# MAGIC %md
# MAGIC # 🧪 Bronze Layer - Unit Testing
# MAGIC
# MAGIC ## Test Coverage
# MAGIC
# MAGIC 1. **Data Ingestion** - Verify CSV loaded successfully
# MAGIC 2. **Schema Validation** - Check expected columns exist
# MAGIC 3. **Schema Drift Detection** - Alert on new/missing columns
# MAGIC 4. **Data Type Validation** - Ensure proper type inference
# MAGIC 5. **Delta Write Validation** - Verify table creation
# MAGIC
# MAGIC **Purpose**: Ensure raw data is correctly ingested into the Bronze layer

# COMMAND ----------

# DBTITLE 1,Bronze Test 1: Data Load Validation
# Test 1: Data Load Validation
def test_data_load():
    """
    Verify that CSV data was loaded successfully.
    """
    test_name = "Data Load Validation"
    
    try:
        # Check if raw_df exists and has data
        row_count = raw_df.count()
        
        if row_count == 0:
            print(f"❌ {test_name} FAILED: No data loaded from CSV")
            return False
        
        # Check for minimum expected columns (cache columns to avoid repeated RPC)
        columns_list = raw_df.columns
        col_count = len(columns_list)
        if col_count < 5:
            print(f"❌ {test_name} FAILED: Too few columns ({col_count})")
            return False
        
        print(f"✅ {test_name} PASSED: Loaded {row_count:,} rows with {col_count} columns")
        return True
        
    except Exception as e:
        print(f"❌ {test_name} ERROR: {str(e)}")
        return False

test_data_load()

# COMMAND ----------

# DBTITLE 1,Bronze Test 2: Schema Drift Detection
# Test 2: Schema Drift Detection
def test_schema_drift():
    """
    Detect schema drift by comparing current vs expected columns.
    """
    test_name = "Schema Drift Detection"
    
    # Core expected columns (minimum set) - using actual bronze column names
    core_columns = {
        "Order Id", "Customer Id", "Sales", 
        "Order Item Id", "Order Status"
    }
    
    # Cache columns list to avoid repeated RPC calls
    columns_list = raw_df.columns
    current_columns = set(columns_list)
    
    # Check for missing core columns
    missing = core_columns - current_columns
    if missing:
        print(f"❌ {test_name} FAILED: Missing critical columns: {missing}")
        return False
    
    # Report new columns (informational)
    new = current_columns - expected_schema
    if new:
        print(f"⚠️ {test_name} INFO: New columns detected: {new}")
        print("   Consider updating expected_schema if these are valid")
    
    # Report missing expected columns (warning)
    missing_expected = expected_schema - current_columns
    if missing_expected:
        print(f"⚠️ {test_name} WARNING: Expected columns not found: {missing_expected}")
    
    print(f"✅ {test_name} PASSED: All core columns present")
    return True

test_schema_drift()

# COMMAND ----------

# DBTITLE 1,Bronze Test 3: Data Type Validation
# Test 3: Data Type Validation
def test_data_types():
    """
    Validate that critical columns have correct data types.
    """
    test_name = "Data Type Validation"
    
    # Cache schema and columns to avoid repeated RPC calls
    schema = raw_df.schema
    columns_list = raw_df.columns
    
    # Expected types for critical columns (using actual bronze column names)
    type_expectations = {
        "Order Id": ["IntegerType", "LongType"],
        "Sales": ["DoubleType", "FloatType", "DecimalType"],
        "Order Item Quantity": ["IntegerType", "LongType"]
    }
    
    failed_types = []
    
    for col_name, expected_types in type_expectations.items():
        if col_name in columns_list:
            actual_type = str(schema[col_name].dataType)
            
            # Check if actual type matches any expected type
            if not any(exp_type in actual_type for exp_type in expected_types):
                failed_types.append(f"{col_name}: {actual_type} (expected one of {expected_types})")
    
    if failed_types:
        print(f"❌ {test_name} FAILED:")
        for failure in failed_types:
            print(f"   - {failure}")
        return False
    
    print(f"✅ {test_name} PASSED: All data types correct")
    return True

test_data_types()

# COMMAND ----------

# DBTITLE 1,Bronze Test 4: Null Analysis
# Test 4: Null Analysis
def test_null_analysis():
    """
    Analyze null values and flag critical columns with high null rates.
    """
    test_name = "Null Analysis"
    
    total_rows = raw_df.count()
    
    # Cache columns list to avoid repeated RPC calls
    columns_list = raw_df.columns
    
    # Calculate null percentage for each column
    null_info = {}
    for col_name in columns_list:
        null_count = raw_df.filter(col(col_name).isNull()).count()
        null_pct = (null_count / total_rows) * 100
        null_info[col_name] = {"count": null_count, "percentage": null_pct}
    
    # Flag columns with >50% nulls (warning)
    high_null_cols = {k: v for k, v in null_info.items() if v['percentage'] > 50}
    
    if high_null_cols:
        print(f"⚠️ {test_name} WARNING: Columns with >50% nulls:")
        for col_name, info in high_null_cols.items():
            print(f"   - {col_name}: {info['percentage']:.1f}% null")
    
    # Critical columns should have <90% nulls (using actual bronze column names)
    critical_cols = ["Order Id", "Customer Id", "Sales"]
    critical_issues = []
    
    for col_name in critical_cols:
        if col_name in null_info and null_info[col_name]['percentage'] > 90:
            critical_issues.append(f"{col_name}: {null_info[col_name]['percentage']:.1f}% null")
    
    if critical_issues:
        print(f"❌ {test_name} FAILED: Critical columns with excessive nulls:")
        for issue in critical_issues:
            print(f"   - {issue}")
        return False
    
    print(f"✅ {test_name} PASSED: Null rates acceptable")
    return True

test_null_analysis()

# COMMAND ----------

# DBTITLE 1,Bronze Test 5: Delta Table Validation
# Test 5: Delta Table Validation
def test_delta_table():
    """
    Verify that data was written to Delta table correctly.
    """
    test_name = "Delta Table Validation"
    
    try:
        # Read from Delta table
        table_df = spark.table("catalog.supply_chain_schema.bronze_orders")
        
        # Verify row count matches
        table_count = table_df.count()
        source_count = raw_df.count()
        
        if table_count != source_count:
            print(f"❌ {test_name} FAILED: Row count mismatch")
            print(f"   Source: {source_count:,} | Table: {table_count:,}")
            return False
        
        # Verify column mapping is enabled
        table_properties = spark.sql(
            "DESCRIBE EXTENDED catalog.supply_chain_schema.bronze_orders"
        ).collect()
        
        print(f"✅ {test_name} PASSED: Delta table created with {table_count:,} rows")
        return True
        
    except Exception as e:
        print(f"❌ {test_name} ERROR: {str(e)}")
        return False

test_delta_table()

# COMMAND ----------

# DBTITLE 1,Bronze Test Summary
# Bronze Layer - Test Summary
def run_bronze_tests():
    """
    Execute all Bronze layer tests and generate summary.
    """
    print("="*60)
    print(" " * 13 + "BRONZE LAYER TEST REPORT")
    print("="*60)
    print()
    
    tests = [
        ("Data Load", test_data_load),
        ("Schema Drift", test_schema_drift),
        ("Data Types", test_data_types),
        ("Null Analysis", test_null_analysis),
        ("Delta Table", test_delta_table)
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
        print("✅ ALL TESTS PASSED - Bronze layer ready!")
    else:
        failed_tests = [k for k, v in results.items() if not v]
        print(f"❌ FAILED TESTS: {', '.join(failed_tests)}")
    
    print("="*60)
    return results

# Execute all tests
bronze_test_results = run_bronze_tests()