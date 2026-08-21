# DEBUG DUMP -- raw generated code, saved for inspection.
# Generated at: 2026-08-21T09:58:05.085140+00:00
# Note: no validation errors

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, regexp_extract
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session (if not running in Databricks)
# spark = SparkSession.builder.appName("JaffleShopPaymentsBronzeIngestion").getOrCreate()

# --- Parameters ---
# Source file path for payments.csv
source_file_path = "/databricks-datasets/retail-org/payments.csv" # Example path, adjust as needed
# Target Delta table name
target_table_name = "workspace.default.bronze_jaffle_shop_payments"

# --- Define Source Schema ---
# The schema is explicitly defined as per requirements, not inferred.
# This hypothetical schema is based on common payment data structures.
source_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("amount", IntegerType(), True) # Assuming amount is in cents
])

# --- Read Source Data ---
# Read the CSV file using the predefined schema.
# 'header' option is set to true as the source CSV is expected to have a header row.
# 'mode' is set to 'FAILFAST' to immediately fail if data does not conform to the schema.
try:
    df_payments = spark.read.format("csv") \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .schema(source_schema) \
        .load(source_file_path)

    # --- Add Audit Columns and Select/Rename for Bronze Layer ---
    # Add ingestion timestamp and source file name for lineage and auditability.
    # Extract only the file name from the full path provided by _metadata.file_path
    df_bronze = df_payments.withColumn("_ingestion_timestamp", current_timestamp()) \
                           .withColumn("_source_file_name", regexp_extract(col("_metadata.file_path"), ".*/([^/]+)$", 1)) \
                           .select(
                               col("id").alias("payment_id"),
                               col("order_id"),
                               col("payment_method"),
                               col("amount"),
                               col("_ingestion_timestamp"),
                               col("_source_file_name")
                           )

    # --- Write to Bronze Delta Table ---
    # Write the DataFrame to the target Delta table in 'overwrite' mode.
    # This ensures idempotency for full refreshes.
    df_bronze.write.format("delta") \
                   .mode("overwrite") \
                   .saveAsTable(target_table_name)

    print(f"Successfully ingested data from '{source_file_path}' to Delta table '{target_table_name}'.")
    print(f"Number of records written: {df_bronze.count()}")

except Exception as e:
    print(f"Error during ingestion from '{source_file_path}' to '{target_table_name}': {e}")
    raise e # Re-raise the exception after logging for Databricks notebook to show failure

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, count, when, array_contains, array
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# This cell sets up a sample PySpark DataFrame to simulate the data
# that would be ingested into the bronze layer from payments.csv.
# In a real ETL pipeline, this DataFrame would be the result of reading
# the source CSV file with the predefined schema.

# Define the hypothetical schema for payments.csv as per the mapping.
# This schema is explicitly defined and not inferred, as per requirements.
payments_source_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create sample data including various data quality issues for demonstration.
sample_payments_data = [
    (1, 101, "credit_card", 1000),
    (2, 102, "coupon", 500),
    (3, 103, "bank_transfer", 2500),
    (4, 104, "gift_card", 750),
    (5, 101, "credit_card", 1200),  # Valid record
    (6, None, "credit_card", 300),   # Fails 'order_id_not_null'
    (7, 105, None, 800),             # Fails 'payment_method_not_null'
    (8, 106, "invalid_method", 900), # Fails 'payment_method_is_valid'
    (9, 107, "credit_card", -100),   # Fails 'amount_is_non_negative'
    (10, 108, "credit_card", None),  # Fails 'amount_not_null'
    (None, 109, "credit_card", 200), # Fails 'payment_id_not_null'
    (1, 110, "gift_card", 600)       # Fails 'payment_id_is_unique' (ID 1 is duplicated)
]

# Create the PySpark DataFrame from the sample data and schema.
df_payments = spark.createDataFrame(sample_payments_data, schema=payments_source_schema)

# Add bronze layer metadata columns as per the schema mapping notes.
# These columns are typically added during the ingestion process and are not
# subject to source data quality rules, but are part of the target bronze schema.
df_payments = df_payments.withColumn("_ingestion_timestamp", lit("2023-10-27T10:00:00Z").cast(TimestampType())) \
                         .withColumn("_source_file_name", lit("payments.csv"))

# Display a sample of the DataFrame to verify its structure and content.
# df_payments.display()

# This cell defines the data quality rules that will be applied to the
# `bronze_jaffle_shop_payments` DataFrame. Each rule is a dictionary
# specifying the check type, target column, severity, and other parameters.

dq_rules = [
    {
        "rule_name": "payment_id_not_null",
        "business_purpose": "Ensure every payment record has a unique identifier.",
        "column": "id",
        "check_type": "not_null",
        "severity": "CRITICAL",
        "failure_message": "Payment ID (id) cannot be null. This is a primary key.",
        "dimensions": ["Completeness", "Integrity"]
    },
    {
        "rule_name": "payment_id_is_unique",
        "business_purpose": "Ensure payment IDs are unique to prevent duplicate records in the bronze layer.",
        "column": "id",
        "check_type": "is_unique",
        "severity": "CRITICAL",
        "failure_message": "Duplicate Payment ID (id) found. Each payment must have a unique identifier.",
        "dimensions": ["Uniqueness", "Integrity"]
    },
    {
        "rule_name": "order_id_not_null",
        "business_purpose": "Ensure every payment is associated with an order.",
        "column": "order_id",
        "check_type": "not_null",
        "severity": "CRITICAL",
        "failure_message": "Order ID cannot be null. Payments must be linked to an order.",
        "dimensions": ["Completeness", "Integrity"]
    },
    {
        "rule_name": "payment_method_not_null",
        "business_purpose": "Ensure the payment method is always specified for a payment.",
        "column": "payment_method",
        "check_type": "not_null",
        "severity": "HIGH",
        "failure_message": "Payment method cannot be null.",
        "dimensions": ["Completeness"]
    },
    {
        "rule_name": "payment_method_is_valid",
        "business_purpose": "Ensure payment methods are from an approved list of values.",
        "column": "payment_method",
        "check_type": "is_in_list",
        "valid_values": ['credit_card', 'coupon', 'bank_transfer', 'gift_card'],
        "severity": "HIGH",
        "failure_message": "Invalid payment method found. Must be one of: 'credit_card', 'coupon', 'bank_transfer', 'gift_card'.",
        "dimensions": ["Validity"]
    },
    {
        "rule_name": "amount_not_null",
        "business_purpose": "Ensure the payment amount is always specified.",
        "column": "amount",
        "check_type": "not_null",
        "severity": "CRITICAL",
        "failure_message": "Payment amount cannot be null.",
        "dimensions": ["Completeness"]
    },
    {
        "rule_name": "amount_is_non_negative",
        "business_purpose": "Ensure payment amounts are non-negative, as negative payments are not expected.",
        "column": "amount",
        "check_type": "is_greater_than_or_equal",
        "threshold": 0,
        "severity": "HIGH",
        "failure_message": "Payment amount cannot be negative. Amounts are stored in cents.",
        "dimensions": ["Validity", "Accuracy"]
    }
]

# This cell contains a reusable function to apply the defined data quality rules
# to any PySpark DataFrame and report the findings.

def validate_dataframe(df: DataFrame, rules: list) -> list:
    """
    Applies a list of data quality rules to a PySpark DataFrame and reports failures.

    Args:
        df (DataFrame): The input PySpark DataFrame to validate.
        rules (list): A list of dictionaries, where each dictionary defines a data quality rule.
                      Each rule dictionary must contain:
                      - "rule_name" (str): Unique name for the rule.
                      - "business_purpose" (str): Description of the rule's purpose.
                      - "column" (str): The column to apply the rule to.
                      - "check_type" (str): Type of check (e.g., "not_null", "is_unique",
                                            "is_in_list", "is_greater_than_or_equal").
                      - "severity" (str): Severity of the failure (e.g., "CRITICAL", "HIGH").
                      - "failure_message" (str): Message to display on failure.
                      - "dimensions" (list): List of data quality dimensions covered.
                      Additional keys may be required based on "check_type" (e.g., "valid_values", "threshold").

    Returns:
        list: A list of dictionaries, each representing a failed rule with details
              such as rule name, severity, failure count, and sample failing records.
    """
    failed_validations = []
    total_records = df.count()

    for rule in rules:
        rule_name = rule["rule_name"]
        column = rule["column"]
        check_type = rule["check_type"]
        severity = rule["severity"]
        failure_message = rule["failure_message"]
        
        failing_records_df = None
        failure_count = 0

        if check_type == "not_null":
            failing_records_df = df.filter(col(column).isNull())
        elif check_type == "is_unique":
            # Identify duplicate values in the specified column
            duplicate_values_df = df.groupBy(column).agg(count(column).alias("count")) \
                                    .filter(col("count") > 1) \
                                    .select(column)
            
            # Join back to the original DataFrame to retrieve full records that have duplicate values
            failing_records_df = df.join(duplicate_values_df, on=column, how="inner")
        elif check_type == "is_in_list":
            valid_values = rule.get("valid_values")
            if not isinstance(valid_values, list):
                raise ValueError(f"Rule '{rule_name}' of type 'is_in_list' requires 'valid_values' as a list.")
            # Filter for records where the column is not null AND its value is not in the valid_values list
            failing_records_df = df.filter(col(column).isNotNull() & ~col(column).isin(valid_values))
        elif check_type == "is_greater_than_or_equal":
            threshold = rule.get("threshold")
            if threshold is None:
                raise ValueError(f"Rule '{rule_name}' of type 'is_greater_than_or_equal' requires 'threshold'.")
            # Filter for records where the column is not null AND its value is less than the threshold
            failing_records_df = df.filter(col(column).isNotNull() & (col(column) < threshold))
        else:
            # Log a warning for unsupported check types and skip the rule
            print(f"WARNING: Unsupported check_type '{check_type}' for rule '{rule_name}'. Skipping this rule.")
            continue

        if failing_records_df:
            failure_count = failing_records_df.count()

        if failure_count > 0:
            # Collect a small sample of failing records for reporting.
            # Be cautious with .collect() on very large datasets.
            sample_failing_records = [row.asDict() for row in failing_records_df.limit(5).collect()]
            
            failed_validations.append({
                "rule_name": rule_name,
                "column": column,
                "check_type": check_type,
                "severity": severity,
                "failure_message": failure_message,
                "failure_count": failure_count,
                "total_records": total_records,
                "failure_percentage": (failure_count / total_records) * 100 if total_records > 0 else 0,
                "sample_failing_records": sample_failing_records,
                "dimensions_covered": rule["dimensions"]
            })
    
    return failed_validations

# This cell executes the data quality validation function and prints the results.
# It also includes logic to handle critical failures, such as potentially halting
# the ETL process or quarantining data.

# Execute the data quality validations on the payments DataFrame.
validation_results = validate_dataframe(df_payments, dq_rules)

# Report the results of the data quality checks.
if not validation_results:
    print("All data quality checks passed successfully for the payments data!")
else:
    print("Data Quality Validation Failures Detected for payments data:")
    for result in validation_results:
        print(f"\n--- Rule: {result['rule_name']} (Severity: {result['severity']}) ---")
        print(f"  Business Purpose: {result['business_purpose']}")
        print(f"  Column: {result['column']}")
        print(f"  Check Type: {result['check_type']}")
        print(f"  Failure Message: {result['failure_message']}")
        print(f"  Failure Count: {result['failure_count']} out of {result['total_records']} records ({result['failure_percentage']:.2f}%)")
        print(f"  DQ Dimensions Covered: {', '.join(result['dimensions_covered'])}")
        print("  Sample Failing Records (up to 5):")
        if result['sample_failing_records']:
            for record in result['sample_failing_records']:
                print(f"    {record}")
        else:
            print("    No sample records available (e.g., if failure count is 0 or an issue occurred).")

# Example of how to handle critical failures:
# If critical failures are detected, the ETL process might need to be halted
# or the failing data quarantined for manual review.
critical_failures = [res for res in validation_results if res["severity"] == "CRITICAL"]
if critical_failures:
    print("\nACTION REQUIRED: CRITICAL data quality failures detected. Consider stopping the pipeline or quarantining affected data.")
    # Uncomment the line below to raise an exception and halt the notebook execution
    # if critical_failures:
    #     raise Exception("Critical data quality failures detected. Halting ETL process.")

# Data Quality Dimensions Coverage Checklist:
# [x] Completeness: Checks for presence of all expected values (e.g., non-null IDs, payment method, amount).
# [x] Uniqueness: Ensures primary key (payment_id) values are unique.
# [x] Validity: Verifies data conforms to defined formats, types, and ranges (e.g., valid payment methods, non-negative amounts).
# [x] Accuracy: Basic checks to ensure data correctness (e.g., amount is not negative). More complex accuracy checks usually require external reference data.
# [ ] Consistency: Checks for consistency across different datasets or over time. (Not explicitly covered in this bronze layer ingestion validation).
# [x] Integrity: Ensures relationships and constraints are maintained (e.g., non-null foreign keys like order_id, unique primary keys).
# [ ] Timeliness: Verifies data is available when expected and up-to-date. (Not covered in this bronze layer ingestion validation).
# [ ] Volume: Checks for expected data volumes or record counts. (Not covered in this bronze layer ingestion validation).