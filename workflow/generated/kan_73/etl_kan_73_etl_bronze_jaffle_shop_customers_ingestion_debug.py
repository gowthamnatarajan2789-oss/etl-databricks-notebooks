# DEBUG DUMP -- raw generated code, saved for inspection.
# Generated at: 2026-08-20T10:45:16.749196+00:00
# Note: no validation errors

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Initialize SparkSession if not already available (e.g., in a local script)
# In Databricks notebooks, 'spark' is pre-initialized.
# spark = SparkSession.builder.appName("BronzeCustomersIngestion").getOrCreate()

# Define parameters for the notebook
# This path should point to the directory containing the customers.csv file
source_file_path = "/databricks-datasets/retail-org/customers/customers.csv"
target_table_name = "workspace.default.bronze_jaffle_shop_customers"

# Define the explicit schema for the customers data.
# This schema is hypothetical, based on common customer data attributes,
# as the original requirements specified 'Infer Schema: false' and no explicit schema was provided.
# It is crucial to confirm this schema with the source system owner.
customers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Read the source CSV file with the defined schema
# 'header' is set to true as CSV files often contain a header row.
# 'inferSchema' is explicitly set to false as per requirements.
# 'cloudFiles' options are not used here as the requirement is for spark.read directly.
df_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .schema(customers_schema) \
    .load(source_file_path)

# Add bronze layer audit columns:
# _ingested_at: Timestamp when the record was ingested into the bronze layer.
# _source_file: The full path of the source file from which the record originated.
# F.col("_metadata.file_path") is used for Unity Catalog compatibility.
df_customers_bronze = df_customers \
    .withColumn("_ingested_at", F.current_timestamp()) \
    .withColumn("_source_file", F.col("_metadata.file_path"))

# Write the transformed DataFrame to the target Delta table.
# The 'overwrite' mode will replace the entire table with the new data.
# 'mergeSchema' is added for robustness, allowing schema evolution if new columns are added
# to the source and schema definition is updated, even in overwrite mode.
df_customers_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table_name)

# Optional: Display a sample of the ingested data and the table count
# df_customers_bronze.show(5, truncate=False)
# print(f"Successfully ingested {df_customers_bronze.count()} records into {target_table_name}")

# spark.stop() # Only if SparkSession was created manually

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, lit, expr, current_timestamp, date_sub, trim, length, array_union, array
from typing import List, Dict, Any

# Define a list of data quality validation rules for the bronze_jaffle_shop_customers table.
# Each rule is a dictionary with the following keys:
# - 'rule_name' (str): A unique, descriptive name for the validation rule.
# - 'business_purpose' (str): Explains why this rule is important from a business perspective.
# - 'type' (str): Indicates if the rule is 'row_level' (applies to individual records)
#                 or 'aggregate' (applies to the entire dataset or a group).
# - 'column' (str, optional): The column(s) the rule primarily applies to. Used for reporting.
# - 'logic' (str or callable):
#     - For 'row_level' rules: A Spark SQL expression string that evaluates to TRUE for valid records
#       and FALSE for invalid records.
#     - For 'aggregate' rules: A callable (lambda function) that takes the DataFrame and performs
#       an aggregation, returning the result (e.g., a Row object or a count).
# - 'check_function' (callable, optional): For 'aggregate' rules, a callable that takes the result
#   of the 'logic' function and returns True if the validation passes, False otherwise.
# - 'severity' (str): 'critical', 'high', 'medium', 'low'. Helps prioritize issues.
# - 'failure_message' (str): The message to display if the rule fails.

validation_rules: List[Dict[str, Any]] = [
    # --- Completeness, Uniqueness, and Validity for 'id' ---
    {
        "rule_name": "customer_id_is_not_null",
        "business_purpose": "Ensure every customer record has a unique identifier. This is a critical primary key.",
        "type": "row_level",
        "column": "id",
        "logic": "id IS NOT NULL",
        "severity": "critical",
        "failure_message": "Customer ID is null. Records with null IDs cannot be uniquely identified."
    },
    {
        "rule_name": "customer_id_is_positive",
        "business_purpose": "Customer IDs should be positive integers, as negative or zero IDs are typically invalid.",
        "type": "row_level",
        "column": "id",
        "logic": "id > 0",
        "severity": "high",
        "failure_message": "Customer ID is not positive. Invalid ID value detected."
    },
    {
        "rule_name": "customer_id_is_unique",
        "business_purpose": "Guarantee that each customer ID is unique across the dataset to prevent data integrity issues and ensure accurate customer tracking.",
        "type": "aggregate",
        "column": "id",
        "logic": lambda df: df.agg(count(col("id")).alias("total_count"), countDistinct(col("id")).alias("distinct_count")).collect()[0],
        "check_function": lambda result: result["total_count"] == result["distinct_count"],
        "severity": "critical",
        "failure_message": "Duplicate customer IDs found. This indicates a severe data integrity problem."
    },

    # --- Completeness and Validity for 'first_name' ---
    {
        "rule_name": "first_name_is_not_null_or_empty",
        "business_purpose": "A customer should have a first name for proper identification and communication.",
        "type": "row_level",
        "column": "first_name",
        "logic": "first_name IS NOT NULL AND TRIM(first_name) != ''",
        "severity": "high",
        "failure_message": "First name is null or empty. Customer cannot be properly identified."
    },
    {
        "rule_name": "first_name_is_alphabetic",
        "business_purpose": "Ensure first names contain only alphabetic characters, spaces, and hyphens, reflecting valid naming conventions.",
        "type": "row_level",
        "column": "first_name",
        "logic": "first_name IS NULL OR first_name RLIKE '^[A-Za-z\\s\\-]+$'", # Allow nulls to be handled by other rules
        "severity": "medium",
        "failure_message": "First name contains invalid characters (e.g., numbers, special symbols)."
    },

    # --- Completeness and Validity for 'last_name' ---
    {
        "rule_name": "last_name_is_not_null_or_empty",
        "business_purpose": "A customer should have a last name for proper identification and communication.",
        "type": "row_level",
        "column": "last_name",
        "logic": "last_name IS NOT NULL AND TRIM(last_name) != ''",
        "severity": "high",
        "failure_message": "Last name is null or empty. Customer cannot be properly identified."
    },
    {
        "rule_name": "last_name_is_alphabetic",
        "business_purpose": "Ensure last names contain only alphabetic characters, spaces, and hyphens, reflecting valid naming conventions.",
        "type": "row_level",
        "column": "last_name",
        "logic": "last_name IS NULL OR last_name RLIKE '^[A-Za-z\\s\\-]+$'", # Allow nulls to be handled by other rules
        "severity": "medium",
        "failure_message": "Last name contains invalid characters (e.g., numbers, special symbols)."
    },

    # --- Completeness, Uniqueness, and Validity for 'email' ---
    {
        "rule_name": "email_is_not_null_or_empty",
        "business_purpose": "Email is a primary contact method; it must be present to communicate with customers.",
        "type": "row_level",
        "column": "email",
        "logic": "email IS NOT NULL AND TRIM(email) != ''",
        "severity": "high",
        "failure_message": "Email address is null or empty. Critical for customer communication."
    },
    {
        "rule_name": "email_format_is_valid",
        "business_purpose": "Validate email addresses against a standard regex pattern to ensure they are deliverable.",
        "type": "row_level",
        "column": "email",
        "logic": "email IS NULL OR email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'", # Allow nulls to be handled by other rules
        "severity": "high",
        "failure_message": "Email address has an invalid format. May lead to communication failures."
    },
    {
        "rule_name": "email_is_unique",
        "business_purpose": "Prevent multiple customer records from sharing the same email, which could indicate duplicate customer profiles.",
        "type": "aggregate",
        "column": "email",
        "logic": lambda df: df.filter(col("email").isNotNull()).agg(count(col("email")).alias("total_count"), countDistinct(col("email")).alias("distinct_count")).collect()[0],
        "check_function": lambda result: result["total_count"] == result["distinct_count"],
        "severity": "medium",
        "failure_message": "Duplicate email addresses found. Investigate potential duplicate customer records."
    },

    # --- Completeness, Validity, and Timeliness for 'created_at' ---
    {
        "rule_name": "created_at_is_not_null",
        "business_purpose": "The creation timestamp is vital for auditing, data lineage, and understanding customer lifecycle.",
        "type": "row_level",
        "column": "created_at",
        "logic": "created_at IS NOT NULL",
        "severity": "high",
        "failure_message": "Created at timestamp is null. Important audit information is missing."
    },
    {
        "rule_name": "created_at_is_valid_date_range",
        "business_purpose": "Ensure the creation timestamp is within a sensible historical range (e.g., after 2000-01-01) and not in the future.",
        "type": "row_level",
        "column": "created_at",
        "logic": "created_at IS NULL OR (created_at <= CURRENT_TIMESTAMP() AND created_at >= '2000-01-01')", # Allow nulls to be handled by other rules
        "severity": "high",
        "failure_message": "Created at timestamp is in the future or before 2000-01-01. Invalid date range."
    },

    # --- Completeness, Validity, and Timeliness for metadata columns ---
    {
        "rule_name": "load_timestamp_is_not_null",
        "business_purpose": "The load timestamp is essential for tracking when data was ingested into the bronze layer, crucial for data freshness and recovery.",
        "type": "row_level",
        "column": "_load_timestamp",
        "logic": "_load_timestamp IS NOT NULL",
        "severity": "critical",
        "failure_message": "Load timestamp is null. Metadata integrity compromised."
    },
    {
        "rule_name": "load_timestamp_is_recent",
        "business_purpose": "Ensure the load timestamp is recent (e.g., within the last day) to confirm timely data ingestion.",
        "type": "row_level",
        "column": "_load_timestamp",
        "logic": "_load_timestamp IS NULL OR (_load_timestamp <= CURRENT_TIMESTAMP() AND _load_timestamp >= DATE_SUB(CURRENT_DATE(), 1))", # Allow nulls to be handled by other rules
        "severity": "high",
        "failure_message": "Load timestamp is in the future or older than 1 day. Indicates potential ingestion delays or errors."
    },
    {
        "rule_name": "source_file_is_not_null_or_empty",
        "business_purpose": "The source file path is critical for data lineage, debugging, and tracing data origin.",
        "type": "row_level",
        "column": "_source_file",
        "logic": "_source_file IS NOT NULL AND TRIM(_source_file) != ''",
        "severity": "critical",
        "failure_message": "Source file path is null or empty. Data lineage is broken."
    },

    # --- Consistency ---
    {
        "rule_name": "first_and_last_name_are_not_identical",
        "business_purpose": "Identify potential data entry errors where first and last names might be accidentally duplicated or swapped.",
        "type": "row_level",
        "column": "first_name, last_name",
        "logic": "first_name IS NULL OR last_name IS NULL OR first_name != last_name",
        "severity": "low",
        "failure_message": "First name and last name are identical. Review for data entry error."
    },

    # --- Volume ---
    {
        "rule_name": "minimum_record_count",
        "business_purpose": "Ensure a minimum number of records are processed, indicating that the source data was available and ingested.",
        "type": "aggregate",
        "column": None,
        "logic": lambda df: df.count(),
        "check_function": lambda count_result: count_result > 0, # At least one record must be present
        "severity": "high",
        "failure_message": "No records found in the DataFrame. Data source may be empty or ingestion failed."
    }
]

def run_validations(df: DataFrame, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Applies a list of data quality validation rules to a PySpark DataFrame.

    This function iterates through a predefined set of validation rules,
    applies them to the input DataFrame, and collects information about
    any failures. It distinguishes between row-level checks (which can
    identify specific failing records) and aggregate checks (which provide
    a summary for the entire dataset).

    Args:
        df (DataFrame): The input PySpark DataFrame to validate.
        rules (List[Dict[str, Any]]): A list of validation rule dictionaries,
                                      structured as defined in `validation_rules`.

    Returns:
        Dict[str, Any]: A dictionary containing:
                        - 'validation_summary': A list of dictionaries, each describing a failed rule
                                                and its associated failure count/details.
                        - 'failed_records_df': A DataFrame containing all records that failed at least
                                               one row-level validation. An additional column,
                                               '_dq_failures', lists the names of the rules failed by
                                               each specific record. This DataFrame will be empty if
                                               no row-level failures occurred.
    """
    failed_rules_summary: List[Dict[str, Any]] = []
    
    # Initialize a temporary column to track row-level failures for each record.
    # It's an array of strings, initially empty.
    df_with_failures = df.withColumn("_dq_failures", array().cast("array<string>"))

    for rule in rules:
        rule_name = rule["rule_name"]
        rule_type = rule["type"]
        severity = rule["severity"]
        failure_message = rule["failure_message"]
        column_name = rule.get("column") # Column might not be present for some aggregate rules

        if rule_type == "row_level":
            # For row-level rules, filter records that do NOT satisfy the logic.
            # The 'logic' is a Spark SQL expression string.
            # We update the '_dq_failures' column for records that fail this specific rule.
            df_with_failures = df_with_failures.withColumn(
                "_dq_failures",
                expr(f"CASE WHEN NOT ({rule['logic']}) THEN array_union(_dq_failures, array('{rule_name}')) ELSE _dq_failures END")
            )
            
        elif rule_type == "aggregate":
            # For aggregate rules, execute the 'logic' callable to get an aggregate result.
            # Then, use the 'check_function' to determine if the rule passed or failed.
            aggregate_result = rule["logic"](df)
            is_successful = rule["check_function"](aggregate_result)

            if not is_successful:
                failed_count = 0
                # Calculate a meaningful 'failed_count' for aggregate rules
                if rule_name in ["customer_id_is_unique", "email_is_unique"]:
                    # Number of records involved in duplicates
                    failed_count = aggregate_result["total_count"] - aggregate_result["distinct_count"]
                elif rule_name == "minimum_record_count":
                    # If count_result is 0, then 0 records passed the minimum check
                    failed_count = 1 if aggregate_result == 0 else 0 # Indicate a single failure event for the rule

                failed_rules_summary.append({
                    "rule_name": rule_name,
                    "severity": severity,
                    "failure_message": failure_message,
                    "failed_count": failed_count,
                    "column": column_name,
                    "aggregate_result": str(aggregate_result) # Convert Row object to string for summary
                })
        else:
            # Log a warning for any rule types not explicitly handled.
            print(f"Warning: Unknown rule type '{rule_type}' for rule '{rule_name}'. Skipping.")

    # After processing all row-level rules, filter the DataFrame to get only the records
    # that failed at least one rule (i.e., their '_dq_failures' array is not empty).
    final_failed_records_df = df_with_failures.filter(length(col("_dq_failures")) > 0)
    
    # Count failures for row-level rules and add to summary
    # This is done after all row-level rules are applied to get the final state of _dq_failures
    # and avoid multiple passes over the data for each rule's count.
    # We can count the number of records that failed each specific rule.
    for rule in rules:
        if rule["type"] == "row_level":
            rule_name = rule["rule_name"]
            # Count how many records failed this specific rule
            failed_count_for_rule = final_failed_records_df.filter(array_contains(col("_dq_failures"), rule_name)).count()
            if failed_count_for_rule > 0:
                failed_rules_summary.append({
                    "rule_name": rule_name,
                    "severity": rule["severity"],
                    "failure_message": rule["failure_message"],
                    "failed_count": failed_count_for_rule,
                    "column": rule.get("column")
                })

    return {
        "validation_summary": failed_rules_summary,
        "failed_records_df": final_failed_records_df
    }

# Example Usage (commented out for direct source code output):
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
# from datetime import datetime
#
# # Initialize Spark Session (if not already in Databricks)
# # spark = SparkSession.builder.appName("DQValidation").getOrCreate()
#
# # Define a hypothetical schema for the bronze_jaffle_shop_customers table
# # This matches the hypothetical schema provided in the prompt.
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("first_name", StringType(), True),
#     StructField("last_name", StringType(), True),
#     StructField("email", StringType(), True),
#     StructField("created_at", TimestampType(), True),
#     StructField("_load_timestamp", TimestampType(), True),
#     StructField("_source_file", StringType(), True)
# ])
#
# # Create a sample DataFrame with some valid and invalid data
# data = [
#     (1, "John", "Doe", "john.doe@example.com", datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_1.csv"), # Valid
#     (2, "Jane", None, "jane.smith@example.com", datetime(2023, 2, 1, 11, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_1.csv"), # last_name is null
#     (3, "Alice", "Wonderland", "invalid-email", datetime(2023, 3, 1, 12, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_2.csv"), # Invalid email
#     (1, "Duplicate", "ID", "dup.id@example.com", datetime(2023, 4, 1, 13, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_2.csv"), # Duplicate ID
#     (4, "Bob", "Bob", "bob.bob@example.com", datetime(2023, 5, 1, 14, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_3.csv"), # First and last name identical
#     (5, "Charlie", "Brown", "charlie.brown@example.com", datetime(2025, 1, 1, 15, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_3.csv"), # created_at in future
#     (6, "Eve", "Adams", "eve.adams@example.com", datetime(2023, 6, 1, 16, 0, 0), None, "customers_4.csv"), # _load_timestamp is null
#     (7, "Frank", "White", "frank.white@example.com", datetime(2023, 7, 1, 17, 0, 0), datetime(2023, 10, 26, 12, 0, 0), ""), # _source_file is empty
#     (8, "Grace", "Hopper", "grace.hopper@example.com", datetime(2023, 8, 1, 18, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_5.csv"), # Valid
#     (9, "Test1", "Test1", "test1@example.com", datetime(2023, 9, 1, 19, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_6.csv"), # Duplicate email (with ID 10)
#     (10, "Test2", "Test2", "test1@example.com", datetime(2023, 9, 2, 19, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_6.csv"), # Duplicate email (with ID 9)
#     (11, "123", "Name", "test@example.com", datetime(2023, 9, 3, 19, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_7.csv"), # first_name not alphabetic
#     (None, "No", "ID", "noid@example.com", datetime(2023, 9, 4, 19, 0, 0), datetime(2023, 10, 26, 12, 0, 0), "customers_8.csv") # Null ID
# ]
#
# # For testing minimum_record_count, you could use an empty DataFrame:
# # empty_data = []
# # df_empty = spark.createDataFrame(empty_data, schema)
# # dq_results_empty = run_validations(df_empty, validation_rules)
# # print("\n--- Empty DataFrame Validation Results ---")
# # for failure in dq_results_empty["validation_summary"]:
# #     print(f"Rule: {failure['rule_name']} | Severity: {failure['severity']} | Failed Count: {failure['failed_count']} | Message: {failure['failure_message']}")
#
# df_customers = spark.createDataFrame(data, schema)
#
# # Run the validations
# dq_results = run_validations(df_customers, validation_rules)
#
# # Print summary of failed rules
# print("--- Data Quality Validation Summary ---")
# if dq_results["validation_summary"]:
#     for failure in dq_results["validation_summary"]:
#         print(f"Rule: {failure['rule_name']} | Severity: {failure['severity']} | Failed Count: {failure['failed_count']} | Column: {failure.get('column', 'N/A')} | Message: {failure['failure_message']}")
# else:
#     print("All aggregate data quality rules passed!")
#
# # Show records that failed row-level validations
# print("\n--- Records Failing Row-Level Validations ---")
# if dq_results["failed_records_df"].count() > 0:
#     dq_results["failed_records_df"].display() # In Databricks, .display() is preferred
# else:
#     print("No records failed row-level data quality validations.")
#
# # Stop Spark Session (if not in Databricks)
# # spark.stop()

# --- Data Quality Dimensions Covered Checklist ---
# [x] Completeness (e.g., ID, names, email, timestamps, source file not null/empty)
# [x] Uniqueness (e.g., Customer ID, Email)
# [x] Validity (e.g., ID positive, email format, names alphabetic, timestamps in range)
# [ ] Accuracy (Requires external reference data or complex business logic not inferable from schema)
# [x] Consistency (e.g., First and Last name not identical)
# [x] Integrity (Covered by uniqueness and not-null checks for primary keys/identifiers)
# [x] Timeliness (e.g., created_at and _load_timestamp not in future, _load_timestamp recent)
# [x] Volume (e.g., Minimum record count)