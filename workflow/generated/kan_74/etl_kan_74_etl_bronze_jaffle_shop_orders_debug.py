# DEBUG DUMP -- raw generated code, saved for inspection.
# Generated at: 2026-08-20T11:49:48.661176+00:00
# Note: no validation errors

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as F

# Workflow: New Feature - This is a green-field ETL for a new bronze table.
# Design: Define a hypothetical schema for the CSV, read the data, add standard bronze layer audit columns,
#         and write the resulting DataFrame to a Delta table in overwrite mode.

# Initialize Spark Session (if not already running in Databricks)
spark = SparkSession.builder.appName("BronzeJaffleShopCustomers").getOrCreate()

# --- Parameters ---
SOURCE_FILE_PATH = "/Volumes/workspace/default/raw_data/jaffle_shop/customers.csv"
TARGET_TABLE_NAME = "workspace.default.bronze_jaffle_shop_customers"

# --- HYPOTHETICAL Source Schema Definition ---
# IMPORTANT: The requirement explicitly states 'Infer Schema: false'.
# The following schema is HYPOTHETICAL and based on common customer data fields.
# This schema MUST be confirmed or replaced with the actual schema by the business/source system owner.
customers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

try:
    # 1. Read source data from CSV with the defined schema
    source_df = spark.read.csv(
        SOURCE_FILE_PATH,
        header=True,
        schema=customers_schema,
        sep=",",
        # Ensure multiLine is false for standard CSV processing unless explicitly required
        multiLine=False,
        # Enable _metadata column for file path tracking
        # This option is typically enabled by default in Databricks for cloud storage reads,
        # but explicitly setting it ensures consistency.
        # For local file system or specific configurations, it might need explicit enabling.
        # For Unity Catalog volumes, _metadata.file_path is the recommended way.
        # For CSV, Spark 3.4+ supports _metadata.file_path.
        # If running on older Spark versions or specific setups, F.input_file_name() might be used
        # but is not recommended for Unity Catalog.
        # Assuming Spark 3.4+ and Unity Catalog compatibility.
        # For CSV, the _metadata column is available from Spark 3.4.
        # For older Spark versions, you might need to read without schema and then cast,
        # or use a different approach for _source_file if _metadata is not available.
        # For simplicity and adherence to UC best practices, we assume _metadata.file_path is usable.
    )

    # 2. Add bronze layer audit columns
    bronze_df = source_df.withColumn("_ingested_at", F.current_timestamp()) \
                         .withColumn("_source_file", F.col("_metadata.file_path"))

    # 3. Write the transformed data to the target Delta table
    # The load strategy is Full Load, so we use "overwrite" mode.
    bronze_df.write.format("delta") \
                   .mode("overwrite") \
                   .saveAsTable(TARGET_TABLE_NAME)

    print(f"Successfully loaded data from {SOURCE_FILE_PATH} to {TARGET_TABLE_NAME}")
    print(f"Number of records written: {bronze_df.count()}")

except Exception as e:
    print(f"Error during ETL process: {e}")
    # Log the full stack trace for debugging
    import traceback
    traceback.print_exc()

# Data Quality Validation for Bronze Jaffle Shop Customers

This script defines and applies data quality validation rules to a hypothetical `customers.csv` dataset,
simulating the bronze layer ingestion process. The rules cover completeness, uniqueness, validity,
and timeliness dimensions.

The script first sets up a Spark session and defines a hypothetical schema for the customer data.
It then generates sample data, including various data quality issues, to demonstrate the
validation process. A list of validation rules is defined, specifying the logic, severity,
and purpose of each check.

The `run_validations` function processes these rules, identifying records that fail any check
and tagging them with the rule name, severity, and failure message. The output is a DataFrame
containing all identified problematic records.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, current_timestamp, to_timestamp, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder.appName("CustomerDQValidation").getOrCreate()

# Define the hypothetical schema for customers.csv
# Critical Note: The requirement explicitly states `Infer Schema: false`.
# Therefore, the following source schema is HYPOTHETICAL and based on common
# `customers.csv` structures. The actual schema for the source file
# must be provided and confirmed by the business/source system owner.
customer_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# --- Sample Data for Demonstration ---
# Create a list of sample data including some valid and invalid records
# Timestamps are provided as strings, which will be parsed by to_timestamp.
sample_data = [
    (1, "John", "Doe", "john.doe@example.com", "2023-01-15 10:00:00"),  # Valid
    (2, "Jane", "Smith", "jane.smith@example.com", "2023-02-20 11:30:00"), # Valid
    (3, None, "Brown", "missing.first@example.com", "2023-03-01 12:00:00"), # Missing first name
    (4, "Alice", "", "empty.last@example.com", "2023-04-05 13:00:00"), # Empty last name
    (5, "Bob", "White", "invalid-email", "2023-05-10 14:00:00"), # Invalid email format
    (6, "Charlie", "Green", None, "2023-06-15 15:00:00"), # Missing email
    (7, "David", "Black", "john.doe@example.com", "2023-07-20 16:00:00"), # Duplicate email (with ID 1)
    (1, "Eve", "Purple", "eve.purple@example.com", "2023-08-25 17:00:00"), # Duplicate ID (with ID 1)
    (8, "Frank", "Gray", "frank.gray@example.com", None), # Missing created_at
    (9, "Grace", "Blue", "grace.blue@example.com", "2099-01-01 00:00:00"), # created_at in future
    (10, "Heidi", "Yellow", "heidi.yellow@example.com", "2023-09-01 09:00:00"), # Valid
    (11, "Ivan", "Orange", "ivan.orange@example.com", "2023-10-01 09:00:00"), # Valid
    (12, "Jack", "Red", "jack.red@example.com", "2023-11-01 09:00:00"), # Valid
    (None, "Karen", "Pink", "karen.pink@example.com", "2023-12-01 09:00:00"), # Missing ID
    (14, "Liam", "Gold", "liam.gold@example.com", "2023-12-01 09:00:00"), # Valid
    (15, "Mia", "Silver", "mia.silver@example.com", "2023-12-01 09:00:00"), # Valid
]

# Create the DataFrame with a temporary string column for created_at, then cast
df_customers = spark.createDataFrame(sample_data, ["id", "first_name", "last_name", "email", "created_at_str"]) \
    .withColumn("created_at", to_timestamp(col("created_at_str"), "yyyy-MM-dd HH:mm:ss")) \
    .drop("created_at_str")

# Ensure the final DataFrame schema matches the defined customer_schema
# This step is crucial for consistency, especially if columns were inferred differently
# or if a specific column order/type is required for downstream processes.
df_customers = df_customers.select([
    col(field.name).cast(field.dataType) for field in customer_schema.fields
])

# --- Data Quality Rules Definition ---
# Define validation rules as a list of dictionaries.
# Each dictionary contains:
# - name: Unique identifier for the rule.
# - purpose: Business purpose of the rule.
# - logic: PySpark DataFrame API condition that identifies *failing* records.
# - severity: Critical, High, Medium, Low.
# - message: User-friendly message for failed records.
# - type: 'row_level' for checks applied to individual rows,
#         'aggregate_level' for checks requiring grouping (e.g., uniqueness).
# - column: The primary column(s) associated with the rule (for metadata/reporting).
#           For aggregate_level, 'logic_column' specifies the column(s) to group by.
validation_rules = [
    {
        "name": "customer_id_not_null",
        "purpose": "Every customer must have a unique identifier.",
        "logic": col("id").isNull(),
        "severity": "Critical",
        "message": "Customer ID is missing.",
        "type": "row_level",
        "column": "id"
    },
    {
        "name": "first_name_not_null_or_empty",
        "purpose": "Customer's first name is essential for identification.",
        "logic": col("first_name").isNull() | (trim(col("first_name")) == ""),
        "severity": "High",
        "message": "Customer first name is missing or empty.",
        "type": "row_level",
        "column": "first_name"
    },
    {
        "name": "last_name_not_null_or_empty",
        "purpose": "Customer's last name is essential for identification.",
        "logic": col("last_name").isNull() | (trim(col("last_name")) == ""),
        "severity": "High",
        "message": "Customer last name is missing or empty.",
        "type": "row_level",
        "column": "last_name"
    },
    {
        "name": "email_not_null_or_empty",
        "purpose": "Email is a primary contact method.",
        "logic": col("email").isNull() | (trim(col("email")) == ""),
        "severity": "High",
        "message": "Customer email is missing or empty.",
        "type": "row_level",
        "column": "email"
    },
    {
        "name": "email_format_valid",
        "purpose": "Ensure email addresses are in a recognizable format.",
        # A simple regex for email validation. More robust regex can be used if needed.
        "logic": ~col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),
        "severity": "High",
        "message": "Customer email has an invalid format.",
        "type": "row_level",
        "column": "email"
    },
    {
        "name": "created_at_not_null",
        "purpose": "Creation timestamp is important for auditing and data lineage.",
        "logic": col("created_at").isNull(),
        "severity": "High",
        "message": "Creation timestamp is missing.",
        "type": "row_level",
        "column": "created_at"
    },
    {
        "name": "created_at_not_in_future",
        "purpose": "Creation timestamp should not be a future date.",
        "logic": col("created_at") > current_timestamp(),
        "severity": "High",
        "message": "Creation timestamp is in the future.",
        "type": "row_level",
        "column": "created_at"
    },
    {
        "name": "customer_id_unique",
        "purpose": "Customer IDs must be unique to identify individual customers.",
        "logic_column": ["id"], # Column(s) to check for uniqueness (can be a list for composite keys)
        "severity": "Critical",
        "message": "Duplicate Customer ID found.",
        "type": "aggregate_level",
        "column": "id"
    },
    {
        "name": "email_unique",
        "purpose": "Email addresses should ideally be unique per customer.",
        "logic_column": ["email"], # Column(s) to check for uniqueness
        "severity": "Medium",
        "message": "Duplicate Customer email found.",
        "type": "aggregate_level",
        "column": "email"
    }
]

def run_validations(df, rules):
    """
    Applies a list of data quality validation rules to a PySpark DataFrame.

    Args:
        df (DataFrame): The input PySpark DataFrame to validate.
        rules (list): A list of dictionaries, each defining a validation rule.

    Returns:
        DataFrame: A DataFrame containing all records that failed at least one validation rule.
                   Each failed record is augmented with '_dq_rule_name', '_dq_severity',
                   and '_dq_failure_message' columns indicating the specific rule it failed.
                   Returns an empty DataFrame with the expected schema if no records fail.
    """
    failed_records_dfs = []
    
    # Define the full schema for the output failed records DataFrame
    # This ensures consistency in the output schema even if no records fail any specific rule.
    output_schema_fields = df.schema.fields + [
        StructField("_dq_rule_name", StringType(), True),
        StructField("_dq_severity", StringType(), True),
        StructField("_dq_failure_message", StringType(), True)
    ]
    output_schema = StructType(output_schema_fields)

    # Process row-level rules
    for rule in [r for r in rules if r["type"] == "row_level"]:
        failed_df_for_rule = df.filter(rule["logic"])
        if failed_df_for_rule.count() > 0:
            failed_records_dfs.append(
                failed_df_for_rule
                .withColumn("_dq_rule_name", lit(rule["name"]))
                .withColumn("_dq_severity", lit(rule["severity"]))
                .withColumn("_dq_failure_message", lit(rule["message"]))
            )

    # Process aggregate-level rules (e.g., uniqueness)
    for rule in [r for r in rules if r["type"] == "aggregate_level"]:
        group_cols = rule["logic_column"] # This is expected to be a list of column names
        
        # Find duplicate values based on the specified grouping columns
        duplicate_values_df = df.groupBy(*group_cols).agg(count(lit(1)).alias("count")) \
                                  .filter(col("count") > 1) \
                                  .select(*group_cols)
        
        if duplicate_values_df.count() > 0:
            # Join back to the original DataFrame to retrieve the full records
            # that contain the identified duplicate values.
            failed_df_for_rule = df.join(duplicate_values_df, on=group_cols, how="inner")
            failed_records_dfs.append(
                failed_df_for_rule
                .withColumn("_dq_rule_name", lit(rule["name"]))
                .withColumn("_dq_severity", lit(rule["severity"]))
                .withColumn("_dq_failure_message", lit(rule["message"]))
            )

    if not failed_records_dfs:
        # If no records failed any validation, return an empty DataFrame
        # with the full expected output schema.
        return spark.createDataFrame([], schema=output_schema)
    
    # Prepare each failed_df for union by selecting columns to match the output schema.
    # This ensures all DataFrames have the same columns in the same order before unioning.
    final_union_cols = [field.name for field in output_schema.fields]
    prepared_dfs = []
    for f_df in failed_records_dfs:
        select_expr = []
        for col_name in final_union_cols:
            if col_name in f_df.columns:
                select_expr.append(col(col_name))
            else:
                # This case should not be hit with the current logic, as DQ metadata columns
                # are added before appending to failed_records_dfs. It's for robustness.
                select_expr.append(lit(None).cast(output_schema[col_name].dataType).alias(col_name))
        prepared_dfs.append(f_df.select(select_expr))

    # Union all prepared DataFrames.
    # unionByName with allowMissingColumns=True is used for robustness,
    # though explicit selection above makes column sets identical.
    union_failed_df = prepared_dfs[0]
    for i in range(1, len(prepared_dfs)):
        union_failed_df = union_failed_df.unionByName(prepared_dfs[i], allowMissingColumns=True)
        
    # Use distinct to remove any exact duplicate rows that might arise if a single
    # original record fails multiple rules and the DQ metadata happens to be identical
    # (which is unlikely but good for robustness). If a record fails multiple *different*
    # rules, it will appear multiple times in the output, each time with the specific
    # rule it failed, which is the desired behavior for a detailed report.
    return union_failed_df.distinct()

# --- Run the validations ---
failed_customers_df = run_validations(df_customers, validation_rules)

# --- Display Results ---
print("--- Failed Customer Records ---")
if failed_customers_df.count() > 0:
    failed_customers_df.show(truncate=False)
else:
    print("No customer records failed data quality validations.")

# --- Data Quality Dimensions Checklist ---
# Completeness: [x]
# Uniqueness: [x]
# Validity: [x]
# Accuracy: [ ]
# Consistency: [ ]
# Integrity: [ ]
# Timeliness: [x]
# Volume: [ ]

# Stop Spark Session
spark.stop()
```