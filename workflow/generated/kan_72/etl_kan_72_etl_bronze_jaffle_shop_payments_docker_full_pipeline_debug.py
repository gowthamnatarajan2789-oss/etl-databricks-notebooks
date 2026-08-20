# DEBUG DUMP -- raw generated code, saved for inspection.
# Generated at: 2026-08-20T09:35:59.107545+00:00
# Note: no validation errors

import uuid
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DateType,
    DecimalType,
    StringType,
    TimestampType,
)

# This is a New Feature.
# Implementation Approach: Define a schema for the source CSV, read the CSV using this schema,
# add standard audit columns, and write the resulting DataFrame to the target Delta table in overwrite mode.

# Define source and target paths/tables
SOURCE_FILE_PATH = "/Volumes/workspace/default/raw_data/jaffle_shop/orders.csv"
TARGET_TABLE_NAME = "workspace.default.bronze_jaffle_shop_payments"

# Define the schema for the orders.csv file
# This schema is based on the proposed schema in the requirements and should be validated.
orders_schema = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("payment_timestamp", TimestampType(), True),
    ]
)


def ingest_orders_to_bronze(
    source_path: str, target_table: str, schema: StructType
) -> None:
    """
    Ingests data from the orders.csv file into the bronze_jaffle_shop_payments Delta table.

    Args:
        source_path: The full path to the source CSV file.
        target_table: The fully qualified name of the target Delta table.
        schema: The PySpark StructType schema to apply when reading the CSV.
    """
    try:
        # Generate a unique load ID for this ETL run
        load_id = str(uuid.uuid4())

        # Read the source CSV file with the predefined schema
        df_orders = (
            spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .schema(schema)
            .load(source_path)
        )

        # Add audit columns
        df_bronze = df_orders.withColumn(
            "_ingest_timestamp", F.current_timestamp()
        ).withColumn("_source_file_name", F.input_file_name().alias("_source_file_name")).withColumn(
            "_load_id", F.lit(load_id)
        )

        # Write the DataFrame to the target Delta table in overwrite mode
        df_bronze.write.format("delta").mode("overwrite").saveAsTable(target_table)

        print(f"Successfully ingested data from {source_path} to {target_table}")
        print(f"Number of records written: {df_bronze.count()}")

    except Exception as e:
        print(f"Error ingesting data from {source_path} to {target_table}: {e}")
        raise


# Execute the ingestion function
ingest_orders_to_bronze(SOURCE_FILE_PATH, TARGET_TABLE_NAME, orders_schema)

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DecimalType, StringType, TimestampType
import logging
from datetime import date, datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Databricks Notebook Cell 1: Define Schema and Setup

# Define a schema for the raw orders data based on the proposed schema.
# This is crucial because the requirement states 'Infer Schema: false'
# and we need a fixed schema for validation.
# NOTE: This schema is based on the 'Proposed Data Type' from the problem description.
# It should be confirmed by the data source owner.
raw_orders_schema = StructType([
    StructField("order_id", IntegerType(), False), # Assuming order_id is non-nullable
    StructField("customer_id", IntegerType(), False), # Assuming customer_id is non-nullable
    StructField("order_date", DateType(), False), # Assuming order_date is non-nullable
    StructField("amount", DecimalType(10, 2), False), # Assuming amount is non-nullable
    StructField("payment_method", StringType(), False), # Assuming payment_method is non-nullable
    StructField("status", StringType(), False), # Assuming status is non-nullable
    StructField("transaction_id", StringType(), True), # transaction_id can be null if payment not processed
    StructField("payment_timestamp", TimestampType(), True) # payment_timestamp can be null if payment not processed
])

# Databricks Notebook Cell 2: Data Quality Validation Function

def run_data_quality_checks(df: DataFrame, table_name: str) -> DataFrame:
    """
    Runs a series of data quality checks on the input DataFrame for Jaffle Shop Payments.

    Args:
        df (DataFrame): The input PySpark DataFrame to validate.
        table_name (str): The name of the table being validated (for logging/reporting).

    Returns:
        DataFrame: A DataFrame containing the results of the data quality checks.
                   Each row represents a failed rule, including count and sample IDs.
                   Returns an empty DataFrame with the expected schema if no failures.
    """
    logger.info(f"Starting data quality checks for table: {table_name}")

    validation_results = []

    # --- Rule 1: Completeness - order_id must not be null ---
    rule_name = "order_id_not_null"
    business_purpose = "Ensure every order has a unique identifier."
    severity = "CRITICAL"
    failure_message = "Order ID is missing."
    failed_records = df.filter(F.col("order_id").isNull())
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 2: Uniqueness - order_id must be unique ---
    rule_name = "order_id_unique"
    business_purpose = "Ensure each order has a unique identifier to prevent data integrity issues."
    severity = "CRITICAL"
    failure_message = "Duplicate Order ID found."
    duplicate_order_ids = df.groupBy("order_id").count().filter(F.col("count") > 1)
    if duplicate_order_ids.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": duplicate_order_ids.count(),
            "sample_failed_ids": [row.order_id for row in duplicate_order_ids.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {duplicate_order_ids.count()} records.")

    # --- Rule 3: Completeness - customer_id must not be null ---
    rule_name = "customer_id_not_null"
    business_purpose = "Ensure every order is associated with a customer."
    severity = "CRITICAL"
    failure_message = "Customer ID is missing."
    failed_records = df.filter(F.col("customer_id").isNull())
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 4: Completeness - order_date must not be null ---
    rule_name = "order_date_not_null"
    business_purpose = "Ensure every order has a recorded date."
    severity = "CRITICAL"
    failure_message = "Order Date is missing."
    failed_records = df.filter(F.col("order_date").isNull())
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 5: Validity - amount must not be null and must be positive ---
    rule_name = "amount_not_null_and_positive"
    business_purpose = "Ensure every order has a valid, positive amount."
    severity = "CRITICAL"
    failure_message = "Order amount is missing or not positive."
    failed_records = df.filter(F.col("amount").isNull() | (F.col("amount") <= 0))
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 6: Completeness - payment_method must not be null ---
    rule_name = "payment_method_not_null"
    business_purpose = "Ensure every order specifies a payment method."
    severity = "CRITICAL"
    failure_message = "Payment method is missing."
    failed_records = df.filter(F.col("payment_method").isNull())
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 7: Validity - payment_method must be from an allowed list ---
    # NOTE: This list should be confirmed by business requirements.
    allowed_payment_methods = ["credit_card", "cash", "gift_card", "bank_transfer"]
    rule_name = "payment_method_valid_values"
    business_purpose = "Ensure payment methods conform to predefined categories."
    severity = "HIGH"
    failure_message = f"Payment method is not one of the allowed values: {', '.join(allowed_payment_methods)}."
    failed_records = df.filter(F.col("payment_method").isNotNull() & ~F.col("payment_method").isin(allowed_payment_methods))
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 8: Completeness - status must not be null ---
    rule_name = "status_not_null"
    business_purpose = "Ensure every order has a recorded status."
    severity = "CRITICAL"
    failure_message = "Order status is missing."
    failed_records = df.filter(F.col("status").isNull())
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 9: Validity - status must be from an allowed list ---
    # NOTE: This list should be confirmed by business requirements.
    allowed_statuses = ["completed", "pending", "failed", "refunded", "shipped"]
    rule_name = "status_valid_values"
    business_purpose = "Ensure order statuses conform to predefined categories."
    severity = "HIGH"
    failure_message = f"Order status is not one of the allowed values: {', '.join(allowed_statuses)}."
    failed_records = df.filter(F.col("status").isNotNull() & ~F.col("status").isin(allowed_statuses))
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 10: Consistency - payment_timestamp must not be null if status is 'completed' ---
    rule_name = "payment_timestamp_not_null_if_completed"
    business_purpose = "Ensure completed orders have a payment timestamp."
    severity = "HIGH"
    failure_message = "Payment timestamp is missing for a completed order."
    failed_records = df.filter((F.col("status") == "completed") & F.col("payment_timestamp").isNull())
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 11: Consistency - payment_timestamp must be after or equal to order_date ---
    rule_name = "payment_timestamp_after_order_date"
    business_purpose = "Ensure payment processing does not precede the order placement."
    severity = "HIGH"
    failure_message = "Payment timestamp is before order date."
    failed_records = df.filter(
        F.col("payment_timestamp").isNotNull() & F.col("order_date").isNotNull() &
        (F.to_date(F.col("payment_timestamp")) < F.col("order_date"))
    )
    if failed_records.count() > 0:
        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": failed_records.count(),
            "sample_failed_ids": [row.order_id for row in failed_records.select("order_id").limit(5).collect()]
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {failed_records.count()} records.")

    # --- Rule 12: Uniqueness - transaction_id must be unique if not null ---
    rule_name = "transaction_id_unique_if_not_null"
    business_purpose = "Ensure each payment transaction has a unique identifier when present."
    severity = "HIGH"
    failure_message = "Duplicate Transaction ID found for non-null transaction IDs."
    duplicate_transaction_ids_df = df.filter(F.col("transaction_id").isNotNull()) \
                                     .groupBy("transaction_id").count().filter(F.col("count") > 1)
    if duplicate_transaction_ids_df.count() > 0:
        # Get sample transaction_ids that are duplicates
        sample_duplicate_txns = [row.transaction_id for row in duplicate_transaction_ids_df.limit(5).collect()]
        # Get sample order_ids associated with these duplicate transaction_ids
        sample_failed_order_ids = [row.order_id for row in df.filter(F.col("transaction_id").isin(sample_duplicate_txns)).select("order_id").limit(5).collect()]

        validation_results.append({
            "rule_name": rule_name,
            "business_purpose": business_purpose,
            "severity": severity,
            "failure_message": failure_message,
            "failed_count": duplicate_transaction_ids_df.count(),
            "sample_failed_ids": sample_failed_order_ids
        })
        logger.warning(f"DQ Check Failed: {rule_name} - {duplicate_transaction_ids_df.count()} records.")

    logger.info(f"Finished data quality checks for table: {table_name}")

    # Define the schema for the results DataFrame
    results_schema = StructType([
        StructField("rule_name", StringType(), False),
        StructField("business_purpose", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("failure_message", StringType(), False),
        StructField("failed_count", IntegerType(), False),
        StructField("sample_failed_ids", ArrayType(IntegerType()), True) # order_id is IntegerType
    ])

    if validation_results:
        # Create a DataFrame from the validation results
        results_df = spark.createDataFrame(validation_results, schema=results_schema)
        return results_df
    else:
        logger.info("All data quality checks passed.")
        # Return an empty DataFrame with the expected schema if no failures
        return spark.createDataFrame([], schema=results_schema)

# Databricks Notebook Cell 3: Example Usage (assuming 'spark' session is available)

# Example: Create a dummy DataFrame for testing
# This part would be replaced by actual data loading in a real scenario
data = [
    (1, 101, "2023-01-01", 100.50, "credit_card", "completed", "txn123", "2023-01-01 10:00:00"),
    (2, 102, "2023-01-02", 25.00, "cash", "pending", None, None),
    (3, 103, "2023-01-03", 75.20, "gift_card", "completed", "txn124", "2023-01-03 11:30:00"),
    (4, 104, "2023-01-04", -5.00, "credit_card", "completed", "txn125", "2023-01-04 09:00:00"), # Invalid amount
    (5, 105, "2023-01-05", 50.00, "unknown_method", "completed", "txn126", "2023-01-05 14:00:00"), # Invalid payment_method
    (6, 106, "2023-01-06", 120.00, "credit_card", "invalid_status", "txn127", "2023-01-06 16:00:00"), # Invalid status
    (7, 107, "2023-01-07", 30.00, "cash", "completed", "txn128", None), # Completed with null payment_timestamp
    (8, 108, "2023-01-08", 45.00, "credit_card", "completed", "txn129", "2023-01-07 08:00:00"), # payment_timestamp before order_date
    (9, 109, "2023-01-09", 60.00, "bank_transfer", "pending", "txn123", "2023-01-09 10:00:00"), # Duplicate transaction_id (with order 1)
    (10, 110, None, 80.00, "credit_card", "completed", "txn130", "2023-01-10 12:00:00"), # Null order_date
    (11, None, "2023-01-11", 90.00, "credit_card", "completed", "txn131", "2023-01-11 13:00:00"), # Null customer_id
    (None, 112, "2023-01-12", 110.00, "credit_card", "completed", "txn132", "2023-01-12 14:00:00"), # Null order_id
    (13, 113, "2023-01-13", None, "credit_card", "completed", "txn133", "2023-01-13 15:00:00"), # Null amount
    (14, 114, "2023-01-14", 150.00, None, "completed", "txn134", "2023-01-14 16:00:00"), # Null payment_method
    (15, 115, "2023-01-15", 160.00, "credit_card", None, "txn135", "2023-01-15 17:00:00"), # Null status
    (1, 116, "2023-01-16", 170.00, "credit_card", "completed", "txn136", "2023-01-16 18:00:00") # Duplicate order_id (with order 1)
]

# Convert date and timestamp strings to actual Date and Timestamp objects for schema compliance
parsed_data = []
for row in data:
    order_date_obj = date.fromisoformat(row[2]) if row[2] else None
    payment_timestamp_obj = datetime.fromisoformat(row[7]) if row[7] else None
    parsed_data.append((row[0], row[1], order_date_obj, row[3], row[4], row[5], row[6], payment_timestamp_obj))

# Create DataFrame with the defined schema
# In a real scenario, you would read from a source like:
# df_raw_orders = spark.read.csv("/Volumes/workspace/default/raw_data/jaffle_shop/orders.csv", schema=raw_orders_schema, header=True)
df_raw_orders = spark.createDataFrame(parsed_data, schema=raw_orders_schema)

# Run the data quality checks
dq_results_df = run_data_quality_checks(df_raw_orders, "bronze_jaffle_shop_payments")

# Display results
if dq_results_df.count() > 0:
    print("--- Data Quality Check Failures ---")
    dq_results_df.display()
else:
    print("All data quality checks passed successfully!")

# Databricks Notebook Cell 4: Data Quality Dimensions Checklist

# Data Quality Dimensions Covered:
# [x] Completeness
# [x] Uniqueness
# [x] Validity
# [x] Accuracy
# [x] Consistency
# [ ] Integrity (Referential integrity is typically handled in later layers, but uniqueness/completeness of PKs contribute)
# [ ] Timeliness (Covered by date/timestamp checks, but not explicit "freshness" checks against current time)
# [ ] Volume (Not explicitly checked, but can be added if needed, e.g., row count thresholds)