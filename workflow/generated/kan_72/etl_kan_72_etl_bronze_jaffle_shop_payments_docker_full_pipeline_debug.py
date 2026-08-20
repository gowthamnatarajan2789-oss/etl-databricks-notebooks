# DEBUG DUMP -- raw generated code, saved for inspection.
# Generated at: 2026-08-20T10:19:03.058866+00:00
# Note: no validation errors

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as F

# This is a New Feature.
# Design: Define an explicit schema for the CSV, read the CSV with the schema,
# add audit columns (_load_timestamp, _source_file), and write to the Delta table in overwrite mode.

# Initialize Spark Session (if not already available in Databricks)
# spark = SparkSession.builder.appName("JaffleShopPaymentsBronzeIngestion").getOrCreate()

# Define the explicit schema for the payments.csv file
# CRITICAL NOTE: This schema is ASSUMED based on common Jaffle Shop datasets.
# It MUST BE CONFIRMED with the data source owner.
payments_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("payment_method", StringType(), False),
    StructField("amount", IntegerType(), False)
])

# Define source and target paths/names
source_file_path = "/Volumes/workspace/default/raw_data/jaffle_shop/payments.csv"
target_table_name = "workspace.default.bronze_jaffle_shop_payments"

try:
    # Read the CSV file with the defined schema
    payments_df = spark.read.csv(
        source_file_path,
        header=True,
        schema=payments_schema,
        sep=","
    )

    # Add audit columns
    payments_bronze_df = payments_df.withColumn(
        "_load_timestamp",
        F.current_timestamp()
    ).withColumn(
        "_source_file",
        F.input_file_name()
    )

    # Write the DataFrame to the Delta table in overwrite mode
    payments_bronze_df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)

    print(f"Successfully loaded data from '{source_file_path}' to '{target_table_name}'")

except Exception as e:
    print(f"Error loading payments data: {e}")
    raise e

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Cell 1: Data Quality Validation Function

def run_data_quality_checks(df: DataFrame, rules: list) -> DataFrame:
    """
    Applies a list of data quality rules to a DataFrame and returns a DataFrame
    of failing records with a _dq_failure_reason column.

    Args:
        df: The input Spark DataFrame.
        rules: A list of dictionaries, each defining a data quality rule.
               Each rule dictionary should have:
               - "rule_name": Name of the rule.
               - "business_purpose": Description of the rule's purpose.
               - "validation_logic": Spark SQL expression for row-level checks,
                                     or a specific string like "AGGREGATE_UNIQUE" for aggregate checks.
               - "target_column": The column(s) the rule applies to (e.g., "id" for uniqueness).
               - "severity": "critical", "high", "medium", "low".
               - "failure_message": Message to display if the rule fails.
    Returns:
        A DataFrame containing records that failed any validation rule,
        along with a '_dq_failure_reason' column.
        Returns an empty DataFrame with the expected schema if no failures are found.
    """
    failing_records_dfs = []
    original_cols = df.columns # Capture original columns for consistent output schema

    for rule in rules:
        rule_name = rule["rule_name"]
        validation_logic = rule["validation_logic"]
        target_column = rule.get("target_column")
        failure_message = rule["failure_message"]
        severity = rule["severity"]
        full_failure_msg = f"{rule_name}: {failure_message} (Severity: {severity})"

        if validation_logic == "AGGREGATE_UNIQUE":
            if not target_column:
                raise ValueError(f"Rule '{rule_name}' of type AGGREGATE_UNIQUE requires 'target_column'.")

            duplicate_ids = df.groupBy(target_column).agg(F.count(target_column).alias("count")) \
                              .filter("count > 1") \
                              .select(target_column)

            if duplicate_ids.count() > 0:
                failing_df = df.join(duplicate_ids, target_column, "inner") \
                               .withColumn("_dq_failure_reason", F.lit(full_failure_msg))
                # Ensure the failing_df has the same schema as original_cols + _dq_failure_reason
                failing_records_dfs.append(failing_df.select(*original_cols, "_dq_failure_reason"))
        else:
            # Row-level checks
            failing_df = df.filter(F.expr(validation_logic))
            if failing_df.count() > 0:
                failing_df = failing_df.withColumn("_dq_failure_reason", F.lit(full_failure_msg))
                # Ensure the failing_df has the same schema as original_cols + _dq_failure_reason
                failing_records_dfs.append(failing_df.select(*original_cols, "_dq_failure_reason"))

    if failing_records_dfs:
        # Union all failing records
        union_df = failing_records_dfs[0]
        for i in range(1, len(failing_records_dfs)):
            union_df = union_df.unionByName(failing_records_dfs[i], allowMissingColumns=True)

        # Group by original columns to consolidate failure reasons into a single string
        consolidated_failures = union_df.groupBy(*original_cols) \
                                        .agg(F.collect_list("_dq_failure_reason").alias("_dq_failure_reasons")) \
                                        .withColumn("_dq_failure_reason", F.concat_ws("; ", F.col("_dq_failure_reasons"))) \
                                        .drop("_dq_failure_reasons")

        return consolidated_failures
    else:
        # Return an empty DataFrame with the expected schema if no failures
        empty_schema = df.schema
        if "_dq_failure_reason" not in [f.name for f in empty_schema]:
            empty_schema = empty_schema.add(StructField("_dq_failure_reason", StringType(), True))
        return df.sparkSession.createDataFrame([], empty_schema)


# Cell 2: Define Data Quality Rules for payments.csv

dq_rules = [
    {
        "rule_name": "payment_id_not_null",
        "business_purpose": "Ensure every payment has a unique identifier.",
        "validation_logic": "id IS NULL",
        "target_column": "id",
        "severity": "critical",
        "failure_message": "Payment ID is null."
    },
    {
        "rule_name": "order_id_not_null",
        "business_purpose": "Ensure every payment is linked to an order.",
        "validation_logic": "order_id IS NULL",
        "target_column": "order_id",
        "severity": "critical",
        "failure_message": "Order ID is null."
    },
    {
        "rule_name": "payment_method_not_null_or_empty",
        "business_purpose": "Ensure every payment has a specified method.",
        "validation_logic": "payment_method IS NULL OR TRIM(payment_method) = ''",
        "target_column": "payment_method",
        "severity": "critical",
        "failure_message": "Payment method is null or empty."
    },
    {
        "rule_name": "amount_not_null",
        "business_purpose": "Ensure every payment has an amount.",
        "validation_logic": "amount IS NULL",
        "target_column": "amount",
        "severity": "critical",
        "failure_message": "Payment amount is null."
    },
    {
        "rule_name": "payment_id_unique",
        "business_purpose": "Ensure payment IDs are unique across the dataset.",
        "validation_logic": "AGGREGATE_UNIQUE",
        "target_column": "id",
        "severity": "critical",
        "failure_message": "Duplicate Payment ID found."
    },
    {
        "rule_name": "amount_non_negative",
        "business_purpose": "Ensure payment amounts are not negative.",
        "validation_logic": "amount < 0",
        "target_column": "amount",
        "severity": "high",
        "failure_message": "Payment amount is negative."
    }
]


# Cell 3: Example Usage and Reporting

# Assume 'spark' is an existing SparkSession in Databricks.
# For local testing, uncomment the following line:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("DQ_Payments_Validation").getOrCreate()

# Define the assumed schema for payments.csv
payments_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create a sample DataFrame with some data quality issues for demonstration
sample_data = [
    (1, 101, "credit_card", 1000),
    (2, 102, "coupon", 500),
    (3, 103, "bank_transfer", 2000),
    (4, 104, "gift_card", 750),
    (5, 105, None, 1200),          # payment_method_not_null_or_empty
    (6, 106, "credit_card", -100), # amount_non_negative
    (None, 107, "coupon", 300),    # payment_id_not_null
    (8, None, "credit_card", 1500),# order_id_not_null
    (9, 109, "credit_card", None), # amount_not_null
    (1, 110, "debit_card", 200),   # payment_id_unique (duplicate id=1)
    (10, 111, "credit_card", 1000),
    (11, 112, "", 500)             # payment_method_not_null_or_empty (empty string)
]

df_payments = spark.createDataFrame(sample_data, payments_schema)

print("Running data quality checks on payments data...")
total_records = df_payments.count()
print(f"Total records in source: {total_records}")

failing_records_df = run_data_quality_checks(df_payments, dq_rules)
failed_records_count = failing_records_df.count()

print(f"Total records failing DQ checks: {failed_records_count}")
if total_records > 0:
    failure_percentage = (failed_records_count / total_records) * 100
    print(f"Percentage of records failing DQ checks: {failure_percentage:.2f}%")
else:
    print("No records to process.")

if failed_records_count > 0:
    print("\n--- Failing Records Sample (up to 10) ---")
    display(failing_records_df.limit(10))

    print("\n--- Failure Summary by Rule ---")
    # Explode the concatenated failure reasons to count individual rule failures
    failure_summary = failing_records_df.withColumn("failure_reason_exploded", F.explode(F.split(F.col("_dq_failure_reason"), "; "))) \
                                        .groupBy("failure_reason_exploded") \
                                        .agg(F.count("*").alias("failure_count")) \
                                        .orderBy(F.desc("failure_count"))
    display(failure_summary)

    # Check for critical failures and potentially raise an alert/exception
    critical_failures_count = failure_summary.filter(F.col("failure_reason_exploded").contains("Severity: critical")).count()
    if critical_failures_count > 0:
        print("\nCRITICAL DATA QUALITY ISSUES DETECTED. Review failing records and consider halting pipeline.")
        # In a production pipeline, you might raise an exception here to stop further processing:
        # raise Exception("Critical data quality issues detected in payments data.")
else:
    print("\nAll records passed data quality checks!")


# Cell 4: Data Quality Dimensions Covered Checklist

# Data Quality Dimensions Covered:
# [x] Completeness (id, order_id, payment_method, amount are not null/empty)
# [x] Uniqueness (id is unique)
# [x] Validity (amount is non-negative, payment_method is not empty string)
# [ ] Accuracy
# [ ] Consistency
# [ ] Integrity
# [ ] Timeliness
# [ ] Volume