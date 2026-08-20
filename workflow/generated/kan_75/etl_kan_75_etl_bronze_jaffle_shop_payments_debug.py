# DEBUG DUMP -- raw generated code, saved for inspection.
# Generated at: 2026-08-20T14:53:44.377154+00:00
# Note: no validation errors

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType

# Define source and target configurations
SOURCE_FILE_PATH = "/Volumes/workspace/default/raw_data/jaffle_shop/payments.csv"
TARGET_CATALOG = "workspace"
TARGET_SCHEMA = "default"
TARGET_TABLE_NAME = "bronze_jaffle_shop_payments"
TARGET_FULL_TABLE_NAME = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE_NAME}"

# 1. Read the source CSV file
# Infer schema and header are present as per requirements
df_source = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(SOURCE_FILE_PATH)
)

# 2. Apply transformations and select columns
df_bronze = (
    df_source.select(
        F.col("id").cast(IntegerType()).alias("id"),
        F.col("order_id").cast(IntegerType()).alias("order_id"),
        F.col("payment_method").cast(StringType()).alias("payment_method"),
        F.col("amount").cast(DoubleType()).alias("amount"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# 3. Write the transformed data to the Delta table
# Load Type: Full Load (Overwrite)
df_bronze.write.format("delta").mode("overwrite").saveAsTable(TARGET_FULL_TABLE_NAME)

print(f"Successfully loaded data into {TARGET_FULL_TABLE_NAME}")

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, count, when, isnull, trim, lower
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

def validate_bronze_jaffle_shop_payments(df: DataFrame) -> DataFrame:
    """
    Performs data quality validations on the bronze_jaffle_shop_payments DataFrame.

    Args:
        df (DataFrame): The input DataFrame for bronze_jaffle_shop_payments.

    Returns:
        DataFrame: A DataFrame containing details of failed data quality rules.
                   Returns an empty DataFrame with the expected schema if no rules are violated.
    """
    validation_results = []

    # Define the schema for the output DataFrame of failed records
    output_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("payment_method", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("rule_name", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("failure_message", StringType(), False)
    ])

    # Rule 1: id_not_null
    # Business Purpose: Ensure every payment record has a unique identifier.
    # Validation Logic: 'id' column should not be null.
    # Severity: Critical
    rule_name_1 = "id_not_null"
    failure_message_1 = "Payment ID cannot be null."
    failed_records_id_null = df.filter(isnull(col("id")))
    if failed_records_id_null.count() > 0:
        validation_results.append(
            failed_records_id_null.withColumn("rule_name", lit(rule_name_1))
                                  .withColumn("severity", lit("Critical"))
                                  .withColumn("failure_message", lit(failure_message_1))
        )

    # Rule 2: id_unique
    # Business Purpose: Ensure each payment record is uniquely identified.
    # Validation Logic: 'id' column should be unique.
    # Severity: Critical
    rule_name_2 = "id_unique"
    failure_message_2 = "Duplicate Payment ID found."
    duplicate_ids = df.groupBy("id").agg(count("*").alias("count")) \
                      .filter(col("count") > 1) \
                      .select("id")
    if duplicate_ids.count() > 0:
        failed_records_id_unique = df.join(duplicate_ids, "id", "inner")
        validation_results.append(
            failed_records_id_unique.withColumn("rule_name", lit(rule_name_2))
                                    .withColumn("severity", lit("Critical"))
                                    .withColumn("failure_message", lit(failure_message_2))
        )

    # Rule 3: order_id_not_null
    # Business Purpose: Ensure every payment is associated with an order.
    # Validation Logic: 'order_id' column should not be null.
    # Severity: Critical
    rule_name_3 = "order_id_not_null"
    failure_message_3 = "Order ID cannot be null for a payment."
    failed_records_order_id_null = df.filter(isnull(col("order_id")))
    if failed_records_order_id_null.count() > 0:
        validation_results.append(
            failed_records_order_id_null.withColumn("rule_name", lit(rule_name_3))
                                        .withColumn("severity", lit("Critical"))
                                        .withColumn("failure_message", lit(failure_message_3))
        )

    # Rule 4: payment_method_valid
    # Business Purpose: Ensure payment methods are from an approved list.
    # Validation Logic: 'payment_method' should be one of ('credit_card', 'coupon', 'bank_transfer', 'gift_card').
    # Severity: High
    rule_name_4 = "payment_method_valid"
    failure_message_4 = "Invalid payment method detected."
    allowed_payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card']
    failed_records_payment_method = df.filter(
        (col("payment_method").isNull()) |
        (~lower(trim(col("payment_method"))).isin(allowed_payment_methods))
    )
    if failed_records_payment_method.count() > 0:
        validation_results.append(
            failed_records_payment_method.withColumn("rule_name", lit(rule_name_4))
                                         .withColumn("severity", lit("High"))
                                         .withColumn("failure_message", lit(failure_message_4))
        )

    # Rule 5: amount_non_negative
    # Business Purpose: Ensure payment amounts are not negative.
    # Validation Logic: 'amount' column should be greater than or equal to 0.
    # Severity: High
    rule_name_5 = "amount_non_negative"
    failure_message_5 = "Payment amount cannot be negative."
    failed_records_amount_negative = df.filter(col("amount") < 0)
    if failed_records_amount_negative.count() > 0:
        validation_results.append(
            failed_records_amount_negative.withColumn("rule_name", lit(rule_name_5))
                                          .withColumn("severity", lit("High"))
                                          .withColumn("failure_message", lit(failure_message_5))
        )

    if validation_results:
        # Union all failed records DataFrames
        # Ensure all DFs have the same columns in the same order before unionByName
        selected_cols = [c.name for c in output_schema]
        return validation_results[0].select(*selected_cols).unionByName(
            *[res.select(*selected_cols) for res in validation_results[1:]]
        )
    else:
        # Return an empty DataFrame with the expected schema if no failures
        return df.sparkSession.createDataFrame([], schema=output_schema)

# Data Quality Dimensions Covered:
# [x] Completeness
# [x] Uniqueness
# [x] Validity
# [x] Accuracy
# [ ] Consistency
# [ ] Integrity
# [ ] Timeliness
# [ ] Volume