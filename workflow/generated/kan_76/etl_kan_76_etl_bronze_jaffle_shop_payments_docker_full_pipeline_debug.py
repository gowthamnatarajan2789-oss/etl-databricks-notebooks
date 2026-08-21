# DEBUG DUMP -- raw generated code, saved for inspection.
# Generated at: 2026-08-21T09:16:38.180774+00:00
# Note: no validation errors

raise ValueError("Explicit schema for 'payments.csv' is missing. Cannot proceed with ETL pipeline development. Please provide the schema (column names and data types) for the source file.")

# The explicit schema for the source `payments.csv` file is missing, as indicated by the provided schema mapping.
#
# The ETL requirements explicitly state:
# * "The process shall not infer the schema from the source file. The schema must be explicitly defined or provided."
# * "Missing Schema Definition: Since `Infer Schema: false`, the explicit schema for `payments.csv` is missing. This is critical for correct data ingestion."
#
# Without a defined schema (column names and their corresponding data types) for the source or target,
# it is impossible to identify critical business rules, mandatory fields, or success criteria
# necessary for generating data quality validation rules.
#
# Therefore, no data quality validation rules can be generated at this time.
#
# Action Required: Please provide the explicit schema (column names and their corresponding data types)
# for the `payments.csv` file to enable the generation of data quality validations.