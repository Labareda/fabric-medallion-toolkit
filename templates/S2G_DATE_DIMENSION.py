# Fabric notebook source
# "S2G - dim_date" — the shared calendar dimension, ONE for the whole model
# (not per-source). Only needs the Gold lakehouse attached. Cheap full
# rebuild every run (a decade of dates is ~4,000 rows), so no merge logic
# needed. Adjust the date range / fiscal year start month if needed, that's
# the only thing likely to change.

# CELL ********************
import fabric_medallion_toolkit as fmt

# CELL ********************
fmt.build_date_dimension(spark, fmt.DateDimensionConfig(
    table_name="gold.dim_date",
    start_date="2023-01-01",
    end_date="2035-12-31",
    fiscal_year_start_month=1,   # 1 = fiscal year == calendar year
))

# CELL ********************
print("S2G - dim_date complete.")
