# Fabric notebook source
# "S2G - dim_date" — shared calendar dimension. Attach Gold lakehouse only.

# CELL ********************
import fabric_medallion_toolkit as fmt

# CELL ********************
fmt.build_date_dimension(spark, fmt.DateDimensionConfig(
    table_name="gold.dim_date",
    start_date="2023-01-01",
    end_date="2035-12-31",
    fiscal_year_start_month=1,
))

# CELL ********************
print("S2G - dim_date complete.")
