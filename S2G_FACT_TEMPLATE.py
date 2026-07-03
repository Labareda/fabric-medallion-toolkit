# Fabric notebook source
# TEMPLATE: "S2G - fact_<name>"
# Copy, rename the notebook, fill in the SQL and dimension lookups below.
# Run this AFTER the dimensions it references. Attach the Silver and Gold
# lakehouses, plus env_medallion_toolkit.

# CELL ********************
from fabric_medallion_toolkit.gold import merge_fact, lookup_dimension_key

# CELL ********************
df_fact_REPLACE_NAME = spark.sql("""
    SELECT
        REPLACE_GRAIN_KEY_COLUMN,
        REPLACE_DIMENSION_NATURAL_KEY_COLUMNS,
        REPLACE_DATE_COLUMNS,
        REPLACE_MEASURE_COLUMNS
    FROM REPLACE_SOURCE_SCHEMA.REPLACE_SOURCE_TABLE
""")

# display(df_fact_REPLACE_NAME)

# One lookup_dimension_key call per dimension this fact relates to. Delete
# calls you don't need, add more by copying a block. Column names come out
# exactly as they are in the dimension (e.g. "resource_key"), so they read
# as unambiguous FKs in the fact's schema.
df_fact_REPLACE_NAME = lookup_dimension_key(
    spark, df_fact_REPLACE_NAME,
    dim_table_name="gold.dim_REPLACE_DIM_NAME",
    dim_natural_key_column="REPLACE_DIM_NATURAL_KEY_COLUMN",
    dim_key_column="REPLACE_DIM_NAME_key",
    fact_join_column="REPLACE_FACT_COLUMN_HOLDING_THAT_NATURAL_KEY",
    # as_of_column="REPLACE_A_DATE_COLUMN_ON_THIS_FACT",  # uncomment for
    # point-in-time resolution against an SCD2 dimension -- omit to always
    # link to that dimension's CURRENT version instead.
)

merge_fact(
    spark, df_fact_REPLACE_NAME,
    table_name="gold.fact_REPLACE_NAME",
    merge_fields=["REPLACE_GRAIN_KEY_COLUMN"],
    surrogate_key_column="REPLACE_NAME_key",
)

# CELL ********************
print("S2G - fact_REPLACE_NAME complete.")
