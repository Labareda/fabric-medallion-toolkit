# Fabric notebook source
# TEMPLATE: "S2G - dim_<name>" (Type-1 dimension, current values only, no history)
# Copy, rename the notebook, fill in the SQL and merge_fields below.
# Attach the Silver and Gold lakehouses, plus env_medallion_toolkit.

# CELL ********************
from fabric_medallion_toolkit.gold import merge_dim

# CELL ********************
df_dim_REPLACE_NAME = spark.sql("""
    SELECT DISTINCT
        REPLACE_NATURAL_KEY_COLUMN,
        REPLACE_OTHER_ATTRIBUTE_COLUMNS
    FROM REPLACE_SOURCE_SCHEMA.REPLACE_SOURCE_TABLE
    WHERE REPLACE_NATURAL_KEY_COLUMN IS NOT NULL
""")

# display(df_dim_REPLACE_NAME)   # uncomment to inspect before merging

merge_dim(
    spark, df_dim_REPLACE_NAME,
    table_name="gold.dim_REPLACE_NAME",
    merge_fields=["REPLACE_NATURAL_KEY_COLUMN"],
    surrogate_key_column="REPLACE_NAME_key",
)

# CELL ********************
print("S2G - dim_REPLACE_NAME complete.")
