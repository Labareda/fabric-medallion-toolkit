# Fabric notebook source
# TEMPLATE: "S2G - dim_<name>" (SCD2 dimension -- keeps history, use when a
# fact should reflect what this record's attributes WERE at the time, not
# what they are today, e.g. a resource's department/role changing over time)
# Copy, rename the notebook, fill in the SQL, merge_fields, and
# tracked_columns below. Attach the Silver and Gold lakehouses, plus
# env_medallion_toolkit.

# CELL ********************
from fabric_medallion_toolkit.gold import merge_scd2

# CELL ********************
df_dim_REPLACE_NAME = spark.sql("""
    SELECT DISTINCT
        REPLACE_NATURAL_KEY_COLUMN,
        REPLACE_ATTRIBUTE_COLUMN_1,
        REPLACE_ATTRIBUTE_COLUMN_2
    FROM REPLACE_SOURCE_SCHEMA.REPLACE_SOURCE_TABLE
    WHERE REPLACE_NATURAL_KEY_COLUMN IS NOT NULL
""")

# display(df_dim_REPLACE_NAME)

merge_scd2(
    spark, df_dim_REPLACE_NAME,
    merge_fields=["REPLACE_NATURAL_KEY_COLUMN"],
    surrogate_key_column="REPLACE_NAME_key",
    table_name="gold.dim_REPLACE_NAME",
    tracked_columns=["REPLACE_ATTRIBUTE_COLUMN_1", "REPLACE_ATTRIBUTE_COLUMN_2"],
    # tracked_columns=None,  # or None to track every non-key column automatically
)

# CELL ********************
print("S2G - dim_REPLACE_NAME complete.")
