# Fabric notebook source
# TEMPLATE: "S2G - dim_<name>" (SCD2 dimension -- keeps history, use when a
# fact should reflect what this record's attributes WERE at the time, not
# what they are today, e.g. a resource's department/role changing over time)
# Copy, rename the notebook, fill in the SQL, merge_fields, and
# tracked_columns below. Attach the Silver and Gold lakehouses, plus
# env_medallion_toolkit.
#
# key_column: keep clearly distinct from merge_fields (Spark column names
# are case-insensitive) -- a "_sk" suffix works well.

# CELL ********************
import fabric_medallion_toolkit as fmt

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

fmt.merge(spark, df_dim_REPLACE_NAME, fmt.TableSchema(
    table_name="gold.dim_REPLACE_NAME",
    table_type="scd2",
    merge_fields=["REPLACE_NATURAL_KEY_COLUMN"],
    key_column="REPLACE_Name_sk",
    tracked_columns=["REPLACE_ATTRIBUTE_COLUMN_1", "REPLACE_ATTRIBUTE_COLUMN_2"],
    # tracked_columns=None,  # or None to track every non-key column automatically
    include_unknown_member=False,  # set True if a fact will look this up
))

# CELL ********************
print("S2G - dim_REPLACE_NAME complete.")
