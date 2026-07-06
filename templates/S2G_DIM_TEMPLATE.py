# Fabric notebook source
# TEMPLATE: "S2G - dim_<name>" (Type-1 dimension, current values only, no history)
# Copy, rename the notebook, fill in the SQL and merge_fields below.
# Attach the Silver and Gold lakehouses, plus env_medallion_toolkit.
#
# key_column is yours to name meaningfully (e.g. "Project_sk"), but keep it
# clearly DISTINCT from merge_fields -- Spark column names are case-
# insensitive by default, so key_column="Project_key" next to a
# merge_fields column "project_key" would silently overwrite it (the
# toolkit will raise a clear error if you do this, rather than corrupting
# data, but easiest to just avoid it -- a "_sk" suffix works well).
#
# Set include_unknown_member=True if any fact will look this dimension up
# and should fall back to a placeholder row instead of NULL for an
# unmatched key.

# CELL ********************
import fabric_medallion_toolkit as fmt

# CELL ********************
df_dim_REPLACE_NAME = spark.sql("""
    SELECT DISTINCT
        REPLACE_NATURAL_KEY_COLUMN,
        REPLACE_OTHER_ATTRIBUTE_COLUMNS
    FROM REPLACE_SOURCE_SCHEMA.REPLACE_SOURCE_TABLE
    WHERE REPLACE_NATURAL_KEY_COLUMN IS NOT NULL
""")

# display(df_dim_REPLACE_NAME)   # uncomment to inspect before merging

fmt.merge(spark, df_dim_REPLACE_NAME, fmt.TableSchema(
    table_name="gold.dim_REPLACE_NAME",
    table_type="dim",
    merge_fields=["REPLACE_NATURAL_KEY_COLUMN"],
    key_column="REPLACE_Name_sk",
    include_unknown_member=False,  # set True if a fact will look this up
))

# CELL ********************
print("S2G - dim_REPLACE_NAME complete.")
