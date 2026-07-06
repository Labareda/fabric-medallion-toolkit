# Fabric notebook source
# TEMPLATE: "S2G - fact_<name>"
# Copy, rename the notebook, fill in the SQL and dimension lookups below.
# Run this AFTER the dimensions it references. Attach the Silver and Gold
# lakehouses, plus env_medallion_toolkit.

# CELL ********************
import fabric_medallion_toolkit as fmt

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

# One lookup_key call per dimension this fact relates to. Delete calls you
# don't need, add more by copying a block. dim_key_column is whatever
# key_column that dimension used when it was built; output_column is what
# to call it here on the fact (can be the same, or different if you're
# pulling from multiple dimensions and want to avoid ambiguity).
df_fact_REPLACE_NAME = fmt.lookup_key(
    spark, df_fact_REPLACE_NAME,
    dim_table_name="gold.dim_REPLACE_DIM_NAME",
    dim_natural_key_column="REPLACE_DIM_NATURAL_KEY_COLUMN",
    dim_key_column="REPLACE_DIM_NAME_sk",
    fact_join_column="REPLACE_FACT_COLUMN_HOLDING_THAT_NATURAL_KEY",
    output_column="REPLACE_DIM_NAME_sk",
    # as_of_column="REPLACE_A_DATE_COLUMN_ON_THIS_FACT",  # uncomment for
    # point-in-time resolution against an SCD2 dimension -- omit to always
    # link to that dimension's CURRENT version instead.
    # default_to_unknown=True,  # uncomment if that dimension has an Unknown
    # member row (TableSchema.include_unknown_member=True) and you want
    # unmatched fact rows to resolve there instead of NULL.
)

fmt.merge(spark, df_fact_REPLACE_NAME, fmt.TableSchema(
    table_name="gold.fact_REPLACE_NAME",
    table_type="fact",
    merge_fields=["REPLACE_GRAIN_KEY_COLUMN"],
    key_column="REPLACE_Name_sk",
))

# CELL ********************
print("S2G - fact_REPLACE_NAME complete.")
