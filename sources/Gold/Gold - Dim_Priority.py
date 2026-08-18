# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Priority
# Sort_Order exists so a priority slicer reads Highest -> Lowest rather than
# alphabetically (High, Highest, Low, Lowest, Medium). Set it as the
# sort-by column for Priority_Name in the semantic model.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_priority",
    table_type="dim",
    key_column="Priority_Key",
    columns={
        "Priority_Id":    {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Priority_Name":  {"type": "string", "default": "Unknown"},
        "Description":    {"type": "string", "default": ""},
        "Sort_Order":     {"type": "int", "default": 9},
    },
)

# CELL ********************
df = spark.sql("""
    SELECT
        p.id          AS Priority_Id,
        p.name        AS Priority_Name,
        p.description AS Description,
        CASE p.name WHEN 'Highest' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3
                    WHEN 'Low' THEN 4 WHEN 'Lowest' THEN 5 ELSE 9 END AS Sort_Order
    FROM Silver.jira.priorities p
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Priority built successfully")
