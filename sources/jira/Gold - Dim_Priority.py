# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_priority",
    table_type="dim",
    key_column="Priority_Key",
    columns={
        "Priority_Id":   {"type": "string", "merge_field": True},
        "Priority_Name": {"type": "string", "default": "Unknown"},
        "Status_Color":  {"type": "string", "default": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        priority_id AS Priority_Id,
        priority_name AS Priority_Name,
        status_color AS Status_Color
    FROM Silver.jira.priorities
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Priority built successfully")
