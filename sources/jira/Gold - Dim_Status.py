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
    table_name=f"{GOLD_SCHEMA}.dim_status",
    table_type="dim",
    key_column="Status_Key",
    columns={
        "Status_Id":       {"type": "string", "merge_field": True},
        "Status_Name":     {"type": "string", "default": "Unknown"},
        "Status_Category": {"type": "string", "default": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        status_id AS Status_Id,
        status_name AS Status_Name,
        status_category AS Status_Category
    FROM Silver.jira.statuses
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Status built successfully")
