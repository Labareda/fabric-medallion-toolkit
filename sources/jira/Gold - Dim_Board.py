# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Project_Code kept as a plain attribute rather than snowflaking to
# Dim_Project -- Board is a minor dimension here (mainly for resourcing-
# view context/filtering, not the primary Gantt hierarchy).
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_board",
    table_type="dim",
    key_column="Board_Key",
    columns={
        "Board_Id":     {"type": "string", "merge_field": True},
        "Board_Name":   {"type": "string", "default": "Unknown"},
        "Board_Type":   {"type": "string", "default": "Unknown"},
        "Project_Code": {"type": "string", "default": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        board_id AS Board_Id,
        board_name AS Board_Name,
        board_type AS Board_Type,
        project_key AS Project_Code
    FROM Silver.jira.boards
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Board built successfully")
