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
    table_name=f"{GOLD_SCHEMA}.dim_project",
    table_type="dim",
    key_column="Project_Key",
    columns={
        "Project_Id":      {"type": "string", "merge_field": True},
        "Project_Code":    {"type": "string", "default": "Unknown"},
        "Project_Name":    {"type": "string", "default": "Unknown"},
        "Project_Type":    {"type": "string", "default": "Unknown"},
        "Project_Style":   {"type": "string", "default": "Unknown"},
        "Is_Private":      {"type": "string", "default": "false"},
        "Lead_Account_Id": {"type": "string", "default": "Unknown"},
        "Lead_Name":       {"type": "string", "default": "Unassigned"},
        "Lead_Active":     {"type": "string", "default": "false"},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        id AS Project_Id,
        key AS Project_Code,
        name AS Project_Name,
        projectTypeKey AS Project_Type,
        style AS Project_Style,
        isPrivate AS Is_Private,
        lead_accountId AS Lead_Account_Id,
        lead_displayName AS Lead_Name,
        lead_active AS Lead_Active
    FROM Silver.jira.projects
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Project built successfully")
