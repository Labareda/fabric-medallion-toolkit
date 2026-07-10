# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Renamed from Dim_User -- "Resource" is the natural project-planning
# term (matches the reference Gantt tool's own "resource workload"
# language), even though the underlying population is exactly the same
# Jira user directory as before.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_resource",
    table_type="dim",
    key_column="Resource_Key",
    columns={
        "Resource_Account_Id": {"type": "string", "merge_field": True},
        "Resource_Name":        {"type": "string", "default": "Unknown"},
        "Email":                {"type": "string", "default": "Unknown"},
        "Is_Active":            {"type": "string", "default": "false"},
        "Account_Type":         {"type": "string", "default": "Unknown"},
        "Time_Zone":            {"type": "string", "default": "Unknown"},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        accountId AS Resource_Account_Id,
        displayName AS Resource_Name,
        emailAddress AS Email,
        active AS Is_Active,
        accountType AS Account_Type,
        timeZone AS Time_Zone
    FROM Silver.jira.users
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_Resource built successfully")
