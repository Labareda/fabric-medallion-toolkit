# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Hierarchy_Level is the field driving the parent-child Gantt structure
# via Fact_Issue.parent_issue_key -- kept as an int, not defaulted to a
# string sentinel, since it needs to stay usable for numeric comparisons.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_issue_type",
    table_type="dim",
    key_column="IssueType_Key",
    columns={
        "IssueType_Id":   {"type": "string", "merge_field": True},
        "IssueType_Name": {"type": "string", "default": "Unknown"},
        "Description":    {"type": "string", "default": "Unknown"},
        "Is_Subtask":     {"type": "string", "default": "false"},
        "Hierarchy_Level": {"type": "int", "default": 0},
    },
)

# MARKDOWN ********************

# ## Build the dimension from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        issue_type_id AS IssueType_Id,
        issue_type_name AS IssueType_Name,
        description AS Description,
        is_subtask AS Is_Subtask,
        hierarchy_level AS Hierarchy_Level
    FROM Silver.jira.issuetypes
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Dim_IssueType built successfully")
