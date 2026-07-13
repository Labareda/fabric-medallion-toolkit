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
    table_name=f"{GOLD_SCHEMA}.fact_worklog",
    table_type="fact",
    key_column="Worklog_Key",
    columns={
        "Worklog_Id":  {"type": "string", "merge_field": True},
        "Issue_Code":  {"type": "string", "default": "Unknown"},
        "Hours_Spent": {"type": "double", "default": 0.0},
        "Logged_Date": {"type": "date", "default": "1900-01-01"},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Resource_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build the fact from Silver, joining every dimension directly

# CELL ********************
# Logged_Date relates directly to Dim_Date.date -- no join needed for
# dates (see Fact_Issue for the full reasoning), just a plain CAST.
df = spark.sql(f"""
    SELECT
        w.worklog_id AS Worklog_Id,
        w.issue_key AS Issue_Code,
        dim_issue.Issue_Key AS Issue_Key,
        w.time_spent_seconds / 3600.0 AS Hours_Spent,
        CAST(w.started AS date) AS Logged_Date,
        resource.Resource_Key AS Resource_Key
    FROM Silver.jira.worklogs w
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON w.issue_id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource
        ON w.author_account_id = resource.Resource_Account_Id
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_Worklog built successfully")
