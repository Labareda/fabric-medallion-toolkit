# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Worklog
# Grain: ONE ROW PER WORKLOG ENTRY.
#
# CHECK COVERAGE BEFORE PROMISING ANYTHING FROM THIS. Worklogs only exist
# where teams actually log time, and on a programme this size that is usually
# some workstreams and not others. Run the coverage query in the validation
# notebook first -- a utilisation dashboard built on 20% coverage is worse
# than no dashboard, because it looks complete.
#
# Note also: this gives hours SPENT. Utilisation is spent / available, and
# Jira holds no availability at all. Fact_Capacity (external feed) is
# required before any utilisation % can be computed.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_worklog",
    table_type="fact",
    key_column="Worklog_Fact_Key",
    columns={
        "Worklog_Id":       {"type": "string", "merge_field": True},
        "Issue_Code":       {"type": "string", "default": "Unknown"},
        "Started_At":       {"type": "timestamp"},
        "Started_Date":     {"type": "date"},
        "Time_Spent_Hours": {"type": "double", "default": 0.0},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Author_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    SELECT
        w.worklog_id AS Worklog_Id,
        w.issue_key  AS Issue_Code,
        w.started    AS Started_At,
        CAST(w.started AS date) AS Started_Date,
        w.time_spent_seconds / 3600.0 AS Time_Spent_Hours,
        dim_issue.Issue_Key,
        res.Resource_Key AS Author_Key
    FROM Silver.jira.worklogs w
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue ON w.issue_id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res    ON w.author_account_id = res.Resource_Account_Id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Worklog built successfully")
