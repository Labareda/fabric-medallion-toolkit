# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Issue_Key (the resolved surrogate) is used directly as the merge field
# -- no separate Issue_Code needed here, since nobody browses a bridge
# table directly; a report author gets the human-readable code by
# relating through Dim_Issue itself. lookup_missing_from is STILL
# declared here even though Issue_Key is a merge field -- it runs before
# merge()'s null-check validation, so if the join below ever fails to
# find a match, this falls back gracefully to Dim_Issue's Unknown row's
# key instead of hard-erroring on a null merge field.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_people_involved",
    table_type="fact",
    key_column="Issue_Resource_Key",
    columns={
        "Issue_Key": {
            "type": "string",
            "merge_field": True,
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Resource_Account_Id": {"type": "string", "merge_field": True},
    },
)

# MARKDOWN ********************

# ## Build the bridge from Silver, joining Dim_Issue directly

# CELL ********************
df = spark.sql(f"""
    SELECT
        dim_issue.Issue_Key AS Issue_Key,
        p.person_account_id AS Resource_Account_Id
    FROM Silver.jira.issue_people_involved p
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON p.issue_id = dim_issue.Issue_Id
    WHERE p.person_account_id IS NOT NULL
""").distinct()

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Bridge_IssuePeopleInvolved built successfully")
