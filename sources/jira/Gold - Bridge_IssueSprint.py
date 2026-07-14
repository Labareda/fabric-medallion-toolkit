# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER ISSUE x SPRINT.
#
# An issue moves through SEVERAL sprints over its life -- carried over when it
# doesn't finish, re-scoped, split. That's a genuine many-to-many. Putting a
# single Sprint_Key on Fact_Issue instead would silently keep only ONE of them
# (whichever the flattener happened to land last), and every "how many issues
# spilled out of Sprint 4" question would quietly return the wrong answer.
#
# table_type="fact" so no Unknown member row is added -- nobody browses a
# bridge directly; the readable attributes come from relating through
# Dim_Issue and Dim_Sprint.
#
# lookup_missing_from is declared on the merge fields even though they're merge
# fields: it runs BEFORE merge()'s null-check validation, so an unmatched join
# falls back to the Unknown row rather than hard-erroring on a null merge field.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.bridge_issue_sprint",
    table_type="fact",
    key_column="Issue_Sprint_Key",
    columns={
        "Issue_Key": {
            "type": "string", "merge_field": True,
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Sprint_Id": {"type": "string", "merge_field": True},
        "Sprint_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_sprint",
                                     "natural_key_column": "Sprint_Id", "key_column": "Sprint_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build the bridge

# CELL ********************
# DISTINCT matters: Silver.jira.issue_sprints can carry the same issue/sprint
# pair more than once if an issue was moved out of a sprint and back into it.
# Without it, merge() would hard-error on the duplicate merge-field combination.
df = spark.sql(f"""
    SELECT DISTINCT
        dim_issue.Issue_Key,
        CAST(s.sprint_id AS string) AS Sprint_Id,
        dim_sprint.Sprint_Key
    FROM Silver.jira.issue_sprints s
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON s.issue_id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_sprint dim_sprint
        ON CAST(s.sprint_id AS string) = dim_sprint.Sprint_Id
    WHERE s.sprint_id IS NOT NULL
""")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Bridge_IssueSprint built successfully")
