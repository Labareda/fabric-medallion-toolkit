# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Dim_Sprint

# CELL ********************
# Sprint is a CUSTOM field in this Jira instance (customfield_10020), not the
# built-in "fields.sprint" -- Silver.jira.issue_sprints already handles that.
# Sprints are deduped here: the same sprint appears on every issue that touched
# it, so DISTINCT collapses them to one row each.
sprint_schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_sprint",
    table_type="dim",
    key_column="Sprint_Key",
    columns={
        "Sprint_Id":     {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Sprint_Name":   {"type": "string", "default": "Unknown"},
        "Sprint_State":  {"type": "string", "default": "Unknown"},
        "Sprint_Goal":   {"type": "string", "default": ""},
        "Board_Id":      {"type": "string", "default": "Unknown"},
        "Start_Date":    {"type": "date"},
        "End_Date":      {"type": "date"},
        "Complete_Date": {"type": "date"},
    },
)

df_sprint = spark.sql("""
    SELECT DISTINCT
        CAST(sprint_id AS string) AS Sprint_Id,
        sprint_name  AS Sprint_Name,
        state        AS Sprint_State,
        goal         AS Sprint_Goal,
        CAST(board_id AS string)  AS Board_Id,
        CAST(start_date AS date)    AS Start_Date,
        CAST(end_date AS date)      AS End_Date,
        CAST(complete_date AS date) AS Complete_Date
    FROM Silver.jira.issue_sprints
    WHERE sprint_id IS NOT NULL
""")

fmt.merge(spark, df_sprint, sprint_schema)
print("Dim_Sprint built successfully")

# MARKDOWN ********************

# ## Bridge_IssueSprint

# CELL ********************
# An issue moves through several sprints over its life (carried over, re-scoped,
# split). That's a genuine many-to-many, so it needs a bridge -- putting a
# single Sprint_Key on Fact_Issue would silently keep only one of them.
bridge_schema = fmt.TableSchema(
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

df_bridge = spark.sql(f"""
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

fmt.merge(spark, df_bridge, bridge_schema)
print("Bridge_IssueSprint built successfully")
