# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Issue_Id is the fact's own grain (matches Dim_Issue's merge field
# exactly) -- Issue_Code is a plain readable attribute, and Issue_Key is
# resolved as a proper foreign key to Dim_Issue via lookup_missing_from,
# same pattern as every other dimension link below. This replaces the
# earlier version, which related to Dim_Issue via a plain shared natural
# key rather than a resolved surrogate -- an inconsistency worth fixing
# now that Dim_Issue itself has a clean id/code/key split.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_issue",
    table_type="fact",
    key_column="Issue_Fact_Key",
    columns={
        "Issue_Id":                 {"type": "string", "merge_field": True},
        "Issue_Code":               {"type": "string", "default": "Unknown"},
        "Story_Points":             {"type": "double"},
        "Original_Estimate_Hours":  {"type": "double"},
        "Remaining_Estimate_Hours": {"type": "double"},
        "Time_Spent_Hours":         {"type": "double"},
        "Start_Date":    {"type": "date", "default": "1900-01-01"},
        "Due_Date":      {"type": "date", "default": "1900-01-01"},
        "Created_Date":  {"type": "date", "default": "1900-01-01"},
        "Resolved_Date": {"type": "date", "default": "1900-01-01"},
        "Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Project_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_project",
                                     "natural_key_column": "Project_Id", "key_column": "Project_Key",
                                     "unknown_value": "Unknown"},
        },
        "Assignee_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
        "Status_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_status",
                                     "natural_key_column": "Status_Id", "key_column": "Status_Key",
                                     "unknown_value": "Unknown"},
        },
        "Priority_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_priority",
                                     "natural_key_column": "Priority_Id", "key_column": "Priority_Key",
                                     "unknown_value": "Unknown"},
        },
        "IssueType_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue_type",
                                     "natural_key_column": "IssueType_Id", "key_column": "IssueType_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build the fact from Silver, joining every dimension directly

# CELL ********************
df = spark.sql(f"""
    SELECT
        i.id AS Issue_Id,
        i.key AS Issue_Code,
        dim_issue.Issue_Key AS Issue_Key,
        project.Project_Key AS Project_Key,
        assignee.Resource_Key AS Assignee_Key,
        status.Status_Key AS Status_Key,
        priority.Priority_Key AS Priority_Key,
        issue_type.IssueType_Key AS IssueType_Key,
        CAST(i.fields_start_date AS date) AS Start_Date,
        CAST(i.fields_duedate AS date) AS Due_Date,
        CAST(i.fields_created AS date) AS Created_Date,
        CAST(i.fields_resolutiondate AS date) AS Resolved_Date,
        i.fields_story_point_estimate AS Story_Points,
        i.fields_timeoriginalestimate / 3600.0 AS Original_Estimate_Hours,
        i.fields_timeestimate / 3600.0 AS Remaining_Estimate_Hours,
        i.fields_timespent / 3600.0 AS Time_Spent_Hours
    FROM Silver.jira.issues i
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue
        ON i.id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_project project
        ON i.fields_project_id = project.Project_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource assignee
        ON i.fields_assignee_accountId = assignee.Resource_Account_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_status status
        ON i.fields_status_id = status.Status_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_priority priority
        ON i.fields_priority_id = priority.Priority_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue_type issue_type
        ON i.fields_issuetype_id = issue_type.IssueType_Id
""")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_Issue built successfully")
