# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Project/Assignee/Status/Priority/IssueType are resolved by a plain JOIN
# below, with lookup_missing_from filling in the Unknown row's key
# wherever that join finds nothing. Dates are different: Dim_Date covers
# every day exhaustively, so a date almost never fails to "match" the way
# a user/project genuinely can -- the only real gap is the SOURCE value
# itself being null, which a plain COALESCE handles directly, no join
# needed at all. The date value itself (not a separate integer key) is
# what relates to Dim_Date.date in Power BI.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_issue",
    table_type="fact",
    key_column="Issue_Fact_Key",
    columns={
        "Issue_Key":                {"type": "string", "merge_field": True},
        "Story_Points":             {"type": "double"},
        "Original_Estimate_Hours":  {"type": "double"},
        "Remaining_Estimate_Hours": {"type": "double"},
        "Time_Spent_Hours":         {"type": "double"},
        "Rank":                     {"type": "string", "default": ""},
        "Start_Date":    {"type": "date"},
        "Due_Date":      {"type": "date"},
        "Created_Date":  {"type": "date"},
        "Resolved_Date": {"type": "date"},
        "Project_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_project",
                                     "natural_key_column": "Project_Id", "key_column": "Project_Key"},
        },
        "Assignee_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key"},
        },
        "Status_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_status",
                                     "natural_key_column": "Status_Id", "key_column": "Status_Key"},
        },
        "Priority_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_priority",
                                     "natural_key_column": "Priority_Id", "key_column": "Priority_Key"},
        },
        "IssueType_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue_type",
                                     "natural_key_column": "IssueType_Id", "key_column": "IssueType_Key"},
        },
    },
)

# MARKDOWN ********************

# ## Build the fact from Silver, joining every dimension directly

# CELL ********************
df = spark.sql(f"""
    SELECT
        i.key AS Issue_Key,
        project.Project_Key AS Project_Key,
        assignee.Resource_Key AS Assignee_Key,
        status.Status_Key AS Status_Key,
        priority.Priority_Key AS Priority_Key,
        issue_type.IssueType_Key AS IssueType_Key,
        COALESCE(CAST(i.fields_start_date AS date), CAST('1900-01-01' AS date)) AS Start_Date,
        COALESCE(CAST(i.fields_duedate AS date), CAST('1900-01-01' AS date)) AS Due_Date,
        COALESCE(CAST(i.fields_created AS date), CAST('1900-01-01' AS date)) AS Created_Date,
        COALESCE(CAST(i.fields_resolutiondate AS date), CAST('1900-01-01' AS date)) AS Resolved_Date,
        i.fields_story_point_estimate AS Story_Points,
        i.fields_timeoriginalestimate / 3600.0 AS Original_Estimate_Hours,
        i.fields_timeestimate / 3600.0 AS Remaining_Estimate_Hours,
        i.fields_timespent / 3600.0 AS Time_Spent_Hours,
        i.fields_rank AS Rank
    FROM Silver.jira.issues i
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
