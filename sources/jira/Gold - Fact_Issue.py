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
        # NO 1900-01-01 default on Start/Due: an unscheduled issue must
        # keep a genuine NULL so the Gantt renders its ROW but draws NO
        # BAR -- which is what Jira does. A sentinel instead draws a
        # phantom bar in 1900 and stretches the time axis a century.
        # ~10,500 of ~12,000 issues have no dates, so this matters a lot.
        "Start_Date":    {"type": "date"},
        "Due_Date":      {"type": "date"},
        "Created_Date":  {"type": "date"},
        "Resolved_Date": {"type": "date"},
        "Rollup_Start_Date": {"type": "date"},
        "Rollup_End_Date":   {"type": "date"},
        "Has_Own_Dates":     {"type": "boolean", "default": False},
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

# ## Roll dates up the hierarchy so summary rows get bars

# CELL ********************
# Most issues have no dates of their own (~10,500 of ~12,000): parents are
# usually undated, and so are most Stories/Bugs. A Gantt that only draws
# an issue's OWN dates therefore leaves nearly everything blank.
#
# A real Gantt gives a summary row a bar spanning its children. That's
# what this does:
#   own dates            -> use them (Has_Own_Dates = true)
#   none, but dated kids -> min(child start) .. max(child due)
#   neither              -> NULL: the row still shows, with no bar
#
# 433 parents have dated children, so this genuinely fills the upper tiers
# rather than being a no-op.
#
# Point the Gantt at Rollup_Start_Date / Rollup_End_Date. Start_Date and
# Due_Date remain available for anything that needs the raw values.
issue_parents = spark.sql(f"""
    SELECT Issue_Id, Parent_Issue_Id
    FROM {GOLD_SCHEMA}.dim_issue
""")
df_for_rollup = df.join(issue_parents, on="Issue_Id", how="left")

df = fmt.rollup_hierarchy_dates(
    df_for_rollup,
    id_column="Issue_Id",
    parent_id_column="Parent_Issue_Id",
    start_column="Start_Date",
    end_column="Due_Date",
).drop("Parent_Issue_Id")

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_Issue built successfully")
