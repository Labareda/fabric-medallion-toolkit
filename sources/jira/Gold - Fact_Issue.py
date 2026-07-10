# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Issue_Key is both the merge field AND how this fact relates to Dim_Issue
# (which carries the hierarchy/parent-child structure) -- everything else
# here is either a measure or a foreign key to an already-built dimension.
# Time tracking fields are converted from Jira's native seconds to hours.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_issue",
    table_type="fact",
    key_column="Issue_Fact_Key",
    columns={
        "Issue_Key":               {"type": "string", "merge_field": True},
        "Story_Points":            {"type": "double"},
        "Original_Estimate_Hours": {"type": "double"},
        "Remaining_Estimate_Hours": {"type": "double"},
        "Time_Spent_Hours":        {"type": "double"},
        "Rank":                    {"type": "string", "default": ""},
    },
)

# MARKDOWN ********************

# ## Build the fact from Silver

# CELL ********************
df = spark.sql("""
    SELECT
        key AS Issue_Key,
        fields_project_id AS project_id,
        fields_assignee_accountId AS assignee_account_id,
        fields_status_id AS status_id,
        fields_priority_id AS priority_id,
        fields_issuetype_id AS issue_type_id,
        CAST(fields_start_date AS date) AS start_date,
        CAST(fields_duedate AS date) AS due_date,
        CAST(fields_created AS date) AS created_date,
        CAST(fields_resolutiondate AS date) AS resolved_date,
        fields_story_point_estimate AS Story_Points,
        fields_timeoriginalestimate / 3600.0 AS Original_Estimate_Hours,
        fields_timeestimate / 3600.0 AS Remaining_Estimate_Hours,
        fields_timespent / 3600.0 AS Time_Spent_Hours,
        fields_rank AS Rank
    FROM Silver.jira.issues
""")

# MARKDOWN ********************

# ## Resolve every dimension foreign key

# CELL ********************
df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_project",
                     dim_natural_key_column="Project_Id", dim_key_column="Project_Key",
                     fact_join_column="project_id", output_column="Project_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_resource",
                     dim_natural_key_column="Resource_Account_Id", dim_key_column="Resource_Key",
                     fact_join_column="assignee_account_id", output_column="Assignee_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_status",
                     dim_natural_key_column="Status_Id", dim_key_column="Status_Key",
                     fact_join_column="status_id", output_column="Status_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_priority",
                     dim_natural_key_column="Priority_Id", dim_key_column="Priority_Key",
                     fact_join_column="priority_id", output_column="Priority_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_issue_type",
                     dim_natural_key_column="IssueType_Id", dim_key_column="IssueType_Key",
                     fact_join_column="issue_type_id", output_column="IssueType_Key", default_to_unknown=True)

# MARKDOWN ********************

# ## Resolve every date foreign key against Dim_Date

# CELL ********************
df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_date",
                     dim_natural_key_column="date", dim_key_column="date_key",
                     fact_join_column="start_date", output_column="Start_Date_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_date",
                     dim_natural_key_column="date", dim_key_column="date_key",
                     fact_join_column="due_date", output_column="Due_Date_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_date",
                     dim_natural_key_column="date", dim_key_column="date_key",
                     fact_join_column="created_date", output_column="Created_Date_Key", default_to_unknown=True)

df = fmt.lookup_key(spark, df, dim_table_name=f"{GOLD_SCHEMA}.dim_date",
                     dim_natural_key_column="date", dim_key_column="date_key",
                     fact_join_column="resolved_date", output_column="Resolved_Date_Key", default_to_unknown=True)

# MARKDOWN ********************

# ## Merge into Gold (wheel handles type coercion, defaults, key generation + MERGE)

# CELL ********************
fmt.merge(spark, df, schema)

# MARKDOWN ********************

# ## Task complete

# CELL ********************
print("Fact_Issue built successfully")
