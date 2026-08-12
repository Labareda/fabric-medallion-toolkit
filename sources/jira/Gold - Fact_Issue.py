# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER ISSUE. The schedule fact -- every date, every measure.
#
# --- PLANNED vs ACTUAL ---
# Confirmed against real data: fields_actual_start / fields_actual_end are
# EMPTY. Nobody's filling those columns in. The client uses:
#   Planned_Start_Date = fields_start_date
#   Planned_End_Date   = fields_target_end
#   Actual_End_Date    = fields_resolutiondate
# and there is NO Actual_Start_Date column, deliberately -- I'm not going to
# derive one from the changelog just to have the column exist. Reporting the
# gap honestly beats fabricating a value that looks precise but isn't.
#
# --- NO SENTINEL DATES ---
# An unscheduled issue keeps a genuine NULL. The Gantt renders its ROW but
# draws NO BAR, matching Jira's own behavior. A 1900-01-01 default instead
# draws a phantom bar a century back and stretches the time axis.
#
# --- ROLLUP ---
# A real Gantt gives a summary row a bar spanning its children. Rollup_Start
# / Rollup_End do that: own dates win; else min(child start)..max(child end);
# else NULL. POINT THE GANTT AT ROLLUP COLUMNS, not raw ones.
#
# --- NO ASSIGNEE_KEY ---
# The lead lives in Fact_ResourceAllocation with Role='Lead'. Adding
# Assignee_Key here would give Dim_Resource a SECOND path to this fact and
# force a deactivated relationship. One path, one place.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_issue",
    table_type="fact",
    key_column="Issue_Fact_Key",
    columns={
        "Issue_Id":   {"type": "string", "merge_field": True},
        "Issue_Code": {"type": "string", "default": "Unknown"},

        # Measures
        "Story_Points":             {"type": "double"},
        "Original_Estimate_Hours":  {"type": "double"},
        "Remaining_Estimate_Hours": {"type": "double"},
        "Time_Spent_Hours":         {"type": "double"},
        "Issue_Count":              {"type": "int", "default": 1},

        # Dates -- no defaults, by design.
        "Planned_Start_Date": {"type": "date"},
        "Planned_End_Date":   {"type": "date"},
        "Actual_End_Date":    {"type": "date"},
        "Created_Date":       {"type": "date"},
        "Updated_Date":       {"type": "date"},
        "Rollup_Start_Date":  {"type": "date"},
        "Rollup_End_Date":    {"type": "date"},
        "Has_Own_Dates":      {"type": "boolean", "default": False},

        # Derived schedule health -- computed HERE, not in DAX, so every tool
        # (Power BI, Tableau, a SQL query) gets the same answer.
        "Duration_Days":      {"type": "int"},
        "Slip_Days":          {"type": "int"},
        "Is_Overdue":         {"type": "boolean", "default": False},

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
    },
)

# MARKDOWN ********************

# ## Build

# CELL ********************
# Duration/Slip computed here so it stays consistent across every tool:
#   Duration_Days  = target_end - start_date         (planned duration)
#   Slip_Days      = resolutiondate - target_end     (positive = late)
#   Is_Overdue     = past due date, not resolved     (as of today)
df = spark.sql(f"""
    SELECT
        i.id AS Issue_Id,
        i.key AS Issue_Code,
        dim_issue.Issue_Key,
        project.Project_Key,
        status.Status_Key,
        priority.Priority_Key,

        CAST(i.fields_start_date AS date)      AS Planned_Start_Date,
        CAST(i.fields_target_end AS date)      AS Planned_End_Date,
        CAST(i.fields_resolutiondate AS date)  AS Actual_End_Date,
        CAST(i.fields_created AS date)         AS Created_Date,
        CAST(i.fields_updated AS date)         AS Updated_Date,

        DATEDIFF(CAST(i.fields_target_end AS date), CAST(i.fields_start_date AS date)) AS Duration_Days,
        DATEDIFF(CAST(i.fields_resolutiondate AS date), CAST(i.fields_target_end AS date)) AS Slip_Days,
        (i.fields_target_end IS NOT NULL
         AND i.fields_resolutiondate IS NULL
         AND CAST(i.fields_target_end AS date) < CURRENT_DATE()) AS Is_Overdue,

        i.fields_story_point_estimate           AS Story_Points,
        i.fields_timeoriginalestimate / 3600.0  AS Original_Estimate_Hours,
        i.fields_timeestimate / 3600.0          AS Remaining_Estimate_Hours,
        i.fields_timespent / 3600.0             AS Time_Spent_Hours,
        1 AS Issue_Count
    FROM Silver.jira.issues i
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue   ON i.id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_project project   ON i.fields_project_id = project.Project_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_status status     ON i.fields_status_id = status.Status_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_priority priority ON i.fields_priority_id = priority.Priority_Id
""")

# MARKDOWN ********************

# ## Roll dates up the hierarchy so summary rows get bars

# CELL ********************
issue_parents = spark.sql(f"SELECT Issue_Id, Parent_Issue_Id FROM {GOLD_SCHEMA}.dim_issue")
df = fmt.rollup_hierarchy_dates(
    df.join(issue_parents, on="Issue_Id", how="left"),
    id_column="Issue_Id", parent_id_column="Parent_Issue_Id",
    start_column="Planned_Start_Date", end_column="Planned_End_Date",
).drop("Parent_Issue_Id")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Fact_Issue built successfully")
