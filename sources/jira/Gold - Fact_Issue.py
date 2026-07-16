# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER ISSUE. This is the schedule fact -- the Gantt's dates and
# the model's only additive measures live here.
#
# --- PLANNED vs ACTUAL ---
# Jira has no "actual start" field. Planned dates are what someone typed in
# (start date / due date). Actual END is what the changelog proves: the
# resolutiondate.
#   Actual_End = resolutiondate
# NO Actual_Start: deriving it needs a per-transition timestamp (when did this
# issue first enter an "In Progress" status), and this instance's
# Silver.jira.history_items has no such timestamp column -- only issue_created
# (the issue's creation, identical on every changelog row for that issue). So
# there's nothing to derive a real actual-start FROM. If a per-change
# timestamp is added to the changelog extract later, reintroduce Actual_Start
# (and Fact_StatusHistory) then. Planned-vs-actual reporting therefore compares
# planned dates against actual END only.
#
# --- NO SENTINEL DATES ---
# An unscheduled issue keeps a genuine NULL. The Gantt then renders its ROW but
# draws NO BAR, which is what Jira does. A 1900-01-01 default instead draws a
# phantom bar a century back and stretches the time axis to reach it. Roughly
# 10,500 of ~12,000 issues have no dates of their own, so this matters a lot.
#
# --- ROLLUP ---
# A real Gantt gives a summary row a bar spanning its children. Rollup_Start /
# Rollup_End do that: own dates win; else min(child start)..max(child end);
# else NULL. POINT THE GANTT AT THE ROLLUP COLUMNS, not the raw ones.
#
# --- NO ASSIGNEE KEY ---
# Deliberately absent. The lead is a person, and every person-to-issue link
# lives in Fact_Resource_Allocation with Role = 'Lead'. Putting Assignee_Key
# here as well would give Dim_Resource a SECOND path into the model and force
# a deactivated relationship. One path, one place.
#
# --- STATUS_KEY / PRIORITY_KEY / ISSUETYPE_KEY LIVE HERE, NOT ON DIM_ISSUE ---
# Foreign keys into dimension tables belong on FACT tables in a star schema.
# Dim_Issue carries none of these -- no keys, no denormalised text -- purely
# the issue's own hierarchy/rank/Gantt-display attributes. A report gets
# Status/Priority/Type by relating Dim_Status/Dim_Priority/Dim_IssueType to
# THIS fact, same as every other dimension here.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_issue",
    table_type="fact",
    key_column="Issue_Fact_Key",
    columns={
        "Issue_Id":   {"type": "string", "merge_field": True},
        "Issue_Code": {"type": "string", "default": "Unknown"},

        # Measures -- the only additive columns in the model.
        "Story_Points":             {"type": "double"},
        "Original_Estimate_Hours":  {"type": "double"},
        "Remaining_Estimate_Hours": {"type": "double"},
        "Time_Spent_Hours":         {"type": "double"},

        # Dates -- no defaults, by design (see note above).
        "Planned_Start_Date": {"type": "date"},
        "Planned_End_Date":   {"type": "date"},
        "Actual_End_Date":    {"type": "date"},
        "Created_Date":       {"type": "date"},
        "Rollup_Start_Date":  {"type": "date"},
        "Rollup_End_Date":    {"type": "date"},
        "Has_Own_Dates":      {"type": "boolean", "default": False},

        # Derived schedule health -- computed here rather than in DAX so every
        # tool (Power BI, Tableau, a SQL query) gets the same answer.
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
        "IssueType_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue_type",
                                     "natural_key_column": "IssueType_Id", "key_column": "IssueType_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Actual start, from the status history

# MARKDOWN ********************

# ## Build the fact

# CELL ********************
df = spark.sql(f"""
    SELECT
        i.id AS Issue_Id,
        i.key AS Issue_Code,
        dim_issue.Issue_Key,
        project.Project_Key,
        status.Status_Key,
        priority.Priority_Key,
        issue_type.IssueType_Key,

        CAST(i.fields_start_date AS date)      AS Planned_Start_Date,
        CAST(i.fields_duedate AS date)         AS Planned_End_Date,
        CAST(i.fields_resolutiondate AS date)  AS Actual_End_Date,
        CAST(i.fields_created AS date)         AS Created_Date,

        DATEDIFF(CAST(i.fields_duedate AS date), CAST(i.fields_start_date AS date)) AS Duration_Days,
        DATEDIFF(CAST(i.fields_resolutiondate AS date), CAST(i.fields_duedate AS date)) AS Slip_Days,
        (i.fields_duedate IS NOT NULL
         AND i.fields_resolutiondate IS NULL
         AND CAST(i.fields_duedate AS date) < CURRENT_DATE()) AS Is_Overdue,

        i.fields_story_point_estimate           AS Story_Points,
        i.fields_timeoriginalestimate / 3600.0  AS Original_Estimate_Hours,
        i.fields_timeestimate / 3600.0          AS Remaining_Estimate_Hours,
        i.fields_timespent / 3600.0             AS Time_Spent_Hours
    FROM Silver.jira.issues i
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue   ON i.id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_project project   ON i.fields_project_id = project.Project_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_status status     ON i.fields_status_id = status.Status_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_priority priority ON i.fields_priority_id = priority.Priority_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue_type issue_type ON i.fields_issuetype_id = issue_type.IssueType_Id
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
