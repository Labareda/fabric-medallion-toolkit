# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Issue
# Grain: ONE ROW PER ISSUE. This is the schedule fact -- the Gantt's dates and
# the model's only additive measures live here.
#
# --- PLANNED vs ACTUAL --- (REVISED)
# The previous version said there was no way to get an actual start, because
# Silver.jira.history_items has no timestamp column. That is true of
# history_items ON ITS OWN -- but Silver.jira.histories DOES carry `created`
# (a real timestamp, mapped in B2S), and history_items carries history_id.
# Join the two and every field change has a timestamp. So:
#   Actual_Start = the FIRST transition into an Active/in-progress status,
#                  read from the changelog (see the CTE below)
#   Actual_End   = resolutiondate
# Both also fall back to the Actual start / Actual end CUSTOM FIELDS, which
# exist on this instance (fields_actual_start / fields_actual_end) and were
# not being read at all before. Own field wins; changelog fills the gap.
# Fact_Status_History is now buildable on the same basis.
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
# Dim_Issue carries none of these -- purely the issue's own hierarchy/rank/
# Gantt-display attributes. A report gets Status/Priority/Type by relating
# Dim_Status/Dim_Priority/Dim_IssueType to THIS fact.
#
# --- DIM_ISSUE <-> FACT_ISSUE IS 1:1, AND THAT IS FINE ---
# Power BI forces bidirectional cross-filtering on a one-to-one relationship,
# so a Status slicer (which reaches this fact) still filters Dim_Issue's
# hierarchy columns. No merge of the two tables is needed.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
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
        # Risk scoring. Gross and residual both kept: the DIFFERENCE between
        # them is mitigation effectiveness, which is the number a Programme
        # Board actually acts on. Risk_Previous_Score gives direction of travel.
        "Risk_Total_Score":         {"type": "double"},
        "Risk_Previous_Score":      {"type": "double"},
        "Likelihood_Score":         {"type": "double"},
        "Severity_Score":           {"type": "double"},
        # Xray's own rollup, kept for reconciliation against Fact_Test.
        "Total_Tests":              {"type": "int"},
        "Passed_Tests":             {"type": "int"},

        # Dates -- no defaults, by design (see note above).
        "Planned_Start_Date": {"type": "date"},
        "Planned_End_Date":   {"type": "date"},
        "Actual_Start_Date":  {"type": "date"},
        "Actual_End_Date":    {"type": "date"},
        "Created_Date":       {"type": "date"},
        "Rollup_Start_Date":  {"type": "date"},
        "Rollup_End_Date":    {"type": "date"},

        # Derived schedule health -- computed here rather than in DAX so every
        # tool (Power BI, Tableau, a SQL query) gets the same answer.
        "Duration_Days":      {"type": "int"},
        "Slip_Days":          {"type": "int"},
        "Age_Days":           {"type": "int"},
        "Lead_Time_Days":     {"type": "int"},
        "Cycle_Time_Days":    {"type": "int"},
        "Is_Overdue":         {"type": "boolean", "default": False},
        "Is_Done":            {"type": "boolean", "default": False},

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
        "Resolution_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resolution",
                                     "natural_key_column": "Resolution_Id", "key_column": "Resolution_Key",
                                     "unknown_value": "Unknown"},
        },
        "IssueType_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue_type",
                                     "natural_key_column": "IssueType_Id", "key_column": "IssueType_Key",
                                     "unknown_value": "Unknown"},
        },
        # Resolved on team NAME, matching Dim_Team's merge field. Issues with
        # no team fall to the Unknown member rather than dropping out.
        "Team_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_team",
                                     "natural_key_column": "Team_Name", "key_column": "Team_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Actual start, from the changelog
# histories has the timestamp, history_items has the field-level detail,
# they join on history_id. First move into any status that is NOT a
# start/queue state is the actual start.

# CELL ********************
actual_start_df = spark.sql(f"""
    SELECT hi.issue_id AS Issue_Id,
           CAST(MIN(h.created) AS date) AS Changelog_Actual_Start
    FROM Silver.jira.history_items hi
    JOIN Silver.jira.histories h ON hi.history_id = h.history_id
    JOIN {GOLD_SCHEMA}.dim_status ds ON ds.Status_Name = hi.new_value_formatted
    WHERE hi.field_name = 'status'
      AND ds.Flow_State IN ('Active', 'Wait')
    GROUP BY hi.issue_id
""")
actual_start_df.createOrReplaceTempView("actual_start")

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
        resolution.Resolution_Key,
        issue_type.IssueType_Key,
        team.Team_Key,

        CAST(i.fields_start_date AS date)      AS Planned_Start_Date,
        -- duedate ONLY, matching the Gantt that already works. fields_target_end
        -- is Jira's planning-tier end date and is often set on Programme/
        -- Release/Initiative rows where duedate is not -- COALESCEing it in
        -- would give summary rows their own bars instead of rolled-up ones,
        -- which CHANGES a chart you have already validated. Switch to the
        -- COALESCE below only after comparing the two side by side:
        --   COALESCE(CAST(i.fields_target_end AS date), CAST(i.fields_duedate AS date))
        CAST(i.fields_duedate AS date)         AS Planned_End_Date,
        -- own field first, changelog second
        COALESCE(CAST(i.fields_actual_start AS date), a.Changelog_Actual_Start) AS Actual_Start_Date,
        COALESCE(CAST(i.fields_actual_end AS date),
                 CAST(i.fields_resolutiondate AS date))  AS Actual_End_Date,
        CAST(i.fields_created AS date)         AS Created_Date,

        DATEDIFF(CAST(i.fields_duedate AS date), CAST(i.fields_start_date AS date)) AS Duration_Days,
        DATEDIFF(CAST(i.fields_resolutiondate AS date), CAST(i.fields_duedate AS date)) AS Slip_Days,
        DATEDIFF(COALESCE(CAST(i.fields_resolutiondate AS date), CURRENT_DATE()),
                 CAST(i.fields_created AS date)) AS Age_Days,
        DATEDIFF(CAST(i.fields_resolutiondate AS date),
                 CAST(i.fields_created AS date)) AS Lead_Time_Days,
        DATEDIFF(CAST(i.fields_resolutiondate AS date),
                 COALESCE(CAST(i.fields_actual_start AS date), a.Changelog_Actual_Start)) AS Cycle_Time_Days,
        (i.fields_duedate IS NOT NULL
         AND i.fields_resolutiondate IS NULL
         AND CAST(i.fields_duedate AS date) < CURRENT_DATE()) AS Is_Overdue,
        i.fields_resolutiondate IS NOT NULL AS Is_Done,

        -- Both story point fields exist on this instance and different teams
        -- populate different ones. Taking only story_point_estimate silently
        -- zeroed every team using the other. COALESCE picks whichever is set.
        COALESCE(i.fields_story_point_estimate, i.fields_story_points) AS Story_Points,
        i.fields_timeoriginalestimate / 3600.0  AS Original_Estimate_Hours,
        i.fields_timeestimate / 3600.0          AS Remaining_Estimate_Hours,
        i.fields_timespent / 3600.0             AS Time_Spent_Hours,
        i.fields_risk_total_score               AS Risk_Total_Score,
        i.fields_risk_previous_score            AS Risk_Previous_Score,
        i.fields_likelihood_score_value         AS Likelihood_Score,
        i.fields_severity_score_value           AS Severity_Score,
        i.fields_total_tests                    AS Total_Tests,
        i.fields_passed_tests                   AS Passed_Tests
    FROM Silver.jira.issues i
    LEFT JOIN actual_start a                      ON i.id = a.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue dim_issue   ON i.id = dim_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_project project   ON i.fields_project_id = project.Project_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_status status     ON i.fields_status_id = status.Status_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_priority priority ON i.fields_priority_id = priority.Priority_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resolution resolution ON i.fields_resolution_id = resolution.Resolution_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue_type issue_type ON i.fields_issuetype_id = issue_type.IssueType_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_team team          ON i.fields_team_name = team.Team_Name
""")

# MARKDOWN ********************

# ## Roll dates up the hierarchy so summary rows get bars

# CELL ********************
issue_parents = spark.sql(f"SELECT Issue_Id, Parent_Issue_Id FROM {GOLD_SCHEMA}.dim_issue")
df = fmt.rollup_hierarchy_dates(
    df.join(issue_parents, on="Issue_Id", how="left"),
    id_column="Issue_Id", parent_id_column="Parent_Issue_Id",
    start_column="Planned_Start_Date", end_column="Planned_End_Date",
    # The wheel's bounded upward walk runs up to max_depth passes, and each
    # pass forces two Spark actions (materialize + an early-stop "did
    # anything change" check) -- on data this size that's job-scheduling
    # overhead, not real compute, and it's most of why this cell was slow.
    # The issue hierarchy is only 7 tiers deep (Programme..Sub-task, see
    # Dim_Issue's RANK_TO_LEVEL), so it can never need more than 7 passes
    # to converge -- capping here instead of the wheel's default of 10
    # trims the guaranteed-wasted passes without changing the result.
    max_depth=7,
    # Has_Own_Dates (the wheel's default out_flag) is dropped -- redundant
    # with Planned_Start_Date IS NOT NULL, simplified out of the model.
).drop("Parent_Issue_Id", "Has_Own_Dates")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Issue built successfully")
