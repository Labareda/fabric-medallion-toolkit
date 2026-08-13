# Fabric notebook source

# MARKDOWN ********************

# ## Validation checks
# Run this ONCE before trusting any dashboard, and again whenever the Jira
# configuration changes. Each check answers a question that, left unasked,
# produces a report that looks right and is wrong.

# CELL ********************
GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## 1. Does a Test belong to more than one Test Set?
# No rows -> flatten Test_Set_Code onto Dim_Issue for a true hierarchy.
# Rows    -> keep Bridge_Test_Set_Test; set-level totals will not sum.

# CELL ********************
display(spark.sql("""
    SELECT test_issue_key, COUNT(DISTINCT test_set_issue_key) AS set_count
    FROM Silver.xray.test_sets
    GROUP BY test_issue_key
    HAVING COUNT(DISTINCT test_set_issue_key) > 1
"""))

# MARKDOWN ********************

# ## 2. What issue types exist at each tier, per project?
# Confirms whether Workstream really is level 1 or 2 depending on project,
# and whether Release exists as a type at all.

# CELL ********************
display(spark.sql("""
    SELECT i.fields_project_key, it.name AS issue_type, it.hierarchylevel,
           COUNT(*) AS issues
    FROM Silver.jira.issues i
    LEFT JOIN Silver.jira.issuetypes it ON i.fields_issuetype_id = it.id
    GROUP BY i.fields_project_key, it.name, it.hierarchylevel
    ORDER BY i.fields_project_key, it.hierarchylevel DESC
"""))

# MARKDOWN ********************

# ## 3. Orphans -- issues that never reach a Programme
# These drop out of every rolled-up total silently. Some are legitimate
# (top-tier items). Anything at Epic or below with no Programme is a broken
# parent chain worth chasing.

# CELL ********************
display(spark.sql(f"""
    SELECT Hierarchy_Level_Name, COUNT(*) AS issues
    FROM {GOLD_SCHEMA}.dim_issue
    WHERE Programme_Label IS NULL
    GROUP BY Hierarchy_Level_Name
    ORDER BY issues DESC
"""))

# MARKDOWN ********************

# ## 4. Workstream coverage
# Dashboard 2 groups on Workstream_Label. Anything not resolving to one is
# invisible in that dashboard.

# CELL ********************
display(spark.sql(f"""
    SELECT COALESCE(Workstream_Label, '(none)') AS workstream, COUNT(*) AS issues
    FROM {GOLD_SCHEMA}.dim_issue
    GROUP BY Workstream_Label
    ORDER BY issues DESC
"""))

# MARKDOWN ********************

# ## 5. Worklog coverage by workstream
# Decides whether the resource/utilisation dashboards are viable at all.
# A workstream with issues but no worklogs cannot appear in them.

# CELL ********************
display(spark.sql(f"""
    SELECT di.Workstream_Label,
           COUNT(DISTINCT di.Issue_Key) AS issues,
           COUNT(DISTINCT fw.Worklog_Id) AS worklogs
    FROM {GOLD_SCHEMA}.dim_issue di
    LEFT JOIN {GOLD_SCHEMA}.fact_worklog fw ON fw.Issue_Key = di.Issue_Key
    GROUP BY di.Workstream_Label
    ORDER BY worklogs DESC
"""))

# MARKDOWN ********************

# ## 6. Hierarchy depth vs the 7 dense level columns
# If max depth exceeds 7, Level_8+ is being silently truncated and the Gantt
# will stop nesting at the wrong place.

# CELL ********************
display(spark.sql(f"SELECT MAX(Depth) AS max_depth FROM {GOLD_SCHEMA}.dim_issue"))

# MARKDOWN ********************

# ## 7. Date coverage -- how much of the Gantt will actually draw a bar?
# Has_Own_Dates false AND Rollup_Start_Date null means the row renders with
# no bar. Expect a lot; confirm it is the expected lot.

# CELL ********************
display(spark.sql(f"""
    SELECT di.Hierarchy_Level_Name,
           COUNT(*) AS issues,
           SUM(CASE WHEN fi.Has_Own_Dates THEN 1 ELSE 0 END) AS own_dates,
           SUM(CASE WHEN fi.Rollup_Start_Date IS NOT NULL THEN 1 ELSE 0 END) AS drawable
    FROM {GOLD_SCHEMA}.fact_issue fi
    JOIN {GOLD_SCHEMA}.dim_issue di ON fi.Issue_Key = di.Issue_Key
    GROUP BY di.Hierarchy_Level_Name
    ORDER BY issues DESC
"""))

# MARKDOWN ********************

# ## 8. Actual start coverage
# Confirms the changelog derivation is working. If Changelog-derived starts
# are zero, check that Silver.jira.histories.created is populated.

# CELL ********************
display(spark.sql(f"""
    SELECT
        COUNT(*) AS issues,
        SUM(CASE WHEN Actual_Start_Date IS NOT NULL THEN 1 ELSE 0 END) AS with_actual_start,
        SUM(CASE WHEN Actual_End_Date IS NOT NULL THEN 1 ELSE 0 END)   AS with_actual_end
    FROM {GOLD_SCHEMA}.fact_issue
"""))

# MARKDOWN ********************

# ## 9. Story point field split
# Shows whether teams are split across the two story point fields. If both
# columns are non-zero, the COALESCE in Fact_Issue is doing real work.

# CELL ********************
display(spark.sql("""
    SELECT
        SUM(CASE WHEN fields_story_point_estimate IS NOT NULL THEN 1 ELSE 0 END) AS uses_estimate,
        SUM(CASE WHEN fields_story_points IS NOT NULL THEN 1 ELSE 0 END)         AS uses_story_points,
        SUM(CASE WHEN fields_story_point_estimate IS NOT NULL
                  AND fields_story_points IS NOT NULL THEN 1 ELSE 0 END)         AS uses_both
    FROM Silver.jira.issues
"""))
