# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test -- the ONE test fact
# Grain: ONE ROW PER XRAY TEST RUN.
#
# CONSOLIDATES what used to be Fact_Test_Run AND Fact_Test_Coverage into a
# single table -- the client's own conclusion: they don't want several test
# facts, they want ONE with everything on it and a flag to pick the current
# state. So:
#   * Is_Latest -- TRUE for each test's most recent run (by Started_On),
#     FALSE for its history. Filter Is_Latest = TRUE in any visual (matrix,
#     card, measure) for "current status per test"; drop the filter for the
#     full run-by-run history. No separate "latest status" measure needed.
#   * Coverage (which requirement a test covers) is NOT re-modelled here --
#     it's an issue-to-issue link, so it lives in Bridge_Issue_Link like
#     every other relationship. "Requirements with no test" is a measure on
#     Dim_Issue (a Requirement with no incoming test link), not a fact table.
#
# Test_Issue_Key is the single relationship to Dim_Issue -- select a TEST and
# see its runs directly. To see the runs of tests LINKED to some other issue
# (a Test Set, a Requirement), that goes through Bridge_Issue_Link, which
# carries each linked test's latest status denormalised (Linked_Test_Status)
# -- see Gold - Bridge_Issue_Link.py. Power BI allows only one active
# relationship to Dim_Issue, so the linked-issue view can't come through a
# relationship here; that's exactly why the bridge carries the status column.
#
# Project_Key / Team_Key are the TEST issue's own project/team (a test is a
# Jira issue with fields_project_id / fields_team_name like any other), so a
# project or team slicer filters tests in one hop, matching every other fact.
#
# Test_Status_Name is NOT stored -- Dim_Test_Status gives the correctly
# formatted (sentence case) display text via Test_Status_Key; a second
# raw-uppercase copy here would reintroduce the ALL-CAPS-in-report problem
# already fixed.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test",
    table_type="fact",
    key_column="Test_Fact_Key",
    columns={
        "Run_Id": {"type": "string", "merge_field": True},

        # Measures / flags
        "Is_Passed":  {"type": "int", "default": 0},
        "Is_Failed":  {"type": "int", "default": 0},
        "Is_Final":   {"type": "int", "default": 0},
        "Run_Count":  {"type": "int", "default": 1},
        # TRUE for each test's most recent run -- the client filters on this
        # for "current status", drops it for full history.
        "Is_Latest":  {"type": "boolean", "default": False},

        "Test_Type":      {"type": "string", "default": "Unknown"},
        "Started_On":     {"type": "timestamp"},
        "Finished_On":    {"type": "timestamp"},
        "Executed_Date":  {"type": "date"},

        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Execution_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Test_Status_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_status",
                "natural_key_column": "Test_Status_Name",
                "key_column": "Test_Status_Key",
                "unknown_value": "Unknown",
            },
        },
        "Executed_By_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_resource",
                "natural_key_column": "Resource_Id",
                "key_column": "Resource_Key",
                "unknown_value": "Unknown",
            },
        },
        # The test issue's own project/team.
        "Project_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_project",
                "natural_key_column": "Project_Id",
                "key_column": "Project_Key",
                "unknown_value": "Unknown",
            },
        },
        "Team_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_team",
                "natural_key_column": "Team_Name",
                "key_column": "Team_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    SELECT
        r.run_id AS Run_Id,

        CASE WHEN r.status_name = 'PASSED' THEN 1 ELSE 0 END AS Is_Passed,
        CASE WHEN r.status_name = 'FAILED' THEN 1 ELSE 0 END AS Is_Failed,
        CASE WHEN r.status_name IN ('PASSED', 'FAILED') THEN 1 ELSE 0 END AS Is_Final,
        1 AS Run_Count,
        -- Is_Latest: most recent run per test by Started_On. nulls_last so a
        -- run with no start date never outranks a real dated one.
        ROW_NUMBER() OVER (
            PARTITION BY r.test_issue_id
            ORDER BY r.started_on DESC NULLS LAST
        ) = 1 AS Is_Latest,

        r.test_type AS Test_Type,
        r.started_on AS Started_On,
        r.finished_on AS Finished_On,
        CAST(r.finished_on AS date) AS Executed_Date,

        test_issue.Issue_Key AS Test_Issue_Key,
        exec_issue.Issue_Key AS Execution_Issue_Key,
        status.Test_Status_Key,
        resource.Resource_Key AS Executed_By_Key,
        proj.Project_Key,
        team.Team_Key

    FROM Silver.xray.test_runs r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue test_issue ON test_issue.Issue_Id = r.test_issue_id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue exec_issue  ON exec_issue.Issue_Id = r.execution_issue_id
    -- LOWER() on both sides: Dim_Test_Status.Test_Status_Name is sentence
    -- case ("Passed"), r.status_name is Xray's raw uppercase ("PASSED") --
    -- case-insensitive so a future casing change can't silently break it.
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status status ON LOWER(status.Test_Status_Name) = LOWER(r.status_name)
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource  ON resource.Resource_Id = r.executed_by_id
    -- Test issue's own project/team, via Silver.jira.issues (a test is a
    -- Jira issue like any other).
    LEFT JOIN Silver.jira.issues ti              ON ti.id = r.test_issue_id
    LEFT JOIN {GOLD_SCHEMA}.dim_project proj      ON proj.Project_Id = ti.fields_project_id
    LEFT JOIN {GOLD_SCHEMA}.dim_team team         ON team.Team_Name = ti.fields_team_name
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test built successfully")
