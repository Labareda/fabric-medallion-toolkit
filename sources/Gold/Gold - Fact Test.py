# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test -- the testing model spine
# Grain: ONE ROW PER TEST SET MEMBERSHIP.
#
# This is the key design decision for user-friendliness.
#
# Why not "one row per test"?
# 142 tests belong to 2 or 3 sets. With a single row per test, displaying
# the hierarchy "Test Set -> Tests" requires a bridge table in Power BI,
# bidirectional relationships, and complex DAX. Totals double-count unless
# carefully managed. The client needs to build and maintain this.
#
# Why "one row per test set membership" instead?
# A test in 2 sets = 2 rows, one for each set it belongs to. This means:
#   * The Power BI matrix is a PLAIN parent-child expand on two FK columns
#     (Test_Set_Key and Test_Key) -- no bridge, no bidirectional filter
#   * "How is this set doing" sums straightforwardly within the set
#   * "How is the programme doing" uses DISTINCTCOUNT(Test_Key)
#   * Shared tests are visible (same test appears in two rows) not hidden
#   * No bridge table in the semantic model at all
#
# The grain also directly mirrors what the client saw in the screenshot:
#   Parent Issue
#     Test Set A
#       Test 1   PASSED
#       Test 2   FAILED
#     Test Set B (same Test 1 appears here too -- correct, it IS in both)
#       Test 1   PASSED
#       Test 3   NOT RUN
#
# Latest run status is denormalised onto each row. There is no separate
# run-history fact -- the test report only needs current status, not a
# run-by-run trend, so Run_Count/Pass_Count/Fail_Count (aggregated across
# every run ever, not just the latest) are as close to history as this
# model goes.
#
# PARENT ISSUE CONTEXT
# Each test set is linked to one or more parent issues (Stories/Requirements)
# via Jira's "Test" link. We carry the FIRST linked parent issue directly
# onto this fact so the client sees:
#   Parent Issue -> Test Set -> Test
# without needing a bridge table in their report. Where a test set has
# multiple parent issues, all are surfaced in Fact_Test_Coverage (separate
# notebook) for the coverage report.
#
# TEST_ISSUE_KEY / PARENT_ISSUE_KEY -- proper relationships to Dim_Issue,
# not just the display-text Test_Code/Parent_Issue_Code. Added so a report
# can relate a blocked test (or its parent) through to Bridge_Issue_Link
# for "what work item is this blocked by" -- Predecessor_Issue_Code on
# Dim_Issue is a comma-joined string, not something a report table can
# list row-by-row.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test",
    table_type="fact",
    key_column="Test_Fact_Key",
    columns={
        # Merge key: composite (set + test)
        "Test_Set_Id":   {"type": "string", "merge_field": True},
        "Test_Id":       {"type": "string", "merge_field": True},

        # Natural keys for display and drill-through
        "Test_Set_Code":     {"type": "string", "default": "Unknown"},
        "Test_Code":         {"type": "string", "default": "Unknown"},
        "Parent_Issue_Code": {"type": "string"},

        # Latest run status -- denormalised for simplicity
        # "NOT RUN" when no run exists, matching Dim_Test_Status
        "Latest_Status":   {"type": "string", "default": "NOT RUN"},
        "Latest_Run_Date": {"type": "date"},
        "Latest_Executor": {"type": "string"},
        "Execution_Code":  {"type": "string"},
        "Run_Count":       {"type": "int", "default": 0},
        "Pass_Count":      {"type": "int", "default": 0},
        "Fail_Count":      {"type": "int", "default": 0},

        # Measures
        "Test_Count":  {"type": "int", "default": 1},
        "Is_Pass":     {"type": "boolean", "default": False},
        "Is_Fail":     {"type": "boolean", "default": False},
        "Is_Blocked":  {"type": "boolean", "default": False},
        "Is_Not_Run":  {"type": "boolean", "default": True},
        "Is_Executed": {"type": "boolean", "default": False},

        # Surrogate FKs
        "Test_Set_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_set",
                "natural_key_column": "Test_Set_Id",
                "key_column": "Test_Set_Key",
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
        "Executor_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_resource",
                "natural_key_column": "Resource_Account_Id",
                "key_column": "Resource_Key",
                "unknown_value": "Unknown",
            },
        },
        # Resolved via the test set's Parent_Issue_Code (the Requirement it
        # covers) -- the Test issue's OWN project/team isn't necessarily the
        # delivery project a report wants to slice by (Xray tests often sit
        # in their own QA project). Null for a test set with no parent link
        # at all; falls to each dimension's Unknown member.
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
        # Proper relationships to Dim_Issue, for BOTH the test issue itself
        # and the parent it covers -- Parent_Issue_Code/Test_Code alone are
        # display text, not something a report can relate through to reach
        # e.g. Bridge_Issue_Link for "what's blocking this test / its
        # parent". Test_Issue_Key is the active path; Parent_Issue_Key
        # points at the SAME Dim_Issue table too, so mark it inactive in
        # the semantic model and use USERELATIONSHIP if both are ever
        # needed in one visual.
        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Parent_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Code",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH
    memberships AS (
        SELECT
            ts.test_set_issue_id  AS Test_Set_Id,
            ts.test_set_issue_key AS Test_Set_Code,
            ts.test_issue_id      AS Test_Id,
            ts.test_issue_key     AS Test_Code
        FROM Silver.xray.test_sets ts
    ),
    latest_runs AS (
        SELECT
            test_issue_id,
            status_name                AS Latest_Status,
            executed_by_id,
            execution_issue_key        AS Execution_Code,
            CAST(started_on AS date)   AS Latest_Run_Date,
            ROW_NUMBER() OVER (
                PARTITION BY test_issue_id
                ORDER BY CAST(started_on AS timestamp) DESC
            ) AS rn
        FROM Silver.xray.test_runs
    ),

    -- Run counts per test
    run_counts AS (
        SELECT
            test_issue_id,
            COUNT(*)                                                    AS Run_Count,
            SUM(CASE WHEN status_name = 'PASSED' THEN 1 ELSE 0 END)     AS Pass_Count,
            SUM(CASE WHEN status_name = 'FAILED' THEN 1 ELSE 0 END)     AS Fail_Count
        FROM Silver.xray.test_runs
        GROUP BY test_issue_id
    ),

    -- First parent issue linked to each test set via a 'Test' link.
    -- Silver.jira.issue_links has NO linked_issue_id column -- only
    -- inward_issue_key / outward_issue_key (strings), and link_type is
    -- just 'Test' (NOT 'Tests'/'is tested by'/'tested by' -- those are
    -- the inward/outward LABELS on that link_type, not values it takes).
    -- Confirmed directly from the Silver data:
    --   outward_issue_key populated -> issue_key "tests" outward_issue_key
    --     => issue_key is the TEST SET, outward_issue_key is the REQUIREMENT
    --   inward_issue_key populated  -> issue_key "is tested by" inward_issue_key
    --     => issue_key is the REQUIREMENT, inward_issue_key is the TEST SET
    -- Both directions normalised to the same (Test_Set_Code, Parent_Issue_Code)
    -- shape before picking the first one per set.
    parent_issues AS (
        SELECT Test_Set_Code, MIN_BY(Parent_Issue_Code, link_id) AS Parent_Issue_Code
        FROM (
            SELECT issue_key AS Test_Set_Code, outward_issue_key AS Parent_Issue_Code, link_id
            FROM Silver.jira.issue_links
            WHERE link_type = 'Test' AND outward_issue_key IS NOT NULL

            UNION ALL

            SELECT inward_issue_key AS Test_Set_Code, issue_key AS Parent_Issue_Code, link_id
            FROM Silver.jira.issue_links
            WHERE link_type = 'Test' AND inward_issue_key IS NOT NULL
        ) x
        GROUP BY Test_Set_Code
    )
    SELECT
        m.Test_Set_Id,
        m.Test_Set_Code,
        m.Test_Id,
        m.Test_Code,
        p.Parent_Issue_Code,

        COALESCE(lr.Latest_Status, 'NOT RUN')       AS Latest_Status,
        lr.Latest_Run_Date,
        u.displayName                               AS Latest_Executor,
        lr.Execution_Code,
        COALESCE(rc.Run_Count,  0)                  AS Run_Count,
        COALESCE(rc.Pass_Count, 0)                  AS Pass_Count,
        COALESCE(rc.Fail_Count, 0)                  AS Fail_Count,

        -- Measures
        1                                                  AS Test_Count,
        COALESCE(lr.Latest_Status, 'NOT RUN') = 'PASSED'   AS Is_Pass,
        COALESCE(lr.Latest_Status, 'NOT RUN') = 'FAILED'   AS Is_Fail,
        COALESCE(lr.Latest_Status, 'NOT RUN') = 'BLOCKED'  AS Is_Blocked,
        lr.Latest_Status IS NULL                           AS Is_Not_Run,
        lr.Latest_Status IS NOT NULL                       AS Is_Executed,

        dts.Test_Set_Key,
        dtstat.Test_Status_Key,
        res.Resource_Key AS Executor_Key,
        proj.Project_Key,
        team.Team_Key,
        test_di.Issue_Key   AS Test_Issue_Key,
        parent_di.Issue_Key AS Parent_Issue_Key

    FROM memberships m
    LEFT JOIN latest_runs lr
           ON lr.test_issue_id = m.Test_Id AND lr.rn = 1
    LEFT JOIN run_counts rc
           ON rc.test_issue_id = m.Test_Id
    LEFT JOIN Silver.jira.users u
           ON u.accountId = lr.executed_by_id
    LEFT JOIN parent_issues p
           ON p.Test_Set_Code = m.Test_Set_Code
    LEFT JOIN Silver.jira.issues parent_issue
           ON parent_issue.key = p.Parent_Issue_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_test_set dts
           ON dts.Test_Set_Id = m.Test_Set_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status dtstat
           ON dtstat.Test_Status_Name = COALESCE(lr.Latest_Status, 'NOT RUN')
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res
           ON res.Resource_Account_Id = lr.executed_by_id
    LEFT JOIN {GOLD_SCHEMA}.dim_project proj
           ON proj.Project_Id = parent_issue.fields_project_id
    LEFT JOIN {GOLD_SCHEMA}.dim_team team
           ON team.Team_Name = parent_issue.fields_team_name
    LEFT JOIN {GOLD_SCHEMA}.dim_issue test_di
           ON test_di.Issue_Id = m.Test_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue parent_di
           ON parent_di.Issue_Code = p.Parent_Issue_Code
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test built successfully")
