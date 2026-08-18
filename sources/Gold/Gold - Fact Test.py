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
# Latest run status is denormalised onto each row. For trend over time,
# Fact_Test_Run (separate notebook) carries every run.
#
# PARENT ISSUE CONTEXT
# Each test set is linked to one or more parent issues (Stories/Requirements)
# via Jira's "is tested by" link. We carry the FIRST linked parent issue
# directly onto this fact so the client sees:
#   Parent Issue -> Test Set -> Test
# without needing to traverse Bridge_Issue_Link in their report.
# Where a test set has multiple parent issues, all are surfaced in
# Fact_Test_Coverage (separate notebook) for the coverage report.
#
# SIMPLIFIED / FIXED --
#   * FIXED: Test_Set_Key, Test_Status_Key, Executor_Key were declared as
#     lookup_missing_from FKs but the query never actually produced them --
#     merge() would have raised "column(s) not present in the DataFrame at
#     all" the first time this ran. Real LEFT JOINs added below.
#   * REMOVED Is_Pass / Is_Fail / Is_Blocked / Is_Not_Run / Is_Executed --
#     Test_Status_Key already relates each row to Dim_Test_Status, which
#     carries these exact flags (see that notebook). Recomputing them here
#     duplicated the dimension; sum through the relationship instead.
#   * REMOVED Run_Count / Pass_Count / Fail_Count -- historical run counts
#     for a test belong to Fact_Test_Run (one row per run, every run ever),
#     which already supports "how many times has this test run / passed /
#     failed" via a straightforward aggregation. Keeping the same numbers on
#     both facts risked them drifting apart. Test_Count (=1) stays -- it is
#     the grain's own counting measure (SUM gives "how many test-set
#     memberships"), not a duplicate of anything on Fact_Test_Run.
#   * ADDED Project_Key / Team_Key so a project/team slicer reaches the test
#     report in one hop, same as every other fact.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test",
    table_type="fact",
    key_column="Test_Fact_Key",
    columns={
        # Merge on the natural composite key: set + test
        "Test_Set_Id":   {"type": "string", "merge_field": True},
        "Test_Id":       {"type": "string", "merge_field": True},

        # Natural keys -- what the client recognises
        "Test_Set_Code": {"type": "string", "default": "Unknown"},
        "Test_Code":     {"type": "string", "default": "Unknown"},
        "Test_Name":     {"type": "string", "default": "Unknown"},

        # Parent issue context (requirement / story this test set covers)
        "Parent_Issue_Code":    {"type": "string"},
        "Parent_Issue_Name":    {"type": "string"},
        "Parent_Issue_Category":{"type": "string"},

        # Test details from the Jira issue
        "Acceptance_Criteria":  {"type": "string", "default": ""},
        "Test_Type":            {"type": "string", "default": "Manual"},
        "Workstream":           {"type": "string", "default": "Unknown"},
        "Release":              {"type": "string", "default": "Unknown"},

        # Latest run status -- denormalised for simplicity
        # "NOT RUN" when no run exists, matching Dim_Test_Status
        "Latest_Status":        {"type": "string", "default": "NOT RUN"},
        "Latest_Run_Date":      {"type": "date"},
        "Latest_Executor":      {"type": "string"},
        "Execution_Code":       {"type": "string"},

        # Measures -- the grain's own counting measure
        "Test_Count":           {"type": "int", "default": 1},

        # FK to Dim_Test_Set (the one relationship this fact needs)
        "Test_Set_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_set",
                "natural_key_column": "Test_Set_Id",
                "key_column": "Test_Set_Key",
                "unknown_value": "Unknown",
            },
        },
        # FK to Dim_Test_Status
        "Test_Status_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_status",
                "natural_key_column": "Test_Status_Name",
                "key_column": "Test_Status_Key",
                "unknown_value": "Unknown",
            },
        },
        # FK to Dim_Resource (last executor)
        "Executor_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_resource",
                "natural_key_column": "Resource_Account_Id",
                "key_column": "Resource_Key",
                "unknown_value": "Unknown",
            },
        },
        # FK to Dim_Project / Dim_Team
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
    WITH
    -- Every test set membership: one row per set-test pair
    memberships AS (
        SELECT
            ts.test_set_issue_id  AS Test_Set_Id,
            ts.test_set_issue_key AS Test_Set_Code,
            ts.test_issue_id      AS Test_Id,
            ts.test_issue_key     AS Test_Code
        FROM Silver.xray.test_sets ts
    ),

    -- Latest run per test (not per set-test -- a run is against the test
    -- itself, regardless of which set it was run through)
    latest_runs AS (
        SELECT
            test_issue_id,
            test_issue_key,
            status_name                             AS Latest_Status,
            test_type                               AS Test_Type,
            executed_by_id,
            execution_issue_key                     AS Execution_Code,
            CAST(started_on AS date)                AS Latest_Run_Date,
            ROW_NUMBER() OVER (
                PARTITION BY test_issue_id
                ORDER BY CAST(started_on AS timestamp) DESC
            )                                       AS rn
        FROM Silver.xray.test_runs
    ),

    -- First parent issue linked to each test set via "is tested by"
    -- Direction = Inward means this test set is the target of "tests" link
    parent_issues AS (
        SELECT
            il.issue_id                             AS test_set_issue_id,
            MIN_BY(i.key,   il.link_id)             AS Parent_Issue_Code,
            MIN_BY(i.fields_summary, il.link_id)    AS Parent_Issue_Name,
            MIN_BY(c.Issue_Category, il.link_id)    AS Parent_Issue_Category,
            MIN_BY(i.fields_project_id, il.link_id) AS Parent_Project_Id,
            MIN_BY(i.fields_team_name, il.link_id)  AS Parent_Team_Name
        FROM Silver.jira.issue_links il
        JOIN Silver.jira.issues i
          ON i.id = il.linked_issue_id
        LEFT JOIN {GOLD_SCHEMA}.dim_issue_type c
          ON c.IssueType_Id = i.fields_issuetype_id
        WHERE il.link_type IN ('Test', 'Tests', 'is tested by', 'tested by')
        GROUP BY il.issue_id
    )

    SELECT
        m.Test_Set_Id,
        m.Test_Set_Code,
        m.Test_Id,
        m.Test_Code,
        CONCAT(m.Test_Code, ': ', COALESCE(i.fields_summary, 'No summary')) AS Test_Name,

        -- Parent issue context
        p.Parent_Issue_Code,
        p.Parent_Issue_Name,
        p.Parent_Issue_Category,

        -- Test issue attributes
        COALESCE(i.fields_customfield_acceptance_criteria,
                 i.fields_acceptance_criteria, '')  AS Acceptance_Criteria,
        COALESCE(lr.Test_Type, 'Manual')            AS Test_Type,
        COALESCE(di.Workstream_Label, 'Unknown')    AS Workstream,
        COALESCE(di.Release_Label,    'Unknown')    AS Release,

        -- Latest run (null-safe: defaults to NOT RUN)
        COALESCE(lr.Latest_Status, 'NOT RUN')       AS Latest_Status,
        lr.Latest_Run_Date,
        u.displayName                               AS Latest_Executor,
        lr.Execution_Code,

        -- Measure
        1                                           AS Test_Count,

        -- FK natural keys, resolved to surrogate keys below
        m.Test_Set_Id                               AS Test_Set_Id_fk,
        COALESCE(lr.Latest_Status, 'NOT RUN')       AS Test_Status_Name,
        lr.executed_by_id                           AS Resource_Account_Id,
        p.Parent_Project_Id                         AS Project_Id,
        p.Parent_Team_Name                          AS Team_Name

    FROM memberships m
    LEFT JOIN Silver.jira.issues i
           ON i.id = m.Test_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di
           ON di.Issue_Id = m.Test_Id
    LEFT JOIN latest_runs lr
           ON lr.test_issue_id = m.Test_Id AND lr.rn = 1
    LEFT JOIN Silver.jira.users u
           ON u.accountId = lr.executed_by_id
    LEFT JOIN parent_issues p
           ON p.test_set_issue_id = m.Test_Set_Id
""")

# MARKDOWN ********************

# ## Resolve the FK surrogate keys the schema declares

# CELL ********************
df = (
    df
    .join(spark.sql(f"SELECT Test_Set_Id, Test_Set_Key FROM {GOLD_SCHEMA}.dim_test_set"),
          on="Test_Set_Id", how="left")
    .join(spark.sql(f"SELECT Test_Status_Name, Test_Status_Key FROM {GOLD_SCHEMA}.dim_test_status"),
          on="Test_Status_Name", how="left")
    .join(spark.sql(f"SELECT Resource_Account_Id, Resource_Key AS Executor_Key FROM {GOLD_SCHEMA}.dim_resource"),
          on="Resource_Account_Id", how="left")
    .join(spark.sql(f"SELECT Project_Id, Project_Key FROM {GOLD_SCHEMA}.dim_project"),
          on="Project_Id", how="left")
    .join(spark.sql(f"SELECT Team_Name, Team_Key FROM {GOLD_SCHEMA}.dim_team"),
          on="Team_Name", how="left")
    .drop("Test_Set_Id_fk", "Resource_Account_Id", "Project_Id", "Team_Name")
)

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test built successfully")
