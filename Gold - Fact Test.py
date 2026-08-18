# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test -- the complete testing fact
# Grain: ONE ROW PER TEST SET MEMBERSHIP (test_set × test).
#
# A test in 2 sets = 2 rows. This removes the need for a bridge table.
# The Power BI matrix is a plain two-level expand with no bidirectional
# filters and no complex DAX.
#
# COVERAGE is handled here too -- no separate Fact_Test_Coverage needed.
# Every test row carries the Requirement_Key of the parent issue that
# links to the test set via "is tested by". This means:
#
#   Requirements Covered  = DISTINCTCOUNT of Requirement_Key where tests exist
#   Requirements Uncovered = requirements in Dim_Issue with no matching
#                            Requirement_Key in this fact
#                          = handled in DAX via EXCEPT(all requirements, covered)
#
# Requirements with NO test set at all have no row here, which is correct:
# they appear in the uncovered DAX measure by subtraction.
#
# STAR SCHEMA: facts carry keys, measures and flags ONLY.
# Descriptive attributes via relationships:
#   Test summary, acceptance criteria  -> Dim_Issue (via Test_Issue_Key)
#   Set workstream, release, lead      -> Dim_Test_Set (via Test_Set_Key)
#   Status colour, coverage status     -> Dim_Test_Status (via Test_Status_Key)
#   Executor name                      -> Dim_Resource (via Executor_Key)
#   Requirement attributes             -> Dim_Issue (via Requirement_Key,
#                                         role-playing relationship)

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
        "Test_Set_Code": {"type": "string", "default": "Unknown"},
        "Test_Code":     {"type": "string", "default": "Unknown"},
        "Parent_Issue_Code": {"type": "string"},

        # Date
        "Latest_Run_Date": {"type": "date"},

        # Measures and flags
        "Run_Count":     {"type": "int", "default": 0},
        "Pass_Count":    {"type": "int", "default": 0},
        "Fail_Count":    {"type": "int", "default": 0},
        "Test_Count":    {"type": "int", "default": 1},
        "Is_Pass":       {"type": "boolean", "default": False},
        "Is_Fail":       {"type": "boolean", "default": False},
        "Is_Blocked":    {"type": "boolean", "default": False},
        "Is_Not_Run":    {"type": "boolean", "default": True},
        "Is_Executed":   {"type": "boolean", "default": False},

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
        # Test issue key -> Dim_Issue for test attributes
        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        # Requirement key -> Dim_Issue (role-playing) for coverage reporting.
        # This is the parent requirement that links to the test set via
        # "is tested by". NULL when no requirement link exists.
        "Requirement_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Parent_Issue_Code",
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
            status_name,
            executed_by_id,
            CAST(started_on AS date)  AS Latest_Run_Date,
            ROW_NUMBER() OVER (
                PARTITION BY test_issue_id
                ORDER BY CAST(started_on AS timestamp) DESC
            ) AS rn
        FROM Silver.xray.test_runs
    ),
    run_counts AS (
        SELECT
            test_issue_id,
            COUNT(*)                                            AS Run_Count,
            SUM(CASE WHEN status_name = 'PASSED' THEN 1 ELSE 0 END) AS Pass_Count,
            SUM(CASE WHEN status_name = 'FAILED' THEN 1 ELSE 0 END) AS Fail_Count
        FROM Silver.xray.test_runs
        GROUP BY test_issue_id
    ),
    -- Parent requirement for each test set, from Silver.jira.issue_links.
    -- Silver structure (confirmed from data):
    --   outward_issue_key populated -> issue_key "tests" outward_issue_key
    --     => issue_key is the TEST SET, outward_issue_key is the REQUIREMENT
    --   inward_issue_key populated  -> issue_key "is tested by" inward_issue_key
    --     => issue_key is the REQUIREMENT, inward_issue_key is the TEST SET
    --   link_type = 'Test'  (NOT 'is tested by' — that is the inward_label)
    parent_requirements AS (
        SELECT test_set_key, MIN_BY(req_key, lnk_id) AS Requirement_Code
        FROM (
            -- Outward: issue_key tests outward_issue_key
            -- => issue_key is Test Set, outward_issue_key is Requirement
            SELECT
                il.issue_key        AS test_set_key,
                il.outward_issue_key AS req_key,
                il.link_id           AS lnk_id
            FROM Silver.jira.issue_links il
            WHERE il.link_type = 'Test'
              AND il.outward_issue_key IS NOT NULL
              AND TRIM(il.outward_issue_key) <> ''

            UNION ALL

            -- Inward: issue_key is tested by inward_issue_key
            -- => issue_key is Requirement, inward_issue_key is Test Set
            SELECT
                il.inward_issue_key  AS test_set_key,
                il.issue_key         AS req_key,
                il.link_id           AS lnk_id
            FROM Silver.jira.issue_links il
            WHERE il.link_type = 'Test'
              AND il.inward_issue_key IS NOT NULL
              AND TRIM(il.inward_issue_key) <> ''
        ) _links
        GROUP BY test_set_key
    )
    SELECT
        m.Test_Set_Id,
        m.Test_Set_Code,
        m.Test_Id,
        m.Test_Code,
        lr.Latest_Run_Date,
        COALESCE(rc.Run_Count,  0)                   AS Run_Count,
        COALESCE(rc.Pass_Count, 0)                   AS Pass_Count,
        COALESCE(rc.Fail_Count, 0)                   AS Fail_Count,
        1                                            AS Test_Count,
        COALESCE(lr.status_name, 'NOT RUN') = 'PASSED'  AS Is_Pass,
        COALESCE(lr.status_name, 'NOT RUN') = 'FAILED'  AS Is_Fail,
        COALESCE(lr.status_name, 'NOT RUN') = 'BLOCKED' AS Is_Blocked,
        lr.status_name IS NULL                           AS Is_Not_Run,
        lr.status_name IS NOT NULL                       AS Is_Executed,
        -- Natural keys for lookup_missing_from safety net
        m.Test_Set_Id                                AS Test_Set_Id_nk,
        COALESCE(lr.status_name, 'NOT RUN')          AS Test_Status_Name,
        lr.executed_by_id                            AS Resource_Account_Id,
        m.Test_Id                                    AS Issue_Id,
        pr.Requirement_Code     AS Parent_Issue_Code,
        -- Surrogate keys from explicit dim joins
        dts.Test_Set_Key,
        ts.Test_Status_Key,
        res.Resource_Key                             AS Executor_Key,
        di.Issue_Key                                 AS Test_Issue_Key,
        req_dim.Issue_Key                            AS Requirement_Key
    FROM memberships m
    LEFT JOIN latest_runs lr
           ON lr.test_issue_id = m.Test_Id AND lr.rn = 1
    LEFT JOIN run_counts rc
           ON rc.test_issue_id = m.Test_Id
    LEFT JOIN parent_requirements pr
           ON pr.test_set_key = m.Test_Set_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_test_set dts
           ON dts.Test_Set_Id = m.Test_Set_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status ts
           ON ts.Test_Status_Name = COALESCE(lr.status_name, 'NOT RUN')
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res
           ON res.Resource_Account_Id = lr.executed_by_id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di
           ON di.Issue_Id = m.Test_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue req_dim
           ON req_dim.Issue_Code = pr.Requirement_Code
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test built successfully")
