# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test -- the testing model spine
# Grain: ONE ROW PER TEST SET MEMBERSHIP.
#
# A test in 2 sets = 2 rows. This removes the need for a bridge table and
# makes the Power BI matrix a plain two-level expand -- no bidirectional
# filters, no complex DAX.
#
# STAR SCHEMA: facts carry keys, measures and flags ONLY.
# Descriptive attributes come from the dimensions via relationships:
#   Test_Name, Acceptance_Criteria, Test_Type -> Dim_Issue (the test)
#   Workstream, Release, Lead_Name            -> Dim_Test_Set
#   Status colour, Coverage_Status            -> Dim_Test_Status
#   Executor name                             -> Dim_Resource

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

        # Natural keys -- kept for display and drill-through
        "Test_Set_Code": {"type": "string", "default": "Unknown"},
        "Test_Code":     {"type": "string", "default": "Unknown"},

        # Parent issue natural key -- the requirement this set covers
        # (not on any dim; the coverage relationship is test set -> requirement,
        # which is many-to-many. This surfaces the primary parent directly
        # on the row so the matrix can show Issue -> Test Set -> Test.)
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
        # Test issue's surrogate key -- joins to Dim_Issue for test attributes
        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
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
    parent_issues AS (
        SELECT
            il.issue_id                          AS test_set_issue_id,
            MIN_BY(i.key, il.link_id)            AS Parent_Issue_Code
        FROM Silver.jira.issue_links il
        JOIN Silver.jira.issues i ON i.id = il.linked_issue_id
        WHERE il.link_type IN ('Test', 'Tests', 'is tested by', 'tested by')
        GROUP BY il.issue_id
    )
    SELECT
        m.Test_Set_Id,
        m.Test_Set_Code,
        m.Test_Id,
        m.Test_Code,
        p.Parent_Issue_Code,
        lr.Latest_Run_Date,
        COALESCE(rc.Run_Count,  0)                  AS Run_Count,
        COALESCE(rc.Pass_Count, 0)                  AS Pass_Count,
        COALESCE(rc.Fail_Count, 0)                  AS Fail_Count,
        1                                           AS Test_Count,
        COALESCE(lr.status_name, 'NOT RUN') = 'PASSED'  AS Is_Pass,
        COALESCE(lr.status_name, 'NOT RUN') = 'FAILED'  AS Is_Fail,
        COALESCE(lr.status_name, 'NOT RUN') = 'BLOCKED' AS Is_Blocked,
        lr.status_name IS NULL                          AS Is_Not_Run,
        lr.status_name IS NOT NULL                      AS Is_Executed,
        -- Natural keys for lookup_missing_from safety net
        m.Test_Set_Id                               AS Test_Set_Id_nk,
        COALESCE(lr.status_name, 'NOT RUN')         AS Test_Status_Name,
        lr.executed_by_id                           AS Resource_Account_Id,
        m.Test_Id                                   AS Issue_Id,
        -- Surrogate keys from explicit dim joins
        dts.Test_Set_Key,
        ts.Test_Status_Key,
        res.Resource_Key                            AS Executor_Key,
        di.Issue_Key                                AS Test_Issue_Key
    FROM memberships m
    LEFT JOIN latest_runs lr
           ON lr.test_issue_id = m.Test_Id AND lr.rn = 1
    LEFT JOIN run_counts rc
           ON rc.test_issue_id = m.Test_Id
    LEFT JOIN parent_issues p
           ON p.test_set_issue_id = m.Test_Set_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_test_set dts
           ON dts.Test_Set_Id = m.Test_Set_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status ts
           ON ts.Test_Status_Name = COALESCE(lr.status_name, 'NOT RUN')
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res
           ON res.Resource_Account_Id = lr.executed_by_id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di
           ON di.Issue_Id = m.Test_Id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test built successfully")
