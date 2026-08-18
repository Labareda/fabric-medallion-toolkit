# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test_Coverage
# Grain: ONE ROW PER REQUIREMENT × TEST SET LINK.
# Uncovered requirements get one row with Test_Set_Code = NULL.
#
# Silver.jira.issue_links structure (confirmed from data):
#   link_type = 'Test'  (NOT 'is tested by' -- that is the inward_label)
#   outward_issue_key populated -> issue_key TESTS outward_issue_key
#     => issue_key is Test Set, outward_issue_key is Requirement
#   inward_issue_key populated  -> issue_key IS TESTED BY inward_issue_key
#     => issue_key is Requirement, inward_issue_key is Test Set
#
# Uses issue_key strings (not numeric ids) as join keys throughout,
# then resolves surrogates at the end via explicit dim joins.
#
# STAR SCHEMA: keys, measures and flags only.
# Requirement details -> Dim_Issue (via Requirement_Key)
# Test set details    -> Dim_TestSet (via Test_Set_Key)

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_coverage",
    table_type="fact",
    key_column="Coverage_Key",
    columns={
        # Merge keys -- use issue key strings, stable and consistent
        "Requirement_Code": {"type": "string", "merge_field": True},
        "Test_Set_Code":    {"type": "string", "merge_field": True},

        # Measures and flags
        "Is_Covered":       {"type": "boolean", "default": False},
        "Tests_In_Set":     {"type": "int",     "default": 0},
        "Tests_Passed":     {"type": "int",     "default": 0},
        "Tests_Failed":     {"type": "int",     "default": 0},
        "Tests_Not_Run":    {"type": "int",     "default": 0},
        "Coverage_Count":   {"type": "int",     "default": 1},

        # Surrogate FKs
        "Requirement_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Code",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Test_Set_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_set",
                "natural_key_column": "Test_Set_Code",
                "key_column": "Test_Set_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH
    -- All requirements (issue key string)
    requirements AS (
        SELECT i.key AS Requirement_Code
        FROM Silver.jira.issues i
        JOIN {GOLD_SCHEMA}.dim_issue_type c
          ON c.IssueType_Id = i.fields_issuetype_id
        WHERE c.Issue_Category = 'Requirement'
    ),

    -- Test sets covering each requirement.
    -- Two directions from Silver, using issue key strings throughout.
    covering_sets AS (
        SELECT DISTINCT Requirement_Code, Test_Set_Code
        FROM (
            -- Outward: issue_key "tests" outward_issue_key
            --   => issue_key is Test Set, outward_issue_key is Requirement
            SELECT
                il.outward_issue_key AS Requirement_Code,
                il.issue_key         AS Test_Set_Code
            FROM Silver.jira.issue_links il
            WHERE il.link_type = 'Test'
              AND il.outward_issue_key IS NOT NULL
              AND TRIM(il.outward_issue_key) <> ''

            UNION ALL

            -- Inward: issue_key "is tested by" inward_issue_key
            --   => issue_key is Requirement, inward_issue_key is Test Set
            SELECT
                il.issue_key         AS Requirement_Code,
                il.inward_issue_key  AS Test_Set_Code
            FROM Silver.jira.issue_links il
            WHERE il.link_type = 'Test'
              AND il.inward_issue_key IS NOT NULL
              AND TRIM(il.inward_issue_key) <> ''
        ) _links
    ),

    -- Test counts per set from Fact_Test (already built)
    -- Joins on Test_Set_Code which is the issue key string
    set_stats AS (
        SELECT
            ft.Test_Set_Code,
            COUNT(*)                                         AS Tests_In_Set,
            SUM(CASE WHEN ft.Is_Pass    THEN 1 ELSE 0 END)  AS Tests_Passed,
            SUM(CASE WHEN ft.Is_Fail    THEN 1 ELSE 0 END)  AS Tests_Failed,
            SUM(CASE WHEN ft.Is_Not_Run THEN 1 ELSE 0 END)  AS Tests_Not_Run
        FROM {GOLD_SCHEMA}.fact_test ft
        GROUP BY ft.Test_Set_Code
    )

    -- Covered requirements
    SELECT
        r.Requirement_Code,
        cs.Test_Set_Code,
        TRUE                            AS Is_Covered,
        COALESCE(ss.Tests_In_Set,  0)   AS Tests_In_Set,
        COALESCE(ss.Tests_Passed,  0)   AS Tests_Passed,
        COALESCE(ss.Tests_Failed,  0)   AS Tests_Failed,
        COALESCE(ss.Tests_Not_Run, 0)   AS Tests_Not_Run,
        1                               AS Coverage_Count,
        -- Surrogate FKs from explicit dim joins
        req_dim.Issue_Key               AS Requirement_Key,
        dts.Test_Set_Key
    FROM requirements r
    JOIN covering_sets cs
      ON cs.Requirement_Code = r.Requirement_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_issue req_dim
      ON req_dim.Issue_Code = r.Requirement_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_test_set dts
      ON dts.Test_Set_Code = cs.Test_Set_Code
    LEFT JOIN set_stats ss
      ON ss.Test_Set_Code = cs.Test_Set_Code

    UNION ALL

    -- Uncovered requirements
    SELECT
        r.Requirement_Code,
        'NONE'                          AS Test_Set_Code,
        FALSE                           AS Is_Covered,
        0, 0, 0, 0,
        1                               AS Coverage_Count,
        req_dim2.Issue_Key              AS Requirement_Key,
        NULL                            AS Test_Set_Key
    FROM requirements r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue req_dim2
      ON req_dim2.Issue_Code = r.Requirement_Code
    WHERE NOT EXISTS (
        SELECT 1 FROM covering_sets cs
        WHERE cs.Requirement_Code = r.Requirement_Code
    )
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Coverage built successfully")
