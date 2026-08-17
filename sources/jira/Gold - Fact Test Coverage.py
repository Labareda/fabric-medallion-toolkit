# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test_Coverage -- requirement to test set links
# Grain: ONE ROW PER REQUIREMENT-TO-TEST-SET LINK, plus one row per
# uncovered requirement (Is_Covered = False).
#
# STAR SCHEMA: facts carry keys, measures and flags ONLY.
# Descriptive attributes come from the dimensions via relationships:
#   Requirement_Name, category, workstream, release -> Dim_Issue
#   Test_Set_Name, workstream, release              -> Dim_Test_Set

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_coverage",
    table_type="fact",
    key_column="Coverage_Key",
    columns={
        # Merge keys
        "Requirement_Id":   {"type": "string", "merge_field": True},
        "Test_Set_Id":      {"type": "string", "merge_field": True},

        # Natural keys kept for display
        "Requirement_Code": {"type": "string", "default": "Unknown"},
        "Test_Set_Code":    {"type": "string"},

        # Measures and flags
        "Is_Covered":       {"type": "boolean", "default": False},
        "Tests_In_Set":     {"type": "int", "default": 0},
        "Tests_Passed":     {"type": "int", "default": 0},
        "Tests_Failed":     {"type": "int", "default": 0},
        "Tests_Not_Run":    {"type": "int", "default": 0},
        "Coverage_Count":   {"type": "int", "default": 1},

        # Surrogate FKs
        "Requirement_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Requirement_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Test_Set_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_test_set",
                "natural_key_column": "Test_Set_Id",
                "key_column": "Test_Set_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH
    requirements AS (
        SELECT
            i.id    AS Requirement_Id,
            i.key   AS Requirement_Code
        FROM Silver.jira.issues i
        JOIN {GOLD_SCHEMA}.dim_issue_type c
          ON c.IssueType_Id = i.fields_issuetype_id
        WHERE c.Issue_Category = 'Requirement'
    ),
    covering_sets AS (
        SELECT
            il.linked_issue_id  AS Requirement_Id,
            il.issue_id         AS Test_Set_Id,
            il.issue_key        AS Test_Set_Code
        FROM Silver.jira.issue_links il
        WHERE il.link_type IN ('Test', 'Tests', 'is tested by', 'tested by')
    ),
    set_stats AS (
        SELECT
            Test_Set_Id,
            COUNT(*)                                              AS Tests_In_Set,
            SUM(CASE WHEN Is_Pass    THEN 1 ELSE 0 END)          AS Tests_Passed,
            SUM(CASE WHEN Is_Fail    THEN 1 ELSE 0 END)          AS Tests_Failed,
            SUM(CASE WHEN Is_Not_Run THEN 1 ELSE 0 END)          AS Tests_Not_Run
        FROM {GOLD_SCHEMA}.fact_test
        GROUP BY Test_Set_Id
    )

    -- Covered requirements
    SELECT
        r.Requirement_Id,
        cs.Test_Set_Id,
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
    JOIN covering_sets cs               ON cs.Requirement_Id = r.Requirement_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue req_dim ON req_dim.Issue_Id = r.Requirement_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_test_set dts  ON dts.Test_Set_Id = cs.Test_Set_Id
    LEFT JOIN set_stats ss              ON ss.Test_Set_Id = cs.Test_Set_Id

    UNION ALL

    -- Uncovered requirements
    SELECT
        r.Requirement_Id,
        'NONE'                          AS Test_Set_Id,
        r.Requirement_Code,
        NULL                            AS Test_Set_Code,
        FALSE                           AS Is_Covered,
        0, 0, 0, 0,
        1                               AS Coverage_Count,
        req_dim2.Issue_Key              AS Requirement_Key,
        NULL                            AS Test_Set_Key
    FROM requirements r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue req_dim2 ON req_dim2.Issue_Id = r.Requirement_Id
    WHERE NOT EXISTS (
        SELECT 1 FROM covering_sets cs WHERE cs.Requirement_Id = r.Requirement_Id
    )
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Coverage built successfully")
