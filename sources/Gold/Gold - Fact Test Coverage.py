# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test_Coverage -- requirement to test set links
# Grain: ONE ROW PER REQUIREMENT-TO-TEST-SET LINK.
#
# This is the coverage fact. It answers:
#   - Which requirements have test sets covering them?
#   - Which requirements have NO test sets (the go-live risk number)?
#   - How many tests pass per requirement?
#
# Kept separate from Fact_Test because the grain is different. Fact_Test
# is about individual tests within a set. This is about which requirements
# are covered at all. A requirement with 3 covering test sets has 3 rows
# here, with the combined pass rate for each set carried along.
#
# If a requirement has NO covering test set, it still appears here as one
# row with Is_Covered = False. This is how "uncovered requirements" reports
# work without complex DAX: just filter Is_Covered = False.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_coverage",
    table_type="fact",
    key_column="Coverage_Key",
    columns={
        "Requirement_Id":       {"type": "string", "merge_field": True},
        "Test_Set_Id":          {"type": "string", "merge_field": True},
        "Requirement_Code":     {"type": "string", "default": "Unknown"},
        "Requirement_Name":     {"type": "string", "default": "Unknown"},
        "Requirement_Category": {"type": "string", "default": "Unknown"},
        "Workstream":           {"type": "string", "default": "Unknown"},
        "Release":              {"type": "string", "default": "Unknown"},
        "Test_Set_Code":        {"type": "string"},
        "Test_Set_Name":        {"type": "string"},
        "Is_Covered":           {"type": "boolean", "default": False},
        "Tests_In_Set":         {"type": "int", "default": 0},
        "Tests_Passed":         {"type": "int", "default": 0},
        "Tests_Failed":         {"type": "int", "default": 0},
        "Tests_Not_Run":        {"type": "int", "default": 0},
        "Coverage_Count":       {"type": "int", "default": 1},
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
    -- All requirements (issue key string)
    requirements AS (
        SELECT
            i.id                        AS Requirement_Id,
            i.key                       AS Requirement_Code,
            i.fields_summary            AS Requirement_Name,
            c.Issue_Category            AS Requirement_Category,
            COALESCE(di.Workstream_Label, 'Unknown') AS Workstream,
            COALESCE(di.Release_Label,    'Unknown') AS Release
        FROM Silver.jira.issues i
        JOIN {GOLD_SCHEMA}.dim_issue_type c
          ON c.IssueType_Id = i.fields_issuetype_id
        WHERE c.Issue_Category = 'Requirement'
    ),

    -- Test sets covering each requirement.
    -- Two directions from Silver, using issue key strings throughout.
    covering_sets AS (
        SELECT
            il.linked_issue_id          AS Requirement_Id,
            il.issue_id                 AS Test_Set_Id,
            il.issue_key                AS Test_Set_Code
        FROM Silver.jira.issue_links il
        WHERE il.link_type IN ('Test', 'Tests', 'is tested by', 'tested by')
    ),

    -- Test counts per set (from Fact_Test already built)
    set_stats AS (
        SELECT
            Test_Set_Id,
            COUNT(*)                                            AS Tests_In_Set,
            SUM(CASE WHEN Is_Pass    THEN 1 ELSE 0 END)        AS Tests_Passed,
            SUM(CASE WHEN Is_Fail    THEN 1 ELSE 0 END)        AS Tests_Failed,
            SUM(CASE WHEN Is_Not_Run THEN 1 ELSE 0 END)        AS Tests_Not_Run
        FROM {GOLD_SCHEMA}.fact_test
        GROUP BY Test_Set_Id
    )

    -- Covered requirements
    SELECT
        r.Requirement_Id,
        cs.Test_Set_Id,
        r.Requirement_Code,
        r.Requirement_Name,
        r.Requirement_Category,
        r.Workstream,
        r.Release,
        cs.Test_Set_Code,
        dts.Test_Set_Name,
        TRUE                    AS Is_Covered,
        COALESCE(ss.Tests_In_Set,  0) AS Tests_In_Set,
        COALESCE(ss.Tests_Passed,  0) AS Tests_Passed,
        COALESCE(ss.Tests_Failed,  0) AS Tests_Failed,
        COALESCE(ss.Tests_Not_Run, 0) AS Tests_Not_Run,
        1                       AS Coverage_Count,
        r.Requirement_Id        AS Requirement_Id_fk,
        cs.Test_Set_Id          AS Test_Set_Id_fk
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
        r.Requirement_Id,
        'NONE'              AS Test_Set_Id,
        r.Requirement_Code,
        r.Requirement_Name,
        r.Requirement_Category,
        r.Workstream,
        r.Release,
        NULL                AS Test_Set_Code,
        NULL                AS Test_Set_Name,
        FALSE               AS Is_Covered,
        0, 0, 0, 0,
        1                   AS Coverage_Count,
        r.Requirement_Id    AS Requirement_Id_fk,
        'NONE'              AS Test_Set_Id_fk
    FROM requirements r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue req_dim2
      ON req_dim2.Issue_Code = r.Requirement_Code
    WHERE NOT EXISTS (
        SELECT 1 FROM covering_sets cs
        WHERE cs.Requirement_Code = r.Requirement_Code
    )
""")

# MARKDOWN ********************

# ## Resolve Project_Key / Team_Key

# CELL ********************
df = (
    df
    .join(spark.sql(f"SELECT Project_Id, Project_Key FROM {GOLD_SCHEMA}.dim_project"),
          on="Project_Id", how="left")
    .join(spark.sql(f"SELECT Team_Name, Team_Key FROM {GOLD_SCHEMA}.dim_team"),
          on="Team_Name", how="left")
    .drop("Project_Id", "Team_Name")
)

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Coverage built successfully")
