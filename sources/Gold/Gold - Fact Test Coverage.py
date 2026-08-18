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
# Kept separate from Fact_Test because the grain is different, and Fact_Test
# genuinely can't answer this on its own: Fact_Test has no row at all for an
# UNCOVERED requirement (there is no test-set membership to produce one), and
# no requirement-level dimension of its own. This fact adds the "no coverage"
# sentinel row per requirement (Is_Covered = False, Test_Set_Id = 'NONE') so
# "uncovered requirements" is a plain filter, not a NOT EXISTS written in DAX.
#
# SIMPLIFIED -- Requirement_Code and Test_Set_Code removed: both are already
# reachable one hop away via Requirement_Key -> Dim_Issue.Issue_Code and
# Test_Set_Key -> Dim_Test_Set.Test_Set_Code, so carrying them here just
# duplicated the dimension. ADDED Project_Key / Team_Key so a project/team
# slicer reaches the coverage report in one hop, same as every other fact.

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
        "Requirement_Name":     {"type": "string", "default": "Unknown"},
        "Requirement_Category": {"type": "string", "default": "Unknown"},
        "Workstream":           {"type": "string", "default": "Unknown"},
        "Release":              {"type": "string", "default": "Unknown"},
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
                "natural_key_column": "Issue_Id",
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
    -- All requirements from Jira
    requirements AS (
        SELECT
            i.id                        AS Requirement_Id,
            i.fields_summary            AS Requirement_Name,
            c.Issue_Category            AS Requirement_Category,
            COALESCE(di.Workstream_Label, 'Unknown') AS Workstream,
            COALESCE(di.Release_Label,    'Unknown') AS Release,
            i.fields_project_id         AS Project_Id,
            i.fields_team_name          AS Team_Name
        FROM Silver.jira.issues i
        JOIN {GOLD_SCHEMA}.dim_issue_type c
          ON c.IssueType_Id = i.fields_issuetype_id
        JOIN {GOLD_SCHEMA}.dim_issue di
          ON di.Issue_Id = i.id
        WHERE c.Issue_Category = 'Requirement'
    ),

    -- Test sets that cover each requirement via issue links
    covering_sets AS (
        SELECT
            il.linked_issue_id          AS Requirement_Id,
            il.issue_id                 AS Test_Set_Id
        FROM Silver.jira.issue_links il
        WHERE il.link_type IN ('Test', 'Tests', 'is tested by', 'tested by')
    ),

    -- Test counts per set, via Dim_Test_Status (not recomputed booleans --
    -- Fact_Test relates to the status dimension the same way this does)
    set_stats AS (
        SELECT
            ft.Test_Set_Id,
            COUNT(*)                                            AS Tests_In_Set,
            SUM(CASE WHEN dts.Is_Pass    THEN 1 ELSE 0 END)     AS Tests_Passed,
            SUM(CASE WHEN dts.Is_Fail    THEN 1 ELSE 0 END)     AS Tests_Failed,
            SUM(CASE WHEN dts.Is_Not_Run THEN 1 ELSE 0 END)     AS Tests_Not_Run
        FROM {GOLD_SCHEMA}.fact_test ft
        LEFT JOIN {GOLD_SCHEMA}.dim_test_status dts ON dts.Test_Status_Key = ft.Test_Status_Key
        GROUP BY ft.Test_Set_Id
    )

    -- Covered requirements
    SELECT
        r.Requirement_Id,
        cs.Test_Set_Id,
        r.Requirement_Name,
        r.Requirement_Category,
        r.Workstream,
        r.Release,
        dts.Test_Set_Name,
        TRUE                    AS Is_Covered,
        COALESCE(ss.Tests_In_Set,  0) AS Tests_In_Set,
        COALESCE(ss.Tests_Passed,  0) AS Tests_Passed,
        COALESCE(ss.Tests_Failed,  0) AS Tests_Failed,
        COALESCE(ss.Tests_Not_Run, 0) AS Tests_Not_Run,
        1                       AS Coverage_Count,
        r.Requirement_Id        AS Requirement_Id_fk,
        cs.Test_Set_Id          AS Test_Set_Id_fk,
        r.Project_Id,
        r.Team_Name
    FROM requirements r
    JOIN covering_sets cs ON cs.Requirement_Id = r.Requirement_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_test_set dts ON dts.Test_Set_Id = cs.Test_Set_Id
    LEFT JOIN set_stats ss ON ss.Test_Set_Id = cs.Test_Set_Id

    UNION ALL

    -- Uncovered requirements (no test set link at all)
    SELECT
        r.Requirement_Id,
        'NONE'              AS Test_Set_Id,
        r.Requirement_Name,
        r.Requirement_Category,
        r.Workstream,
        r.Release,
        NULL                AS Test_Set_Name,
        FALSE               AS Is_Covered,
        0, 0, 0, 0,
        1                   AS Coverage_Count,
        r.Requirement_Id    AS Requirement_Id_fk,
        'NONE'              AS Test_Set_Id_fk,
        r.Project_Id,
        r.Team_Name
    FROM requirements r
    WHERE NOT EXISTS (
        SELECT 1 FROM covering_sets cs WHERE cs.Requirement_Id = r.Requirement_Id
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
