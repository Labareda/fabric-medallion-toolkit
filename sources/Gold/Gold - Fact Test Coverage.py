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
#
# Merge key is Requirement_Code / Test_Set_Code (issue key strings) --
# stable, human-readable, what the report and drill-through use, AND what
# Silver.jira.issue_links itself is keyed on for the linked side of a link
# (it has no numeric id for the other issue, only inward/outward_issue_key
# -- see covering_sets below). Joins stay Code-based throughout rather than
# switching to ids partway through.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_coverage",
    table_type="fact",
    key_column="Coverage_Key",
    columns={
        "Requirement_Code":     {"type": "string", "merge_field": True},
        "Test_Set_Code":        {"type": "string", "merge_field": True},
        "Requirement_Name":     {"type": "string", "default": "Unknown"},
        "Requirement_Category": {"type": "string", "default": "Unknown"},
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
    },
)

# CELL ********************
df = spark.sql(f"""
    WITH
    -- All requirements from Jira
    requirements AS (
        SELECT
            i.key                       AS Requirement_Code,
            i.fields_summary            AS Requirement_Name,
            c.Issue_Category            AS Requirement_Category
        FROM Silver.jira.issues i
        JOIN {GOLD_SCHEMA}.dim_issue_type c
          ON c.IssueType_Id = i.fields_issuetype_id
        WHERE c.Issue_Category = 'Requirement'
    ),

    -- Test sets that cover each requirement via issue links. Silver.jira.
    -- issue_links has NO linked_issue_id column -- only inward_issue_key /
    -- outward_issue_key (strings), and link_type is just 'Test' (NOT
    -- 'Tests'/'is tested by'/'tested by' -- those are the inward/outward
    -- LABELS on that same link_type, not values link_type itself takes).
    -- Confirmed directly from the Silver data:
    --   outward_issue_key populated -> issue_key "tests" outward_issue_key
    --     => issue_key is the TEST SET, outward_issue_key is the REQUIREMENT
    --   inward_issue_key populated  -> issue_key "is tested by" inward_issue_key
    --     => issue_key is the REQUIREMENT, inward_issue_key is the TEST SET
    -- A link is captured from whichever side Jira recorded it on, so both
    -- directions have to be normalised to the same (Test_Set_Code,
    -- Requirement_Code) shape here.
    covering_sets AS (
        SELECT issue_key AS Test_Set_Code, outward_issue_key AS Requirement_Code
        FROM Silver.jira.issue_links
        WHERE link_type = 'Test' AND outward_issue_key IS NOT NULL

        UNION ALL

        SELECT inward_issue_key AS Test_Set_Code, issue_key AS Requirement_Code
        FROM Silver.jira.issue_links
        WHERE link_type = 'Test' AND inward_issue_key IS NOT NULL
    ),

    -- Test counts per set, via Dim_Test_Status flags already on Fact_Test
    set_stats AS (
        SELECT
            ft.Test_Set_Code,
            COUNT(*)                                     AS Tests_In_Set,
            SUM(CASE WHEN ft.Is_Pass    THEN 1 ELSE 0 END) AS Tests_Passed,
            SUM(CASE WHEN ft.Is_Fail    THEN 1 ELSE 0 END) AS Tests_Failed,
            SUM(CASE WHEN ft.Is_Not_Run THEN 1 ELSE 0 END) AS Tests_Not_Run
        FROM {GOLD_SCHEMA}.fact_test ft
        GROUP BY ft.Test_Set_Code
    )

    -- Covered requirements
    SELECT
        r.Requirement_Code,
        cs.Test_Set_Code,
        r.Requirement_Name,
        r.Requirement_Category,
        dts.Test_Set_Name,
        TRUE                    AS Is_Covered,
        COALESCE(ss.Tests_In_Set,  0) AS Tests_In_Set,
        COALESCE(ss.Tests_Passed,  0) AS Tests_Passed,
        COALESCE(ss.Tests_Failed,  0) AS Tests_Failed,
        COALESCE(ss.Tests_Not_Run, 0) AS Tests_Not_Run,
        1                       AS Coverage_Count,
        di.Issue_Key            AS Requirement_Key,
        dts.Test_Set_Key        AS Test_Set_Key
    FROM requirements r
    JOIN covering_sets cs ON cs.Requirement_Code = r.Requirement_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di     ON di.Issue_Code = r.Requirement_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_test_set dts ON dts.Test_Set_Code = cs.Test_Set_Code
    LEFT JOIN set_stats ss                   ON ss.Test_Set_Code = cs.Test_Set_Code

    UNION ALL

    -- Uncovered requirements (no test set link at all)
    SELECT
        r.Requirement_Code,
        'NONE'              AS Test_Set_Code,
        r.Requirement_Name,
        r.Requirement_Category,
        NULL                AS Test_Set_Name,
        FALSE               AS Is_Covered,
        0, 0, 0, 0,
        1                   AS Coverage_Count,
        di.Issue_Key        AS Requirement_Key,
        CAST(NULL AS STRING) AS Test_Set_Key
    FROM requirements r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di ON di.Issue_Code = r.Requirement_Code
    WHERE NOT EXISTS (
        SELECT 1 FROM covering_sets cs WHERE cs.Requirement_Code = r.Requirement_Code
    )
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Coverage built successfully")
