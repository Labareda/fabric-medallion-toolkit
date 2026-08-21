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
# Kept separate from Fact_Test_Run because the grain is different.
# Fact_Test_Run is about individual test RUNS. This is about which
# requirements are covered at all. A requirement with 3 covering test sets
# has 3 rows here, with the combined pass rate for each set carried along.
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
        # Requirement_Name/Requirement_Category dropped -- Name duplicated
        # Dim_Issue.Summary (reachable via Requirement_Key below), and
        # Category was a CONSTANT: the requirements CTE only ever selects
        # issues WHERE Issue_Category = 'Requirement', so every row said
        # the same word. Neither carried information a relationship
        # couldn't already give for free.
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
        # Resolved via the Requirement itself (this fact's own anchor
        # issue), unlike Fact_Test which has to go via a parent link.
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
            i.key                       AS Requirement_Code,
            i.fields_project_id         AS Project_Id,
            i.fields_team_name          AS Team_Name
        FROM Silver.jira.issues i
        JOIN {GOLD_SCHEMA}.dim_issue_type c
          ON c.IssueType_Id = i.fields_issuetype_id
        WHERE c.Issue_Category = 'Requirement'
    ),

    -- Requirement <-> "Test"-link ANCHOR pairs. The anchor is NOT always an
    -- individual Test -- confirmed against two different real examples:
    -- TRAN-2926 (an individual Test) directly "tests" a Requirement, but
    -- ALSO TRAN-267 (a TEST SET) directly "tests" TF-6248 -- Jira doesn't
    -- restrict which issue type a 'Test' link sits on, and this client's
    -- data genuinely uses both. Kept as "Anchor_Code" here, not "Test_Code"
    -- -- resolving which individual test(s) it actually means is the next
    -- CTE's job.
    --   outward_issue_key populated -> issue_key "tests" outward_issue_key
    --     => issue_key is the ANCHOR, outward_issue_key is the REQUIREMENT
    --   inward_issue_key populated  -> issue_key "is tested by" inward_issue_key
    --     => issue_key is the REQUIREMENT, inward_issue_key is the ANCHOR
    --
    -- UNION (not UNION ALL): the Jira REST API attaches each link to BOTH
    -- linked issues' own issuelinks field, so a per-issue Silver ingestion
    -- captures the SAME link twice under two different link_ids -- once
    -- anchored on the Anchor (outward_issue_key populated), once anchored
    -- on the Requirement (inward_issue_key populated). Both rows produce
    -- the identical (Anchor_Code, Requirement_Code) pair, which is a real
    -- duplicate, not new information. Plain UNION dedups it.
    test_link_anchors AS (
        SELECT issue_key AS Anchor_Code, outward_issue_key AS Requirement_Code
        FROM Silver.jira.issue_links
        WHERE link_type = 'Test' AND outward_issue_key IS NOT NULL

        UNION

        SELECT inward_issue_key AS Anchor_Code, issue_key AS Requirement_Code
        FROM Silver.jira.issue_links
        WHERE link_type = 'Test' AND inward_issue_key IS NOT NULL
    ),

    -- Resolve each anchor to the individual Test(s) it actually represents.
    -- Per the client (Jira issuetypes that connect to Xray test data: Test,
    -- Test Execution, Test Set, Test Plan, Sub Test Execution), a 'Test'
    -- link can anchor on any container, not just an individual Test:
    --   Test Set / Test Plan -> expand to every test in that container
    --     (Xray's own issuetype description for Test Plan: "aggregate all
    --     executions for those tests" -- same container shape as a Set).
    --   Test Execution / Sub Test Execution -> no separate Silver table
    --     distinguishes the two (test_runs.execution_issue_key covers
    --     both), so both expand identically: every test actually RUN
    --     under that execution.
    -- Anything matching none of these is already an individual test and
    -- covers just itself.
    covering_tests AS (
        SELECT ts.test_issue_key AS Test_Code, tla.Requirement_Code
        FROM test_link_anchors tla
        JOIN Silver.xray.test_sets ts ON ts.test_set_issue_key = tla.Anchor_Code

        UNION

        SELECT tp.test_issue_key AS Test_Code, tla.Requirement_Code
        FROM test_link_anchors tla
        JOIN Silver.xray.test_plans tp ON tp.test_plan_issue_key = tla.Anchor_Code

        UNION

        SELECT DISTINCT tr.test_issue_key AS Test_Code, tla.Requirement_Code
        FROM test_link_anchors tla
        JOIN Silver.xray.test_runs tr ON tr.execution_issue_key = tla.Anchor_Code

        UNION

        SELECT tla.Anchor_Code AS Test_Code, tla.Requirement_Code
        FROM test_link_anchors tla
        WHERE NOT EXISTS (
            SELECT 1 FROM Silver.xray.test_sets ts2
            WHERE ts2.test_set_issue_key = tla.Anchor_Code
        )
        AND NOT EXISTS (
            SELECT 1 FROM Silver.xray.test_plans tp2
            WHERE tp2.test_plan_issue_key = tla.Anchor_Code
        )
        AND NOT EXISTS (
            SELECT 1 FROM Silver.xray.test_runs tr2
            WHERE tr2.execution_issue_key = tla.Anchor_Code
        )
    ),

    -- A covered TEST rolls up to whichever Test Set(s) it belongs to, via
    -- Silver.xray.test_sets MEMBERSHIP -- a separate relationship from the
    -- Requirement link above (one test can be in several sets, or none).
    -- A test not in any set still covers its requirement -- kept under the
    -- 'NONE' sentinel so coverage isn't lost just because the test wasn't
    -- organised into a set.
    covering_sets AS (
        SELECT DISTINCT
            ct.Requirement_Code,
            COALESCE(ts.test_set_issue_key, 'NONE') AS Test_Set_Code
        FROM covering_tests ct
        LEFT JOIN Silver.xray.test_sets ts ON ts.test_issue_key = ct.Test_Code
    ),

    -- Fact_Test_Run is RUN grain (Gold - Fact_Test_Run.py replaced the old
    -- Fact_Test) -- reduce to one row per test (its LATEST run) before
    -- counting, or a test re-run several times would inflate Tests_Passed/
    -- Failed with its own history instead of counting its current state once.
    latest_run_per_test AS (
        SELECT
            ftr.Test_Issue_Key, ftr.Test_Set_Code, ftr.Test_Status_Key,
            ROW_NUMBER() OVER (PARTITION BY ftr.Test_Issue_Key ORDER BY ftr.Started_On DESC) AS rn
        FROM {GOLD_SCHEMA}.fact_test_run ftr
    ),
    -- Test counts per set, off each test's current (latest-run) status.
    set_stats AS (
        SELECT
            lr.Test_Set_Code,
            COUNT(*)                                          AS Tests_In_Set,
            SUM(CASE WHEN dtstat.Is_Pass    THEN 1 ELSE 0 END) AS Tests_Passed,
            SUM(CASE WHEN dtstat.Is_Fail    THEN 1 ELSE 0 END) AS Tests_Failed,
            SUM(CASE WHEN dtstat.Is_Not_Run THEN 1 ELSE 0 END) AS Tests_Not_Run
        FROM latest_run_per_test lr
        LEFT JOIN {GOLD_SCHEMA}.dim_test_status dtstat ON dtstat.Test_Status_Key = lr.Test_Status_Key
        WHERE lr.rn = 1
        GROUP BY lr.Test_Set_Code
    )

    -- Covered requirements
    SELECT
        r.Requirement_Code,
        cs.Test_Set_Code,
        dts.Test_Set_Name,
        TRUE                    AS Is_Covered,
        COALESCE(ss.Tests_In_Set,  0) AS Tests_In_Set,
        COALESCE(ss.Tests_Passed,  0) AS Tests_Passed,
        COALESCE(ss.Tests_Failed,  0) AS Tests_Failed,
        COALESCE(ss.Tests_Not_Run, 0) AS Tests_Not_Run,
        1                       AS Coverage_Count,
        di.Issue_Key            AS Requirement_Key,
        dts.Test_Set_Key        AS Test_Set_Key,
        proj.Project_Key,
        team.Team_Key
    FROM requirements r
    JOIN covering_sets cs ON cs.Requirement_Code = r.Requirement_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di     ON di.Issue_Code = r.Requirement_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_test_set dts ON dts.Test_Set_Code = cs.Test_Set_Code
    LEFT JOIN set_stats ss                   ON ss.Test_Set_Code = cs.Test_Set_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_project proj ON proj.Project_Id = r.Project_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_team team    ON team.Team_Name = r.Team_Name

    UNION ALL

    -- Uncovered requirements (no test set link at all)
    SELECT
        r.Requirement_Code,
        'NONE'              AS Test_Set_Code,
        NULL                AS Test_Set_Name,
        FALSE               AS Is_Covered,
        0, 0, 0, 0,
        1                   AS Coverage_Count,
        di.Issue_Key        AS Requirement_Key,
        CAST(NULL AS STRING) AS Test_Set_Key,
        proj.Project_Key,
        team.Team_Key
    FROM requirements r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di     ON di.Issue_Code = r.Requirement_Code
    LEFT JOIN {GOLD_SCHEMA}.dim_project proj ON proj.Project_Id = r.Project_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_team team    ON team.Team_Name = r.Team_Name
    WHERE NOT EXISTS (
        SELECT 1 FROM covering_sets cs WHERE cs.Requirement_Code = r.Requirement_Code
    )
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Coverage built successfully")
