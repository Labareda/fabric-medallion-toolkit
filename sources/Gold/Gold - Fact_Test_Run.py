# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test_Run -- the ONLY test fact, keyed directly on the Test issue
# Grain: ONE ROW PER XRAY TEST RUN.
#
# REPLACES the old Fact_Test (test-SET-membership grain). That table
# duplicated this one's job -- both derived status from Silver.xray.
# test_runs, just aggregated differently -- and routed every "which issue
# is this test linked to" question through Test Set membership, which is
# the WRONG anchor: Xray links (coverage, blocking) sit on the TEST issue
# itself, for ANY issue type on the other end (a Story, a Change Request,
# even another Test) -- confirmed against a live example (TRAN-2182 is a
# Test, blocked by TF-18118, via a plain Silver.jira.issue_links row,
# exactly like Bridge_Issue_Link already reads -- nothing Test-Set-shaped
# about it). Test_Issue_Key here is a single, unambiguous relationship to
# Dim_Issue -- select ANY issue and see whether it IS a test or is LINKED
# to one, no USERELATIONSHIP gymnastics, no second fact to keep in sync.
#
# Test Set / Test Plan / Test Execution membership is NOT modelled here at
# all, not even as a grouping-label column -- an earlier version of this
# file carried Test_Set_Code (MIN_BY, one "primary" set per test) for
# exactly that, but Bridge_Issue_Link now carries Test Set/Test Plan/Test
# Execution containment as real rows (both directions), which is STRICTLY
# BETTER: it shows every set/plan/execution a test belongs to, not one
# arbitrarily-picked "primary" one. Keeping Test_Set_Code here too would
# have been the exact same fact stored twice. Blocking relationships
# (Blocked/Blocks) were never modelled here either, same reasoning --
# Bridge_Issue_Link, filtered to Link Type Name = 'Blocks'/'Test Set'/
# 'Test Plan'/'Test Execution', answers all of it for ANY issue through
# the shared Dim_Issue relationship. This fact only carries what's
# genuinely intrinsic to a RUN: status, timing, who executed it.
#
# "Latest status" is deliberately NOT a stored column -- with one row per
# RUN, "latest per test" is a MAX(Started_On) per Test_Issue_Key, which
# the semantic model does with one measure that stays correct as new runs
# land, rather than a value baked in at Gold-build time.
#
# Is_Passed/Is_Failed kept as stored 0/1 flags (not pushed to Dim_Test_
# Status the way the old Fact_Test's booleans were) -- at RUN grain,
# summing across potentially thousands of rows, a plain SUM() on the fact
# is simpler than a relationship hop for the same thing. Test_Status_Name
# is NOT stored, though -- Dim_Test_Status already gives the correctly
# formatted (sentence case) display text via Test_Status_Key, and storing
# a second, raw-uppercase copy here would reintroduce the exact
# ALL-CAPS-in-the-report problem already fixed.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_run",
    table_type="fact",
    key_column="Test_Run_Fact_Key",
    columns={
        "Run_Id": {"type": "string", "merge_field": True},

        # Measures
        "Is_Passed":  {"type": "int", "default": 0},
        "Is_Failed":  {"type": "int", "default": 0},
        "Is_Final":   {"type": "int", "default": 0},
        "Run_Count":  {"type": "int", "default": 1},

        "Test_Type":      {"type": "string", "default": "Unknown"},
        "Started_On":     {"type": "timestamp"},
        "Finished_On":    {"type": "timestamp"},
        "Executed_Date":  {"type": "date"},

        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
        "Execution_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
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
        "Executed_By_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_resource",
                "natural_key_column": "Resource_Id",
                "key_column": "Resource_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    SELECT
        r.run_id AS Run_Id,

        CASE WHEN r.status_name = 'PASSED' THEN 1 ELSE 0 END AS Is_Passed,
        CASE WHEN r.status_name = 'FAILED' THEN 1 ELSE 0 END AS Is_Failed,
        CASE WHEN r.status_name IN ('PASSED', 'FAILED') THEN 1 ELSE 0 END AS Is_Final,
        1 AS Run_Count,

        r.test_type AS Test_Type,
        r.started_on AS Started_On,
        r.finished_on AS Finished_On,
        CAST(r.finished_on AS date) AS Executed_Date,

        test_issue.Issue_Key AS Test_Issue_Key,
        exec_issue.Issue_Key AS Execution_Issue_Key,
        status.Test_Status_Key,
        resource.Resource_Key AS Executed_By_Key

    FROM Silver.xray.test_runs r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue test_issue ON test_issue.Issue_Id = r.test_issue_id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue exec_issue  ON exec_issue.Issue_Id = r.execution_issue_id
    -- LOWER() on both sides: Dim_Test_Status.Test_Status_Name is sentence
    -- case ("Passed"), r.status_name is Xray's raw uppercase ("PASSED") --
    -- same case-insensitive join as Gold - Fact Test.py, for the same
    -- reason (a future casing change on either side can't silently break it).
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status status ON LOWER(status.Test_Status_Name) = LOWER(r.status_name)
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource  ON resource.Resource_Id = r.executed_by_id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Run built successfully")
