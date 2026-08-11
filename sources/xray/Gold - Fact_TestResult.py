# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# Grain: ONE ROW PER TEST RUN (Option B -- full execution history, not just the
# latest result per test). A test run is a single execution of one test inside
# one test execution, with one status.
#
# This grain answers BOTH questions from one fact:
#   - "which tests pass/fail right now" -> a DAX measure taking the latest FINAL
#     run per test (see the measures note at the bottom)
#   - "what's our pass-rate trend / how flaky is this test" -> the full history
#     is right here, no re-extraction needed
# Option A (latest-only) would have answered only the first and thrown away the
# data behind the second.
#
# CONFORMED TO THE EXISTING MODEL, no new issue dimension. Tests and executions
# are Jira issues, so both keys resolve to the Dim_Issue already built:
#   Test_Issue_Key      -> the test being run (the primary Dim_Issue relationship)
#   Execution_Issue_Key -> the execution it ran under (a second, INACTIVE role)
# Two paths from Dim_Issue to this fact, so ONE must be inactive -- Test is the
# active one (that's what "these tests, pass/fail" hangs off); Execution is
# reached via USERELATIONSHIP when a report needs it.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_result",
    table_type="fact",
    key_column="Test_Result_Key",
    columns={
        "Run_Id":              {"type": "string", "merge_field": True},

        # Measures
        "Is_Passed":  {"type": "int", "default": 0},
        "Is_Failed":  {"type": "int", "default": 0},
        "Is_Final":   {"type": "int", "default": 0},
        "Run_Count":  {"type": "int", "default": 1},

        # Degenerate / context
        "Test_Type":       {"type": "string", "default": "Unknown"},
        "Started_On":      {"type": "timestamp"},
        "Finished_On":     {"type": "timestamp"},
        "Executed_Date":   {"type": "date"},

        # Test being run -- the ACTIVE Dim_Issue relationship.
        "Test_Issue_Code": {"type": "string", "default": "Unknown"},
        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        # Execution it ran under -- resolved to the SAME Dim_Issue, related
        # INACTIVE in Power BI (second path). Kept so drill-through to the
        # execution works via USERELATIONSHIP.
        "Execution_Issue_Code": {"type": "string", "default": "Unknown"},
        "Execution_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        # Status -> Dim_TestStatus, matched on NAME (that dim's merge field).
        # NORMALIZED match (UPPER + TRIM on both sides) -- the five status
        # names seeded in Dim_TestStatus were read off the Xray Settings
        # screen (a UI label), not off Silver.xray.test_runs' actual
        # status_name values, and those two aren't guaranteed to agree on
        # casing or whitespace ("TO DO" vs "To Do" vs " To Do "). An exact
        # string match would silently collapse any mismatched row onto the
        # Unknown status row with no error -- normalizing both sides removes
        # that failure mode. See the diagnostic below the build query, which
        # confirms whether this was ever actually needed.
        "Test_Status_Name": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_test_status",
                                     "natural_key_column": "Test_Status_Name", "key_column": "Test_Status_Key",
                                     "unknown_value": "Unknown"},
        },
        "Test_Status_Key": {"type": "string", "default": "Unknown"},

        "Executed_By_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# MARKDOWN ********************

# ## Build the fact from Silver

# CELL ********************
# Is_Passed / Is_Failed / Is_Final are derived HERE, from the status name, so
# every tool (Power BI, Tableau, a SQL query) gets the same answer without
# re-implementing the mapping. They match Dim_TestStatus exactly.
#
# Executed_Date = the DATE of finished_on (the run's outcome date). A run with no
# finished_on (TO DO / EXECUTING) has a null Executed_Date -- correct, it hasn't
# produced a result yet -- so it won't relate to Dim_Date, which is what we want.
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

        r.test_issue_key AS Test_Issue_Code,
        test_issue.Issue_Key AS Test_Issue_Key,

        r.execution_issue_key AS Execution_Issue_Code,
        exec_issue.Issue_Key AS Execution_Issue_Key,

        r.status_name AS Test_Status_Name,
        status.Test_Status_Key AS Test_Status_Key,

        resource.Resource_Key AS Executed_By_Key
    FROM Silver.xray.test_runs r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue test_issue
        ON r.test_issue_id = test_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue exec_issue
        ON r.execution_issue_id = exec_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status status
        ON UPPER(TRIM(r.status_name)) = status.Test_Status_Name
    LEFT JOIN {GOLD_SCHEMA}.dim_resource resource
        ON r.executed_by_id = resource.Resource_Account_Id
""")

# MARKDOWN ********************

# ## Report any status name that didn't resolve to Dim_TestStatus

# CELL ********************
# The join above is normalized (UPPER+TRIM), so this only fires for a status
# name that's GENUINELY not one of the five seeded rows -- not a casing
# difference, which normalization already absorbs. If this prints anything,
# Dim_TestStatus needs a new row for it (and its Coverage_Status set to match
# whatever Xray's own Settings screen says for it).
unresolved_statuses = (
    df.filter("Test_Status_Key = 'Unknown' OR Test_Status_Key IS NULL")
      .select("Test_Status_Name").distinct().collect()
)
if unresolved_statuses:
    names = [r["Test_Status_Name"] for r in unresolved_statuses]
    print(f"NOTE: these status_name value(s) did not match any row in Dim_TestStatus, even after "
          f"normalizing case/whitespace: {names} -- add them to Dim_TestStatus's seed.")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Fact_TestResult built successfully")

# MARKDOWN ********************

# ## Power BI measures (reference -- build these in the semantic model)

# CELL ********************
# LATEST RESULT PER TEST -- the client's "which pass/fail now" view. Uses the
# latest FINAL run (Is_Final=1), so an in-flight re-run never hides a settled
# result:
#
#   Latest Result =
#   VAR LastFinal =
#       CALCULATE(
#           MAX(Fact_Test_Result[Finished_On]),
#           Fact_Test_Result[Is_Final] = 1,
#           ALLEXCEPT(Fact_Test_Result, Dim_Issue[Issue_Key]))
#   RETURN
#       CALCULATE(
#           SELECTEDVALUE(Dim_Test_Status[Category]),
#           Fact_Test_Result[Finished_On] = LastFinal)
#
# PASS RATE (over whatever's in filter context -- project, sprint, date range):
#   Pass Rate = DIVIDE(SUM(Fact_Test_Result[Is_Passed]),
#                      SUM(Fact_Test_Result[Is_Final]))
#
# EXECUTION drill-through (activate the inactive role only where needed):
#   Runs by Execution =
#     CALCULATE(SUM(Fact_Test_Result[Run_Count]),
#       USERELATIONSHIP(Fact_Test_Result[Execution_Issue_Key], Dim_Issue[Issue_Key]))
