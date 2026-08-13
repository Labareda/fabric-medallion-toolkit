# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test_Run
# Grain: ONE ROW PER XRAY TEST RUN (a test executed within an execution).
#
# Is_Latest_Run is flagged HERE rather than solved repeatedly in DAX, because
# the two questions need different rows and mixing them is the usual source
# of a pass rate nobody can reconcile:
#   * "what is the current pass rate"  -> latest run per test only
#   * "how has the pass rate moved"    -> every run
#
# Both Test_Issue_Key and Execution_Issue_Key resolve to Dim_Issue, because
# Xray Tests and Test Executions are themselves Jira issues. Execution gets a
# role-playing copy of Dim_Issue in the semantic model; Test is the active path.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_run",
    table_type="fact",
    key_column="Test_Run_Key",
    columns={
        "Run_Id":             {"type": "string", "merge_field": True},
        "Test_Issue_Code":    {"type": "string", "default": "Unknown"},
        "Execution_Issue_Code": {"type": "string", "default": "Unknown"},
        "Execution_Summary":  {"type": "string", "default": ""},
        "Test_Type":          {"type": "string", "default": "Unknown"},
        "Started_At":         {"type": "timestamp"},
        "Started_Date":       {"type": "date"},
        "Finished_At":        {"type": "timestamp"},
        "Finished_Date":      {"type": "date"},
        "Duration_Mins":      {"type": "double"},
        "Run_Count":          {"type": "int", "default": 1},
        "Is_Pass":            {"type": "boolean", "default": False},
        "Is_Fail":            {"type": "boolean", "default": False},
        "Is_Blocked":         {"type": "boolean", "default": False},
        "Is_Latest_Run":      {"type": "boolean", "default": False},
        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Execution_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
        },
        "Test_Status_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_test_status",
                                     "natural_key_column": "Test_Status_Name", "key_column": "Test_Status_Key",
                                     "unknown_value": "Unknown"},
        },
        "Executed_By_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_resource",
                                     "natural_key_column": "Resource_Account_Id", "key_column": "Resource_Key",
                                     "unknown_value": "Unknown"},
        },
    },
)

# CELL ********************
df = spark.sql(f"""
    SELECT
        r.run_id                AS Run_Id,
        r.test_issue_key        AS Test_Issue_Code,
        r.execution_issue_key   AS Execution_Issue_Code,
        r.execution_summary     AS Execution_Summary,
        r.test_type             AS Test_Type,
        CAST(r.started_on AS timestamp)  AS Started_At,
        CAST(r.started_on AS date)       AS Started_Date,
        CAST(r.finished_on AS timestamp) AS Finished_At,
        CAST(r.finished_on AS date)      AS Finished_Date,
        (BIGINT(CAST(r.finished_on AS timestamp)) - BIGINT(CAST(r.started_on AS timestamp))) / 60.0 AS Duration_Mins,
        1 AS Run_Count,
        r.status_name = 'PASSED'  AS Is_Pass,
        r.status_name = 'FAILED'  AS Is_Fail,
        r.status_name = 'BLOCKED' AS Is_Blocked,
        ROW_NUMBER() OVER (PARTITION BY r.test_issue_id
                           ORDER BY CAST(r.started_on AS timestamp) DESC) = 1 AS Is_Latest_Run,
        test_issue.Issue_Key      AS Test_Issue_Key,
        exec_issue.Issue_Key      AS Execution_Issue_Key,
        ts.Test_Status_Key,
        res.Resource_Key          AS Executed_By_Key
    FROM Silver.xray.test_runs r
    LEFT JOIN {GOLD_SCHEMA}.dim_issue test_issue ON r.test_issue_id = test_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue exec_issue ON r.execution_issue_id = exec_issue.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status ts   ON r.status_name = ts.Test_Status_Name
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res     ON r.executed_by_id = res.Resource_Account_Id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Run built successfully")
