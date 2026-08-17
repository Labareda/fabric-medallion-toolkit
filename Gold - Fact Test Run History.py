# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test_Run_History -- every run, for trend analysis
# Grain: ONE ROW PER XRAY RUN.
#
# Separate from Fact_Test deliberately. Fact_Test answers "what is the
# current state" -- it has one row per set-test pair with the latest status.
# This table answers "how has it moved" -- every run ever, for trend charts,
# first-time pass rate, and cycle time.
#
# The two tables share Dim_Test_Set and Dim_Test_Status, and both connect
# to Dim_Date. Keep them separate and the report pages stay clean:
# the summary matrix uses Fact_Test, the trend chart uses this.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_run_history",
    table_type="fact",
    key_column="Run_History_Key",
    columns={
        "Run_Id":           {"type": "string", "merge_field": True},
        "Test_Id":          {"type": "string", "merge_field": True},
        "Test_Code":        {"type": "string", "default": "Unknown"},
        "Execution_Code":   {"type": "string", "default": "Unknown"},
        "Execution_Name":   {"type": "string", "default": ""},
        "Test_Type":        {"type": "string", "default": "Manual"},
        "Status_Name":      {"type": "string", "default": "Unknown"},
        "Started_Date":     {"type": "date"},
        "Finished_Date":    {"type": "date"},
        "Duration_Mins":    {"type": "double"},
        "Run_Number":       {"type": "int", "default": 1},
        "Is_Latest_Run":    {"type": "boolean", "default": False},
        "Is_First_Run":     {"type": "boolean", "default": False},
        "Is_Pass":          {"type": "boolean", "default": False},
        "Is_Fail":          {"type": "boolean", "default": False},
        "Is_Blocked":       {"type": "boolean", "default": False},
        "Passed_First_Time":{"type": "boolean", "default": False},
        "Run_Count":        {"type": "int", "default": 1},
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
    },
)

# CELL ********************
# Window functions must run before the dim joins -- compute them in a CTE
# so row numbers are based on the raw run data, not affected by join fanout.
df = spark.sql(f"""
    WITH ranked AS (
        SELECT
            r.run_id,
            r.test_issue_id,
            r.test_issue_key,
            r.execution_issue_key,
            r.execution_summary,
            r.test_type,
            r.status_name,
            CAST(r.started_on  AS date)                             AS Started_Date,
            CAST(r.finished_on AS date)                             AS Finished_Date,
            (BIGINT(CAST(r.finished_on AS timestamp))
             - BIGINT(CAST(r.started_on AS timestamp))) / 60.0     AS Duration_Mins,
            ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_id
                ORDER BY CAST(r.started_on AS timestamp))           AS Run_Number,
            ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_id
                ORDER BY CAST(r.started_on AS timestamp) DESC) = 1  AS Is_Latest_Run,
            ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_id
                ORDER BY CAST(r.started_on AS timestamp)) = 1       AS Is_First_Run,
            r.status_name = 'PASSED'                                AS Is_Pass,
            r.status_name = 'FAILED'                                AS Is_Fail,
            r.status_name = 'BLOCKED'                               AS Is_Blocked,
            (ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_id
                ORDER BY CAST(r.started_on AS timestamp)) = 1
             AND r.status_name = 'PASSED')                          AS Passed_First_Time,
            1                                                       AS Run_Count,
            r.executed_by_id
        FROM Silver.xray.test_runs r
    )
    SELECT
        rk.run_id                       AS Run_Id,
        rk.test_issue_id                AS Test_Id,
        rk.test_issue_key               AS Test_Code,
        rk.execution_issue_key          AS Execution_Code,
        rk.execution_summary            AS Execution_Name,
        rk.test_type                    AS Test_Type,
        rk.status_name                  AS Status_Name,
        rk.Started_Date,
        rk.Finished_Date,
        rk.Duration_Mins,
        rk.Run_Number,
        rk.Is_Latest_Run,
        rk.Is_First_Run,
        rk.Is_Pass,
        rk.Is_Fail,
        rk.Is_Blocked,
        rk.Passed_First_Time,
        rk.Run_Count,
        -- Natural keys for lookup_missing_from (safety net for any join misses)
        rk.status_name                  AS Test_Status_Name,
        rk.executed_by_id               AS Resource_Account_Id,
        -- Surrogate keys resolved by explicit joins to the dim tables
        ts.Test_Status_Key,
        res.Resource_Key                AS Executor_Key
    FROM ranked rk
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status ts
           ON ts.Test_Status_Name = rk.status_name
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res
           ON res.Resource_Account_Id = rk.executed_by_id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Run_History built successfully")
