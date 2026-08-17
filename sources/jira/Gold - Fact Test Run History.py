# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test_Run_History -- every Xray run, for trend analysis
# Grain: ONE ROW PER RUN.
#
# STAR SCHEMA: facts carry keys, measures and flags ONLY.
# Descriptive attributes come from the dimensions via relationships:
#   Test summary, test type, acceptance criteria -> Dim_Issue
#   Status name, colour, coverage status        -> Dim_Test_Status
#   Executor name                               -> Dim_Resource
#
# Passed_First_Time is kept as a flag here because it is a computed
# boolean derived from a window function -- it does not belong on any
# dimension and cannot be recreated from a simple relationship join.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_run_history",
    table_type="fact",
    key_column="Run_History_Key",
    columns={
        # Merge keys
        "Run_Id":        {"type": "string", "merge_field": True},
        "Test_Id":       {"type": "string", "merge_field": True},

        # Natural keys kept for display and drill-through
        "Test_Code":     {"type": "string", "default": "Unknown"},
        "Execution_Code":{"type": "string", "default": "Unknown"},

        # Dates
        "Started_Date":  {"type": "date"},
        "Finished_Date": {"type": "date"},

        # Measures
        "Duration_Mins": {"type": "double"},
        "Run_Count":     {"type": "int", "default": 1},
        "Run_Number":    {"type": "int", "default": 1},

        # Flags -- computed here, not available on any dimension
        "Is_Latest_Run":     {"type": "boolean", "default": False},
        "Is_First_Run":      {"type": "boolean", "default": False},
        "Is_Pass":           {"type": "boolean", "default": False},
        "Is_Fail":           {"type": "boolean", "default": False},
        "Is_Blocked":        {"type": "boolean", "default": False},
        "Passed_First_Time": {"type": "boolean", "default": False},

        # Surrogate FKs
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
        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_issue",
                "natural_key_column": "Issue_Id",
                "key_column": "Issue_Key",
                "unknown_value": "Unknown",
            },
        },
    },
)

# CELL ********************
# Window functions run in the CTE on raw data before the dim joins,
# preventing join fanout from affecting row numbers.
df = spark.sql(f"""
    WITH ranked AS (
        SELECT
            r.run_id,
            r.test_issue_id,
            r.test_issue_key,
            r.execution_issue_key,
            r.status_name,
            r.executed_by_id,
            CAST(r.started_on  AS date)                              AS Started_Date,
            CAST(r.finished_on AS date)                              AS Finished_Date,
            (BIGINT(CAST(r.finished_on AS timestamp))
             - BIGINT(CAST(r.started_on AS timestamp))) / 60.0      AS Duration_Mins,
            ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_id
                ORDER BY CAST(r.started_on AS timestamp))            AS Run_Number,
            ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_id
                ORDER BY CAST(r.started_on AS timestamp) DESC) = 1   AS Is_Latest_Run,
            ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_id
                ORDER BY CAST(r.started_on AS timestamp)) = 1        AS Is_First_Run,
            r.status_name = 'PASSED'                                 AS Is_Pass,
            r.status_name = 'FAILED'                                 AS Is_Fail,
            r.status_name = 'BLOCKED'                                AS Is_Blocked,
            (ROW_NUMBER() OVER (
                PARTITION BY r.test_issue_id
                ORDER BY CAST(r.started_on AS timestamp)) = 1
             AND r.status_name = 'PASSED')                           AS Passed_First_Time
        FROM Silver.xray.test_runs r
    )
    SELECT
        rk.run_id                   AS Run_Id,
        rk.test_issue_id            AS Test_Id,
        rk.test_issue_key           AS Test_Code,
        rk.execution_issue_key      AS Execution_Code,
        rk.Started_Date,
        rk.Finished_Date,
        rk.Duration_Mins,
        1                           AS Run_Count,
        rk.Run_Number,
        rk.Is_Latest_Run,
        rk.Is_First_Run,
        rk.Is_Pass,
        rk.Is_Fail,
        rk.Is_Blocked,
        rk.Passed_First_Time,
        -- Natural keys for lookup_missing_from safety net
        rk.status_name              AS Test_Status_Name,
        rk.executed_by_id           AS Resource_Account_Id,
        rk.test_issue_id            AS Issue_Id,
        -- Surrogate keys from explicit dim joins
        ts.Test_Status_Key,
        res.Resource_Key            AS Executor_Key,
        di.Issue_Key                AS Test_Issue_Key
    FROM ranked rk
    LEFT JOIN {GOLD_SCHEMA}.dim_test_status ts
           ON ts.Test_Status_Name = rk.status_name
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res
           ON res.Resource_Account_Id = rk.executed_by_id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di
           ON di.Issue_Id = rk.test_issue_id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Run_History built successfully")
