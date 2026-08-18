# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Test_Run -- every run, for trend analysis
# Grain: ONE ROW PER XRAY RUN.
#
# Separate from Fact_Test deliberately. Fact_Test answers "what is the
# current state" -- it has one row per set-test pair with the latest status.
# This table answers "how has it moved" -- every run ever, for trend charts,
# first-time pass rate, and cycle time.
#
# CONSOLIDATED -- this replaces the two previously-overlapping run tables
# (fact_test_run and fact_test_run_history, same grain, different columns).
# This keeps fact_test_run_history's richer trend columns (Run_Number,
# Is_First_Run, Passed_First_Time) and takes the table/notebook NAME
# "Fact_Test_Run" from the simpler one, so downstream references and the
# orchestration step name don't change. Test_Issue_Key (FK to Dim_Issue) is
# added so the run can be related straight to its test issue without going
# through Test_Code.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_test_run",
    table_type="fact",
    key_column="Test_Run_Key",
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
        "Passed_First_Time":{"type": "boolean", "default": False},
        "Run_Count":        {"type": "int", "default": 1},
        "Test_Issue_Key": {
            "type": "string",
            "lookup_missing_from": {"table": f"{GOLD_SCHEMA}.dim_issue",
                                     "natural_key_column": "Issue_Id", "key_column": "Issue_Key",
                                     "unknown_value": "Unknown"},
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
df = spark.sql(f"""
    SELECT
        r.run_id                                                AS Run_Id,
        r.test_issue_id                                         AS Test_Id,
        r.test_issue_key                                        AS Test_Code,
        r.execution_issue_key                                   AS Execution_Code,
        r.execution_summary                                     AS Execution_Name,
        r.test_type                                             AS Test_Type,
        r.status_name                                           AS Status_Name,
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
        -- True when this is the first run AND it passed
        (ROW_NUMBER() OVER (
            PARTITION BY r.test_issue_id
            ORDER BY CAST(r.started_on AS timestamp)) = 1
         AND r.status_name = 'PASSED')                          AS Passed_First_Time,
        1                                                       AS Run_Count,
        r.test_issue_id                                         AS Issue_Id,
        r.status_name                                           AS Test_Status_Name,
        r.executed_by_id                                        AS Resource_Account_Id
    FROM Silver.xray.test_runs r
""")

# CELL ********************
df = (
    df
    .join(spark.sql(f"SELECT Issue_Id, Issue_Key AS Test_Issue_Key FROM {GOLD_SCHEMA}.dim_issue"),
          on="Issue_Id", how="left")
    .join(spark.sql(f"SELECT Test_Status_Name, Test_Status_Key FROM {GOLD_SCHEMA}.dim_test_status"),
          on="Test_Status_Name", how="left")
    .join(spark.sql(f"SELECT Resource_Account_Id, Resource_Key AS Executor_Key FROM {GOLD_SCHEMA}.dim_resource"),
          on="Resource_Account_Id", how="left")
    .drop("Issue_Id", "Resource_Account_Id")
)

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Test_Run built successfully")
