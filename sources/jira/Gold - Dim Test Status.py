# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Test_Status
# Xray run statuses plus one synthetic row: "NOT RUN".
# Silver.xray.statuses has three columns only: name, description, color.
# All other attributes (Is_Pass, Is_Fail, Coverage_Status, Sort_Order)
# are derived here from the name, which Xray keeps stable.
#
# "NOT RUN" is synthetic -- it is the absence of a run, not an Xray status.
# Adding it as a real dimension member means the test matrix shows it as a
# counted value rather than a blank, which matters before go-live.

# CELL ********************
import fabric_medallion_toolkit as fmt
from pyspark.sql import Row

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_test_status",
    table_type="dim",
    key_column="Test_Status_Key",
    columns={
        "Test_Status_Name":       {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Description":            {"type": "string", "default": ""},
        "Colour":                 {"type": "string", "default": "#CCCCCC"},
        # Derived from name -- Xray status names are stable
        "Coverage_Status":        {"type": "string", "default": "NOTRUN"},
        "Is_Pass":                {"type": "boolean", "default": False},
        "Is_Fail":                {"type": "boolean", "default": False},
        "Is_Blocked":             {"type": "boolean", "default": False},
        "Is_Not_Run":             {"type": "boolean", "default": False},
        "Test_Status_Sort_Order": {"type": "int", "default": 9},
    },
)

# CELL ********************
from_xray = spark.sql("""
    SELECT
        s.name                                     AS Test_Status_Name,
        COALESCE(s.description, '')                AS Description,
        COALESCE(s.color, '#CCCCCC')               AS Colour,
        CASE s.name
            WHEN 'PASSED'  THEN 'OK'
            WHEN 'FAILED'  THEN 'NOK'
            ELSE 'NOTRUN'
        END                                        AS Coverage_Status,
        s.name = 'PASSED'                          AS Is_Pass,
        s.name = 'FAILED'                          AS Is_Fail,
        s.name = 'BLOCKED'                         AS Is_Blocked,
        false                                      AS Is_Not_Run,
        CASE s.name
            WHEN 'PASSED'    THEN 1
            WHEN 'FAILED'    THEN 2
            WHEN 'BLOCKED'   THEN 3
            WHEN 'EXECUTING' THEN 4
            WHEN 'TO DO'     THEN 5
            ELSE 9
        END                                        AS Test_Status_Sort_Order
    FROM Silver.xray.statuses s
""")

# Synthetic "NOT RUN" row -- not in Xray, represents tests with no run yet
not_run = spark.createDataFrame([Row(
    Test_Status_Name="NOT RUN",
    Description="Test has not been executed yet",
    Colour="#AAAAAA",
    Coverage_Status="NOTRUN",
    Is_Pass=False,
    Is_Fail=False,
    Is_Blocked=False,
    Is_Not_Run=True,
    Test_Status_Sort_Order=6,
)])

df = from_xray.union(not_run)

# CELL ********************
fmt.merge(spark, df, schema)
print("Dim_Test_Status built successfully")
