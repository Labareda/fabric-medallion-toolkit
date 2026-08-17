# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Test_Status
# Xray run statuses plus one synthetic row: "Not Run".
# "Not Run" is not in Xray -- it is the absence of a run. Adding it here
# means Fact_Test can carry it as a proper status and the matrix shows
# "Not Run" as a real value rather than a blank, which is the most
# important number in a pre-go-live report.

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
        "Test_Status_Name":     {"type": "string", "merge_field": True, "missing": "Unknown"},
        "Description":          {"type": "string", "default": ""},
        "Colour":               {"type": "string", "default": "#CCCCCC"},
        "Is_Final":             {"type": "boolean", "default": False},
        "Coverage_Status":      {"type": "string", "default": "NOTRUN"},
        "Is_Pass":              {"type": "boolean", "default": False},
        "Is_Fail":              {"type": "boolean", "default": False},
        "Is_Blocked":           {"type": "boolean", "default": False},
        "Is_Not_Run":           {"type": "boolean", "default": False},
        "Test_Status_Sort_Order": {"type": "int", "default": 9},
    },
)

# CELL ********************
from_xray = spark.sql("""
    SELECT
        s.name                                        AS Test_Status_Name,
        COALESCE(s.description, '')                   AS Description,
        COALESCE(s.color, '#CCCCCC')                  AS Colour,
        COALESCE(CAST(s.final AS boolean), false)     AS Is_Final,
        CASE s.name WHEN 'PASSED' THEN 'OK'
                    WHEN 'FAILED' THEN 'NOK'
                    ELSE 'NOTRUN' END                 AS Coverage_Status,
        s.name = 'PASSED'                             AS Is_Pass,
        s.name = 'FAILED'                             AS Is_Fail,
        s.name = 'BLOCKED'                            AS Is_Blocked,
        false                                         AS Is_Not_Run,
        CASE s.name
            WHEN 'PASSED'    THEN 1
            WHEN 'FAILED'    THEN 2
            WHEN 'BLOCKED'   THEN 3
            WHEN 'EXECUTING' THEN 4
            WHEN 'TO DO'     THEN 5
            ELSE 9 END                               AS Test_Status_Sort_Order
    FROM Silver.xray.statuses s
""")

# The synthetic "Not Run" row -- not in Xray, derived from absence of a run
not_run = spark.createDataFrame([Row(
    Test_Status_Name="NOT RUN",
    Description="Test has not been executed yet",
    Colour="#AAAAAA",
    Is_Final=False,
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
