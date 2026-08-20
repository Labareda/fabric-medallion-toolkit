# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Test_Status
# Xray run statuses plus one synthetic row: "Not Run".
# "Not Run" is not in Xray -- it is the absence of a run. Adding it here
# means Fact_Test can carry it as a proper status and the matrix shows
# "Not Run" as a real value rather than a blank, which is the most
# important number in a pre-go-live report.
#
# SIMPLIFIED -- Colour removed (no report used it; add back if a matrix
# needs conditional formatting driven from data rather than the report's
# own theme). Coverage_Status is a simplified rollup for coverage-style
# reporting: Passed/Failed/Blocked get their own value, Executing/To Do
# collapse to "Not run" (coverage cares whether a result exists yet, not
# which not-yet-finished state it's in). The Is_Pass/Is_Fail/Is_Blocked/
# Is_Not_Run flags are kept here (not on Fact_Test) -- Fact_Test relates to
# this dimension via Test_Status_Key, so a report sums these through the
# relationship instead of the fact recomputing the same booleans a second
# time.
#
# Test_Status_Name / Coverage_Status are sentence case ("Passed", not
# "PASSED") -- Xray's raw status names are all-caps. This used to be
# reformatted in Power Query instead of here, specifically because
# Test_Status_Name is a MERGE key other queries join on by exact string --
# reformatting it here means Gold - Fact Test.py's join has to match on
# LOWER() now instead of an exact string, so a future casing change here
# can't silently break it again.
#
# Is_Final is HAND-MAINTAINED, not read from Xray -- S2B - Xray.py's
# getStatuses query only ever requests name/description/color; Xray's
# GraphQL API does not expose "Final" at all (confirmed against Xray's own
# docs -- it's a Settings-screen-only concept). The previous version read
# s.final anyway, a column that was never actually extracted.

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
        "Is_Final":             {"type": "boolean", "default": False},
        "Coverage_Status":      {"type": "string", "default": "Not run"},
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
        CONCAT(UPPER(SUBSTRING(s.name, 1, 1)), LOWER(SUBSTRING(s.name, 2))) AS Test_Status_Name,
        COALESCE(s.description, '')                   AS Description,
        s.name IN ('PASSED', 'FAILED')                AS Is_Final,
        CASE s.name WHEN 'PASSED'  THEN 'Passed'
                    WHEN 'FAILED'  THEN 'Failed'
                    WHEN 'BLOCKED' THEN 'Blocked'
                    ELSE 'Not run' END                AS Coverage_Status,
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
    Test_Status_Name="Not run",
    Description="Test has not been executed yet",
    Is_Final=False,
    Coverage_Status="Not run",
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
