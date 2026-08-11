# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt
from pyspark.sql import Row

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# A small conformed dimension for Xray test-run statuses. Kept as its own
# dimension rather than folded onto the fact so the report can slice AND so
# the Is_Final / Category / Coverage_Status groupings live in one place.
#
# SOURCED FROM Silver.xray.test_runs, not hand-typed -- Test_Status_Name is
# built from the DISTINCT status_name values actually present in the data,
# so this table can never drift from reality and a brand-new status shows up
# as its own row automatically rather than silently defaulting to Unknown
# until someone notices.
#
# Is_Final / Coverage_Status CANNOT come from the data, though, no matter
# how this table is sourced -- Xray's Test Runs never carry "is this status
# final" or "what coverage bucket does it count toward", those only exist on
# the Xray Settings -> Test Statuses screen (the Final checkbox, the
# Coverage Status dropdown), which nothing in this pipeline extracts. So
# they're still a small hand-maintained CONFIG_STATUS_META lookup below,
# copied from that screen -- confirmed against this client's actual config:
#   PASSED  -- Final, Coverage OK
#   FAILED  -- Final, Coverage NOK
#   TO DO / EXECUTING / BLOCKED -- not final, Coverage NOTRUN
# Any status_name found in the data but NOT in CONFIG_STATUS_META defaults to
# Is_Final=False / Coverage_Status='NOTRUN' -- safe (never wrongly counts an
# unrecognized status as a settled Pass) but gets flagged by the diagnostic
# below so it doesn't go unnoticed.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_test_status",
    table_type="dim",
    key_column="Test_Status_Key",
    columns={
        "Test_Status_Name": {"type": "string",  "merge_field": True},
        "Category":         {"type": "string",  "default": "Unknown"},
        "Coverage_Status":  {"type": "string",  "default": "NOTRUN"},
        "Is_Final":         {"type": "boolean", "default": False},
        "Is_Passed":        {"type": "boolean", "default": False},
        "Sort_Order":       {"type": "int",     "default": 99},
    },
)

# MARKDOWN ********************

# ## Build: distinct status names from real data, enriched with config that can't be derived

# CELL ********************
# The only hand-maintained part of this notebook. Update this dict if the
# client adds a custom status in Xray or changes Final/Coverage settings --
# match keys are UPPER(TRIM(...))'d against the data below, so exact casing
# here doesn't matter.
CONFIG_STATUS_META = {
    "PASSED":    {"Category": "Passed",      "Coverage_Status": "OK",     "Is_Final": True,  "Is_Passed": True,  "Sort_Order": 1},
    "FAILED":    {"Category": "Failed",      "Coverage_Status": "NOK",    "Is_Final": True,  "Is_Passed": False, "Sort_Order": 2},
    "EXECUTING": {"Category": "In Progress", "Coverage_Status": "NOTRUN", "Is_Final": False, "Is_Passed": False, "Sort_Order": 3},
    "TO DO":     {"Category": "Not Run",     "Coverage_Status": "NOTRUN", "Is_Final": False, "Is_Passed": False, "Sort_Order": 4},
    "BLOCKED":   {"Category": "Blocked",     "Coverage_Status": "NOTRUN", "Is_Final": False, "Is_Passed": False, "Sort_Order": 5},
}

distinct_statuses = spark.sql("""
    SELECT DISTINCT UPPER(TRIM(status_name)) AS Test_Status_Name
    FROM Silver.xray.test_runs
    WHERE status_name IS NOT NULL
""").collect()

rows = []
unmapped = []
for r in distinct_statuses:
    name = r["Test_Status_Name"]
    meta = CONFIG_STATUS_META.get(name)
    if meta is None:
        unmapped.append(name)
        meta = {"Category": "Unknown", "Coverage_Status": "NOTRUN", "Is_Final": False, "Is_Passed": False, "Sort_Order": 99}
    rows.append(Row(Test_Status_Name=name, **meta))

df = spark.createDataFrame(rows)

# MARKDOWN ********************

# ## Flag any status present in the data but missing from CONFIG_STATUS_META

# CELL ********************
# Safe default either way (never wrongly counts an unmapped status as a
# settled Pass), but silent is worse than a printed note -- this is the one
# thing in this notebook that needs a human to go check Xray's Settings
# screen and add a line to CONFIG_STATUS_META above.
if unmapped:
    print(f"NOTE: status_name value(s) found in Silver.xray.test_runs with no entry in "
          f"CONFIG_STATUS_META: {unmapped} -- defaulted to not-final/NOTRUN. Check Xray Settings "
          f"-> Test Statuses and add them above.")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Dim_TestStatus built successfully")
