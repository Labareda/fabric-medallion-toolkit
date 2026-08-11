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
# SOURCED FROM Silver.xray.statuses -- Xray's getStatuses GraphQL query,
# real configuration data (name/description/color), not a hand-typed guess
# and not merely "whatever's shown up in test_runs so far." This is the
# COMPLETE list of statuses configured for the instance, including any
# status that's been set up in Xray Settings but never yet used on a real
# test run -- the distinct-from-test-runs approach this replaced couldn't
# see those at all.
#
# Is_Final / Coverage_Status STILL cannot come from the data, no matter the
# source -- checked directly against Xray's own documented schema: neither
# field appears anywhere on getStatuses or on any status{} selection
# elsewhere in their GraphQL API. They only exist on the Xray Settings ->
# Test Statuses screen (the Final checkbox, the Coverage Status dropdown),
# which Xray simply does not expose through this API. So they remain a
# small hand-maintained CONFIG_STATUS_META lookup below, copied from that
# screen -- confirmed against this client's actual config:
#   PASSED  -- Final, Coverage OK
#   FAILED  -- Final, Coverage NOK
#   TO DO / EXECUTING / BLOCKED -- not final, Coverage NOTRUN
# A status present in Silver.xray.statuses but missing from
# CONFIG_STATUS_META defaults to Is_Final=False / Coverage_Status='NOTRUN'
# -- safe (never wrongly counts an unrecognized status as a settled Pass)
# but flagged by the diagnostic below so it doesn't go unnoticed.
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.dim_test_status",
    table_type="dim",
    key_column="Test_Status_Key",
    columns={
        "Test_Status_Name": {"type": "string",  "merge_field": True},
        "Description":      {"type": "string",  "default": ""},
        "Color":            {"type": "string",  "default": ""},
        "Category":         {"type": "string",  "default": "Unknown"},
        "Coverage_Status":  {"type": "string",  "default": "NOTRUN"},
        "Is_Final":         {"type": "boolean", "default": False},
        "Is_Passed":        {"type": "boolean", "default": False},
        "Sort_Order":       {"type": "int",     "default": 99},
    },
)

# MARKDOWN ********************

# ## Build: real status config from Xray, enriched with what can't be extracted

# CELL ********************
# The only hand-maintained part of this notebook, and only because Xray's API
# doesn't expose these two fields at all. Update this dict if the client adds
# a custom status in Xray or changes Final/Coverage settings -- match keys are
# UPPER(TRIM(...))'d against the real data below, so exact casing here
# doesn't matter.
CONFIG_STATUS_META = {
    "PASSED":    {"Category": "Passed",      "Coverage_Status": "OK",     "Is_Final": True,  "Is_Passed": True,  "Sort_Order": 1},
    "FAILED":    {"Category": "Failed",      "Coverage_Status": "NOK",    "Is_Final": True,  "Is_Passed": False, "Sort_Order": 2},
    "EXECUTING": {"Category": "In Progress", "Coverage_Status": "NOTRUN", "Is_Final": False, "Is_Passed": False, "Sort_Order": 3},
    "TO DO":     {"Category": "Not Run",     "Coverage_Status": "NOTRUN", "Is_Final": False, "Is_Passed": False, "Sort_Order": 4},
    "BLOCKED":   {"Category": "Blocked",     "Coverage_Status": "NOTRUN", "Is_Final": False, "Is_Passed": False, "Sort_Order": 5},
}

if not spark.catalog.tableExists("Silver.xray.statuses"):
    raise RuntimeError(
        "Silver.xray.statuses doesn't exist -- run S2B - Xray (its getStatuses step) "
        "and B2S - Xray before this notebook."
    )

real_statuses = spark.sql("""
    SELECT
        UPPER(TRIM(name)) AS Test_Status_Name,
        description        AS Description,
        color              AS Color
    FROM Silver.xray.statuses
    WHERE name IS NOT NULL
""").collect()

rows = []
unmapped = []
for r in real_statuses:
    name = r["Test_Status_Name"]
    meta = CONFIG_STATUS_META.get(name)
    if meta is None:
        unmapped.append(name)
        meta = {"Category": "Unknown", "Coverage_Status": "NOTRUN", "Is_Final": False, "Is_Passed": False, "Sort_Order": 99}
    rows.append(Row(Test_Status_Name=name, Description=r["Description"] or "", Color=r["Color"] or "", **meta))

df = spark.createDataFrame(rows)

# MARKDOWN ********************

# ## Flag any status present in Xray's config but missing from CONFIG_STATUS_META

# CELL ********************
# Safe default either way (never wrongly counts an unmapped status as a
# settled Pass), but silent is worse than a printed note -- this is the one
# thing in this notebook that needs a human to check Xray Settings -> Test
# Statuses and add a line to CONFIG_STATUS_META above.
if unmapped:
    print(f"NOTE: status(es) found in Silver.xray.statuses with no entry in CONFIG_STATUS_META: "
          f"{unmapped} -- defaulted to not-final/NOTRUN. Check Xray Settings -> Test Statuses "
          f"and add them above.")

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Dim_TestStatus built successfully")
