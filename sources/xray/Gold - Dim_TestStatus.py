# Fabric notebook source

# MARKDOWN ********************

# ## Import environment and required packages

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# MARKDOWN ********************

# ## Declare the table schema

# CELL ********************
# A small conformed dimension for Xray test-run statuses. Kept as its own
# dimension rather than folded onto the fact so the report can slice AND so the
# Is_Final / Category / Coverage_Status groupings live in one place.
#
# This client uses the five DEFAULT Xray statuses, no custom ones (confirmed
# from Xray Settings -> Test Statuses):
#   PASSED / FAILED  -- Is_Final = true  (a settled result)
#   TO DO / EXECUTING / BLOCKED -- Is_Final = false (not a settled result)
#
# Is_Final is the important flag. "Latest result" reporting must mean latest
# FINAL result -- otherwise a test that passed yesterday and is re-EXECUTING
# today would show as "In Progress" and its pass would vanish. The DAX measures
# in Fact_Test_Result key off this.
#
# Coverage_Status is copied VERBATIM from Xray's own Test Statuses settings
# screen (Jira -> Xray Settings -> Test Statuses -> "Test Coverage Status"
# column) -- OK / NOK / NOTRUN. It is Xray's own definition of what counts as
# "covered" for a requirement, not a mapping invented here. This is what a
# requirement's coverage rollup (see Bridge_TestCoverage) should aggregate
# over, so the client's report agrees with what they'd see in Xray's own
# Test Coverage Report.
#
# Category collapses the five statuses into report-friendly buckets. If the
# client ever adds a custom status in Xray, add a row here -- the fact's
# lookup_missing_from will land unmapped statuses on the Unknown row until then,
# so nothing breaks, it just needs categorizing (and a Coverage_Status set on
# the new row in Xray's own settings, mirrored here).
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

# ## Build the dimension (static seed -- these are configuration, not source data)

# CELL ********************
# Seeded as a literal rather than read from Silver: statuses are Xray CONFIG,
# not transactional data, and seeding them means the dimension is complete even
# before the first test run lands. The names must match status_name in
# Silver.xray.test_runs EXACTLY (case included) for the fact's join to resolve.
# Coverage_Status values are copied from the client's own Xray Test Statuses
# screen, not invented here.
from pyspark.sql import Row

rows = [
    Row(Test_Status_Name="PASSED",    Category="Passed",      Coverage_Status="OK",     Is_Final=True,  Is_Passed=True,  Sort_Order=1),
    Row(Test_Status_Name="FAILED",    Category="Failed",      Coverage_Status="NOK",    Is_Final=True,  Is_Passed=False, Sort_Order=2),
    Row(Test_Status_Name="EXECUTING", Category="In Progress", Coverage_Status="NOTRUN", Is_Final=False, Is_Passed=False, Sort_Order=3),
    Row(Test_Status_Name="TO DO",     Category="Not Run",     Coverage_Status="NOTRUN", Is_Final=False, Is_Passed=False, Sort_Order=4),
    Row(Test_Status_Name="BLOCKED",   Category="Blocked",     Coverage_Status="NOTRUN", Is_Final=False, Is_Passed=False, Sort_Order=5),
]
df = spark.createDataFrame(rows)

# MARKDOWN ********************

# ## Merge into Gold

# CELL ********************
fmt.merge(spark, df, schema)

# CELL ********************
print("Dim_TestStatus built successfully")
