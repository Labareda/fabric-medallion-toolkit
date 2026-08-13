# Fabric notebook source

# MARKDOWN ********************

# ## Dim_Date -- one calendar, eight roles
# Built by the wheel, then given the Unknown/sentinel row so fact rows with a
# null date still join. EVERY date relationship in the model points here:
# Created, Planned Start, Planned End, Rollup Start, Rollup End, Actual End,
# Snapshot, Worklog Started. One active, the rest inactive + USERELATIONSHIP.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
# Range starts before the earliest Jira created date and runs well past the
# latest target end -- widen if the programme extends. Fiscal year = calendar
# year here; set fiscal_year_start_month=4 if the client reports on an April FY.
fmt.build_date_dimension(
    spark,
    fmt.DateDimensionConfig(
        table_name=f"{GOLD_SCHEMA}.dim_date",
        start_date="2020-01-01",
        end_date="2032-12-31",
        fiscal_year_start_month=1,
    ),
)

# CELL ********************
# Sentinel row. Fact_Issue deliberately keeps NULL dates (see its notes), so
# this is for the few facts that DO need a guaranteed match rather than a blank.
fmt.add_date_dimension_sentinel(spark, f"{GOLD_SCHEMA}.dim_date", sentinel_date="1900-01-01")

# CELL ********************
print("Dim_Date built successfully")
