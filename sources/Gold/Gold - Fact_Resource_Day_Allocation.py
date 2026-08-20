# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Resource_Day_Allocation -- capacity / conflict detection
# Grain: ONE ROW PER RESOURCE PER ISSUE PER WORKING DAY the issue is
# planned to run through.
#
# WHY THIS EXISTS: Fact_Resource_Allocation is issue-level (one row per
# person per issue per role) -- it can answer "who's on this issue" but not
# "is this person double-booked on Tuesday". Detecting a conflict needs an
# actual per-day number to compare against Dim_Resource.Daily_Capacity_Hours,
# and Jira has no such number anywhere -- there is no "hours per day" field
# on an issue. This table MANUFACTURES one, on explicit assumptions:
#
#   1. Only LEAD allocations count towards effort (Dim_Resource_Role.Contributes_To_
#      Effort) -- matches Fact_Resource_Allocation's own Allocation_Weight
#      logic. Someone Involved isn't assumed to be spending hours on it.
#   2. An issue's Original_Estimate_Hours is spread EVENLY across every
#      working day (Mon-Fri) in its OWN Planned_Start_Date..Planned_End_Date
#      range -- not the rolled-up range (a parent/summary row's rollup
#      spans its whole subtree, which would double-count every child's
#      hours again under the parent). Real work is never actually this
#      even, but there's no finer-grained signal to spread it by instead.
#   3. Issues missing a planned date range, or missing/zero
#      Original_Estimate_Hours, are EXCLUDED rather than guessed at -- an
#      issue with no way to place its hours in time contributes nothing
#      here, not a wrong day.
#   4. Weekends are excluded outright (Dim_Date.is_weekend); public
#      holidays are NOT accounted for -- there's no holiday calendar in
#      this model.
#
# Change any of these and this table's numbers move -- they are estimates
# for spotting likely conflicts, not a payroll-grade timesheet.
#
# READS TWO OTHER FACTS (Fact_Resource_Allocation, Fact_Issue) -- the one
# other exception to "facts only depend on other facts here, never
# implicitly" (see Orchestration's own comment; Fact_Test_Coverage is the
# other one). Must run after both.

# CELL ********************
import fabric_medallion_toolkit as fmt

GOLD_SCHEMA = "Gold.gold"

# CELL ********************
schema = fmt.TableSchema(
    table_name=f"{GOLD_SCHEMA}.fact_resource_day_allocation",
    table_type="fact",
    key_column="Resource_Day_Allocation_Key",
    columns={
        "Resource_Id": {"type": "string", "merge_field": True},
        "Issue_Id":            {"type": "string", "merge_field": True},
        "Date":                {"type": "date", "merge_field": True},
        "Allocated_Hours":     {"type": "double", "default": 0.0},
        "Resource_Key": {
            "type": "string",
            "lookup_missing_from": {
                "table": f"{GOLD_SCHEMA}.dim_resource",
                "natural_key_column": "Resource_Id",
                "key_column": "Resource_Key",
                "unknown_value": "Unknown",
            },
        },
        "Issue_Key": {
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
df = spark.sql(f"""
    WITH
    -- Lead allocations on issues with a real planned range and a real
    -- estimate -- see assumptions 1 and 3 above.
    lead_effort AS (
        SELECT
            fra.Resource_Id,
            fra.Issue_Id,
            fi.Planned_Start_Date,
            fi.Planned_End_Date,
            fi.Original_Estimate_Hours
        FROM {GOLD_SCHEMA}.fact_resource_allocation fra
        JOIN {GOLD_SCHEMA}.dim_resourcerole role
          ON role.Role_Name = fra.Role_Name AND role.Contributes_To_Effort = TRUE
        JOIN {GOLD_SCHEMA}.fact_issue fi
          ON fi.Issue_Id = fra.Issue_Id
        WHERE fi.Planned_Start_Date IS NOT NULL
          AND fi.Planned_End_Date IS NOT NULL
          AND fi.Planned_End_Date >= fi.Planned_Start_Date
          AND fi.Original_Estimate_Hours IS NOT NULL
          AND fi.Original_Estimate_Hours > 0
    ),
    -- One row per working day in range -- assumption 4.
    working_days AS (
        SELECT
            le.Resource_Id, le.Issue_Id, le.Original_Estimate_Hours,
            d.date AS Alloc_Date
        FROM lead_effort le
        JOIN {GOLD_SCHEMA}.dim_date d
          ON d.date BETWEEN le.Planned_Start_Date AND le.Planned_End_Date
         AND d.is_weekend = FALSE
    ),
    day_counts AS (
        SELECT Resource_Id, Issue_Id, COUNT(*) AS Working_Day_Count
        FROM working_days
        GROUP BY Resource_Id, Issue_Id
    )
    -- Even spread across the working days -- assumption 2.
    SELECT
        wd.Resource_Id,
        wd.Issue_Id,
        wd.Alloc_Date AS Date,
        wd.Original_Estimate_Hours / dc.Working_Day_Count AS Allocated_Hours,
        res.Resource_Key,
        di.Issue_Key
    FROM working_days wd
    JOIN day_counts dc
      ON dc.Resource_Id = wd.Resource_Id AND dc.Issue_Id = wd.Issue_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_resource res ON res.Resource_Id = wd.Resource_Id
    LEFT JOIN {GOLD_SCHEMA}.dim_issue di     ON di.Issue_Id = wd.Issue_Id
""")

# CELL ********************
fmt.merge(spark, df, schema)
print("Fact_Resource_Day_Allocation built successfully")
