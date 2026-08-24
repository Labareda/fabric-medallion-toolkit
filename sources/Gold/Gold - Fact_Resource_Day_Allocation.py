# Fabric notebook source

# MARKDOWN ********************

# ## Fact_Resource_Day_Allocation -- who is working on what, day by day
# Grain: ONE ROW PER RESOURCE PER ISSUE PER WORKING DAY the issue runs through.
#
# WHY THIS EXISTS: it time-phases the resource<->issue links so the resource
# report can, with a PLAIN COUNT (no cross-fact DAX), answer "how many tasks
# is each person on, week by week" and "which tasks" -- and, via the spread
# hours, "is this person over capacity on a given day".
#
# WHAT'S INCLUDED (assumptions, all explicit):
#   1. BOTH Lead AND Involved links (from Fact_Resource_Allocation) -- the
#      client wants to see everyone's load, assigned or involved. Role_Name
#      is carried so a report can split or filter by it.
#   2. Any issue with a real planned range (Planned_Start..Planned_End) is
#      included, ESTIMATE OR NOT -- a task still counts towards someone's
#      week even if nobody has estimated it. Issues with NO planned dates are
#      excluded here and surfaced instead on the "Missing Dates" report (via
#      Dim_Issue.Missing_Planned_Dates) so the client can add them in Jira.
#   3. Allocated_Hours: the issue's Original_Estimate_Hours spread evenly
#      across its working days, but ONLY for the Lead (Allocation_Weight = 1)
#      and only when an estimate exists -- Involved links and un-estimated
#      issues contribute 0 hours (they still contribute a row, so they COUNT
#      as tasks, but they don't inflate capacity/conflict numbers).
#   4. Weekends excluded (Dim_Date.is_weekend); no public-holiday calendar.
#
# READS TWO OTHER FACTS (Fact_Resource_Allocation, Fact_Issue) -- one of the
# model's few fact-reads-fact exceptions. Must run after both.

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
        "Role_Name":           {"type": "string", "default": "Unknown"},
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
    -- Every resource<->issue link (Lead AND Involved) on issues with a real
    -- planned range. Estimate NOT required (assumption 2). Weight = 1 for
    -- Lead, 0 for Involved (drives hours; 0 = counts but no capacity load).
    allocations AS (
        SELECT
            fra.Resource_Id,
            fra.Issue_Id,
            role.Role_Name,
            fra.Allocation_Weight,
            fi.Planned_Start_Date,
            fi.Planned_End_Date,
            COALESCE(fi.Original_Estimate_Hours, 0) AS Original_Estimate_Hours
        FROM {GOLD_SCHEMA}.fact_resource_allocation fra
        JOIN {GOLD_SCHEMA}.dim_resourcerole role
          ON role.Resource_Role_Key = fra.Resource_Role_Key
        JOIN {GOLD_SCHEMA}.fact_issue fi
          ON fi.Issue_Id = fra.Issue_Id
        WHERE fi.Planned_Start_Date IS NOT NULL
          AND fi.Planned_End_Date IS NOT NULL
          AND fi.Planned_End_Date >= fi.Planned_Start_Date
    ),
    -- One row per working day in range -- assumption 4.
    working_days AS (
        SELECT
            a.Resource_Id, a.Issue_Id, a.Role_Name, a.Allocation_Weight,
            a.Original_Estimate_Hours,
            d.date AS Alloc_Date
        FROM allocations a
        JOIN {GOLD_SCHEMA}.dim_date d
          ON d.date BETWEEN a.Planned_Start_Date AND a.Planned_End_Date
         AND d.is_weekend = FALSE
    ),
    day_counts AS (
        SELECT Resource_Id, Issue_Id, COUNT(*) AS Working_Day_Count
        FROM working_days
        GROUP BY Resource_Id, Issue_Id
    )
    -- Hours: estimate spread across working days, but only for the Lead
    -- (weight 1) -- Involved and un-estimated => 0 hours (assumption 3).
    SELECT
        wd.Resource_Id,
        wd.Issue_Id,
        wd.Alloc_Date AS Date,
        wd.Role_Name,
        (wd.Original_Estimate_Hours / dc.Working_Day_Count) * wd.Allocation_Weight AS Allocated_Hours,
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
